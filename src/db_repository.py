"""Capa de persistencia en DuckDB para datos crudos (raw data) y estadísticas de auditoría del pipeline."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from uuid import uuid4

import duckdb
import pandas as pd


@dataclass(frozen=True)
class DatabaseStats:
    """
    Estadísticas de reconciliación OLAP calculadas directamente por DuckDB.
    Se usan para comparar contra el cálculo en memoria y validar la integridad matemática.
    """
    row_count: int
    avg_price: float | None
    min_price: float | None
    max_price: float | None


@dataclass(frozen=True)
class PipelineStatisticsRecord:
    """
    Representa una fila de auditoría para un micro-batch procesado.
    Conserva un snapshot de métricas para trazabilidad y reconciliación.
    """
    batch_id: str
    file_name: str
    processed_at: datetime
    batch_rows: int
    total_accumulated_rows: int
    accumulated_sum: float
    accumulated_avg: float | None
    global_min_price: float | None
    global_max_price: float | None


class DuckDBRepository:
    """
    Administra un Data Warehouse local en DuckDB y las escrituras del estado de auditoría.
    Implementa el protocolo de Context Manager (with...) para asegurar el cierre de conexiones.
    """

    def __init__(self, db_path: str | Path = "data_warehouse.db") -> None:
        self.db_path = Path(db_path)
        self._connection: duckdb.DuckDBPyConnection | None = None

    # Métodos mágicos para permitir el uso de "with DuckDBRepository() as repo:"
    def __enter__(self) -> "DuckDBRepository":
        self.connect()
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.close()

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        """Devuelve una conexión activa a DuckDB, instanciándola si es necesario."""
        if self._connection is None:
            self.connect()
        if self._connection is None:
            raise RuntimeError("No se pudo crear la conexión a DuckDB.")
        return self._connection

    def connect(self) -> None:
        """Abre la base de datos local DuckDB."""
        if self._connection is None:
            self._connection = duckdb.connect(str(self.db_path))

    def close(self) -> None:
        """Cierra la conexión a DuckDB de forma segura liberando recursos."""
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def initialize_schema(self) -> None:
        """
        Crea las tablas del Data Warehouse si no existen.
        Se usa un modelo de 3 tablas:
        1. raw_transactions: Capa Bronze/Silver con los datos limpios.
        2. pipeline_statistics_history: Historial de métricas para auditoría.
        3. rejected_transactions: Dead Letter Queue (DLQ) para registros anómalos.
        """
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS raw_transactions (
                "timestamp" TIMESTAMP,
                price DOUBLE,
                user_id BIGINT,
                file_name VARCHAR,
                loaded_at TIMESTAMP,
                batch_id VARCHAR
            );
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS pipeline_statistics_history (
                batch_id VARCHAR PRIMARY KEY,
                file_name VARCHAR,
                processed_at TIMESTAMP,
                batch_rows BIGINT,
                total_accumulated_rows BIGINT,
                accumulated_sum DOUBLE,
                accumulated_avg DOUBLE,
                global_min_price DOUBLE,
                global_max_price DOUBLE
            );
            """
        )
        self.connection.execute(
            """
            CREATE TABLE IF NOT EXISTS rejected_transactions (
                "timestamp" TIMESTAMP,
                price VARCHAR,
                user_id BIGINT,
                file_name VARCHAR,
                reject_reason VARCHAR,
                batch_id VARCHAR
            );
            """
        )

    def get_actual_db_stats(self) -> DatabaseStats:
        """
        Consulta analítica directa sobre los datos crudos.
        Solo se utiliza al final para la reconciliación (comprobación de resultados)
        requerida por la prueba.
        """
        row = self.connection.execute(
            """
            SELECT
                COUNT(*),
                AVG(price),
                MIN(price),
                MAX(price)
            FROM raw_transactions
            WHERE price IS NOT NULL;
            """
        ).fetchone()

        if row is None:
            return DatabaseStats(
                row_count=0,
                avg_price=None,
                min_price=None,
                max_price=None,
            )

        return DatabaseStats(
            row_count=int(row[0]),
            avg_price=float(row[1]) if row[1] is not None else None,
            min_price=float(row[2]) if row[2] is not None else None,
            max_price=float(row[3]) if row[3] is not None else None,
        )

    def persist_batch(
        self,
        transactions: pd.DataFrame,
        statistics: PipelineStatisticsRecord,
    ) -> None:
        """
        Persiste un micro-batch de datos limpios y su estado de auditoría de forma ATÓMICA.
        Si falla la inserción de los datos crudos o de las métricas, se hace ROLLBACK de ambos.
        """
        self.connection.execute("BEGIN TRANSACTION;")
        try:
            self.insert_raw_transactions(transactions, statistics)
            self.insert_statistics_record(statistics)
            self.connection.execute("COMMIT;")
        except Exception:
            self.connection.execute("ROLLBACK;")
            raise

    def insert_raw_transactions(
        self,
        transactions: pd.DataFrame,
        statistics: PipelineStatisticsRecord,
    ) -> None:
        """
        Inserta un micro-batch limpio en raw_transactions.
        Utiliza el registro de vistas virtuales de DuckDB para ingestar 
        el DataFrame de Pandas a alta velocidad sin bucles.
        """
        if transactions.empty:
            return

        # Enriquecemos el DataFrame con metadatos del pipeline para trazabilidad
        loaded_transactions = transactions.copy()
        loaded_transactions["file_name"] = statistics.file_name
        loaded_transactions["loaded_at"] = statistics.processed_at
        loaded_transactions["batch_id"] = statistics.batch_id

        # Nombre temporal único para evitar colisiones de hilos/procesos
        view_name = f"batch_{uuid4().hex}"

        # Código útil para debug (comentado)
        # print("\n--- TIPOS DE loaded_transactions ---")
        # print(loaded_transactions.dtypes)
        # print("\n--- TIPOS DETALLADOS ---")
        # for column in loaded_transactions.columns:
        #     print(
        #         f"{column}: "
        #         f"dtype={loaded_transactions[column].dtype}, "
        #         f"tipo={type(loaded_transactions[column].dtype)}"
        #     )
        # print("------------------------------------\n")

        # Registramos el DataFrame como una tabla temporal en DuckDB
        self.connection.register(view_name, loaded_transactions)
        try:
            # Transferimos los datos usando SQL, delegando el rendimiento a DuckDB
            self.connection.execute(
                f"""
                INSERT INTO raw_transactions (
                    "timestamp",
                    price,
                    user_id,
                    file_name,
                    loaded_at,
                    batch_id
                )
                SELECT
                    "timestamp",
                    price,
                    user_id,
                    file_name,
                    loaded_at,
                    batch_id
                FROM {view_name};
                """
            )
        finally:
            # Siempre limpiamos la vista temporal, incluso si hay error
            self.connection.unregister(view_name)

    def insert_statistics_record(self, statistics: PipelineStatisticsRecord) -> None:
        """Inserta el snapshot del estado matemático del pipeline para este micro-batch."""
        self.connection.execute(
            """
            INSERT INTO pipeline_statistics_history (
                batch_id,
                file_name,
                processed_at,
                batch_rows,
                total_accumulated_rows,
                accumulated_sum,
                accumulated_avg,
                global_min_price,
                global_max_price
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            [
                statistics.batch_id,
                statistics.file_name,
                statistics.processed_at,
                statistics.batch_rows,
                statistics.total_accumulated_rows,
                statistics.accumulated_sum,
                statistics.accumulated_avg,
                statistics.global_min_price,
                statistics.global_max_price,
            ],
        )

    def fetch_statistics_history(self, limit: int = 10) -> list[dict[str, object]]:
        """Devuelve los registros recientes de auditoría para inspección en terminal."""
        rows = self.connection.execute(
            """
            SELECT
                batch_id,
                file_name,
                processed_at,
                batch_rows,
                total_accumulated_rows,
                accumulated_sum,
                accumulated_avg,
                global_min_price,
                global_max_price
            FROM pipeline_statistics_history
            ORDER BY processed_at DESC, batch_id DESC
            LIMIT ?;
            """,
            [limit],
        ).fetchall()

        columns = [
            "batch_id",
            "file_name",
            "processed_at",
            "batch_rows",
            "total_accumulated_rows",
            "accumulated_sum",
            "accumulated_avg",
            "global_min_price",
            "global_max_price",
        ]
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def fetch_raw_sample(self, limit: int = 5) -> list[dict[str, object]]:
        """Devuelve una pequeña muestra de datos crudos insertados para validación visual."""
        rows = self.connection.execute(
            """
            SELECT
                "timestamp",
                price,
                user_id,
                file_name,
                loaded_at,
                batch_id
            FROM raw_transactions
            ORDER BY loaded_at DESC, batch_id DESC
            LIMIT ?;
            """,
            [limit],
        ).fetchall()

        columns = [
            "timestamp",
            "price",
            "user_id",
            "file_name",
            "loaded_at",
            "batch_id",
        ]
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def insert_rejected_records(
        self, 
        rejected_df: pd.DataFrame, 
        file_name: str, 
        batch_id: str
    ) -> None:
        """Inserta en DuckDB los registros que no pasaron la validación (Patrón DLQ)."""
        if rejected_df.empty:
            return

        # Convertimos cada registro del DataFrame en una tupla nativa de Python.
        # Esto evita problemas de compatibilidad de tipos (Nulls/NaT/NaN) entre Pandas
        # y DuckDB al inyectar datos inconsistentes o sucios.
        records = [
            (
                row["timestamp"].to_pydatetime()
                if pd.notna(row["timestamp"])
                else None,
                str(row["price"]) if pd.notna(row["price"]) else None,
                int(row["user_id"]) if pd.notna(row["user_id"]) else None,
                file_name,
                str(row["reject_reason"]),
                batch_id,
            )
            for _, row in rejected_df.iterrows()
        ]

        self.connection.executemany(
            """
            INSERT INTO rejected_transactions (
                "timestamp",
                price,
                user_id,
                file_name,
                reject_reason,
                batch_id
            )
            VALUES (?, ?, ?, ?, ?, ?);
            """,
            records,
        )
