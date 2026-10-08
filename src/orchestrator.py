"""Orquestación del pipeline ETL para la ingesta de CSV en micro-batches."""

import json
from argparse import ArgumentParser
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from math import inf
from pathlib import Path
from typing import Any
from uuid import uuid4

from src.csv_parser import CSVStreamParser
from src.db_repository import DatabaseStats, DuckDBRepository, PipelineStatisticsRecord
from src.stats_calculator import (
    AccumulatedMetrics,
    BatchMetrics,
    BatchMetricsCalculator,
)


@dataclass
class PipelineState:
    """Estado incremental persistible del pipeline."""

    global_count: int = 0
    global_sum: float = 0.0
    global_min: float = inf
    global_max: float = -inf
    processed_files: set[str] = field(default_factory=set)

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PipelineState":
        """Construye el estado desde el diccionario leído del archivo JSON."""
        processed_files = payload.get("processed_files", [])
        if not isinstance(processed_files, list):
            raise ValueError(
                "'processed_files' debe ser una lista en pipeline_state.json."
            )

        return cls(
            global_count=int(payload.get("global_count", 0)),
            global_sum=float(payload.get("global_sum", 0.0)),
            global_min=cls._parse_float(payload.get("global_min"), default=inf),
            global_max=cls._parse_float(payload.get("global_max"), default=-inf),
            processed_files={str(file_name) for file_name in processed_files},
        )

    def to_dict(self) -> dict[str, Any]:
        """Serializa el estado con las claves contractuales del archivo JSON."""
        return {
            "global_count": self.global_count,
            "global_sum": self.global_sum,
            "global_min": self.global_min,
            "global_max": self.global_max,
            "processed_files": sorted(self.processed_files),
        }

    def to_metrics(self) -> AccumulatedMetrics:
        """Adapta el estado persistido al modelo matemático usado por el calculador."""
        accumulated_avg = (
            self.global_sum / self.global_count if self.global_count else None
        )
        return AccumulatedMetrics(
            total_rows=self.global_count,
            accumulated_sum=self.global_sum,
            accumulated_avg=accumulated_avg,
            global_min_price=self.global_min if self.global_count else None,
            global_max_price=self.global_max if self.global_count else None,
        )

    def update_from_metrics(self, metrics: AccumulatedMetrics) -> None:
        """Sincroniza el estado persistible con las métricas acumuladas actuales."""
        self.global_count = metrics.total_rows
        self.global_sum = metrics.accumulated_sum
        self.global_min = (
            metrics.global_min_price if metrics.global_min_price is not None else inf
        )
        self.global_max = (
            metrics.global_max_price if metrics.global_max_price is not None else -inf
        )

    @staticmethod
    def _parse_float(value: object, default: float) -> float:
        """Convierte valores numéricos del JSON, tolerando nulos en estados vacíos."""
        if value is None:
            return default
        return float(value)


class PipelineOrchestrator:
    """
    Coordina el análisis de CSV, la acumulación matemática y la persistencia en DuckDB.
    Actúa como el controlador principal asegurando que el consumo de memoria se
    mantenga constante (O(1)) sin importar el tamaño de los archivos.
    """

    VALIDATION_FILE_NAME = "validation.csv"

    def __init__(
        self,
        data_dir: str | Path = "data",
        db_path: str | Path = "data_warehouse.db",
        chunk_size: int = 10,
        state_path: str | Path = "pipeline_state.json",
    ) -> None:
        self.data_dir = Path(data_dir)
        self.db_path = Path(db_path)
        self.state_path = Path(state_path)
        # El parser lee los archivos por fragmentos para no saturar la memoria RAM.
        self.parser = CSVStreamParser(chunk_size=chunk_size)
        self.calculator = BatchMetricsCalculator()
        self.repository = DuckDBRepository(db_path=self.db_path)
        self.pipeline_state = self._load_pipeline_state()

    def run(self) -> None:
        """
        Ejecuta el ciclo de vida completo
        1. Descubre los CSV fuente disponibles en data_dir.
        2. Omite los archivos ya registrados en pipeline_state.json.
        3. Procesa los archivos pendientes y actualiza el estado persistido.
        4. Procesa validation.csv como comprobación explícita si existe.
        5. Imprime el estado final y la reconciliación contra DuckDB.
        """
        csv_files = self._discover_csv_files(include_validation=False)
        validation_file = self.data_dir / self.VALIDATION_FILE_NAME

        # Manejador de contexto para asegurar que la conexión a la base de datos se cierre correctamente
        with self.repository:
            self.repository.initialize_schema()
            # Recuperamos el estado desde JSON para continuar sin recalcular históricos.
            state = self.pipeline_state.to_metrics()

            print("Procesando archivos CSV fuente del data lake local...")
            state = self.process_files(csv_files, state)
            self.print_database_snapshot("Estado después de la carga inicial")
            self.print_reconciliation_stats("Reconciliación inicial (Consulta OLAP)")

            if validation_file.exists():
                print("Procesando validation.csv como comprobación final...")
                self.process_files([validation_file], state)
                self.print_database_snapshot("Estado después de validation.csv")
                self.print_reconciliation_stats(
                    "Reconciliación final con validation.csv"
                )

    def process_files(
        self,
        file_paths: Iterable[Path],
        initial_state: AccumulatedMetrics | None = None,
    ) -> AccumulatedMetrics:
        """
        Procesa archivos secuencialmente implementando tolerancia a fallos mediante un
        patrón DLQ (Dead Letter Queue) para registros inválidos.
        """
        state = initial_state or self.pipeline_state.to_metrics()

        for file_path in file_paths:
            # Validamos idempotencia: si el archivo ya se procesó, lo saltamos
            if file_path.name in self.pipeline_state.processed_files:
                print(f"Omitiendo {file_path.name}: El archivo ya fue procesado previamente.")
                continue

            # Iteramos sobre el archivo en micro-batches.
            # El parser bifurca los datos: válidos para el flujo, rechazados para el DLQ.
            for chunk_index, (valid_chunk, rejected_chunk) in enumerate(
                self.parser.iter_chunks(file_path),
                start=1,
            ):
                batch_id = self._build_batch_id(file_path, chunk_index)

                # 1. DLQ (Dead Letter Queue): Guardamos historial de registros con precios nulos
                if not rejected_chunk.empty:
                    self.repository.insert_rejected_records(
                        rejected_chunk,
                        file_path.name,
                        batch_id,
                    )

                # 2. Protección: Si todo el lote era inválido, pasamos al siguiente
                if valid_chunk.empty:
                    continue

                # 3. Flujo Analítico: Calculamos métricas sobre datos limpios
                batch_metrics = self.calculator.calculate(valid_chunk)
                state = self.calculator.accumulate(batch_metrics, state)
                processed_at = datetime.now(UTC).replace(tzinfo=None)

                # Armamos el registro de auditoría (snapshot del pipeline en este instante)
                statistics = PipelineStatisticsRecord(
                    batch_id=batch_id,
                    file_name=file_path.name,
                    processed_at=processed_at,
                    batch_rows=batch_metrics.row_count,
                    total_accumulated_rows=state.total_rows,
                    accumulated_sum=state.accumulated_sum,
                    accumulated_avg=state.accumulated_avg,
                    global_min_price=state.global_min_price,
                    global_max_price=state.global_max_price,
                )

                # Persistimos atómicamente los datos crudos y su auditoría
                self.repository.persist_batch(valid_chunk, statistics)
                self._print_batch_summary(file_path, batch_id, batch_metrics, state)

            self.pipeline_state.update_from_metrics(state)
            self.pipeline_state.processed_files.add(file_path.name)
            self._save_pipeline_state()

        return state

    def _load_pipeline_state(self) -> PipelineState:
        """Carga el estado persistido del pipeline o crea uno nuevo si no existe."""
        if not self.state_path.exists():
            return PipelineState()

        with self.state_path.open(encoding="utf-8") as state_file:
            payload = json.load(state_file)

        if not isinstance(payload, dict):
            raise ValueError("pipeline_state.json debe contener un objeto JSON.")

        return PipelineState.from_dict(payload)

    def _save_pipeline_state(self) -> None:
        """Sobrescribe el estado del pipeline al completar un archivo."""
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        temporary_path = self.state_path.with_name(f"{self.state_path.name}.tmp")
        serialized_state = (
            json.dumps(self.pipeline_state.to_dict(), indent=2, sort_keys=True) + "\n"
        )

        try:
            temporary_path.write_text(serialized_state, encoding="utf-8")
            temporary_path.replace(self.state_path)
        except PermissionError:
            self.state_path.write_text(serialized_state, encoding="utf-8")
            temporary_path.unlink(missing_ok=True)

    def print_database_snapshot(self, title: str) -> None:
        """Imprime una muestra de la persistencia de forma ordenada y legible en la terminal."""
        print(f"\n{'='*80}")
        print(f" {title.upper()} ".center(80, '='))
        print(f"{'='*80}")

        print("\n[Auditoría] Registros recientes (pipeline_statistics_history):")
        history = self.repository.fetch_statistics_history(limit=5)
        self._print_records(history)

        print("[Raw] Muestra reciente de datos insertados (raw_transactions):")
        raw_data = self.repository.fetch_raw_sample(limit=5)
        self._print_records(raw_data)
        print("="*80 + "\n")

    @staticmethod
    def _print_records(records: list[dict[str, object]]) -> None:
        """
        Formatea e imprime registros en modo 'vertical' (estilo base de datos) 
        para evitar desbordamientos horizontales en la terminal.
        """
        if not records:
            print("   [!] No hay registros para mostrar.\n")
            return
            
        for i, row in enumerate(records, start=1):
            print(f"  [ Fila {i} ] ".ljust(50, '-'))
            for key, value in row.items():
                # Redondeamos los flotantes para limpiar el exceso de decimales
                if isinstance(value, float):
                    val_str = f"{value:.2f}"
                else:
                    val_str = str(value)
                
                # Alinear las llaves a la izquierda con un ancho fijo de 25 caracteres
                print(f"    {key:<25} : {val_str}")
        print()

    def print_reconciliation_stats(self, title: str) -> None:
        """
        Ejecuta la consulta SQL analítica directa contra la base de datos para
        demostrar que nuestras métricas incrementales coinciden matemáticamente.
        """
        stats = self.repository.get_actual_db_stats()
        print(f"\n{title}")
        print(self._format_database_stats(stats))
        print()

    def _discover_csv_files(self, include_validation: bool = False) -> list[Path]:
        """Busca todos los CSV disponibles en el directorio configurado."""
        files = sorted(
            file_path
            for file_path in self.data_dir.glob("*.csv")
            if include_validation or file_path.name != self.VALIDATION_FILE_NAME
        )
        if not files:
            raise FileNotFoundError(
                f"No se encontraron archivos CSV fuente en {self.data_dir}."
            )
        return files

    def _build_batch_id(self, file_path: Path, chunk_index: int) -> str:
        """Genera un identificador único y rastreable para cada micro-batch."""
        return f"{file_path.stem}-lote-{chunk_index:04d}-{uuid4().hex[:8]}"

    def _print_batch_summary(
        self,
        file_path: Path,
        batch_id: str,
        batch_metrics: BatchMetrics,
        state: AccumulatedMetrics,
    ) -> None:
        """Imprime en consola las estadísticas actualizadas durante la ejecución por cada lote."""
        print(
            " | ".join(
                [
                    f"id_lote={batch_id}",
                    f"archivo={file_path.name}",
                    f"filas_lote={batch_metrics.row_count}",
                    f"suma_lote={batch_metrics.price_sum:.2f}",
                    f"total_filas={state.total_rows}",
                    "promedio_acumulado="
                    f"{self._format_optional_float(state.accumulated_avg)}",
                    "min_global="
                    f"{self._format_optional_float(state.global_min_price)}",
                    "max_global="
                    f"{self._format_optional_float(state.global_max_price)}",
                ]
            )
        )

    @staticmethod
    def _format_optional_float(value: float | None) -> str:
        """Formatea los flotantes a 2 decimales manejando correctamente los nulos."""
        return "None" if value is None else f"{value:.2f}"

    def _format_database_stats(self, stats: DatabaseStats) -> str:
        """Formatea el resultado de la comprobación SQL."""
        return " | ".join(
            [
                f"total_filas_db={stats.row_count}",
                f"promedio_db={self._format_optional_float(stats.avg_price)}",
                f"minimo_db={self._format_optional_float(stats.min_price)}",
                f"maximo_db={self._format_optional_float(stats.max_price)}",
            ]
        )


def build_argument_parser() -> ArgumentParser:
    """Construye el analizador de argumentos para ejecución desde la terminal."""
    parser = ArgumentParser(description="Ejecuta el pipeline ETL por micro-batches.")
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Directorio que contiene los archivos CSV.",
    )
    parser.add_argument(
        "--db-path",
        default="data_warehouse.db",
        help="Ruta donde se almacenará la base de datos DuckDB.",
    )
    parser.add_argument(
        "--chunk-size",
        default=10,
        type=int,
        help="Cantidad de filas procesadas por cada lote de Pandas.",
    )
    parser.add_argument(
        "--state-path",
        default="pipeline_state.json",
        help="Ruta del archivo JSON donde se persiste el estado incremental.",
    )
    return parser


def main() -> None:
    """Punto de entrada principal para ejecutar el pipeline completo."""
    args = build_argument_parser().parse_args()
    orchestrator = PipelineOrchestrator(
        data_dir=args.data_dir,
        db_path=args.db_path,
        chunk_size=args.chunk_size,
        state_path=args.state_path,
    )
    orchestrator.run()


if __name__ == "__main__":
    main()
