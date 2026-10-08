"""Utilidades de lectura en streaming para la ingesta de micro-batches desde archivos CSV."""

from collections.abc import Iterator
from pathlib import Path
from typing import Final

import pandas as pd


class CSVStreamParser:
    """
    Lee y procesa archivos CSV secuencialmente en micro-batches limpios con tipos explícitos.
    Implementa lectura perezosa (Lazy Evaluation) con generadores (`yield`) para garantizar 
    que el consumo de memoria RAM se mantenga en O(1) sin cargar todo el archivo.
    """

    # Columnas contractuales obligatorias para la ingesta
    REQUIRED_COLUMNS: Final[tuple[str, str, str]] = (
        "timestamp",
        "price",
        "user_id",
    )

    def __init__(self, chunk_size: int = 10) -> None:
        if chunk_size <= 0:
            raise ValueError("El tamaño del fragmento (chunk_size) debe ser mayor a cero.")
        self.chunk_size = chunk_size

    def iter_chunks(
        self, file_path: str | Path
    ) -> Iterator[tuple[pd.DataFrame, pd.DataFrame]]:
        """
        Lee el archivo CSV en fragmentos (chunks) y entrega una tupla por cada iteración:
        (datos_validos, datos_rechazados).
        """
        csv_path = Path(file_path)
        
        # pd.read_csv con 'chunksize' retorna un TextFileReader (Streaming)
        for chunk in pd.read_csv(csv_path, chunksize=self.chunk_size):
            # 1. Validación de contrato de datos de esquema
            self._validate_columns(chunk, csv_path)
            
            # 2. Sanitización y bifurcación de registros válidos e inválidos (DLQ)
            valid_chunk, rejected_chunk = self._clean_chunk(chunk)
            
            # 3. Retorno perezoso al orquestador sin acumular en memoria
            yield valid_chunk, rejected_chunk

    def _validate_columns(self, chunk: pd.DataFrame, file_path: Path) -> None:
        """Valida que el archivo CSV contenga el conjunto mínimo de columnas requeridas."""
        missing_columns = set(self.REQUIRED_COLUMNS).difference(chunk.columns)
        if missing_columns:
            missing = ", ".join(sorted(missing_columns))
            raise ValueError(
                f"El archivo '{file_path.name}' no contiene las columnas requeridas: {missing}"
            )

    def _clean_chunk(self, chunk: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Aplica tipado estricto y bifurca el micro-batch en dos DataFrames:
        - Válidos: Registros numéricos y limpios listos para el pipeline analítico.
        - Rechazados: Registros anómalos o con precios nulos para la cola de errores (DLQ).
        """
        # Seleccionamos únicamente las columnas requeridas
        clean = chunk.loc[:, self.REQUIRED_COLUMNS].copy()
        
        # Conversión explícita de tipos con coerción de errores a NaN
        clean["timestamp"] = pd.to_datetime(clean["timestamp"], errors="coerce")
        clean["price"] = pd.to_numeric(clean["price"], errors="coerce")
        # Int64 (con 'I' mayúscula) permite enteros nulos en Pandas sin castear todo a flotante
        clean["user_id"] = pd.to_numeric(clean["user_id"], errors="coerce").astype("Int64")

        # Regla de Calidad de Datos: Se rechazan las filas cuyo precio sea nulo o inválido
        bad_price_mask = clean["price"].isna()
        
        # Bifurcación física de los datos
        valid_records = clean[~bad_price_mask].reset_index(drop=True)
        rejected_records = clean[bad_price_mask].copy().reset_index(drop=True)
        
        # Etiquetado de auditoría para la tabla de rechazos (DLQ)
        if not rejected_records.empty:
            rejected_records["reject_reason"] = "Precio nulo o formato numérico inválido"

        return valid_records, rejected_records