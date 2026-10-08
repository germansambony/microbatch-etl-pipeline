"""Cálculo de estadísticas incrementales para el procesamiento por micro-batches."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


@dataclass(frozen=True)
class BatchMetrics:
    """
    Estadísticas calculadas de forma aislada para un solo lote (chunk) de datos.
    No conoce el estado global, solo lo que hay en el fragmento actual.
    """
    row_count: int                      #Recuento
    price_sum: float                    #Suma de precios
    min_price: float | None             #Mínimo precio            
    max_price: float | None             #Máximo precio


@dataclass(frozen=True)
class AccumulatedMetrics:
    """
    Estado global de las estadísticas. 
    Se actualiza de forma incremental después de cada micro-batch sin necesidad
    de volver a leer los datos históricos.
    """
    total_rows: int = 0                 #Recuento total de filas
    accumulated_sum: float = 0.0
    accumulated_avg: float | None = None
    global_min_price: float | None = None
    global_max_price: float | None = None


class BatchMetricsCalculator:
    """Clase encargada de calcular métricas locales y fusionarlas con el estado global."""

    def calculate(self, chunk: "pd.DataFrame") -> BatchMetrics:
        """
        Calcula el conteo, suma, mínimo y máximo exclusivamente para el DataFrame actual.
        Ignora los valores nulos o inválidos para no alterar las matemáticas.
        """
        import pandas as pd

        if "price" not in chunk.columns:
            raise ValueError("El lote de datos debe contener la columna 'price'.")

        # Forzamos la conversión a numérico y eliminamos nulos
        prices = pd.to_numeric(chunk["price"], errors="coerce").dropna()
        
        if prices.empty:
            return BatchMetrics(
                row_count=0,
                price_sum=0.0,
                min_price=None,
                max_price=None,
            )

        return BatchMetrics(
            row_count=int(prices.count()),
            price_sum=float(prices.sum()),
            min_price=float(prices.min()),
            max_price=float(prices.max()),
        )

    def accumulate(
        self,
        batch_metrics: BatchMetrics,
        previous: AccumulatedMetrics | None = None,
    ) -> AccumulatedMetrics:
        """
        Fusiona las métricas del lote actual con el estado global acumulado.
        
        Aquí se aplica el cálculo incremental:
        Nuevo Promedio = (Suma Anterior + Suma Actual) / (Filas Anteriores + Filas Actuales)
        """
        previous_state = previous or AccumulatedMetrics()
        
        # Sumamos las filas y los precios
        total_rows = previous_state.total_rows + batch_metrics.row_count
        accumulated_sum = previous_state.accumulated_sum + batch_metrics.price_sum
        
        # Protegemos contra división por cero si el pipeline está vacío
        accumulated_avg = accumulated_sum / total_rows if total_rows else None

        return AccumulatedMetrics(
            total_rows=total_rows,
            accumulated_sum=accumulated_sum,
            accumulated_avg=accumulated_avg,
            global_min_price=self._merge_min(
                previous_state.global_min_price,
                batch_metrics.min_price,
            ),
            global_max_price=self._merge_max(
                previous_state.global_max_price,
                batch_metrics.max_price,
            ),
        )

    @staticmethod
    def _merge_min(previous: float | None, current: float | None) -> float | None:
        """Compara el mínimo global histórico con el mínimo del lote actual."""
        if previous is None:
            return current
        if current is None:
            return previous
        return min(previous, current)

    @staticmethod
    def _merge_max(previous: float | None, current: float | None) -> float | None:
        """Compara el máximo global histórico con el máximo del lote actual."""
        if previous is None:
            return current
        if current is None:
            return previous
        return max(previous, current)