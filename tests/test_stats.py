"""Unit tests for incremental pipeline statistics."""

from src.stats_calculator import (
    AccumulatedMetrics,
    BatchMetrics,
    BatchMetricsCalculator,
)

# Prueba 1: Verifica el estado inicial con el primer lote de datos.
def test_accumulate_first_batch() -> None:
    # 1. Arrange: Preparamos los objetos necesarios.
    calculator = BatchMetricsCalculator()
    # Simulamos las métricas obtenidas de un primer fragmento (micro-lote)
    batch = BatchMetrics(
        row_count=3,
        price_sum=60.0,
        min_price=10.0,
        max_price=30.0,
    )

    # 2. Act: Ejecutamos el método que queremos probar (acumular métricas).
    # Como es el primer lote, no pasamos un estado anterior.
    state = calculator.accumulate(batch)

    # 3. Assert: Verificamos que los resultados sean los esperados matemáticamente.
    assert state.total_rows == 3          # Debe registrar 3 filas.
    assert state.accumulated_sum == 60.0  # La suma total debe ser 60.0.
    assert state.accumulated_avg == 20.0  # El promedio (60 / 3) debe ser 20.0.
    assert state.global_min_price == 10.0 # El mínimo global debe ser el del lote.
    assert state.global_max_price == 30.0 # El máximo global debe ser el del lote.


# Prueba 2: Verifica cómo se comporta el acumulador al recibir un lote subsecuente.
def test_accumulate_multiple_batches_keeps_global_bounds() -> None:
    calculator = BatchMetricsCalculator()
    
    # 1. Arrange: Simulamos un estado ya existente (el resultado de procesar lotes anteriores).
    previous = AccumulatedMetrics(
        total_rows=3,
        accumulated_sum=60.0,
        accumulated_avg=20.0,
        global_min_price=10.0,
        global_max_price=30.0,
    )
    # Simulamos un nuevo lote que acaba de llegar.
    batch = BatchMetrics(
        row_count=2,
        price_sum=100.0,
        min_price=40.0, # Notar que este mínimo (40) es MAYOR al global existente (10)
        max_price=60.0, # Este máximo (60) es MAYOR al global existente (30)
    )

    # 2. Act: Acumulamos el nuevo lote sobre el estado previo.
    state = calculator.accumulate(batch, previous)

    # 3. Assert: Verificamos la correcta agregación incremental.
    assert state.total_rows == 5           # 3 anteriores + 2 nuevas = 5
    assert state.accumulated_sum == 160.0  # 60.0 anteriores + 100.0 nuevas = 160.0
    assert state.accumulated_avg == 32.0   # 160.0 suma total / 5 filas totales = 32.0
    assert state.global_min_price == 10.0  # Mantiene el 10.0 porque 10 < 40
    assert state.global_max_price == 60.0  # Actualiza a 60.0 porque 60 > 30


# Prueba 3: Verifica el comportamiento límite cuando un lote viene vacío (ej: puras filas inválidas).
def test_empty_batch_does_not_change_state() -> None:
    calculator = BatchMetricsCalculator()
    
    # 1. Arrange: Estado previo establecido.
    previous = AccumulatedMetrics(
        total_rows=5,
        accumulated_sum=160.0,
        accumulated_avg=32.0,
        global_min_price=10.0,
        global_max_price=60.0,
    )
    # Lote vacío (cero métricas).
    empty_batch = BatchMetrics(
        row_count=0,
        price_sum=0.0,
        min_price=None,
        max_price=None,
    )

    # 2. Act: Acumular un lote vacío.
    state = calculator.accumulate(empty_batch, previous)

    # 3. Assert: El estado debe permanecer exactamente igual al previo.
    assert state == previous