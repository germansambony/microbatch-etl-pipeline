"""Integration tests for the micro-batch ELT flow."""

import json
from datetime import datetime
from pathlib import Path

import pytest

from src.csv_parser import CSVStreamParser
from src.db_repository import DuckDBRepository, PipelineStatisticsRecord
from src.orchestrator import PipelineOrchestrator
from src.stats_calculator import AccumulatedMetrics, BatchMetricsCalculator


# Prueba 1: Verifica el flujo básico: Leer CSV -> Calcular métricas -> Guardar en DuckDB.
def test_microbatch_pipeline_persists_rows_and_incremental_stats(
    tmp_path: Path,
) -> None:
    # 1. Arrange: Creamos un archivo CSV temporal simulando nuestros datos reales.
    csv_path = tmp_path / "transactions.csv"
    csv_path.write_text(
        "\n".join(
            [
                "timestamp,price,user_id",
                "2026-01-01,10,1",
                "2026-01-02,,2",    # ¡Fila intencionalmente inválida (precio vacío)!
                "2026-01-03,30,3",
                "2026-01-04,50,4",
            ]
        ),
        encoding="utf-8",
    )

    # Inicializamos los componentes del pipeline.
    parser = CSVStreamParser(chunk_size=2) # Leemos de a 2 filas para simular micro-batches.
    calculator = BatchMetricsCalculator()

    # Usamos una base de datos DuckDB en memoria (se borra al terminar la prueba).
    with DuckDBRepository(":memory:") as repository:
        repository.initialize_schema()
        state = AccumulatedMetrics()

        # 2. Act: Ejecutamos el flujo iterando sobre los micro-batches.
        for chunk_index, (valid_chunk, rejected_chunk) in enumerate(
            parser.iter_chunks(csv_path),
            start=1,
        ):
            batch_id = f"transactions-chunk-{chunk_index:04d}"

            # Si hay registros rechazados (la fila sin precio), se insertan en la tabla de rechazados.
            if not rejected_chunk.empty:
                repository.insert_rejected_records(
                    rejected_chunk,
                    csv_path.name,
                    batch_id,
                )

            # Si el chunk válido está vacío, continuamos con el siguiente.
            if valid_chunk.empty:
                continue

            # Calculamos las métricas del lote válido y actualizamos el estado global.
            batch_metrics = calculator.calculate(valid_chunk)
            state = calculator.accumulate(batch_metrics, state)
            
            # Persistimos las filas del micro-batch y el registro histórico de estadísticas en la BD.
            repository.persist_batch(
                transactions=valid_chunk,
                statistics=PipelineStatisticsRecord(
                    batch_id=batch_id,
                    file_name=csv_path.name,
                    processed_at=datetime(2026, 1, 1, 12, chunk_index),
                    batch_rows=batch_metrics.row_count,
                    total_accumulated_rows=state.total_rows,
                    accumulated_sum=state.accumulated_sum,
                    accumulated_avg=state.accumulated_avg,
                    global_min_price=state.global_min_price,
                    global_max_price=state.global_max_price,
                ),
            )

        # Consultamos los resultados guardados en la BD para validarlos.
        db_stats = repository.get_actual_db_stats()
        latest_audit_record = repository.fetch_statistics_history(limit=1)[0]

        # 3. Assert: Comprobamos que el cálculo en memoria es correcto.
        assert state.total_rows == 3 # Eran 4 filas, 1 fue rechazada.
        assert state.accumulated_sum == 90.0
        assert state.accumulated_avg == pytest.approx(30.0) # approx() maneja problemas de precisión de punto flotante.
        assert state.global_min_price == 10.0
        assert state.global_max_price == 50.0

        # Verificamos que el registro histórico en DuckDB coincide con el estado final en memoria.
        assert latest_audit_record["total_accumulated_rows"] == state.total_rows
        assert latest_audit_record["accumulated_sum"] == state.accumulated_sum
        assert latest_audit_record["accumulated_avg"] == pytest.approx(
            state.accumulated_avg
        )
        assert latest_audit_record["global_min_price"] == state.global_min_price
        assert latest_audit_record["global_max_price"] == state.global_max_price
        
        # Verificamos que la "foto" actual de la tabla real en BD (haciendo el query real) coincide.
        assert db_stats.row_count == 3
        assert db_stats.avg_price == pytest.approx(state.accumulated_avg)
        assert db_stats.min_price == state.global_min_price
        assert db_stats.max_price == state.global_max_price


# Prueba 2: Verifica que el Orquestador guarda su estado (checkpoint) y evita reprocesar archivos.
def test_orchestrator_persists_json_state_and_skips_processed_files(
    tmp_path: Path,
) -> None:
    # 1. Arrange: Preparamos dos archivos, uno inicial y otro de "validación".
    csv_path = tmp_path / "transactions.csv"
    validation_path = tmp_path / "validation.csv"
    csv_path.write_text(
        "\n".join(
            [
                "timestamp,price,user_id",
                "2026-01-01,10,1",
                "2026-01-02,,2",
                "2026-01-03,30,3",
                "2026-01-04,50,4",
            ]
        ),
        encoding="utf-8",
    )
    validation_path.write_text(
        "\n".join(
            [
                "timestamp,price,user_id",
                "2026-01-05,70,5",
            ]
        ),
        encoding="utf-8",
    )
    
    # Definimos rutas para guardar el estado y la base de datos en el directorio temporal.
    state_path = tmp_path / "pipeline_state.json"
    db_path = tmp_path / "warehouse.db"

    # Instanciamos el primer orquestador.
    orchestrator = PipelineOrchestrator(
        data_dir=tmp_path,
        db_path=db_path,
        chunk_size=2,
        state_path=state_path,
    )

    # 2. Act (Parte 1): Ejecutamos la primera vez con el primer archivo.
    with orchestrator.repository:
        orchestrator.repository.initialize_schema()

        # Procesamos el archivo la primera vez.
        state = orchestrator.process_files([csv_path])
        # Intentamos procesarlo de nuevo. El orquestador debería detectarlo en el 'state' y omitirlo.
        orchestrator.process_files([csv_path], state)

        db_stats = orchestrator.repository.get_actual_db_stats()

    # Leemos el archivo JSON donde el orquestador debió guardar su estado.
    persisted_state = json.loads(state_path.read_text(encoding="utf-8"))

    # 3. Assert (Parte 1): Validamos que el JSON refleje el procesamiento de un solo archivo.
    assert persisted_state == {
        "global_count": 3,
        "global_sum": 90.0,
        "global_min": 10.0,
        "global_max": 50.0,
        "processed_files": ["transactions.csv"], # Aquí registra que ya lo procesó.
    }
    assert db_stats.row_count == 3
    assert db_stats.avg_price == pytest.approx(30.0)

    # 4. Act (Parte 2): Simulamos que el pipeline se reinicia (creamos un nuevo orquestador).
    # Como le pasamos la misma ruta de 'state_path', debe recuperar el estado anterior.
    resumed_orchestrator = PipelineOrchestrator(
        data_dir=tmp_path,
        db_path=db_path,
        chunk_size=2,
        state_path=state_path,
    )

    with resumed_orchestrator.repository:
        resumed_orchestrator.repository.initialize_schema()
        # Le enviamos un nuevo archivo (el de validación).
        resumed_orchestrator.process_files([validation_path])

        resumed_db_stats = resumed_orchestrator.repository.get_actual_db_stats()

    resumed_state = json.loads(state_path.read_text(encoding="utf-8"))

    # 5. Assert (Parte 2): Verificamos que el estado se actualizó correctamente con el nuevo archivo.
    assert resumed_state == {
        "global_count": 4, # 3 anteriores + 1 nuevo
        "global_sum": 160.0,
        "global_min": 10.0,
        "global_max": 70.0,
        "processed_files": ["transactions.csv", "validation.csv"], # Ahora registra ambos.
    }
    assert resumed_db_stats.row_count == 4
    assert resumed_db_stats.avg_price == pytest.approx(40.0)


# Prueba 3: Simula la llegada de archivos en lote y cómo el pipeline procesa solo las novedades.
def test_run_discovers_all_csv_files_and_processes_only_new_arrivals(
    tmp_path: Path,
) -> None:
    # 1. Arrange: Creamos un par de archivos iniciales.
    (tmp_path / "batch-a.csv").write_text(
        "\n".join(
            [
                "timestamp,price,user_id",
                "2026-01-01,10,1",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "batch-b.csv").write_text(
        "\n".join(
            [
                "timestamp,price,user_id",
                "2026-01-02,20,2",
            ]
        ),
        encoding="utf-8",
    )

    state_path = tmp_path / "pipeline_state.json"
    db_path = tmp_path / "warehouse.db"

    # 2. Act (Primera Ejecución): Corremos el pipeline usando el método principal .run()
    first_run = PipelineOrchestrator(
        data_dir=tmp_path,
        db_path=db_path,
        chunk_size=1,
        state_path=state_path,
    )
    first_run.run()

    first_state = json.loads(state_path.read_text(encoding="utf-8"))

    # 3. Assert (Primera Ejecución): Validamos que descubrió y procesó los dos primeros archivos.
    assert first_state == {
        "global_count": 2,
        "global_sum": 30.0,
        "global_min": 10.0,
        "global_max": 20.0,
        "processed_files": ["batch-a.csv", "batch-b.csv"],
    }

    # 4. Act (Segunda Ejecución): Simulamos que "llegan" nuevos archivos a la carpeta.
    (tmp_path / "batch-c.csv").write_text(
        "\n".join(
            [
                "timestamp,price,user_id",
                "2026-01-03,30,3",
            ]
        ),
        encoding="utf-8",
    )
    (tmp_path / "batch-d.csv").write_text(
        "\n".join(
            [
                "timestamp,price,user_id",
                "2026-01-04,40,4",
            ]
        ),
        encoding="utf-8",
    )

    # Volvemos a instanciar y ejecutar el pipeline. Debería ignorar A y B, y procesar C y D.
    second_run = PipelineOrchestrator(
        data_dir=tmp_path,
        db_path=db_path,
        chunk_size=1,
        state_path=state_path,
    )
    second_run.run()

    second_state = json.loads(state_path.read_text(encoding="utf-8"))

    # Consultamos la BD final.
    with DuckDBRepository(db_path) as repository:
        repository.initialize_schema()
        db_stats = repository.get_actual_db_stats()

    # 5. Assert (Segunda Ejecución): Comprobamos que el estado global integró todos los archivos.
    assert second_state == {
        "global_count": 4,
        "global_sum": 100.0,
        "global_min": 10.0,
        "global_max": 40.0,
        "processed_files": [
            "batch-a.csv",
            "batch-b.csv",
            "batch-c.csv",
            "batch-d.csv",
        ],
    }
    assert db_stats.row_count == 4
    assert db_stats.avg_price == pytest.approx(25.0)


# Prueba 4: Verifica el cumplimiento del requisito de excluir el archivo "validation.csv" por defecto.
def test_orchestrator_discovers_validation_as_explicit_verification_file(
    tmp_path: Path,
) -> None:
    # 1. Arrange: Creamos un archivo normal y el archivo especial "validation.csv"
    source_path = tmp_path / "2012-1.csv"
    validation_path = tmp_path / "validation.csv"
    source_path.write_text(
        "\n".join(
            [
                "timestamp,price,user_id",
                "2026-01-01,10,1",
            ]
        ),
        encoding="utf-8",
    )
    validation_path.write_text(
        "\n".join(
            [
                "timestamp,price,user_id",
                "2026-01-02,20,2",
            ]
        ),
        encoding="utf-8",
    )

    orchestrator = PipelineOrchestrator(
        data_dir=tmp_path,
        db_path=tmp_path / "warehouse.db",
        chunk_size=1,
        state_path=tmp_path / "pipeline_state.json",
    )

    # 2. Act: Llamamos al método interno de búsqueda de archivos.
    # Búsqueda normal (por defecto).
    source_files = orchestrator._discover_csv_files()
    
    # Búsqueda indicando explícitamente que incluya el de validación.
    all_files = orchestrator._discover_csv_files(include_validation=True)

    # 3. Assert: Comprobamos que respeta la regla de exclusión/inclusión.
    assert [file_path.name for file_path in source_files] == ["2012-1.csv"]
    assert [file_path.name for file_path in all_files] == [
        "2012-1.csv",
        "validation.csv",
    ]