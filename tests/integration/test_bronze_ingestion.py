import os
from datetime import UTC, datetime
from pathlib import Path

import pytest

from retail_lakehouse.config import load_settings
from retail_lakehouse.ingestion.bronze import ingest_manifest_to_bronze
from retail_lakehouse.ingestion.landing import FileRegistry, stage_file
from retail_lakehouse.spark import create_spark_session

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.name == "nt", reason="Native Windows Spark requires winutils.exe"),
]


def test_same_landing_file_is_committed_to_delta_once(tmp_path: Path) -> None:
    source = tmp_path / "source" / "orders.jsonl"
    source.parent.mkdir()
    source.write_text(
        '{"order_id":"O1","amount":"10.00"}\n{"order_id":"O2","amount":"20.00"}\n',
        encoding="utf-8",
    )
    landing = tmp_path / "landing"
    result = stage_file(
        source,
        landing,
        FileRegistry(landing / "_control" / "processed_files.json"),
        source_name="orders",
        source_system="INTEGRATION_TEST",
        batch_id="B1",
        run_id="RUN-1",
        ingested_at=datetime(2026, 1, 1, tzinfo=UTC),
    )

    project_root = Path(__file__).resolve().parents[2]
    settings = load_settings("test", project_root)
    spark = create_spark_session(settings)
    bronze_root = tmp_path / "bronze"
    try:
        first = ingest_manifest_to_bronze(spark, Path(result.manifest_path), bronze_root)
        second = ingest_manifest_to_bronze(spark, Path(result.manifest_path), bronze_root)
        actual_count = spark.read.format("delta").load(first.bronze_path).count()
    finally:
        spark.stop()

    assert first.rows_read == 2
    assert second.rows_read == 2
    assert actual_count == 2
