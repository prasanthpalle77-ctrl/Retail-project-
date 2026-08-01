import json
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from retail_lakehouse.config import load_settings
from retail_lakehouse.spark import create_spark_session
from retail_lakehouse.streaming import StreamingQueryConfig, start_file_stream

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.name == "nt", reason="Native Windows Spark requires winutils.exe"),
]


def _write_events(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _customer_event(
    event_id: str,
    event_type: str,
    timestamp: datetime,
    *,
    session_id: str = "SESSION1",
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "session_id": session_id,
        "customer_id": "C1",
        "event_type": event_type,
        "product_id": "P1",
        "event_timestamp": timestamp.isoformat(),
        "page_url": "/products/P1",
        "device_type": "MOBILE",
        "browser": "CHROME",
        "campaign_id": "CMP1",
        "source": "TEST",
        "ingestion_timestamp": timestamp.isoformat(),
    }


def _inventory_event(
    event_id: str,
    product_id: str,
    timestamp: datetime,
    quantity: int,
) -> dict[str, object]:
    return {
        "inventory_event_id": event_id,
        "product_id": product_id,
        "store_id": "S1",
        "event_type": "SNAPSHOT",
        "quantity_change": 0,
        "quantity_on_hand": quantity,
        "reorder_level": 5,
        "event_timestamp": timestamp.isoformat(),
        "source_system": "TEST",
    }


def _run_available_now(spark: object, config: StreamingQueryConfig) -> None:
    query = start_file_stream(spark, config)
    assert query.awaitTermination(180)
    assert query.exception() is None


def test_customer_stream_checkpoint_replay_and_late_quarantine(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    spark = create_spark_session(load_settings("test", root))
    source = tmp_path / "customer-source"
    source.mkdir()
    timestamp = datetime(2026, 1, 10, 12, tzinfo=UTC)
    config = StreamingQueryConfig(
        dataset_name="customer_events",
        source_path=source,
        checkpoint_path=tmp_path / "checkpoints" / "customer",
        silver_root=tmp_path / "silver",
        gold_root=tmp_path / "gold",
        quarantine_root=tmp_path / "quarantine",
        rules_path=root / "configs" / "data_quality_rules.yml",
        stream_name="customer-test-stream",
        allowed_lateness_hours=24,
    )
    first = _customer_event("E1", "PRODUCT_VIEW", timestamp)
    _write_events(
        source / "batch-001.json",
        [first, first, _customer_event("E2", "PURCHASE", timestamp)],
    )
    try:
        _run_available_now(spark, config)
        _write_events(
            source / "batch-002.json",
            [
                _customer_event("E-LATE", "PRODUCT_VIEW", timestamp - timedelta(days=2)),
                _customer_event(
                    "E3", "PRODUCT_VIEW", timestamp + timedelta(hours=1), session_id="SESSION2"
                ),
            ],
        )
        _run_available_now(spark, config)
        _write_events(
            source / "batch-003.json",
            [
                _customer_event(
                    "E3", "PRODUCT_VIEW", timestamp + timedelta(hours=1), session_id="SESSION2"
                )
            ],
        )
        _run_available_now(spark, config)
        _run_available_now(spark, config)

        silver_count = (
            spark.read.format("delta").load(str(tmp_path / "silver" / "customer_events")).count()
        )
        late_count = (
            spark.read.format("delta")
            .load(str(tmp_path / "quarantine" / "stream_late" / "customer_events"))
            .count()
        )
        audits = spark.read.format("delta").load(str(tmp_path / "silver" / "_stream_batch_audit"))
        audit_totals = audits.agg(
            {"rows_read": "sum", "within_batch_duplicates": "sum", "replayed_rows": "sum"}
        ).first()
        audit_count = audits.count()
        funnel = (
            spark.read.format("delta")
            .load(str(tmp_path / "gold" / "streaming_channel_funnel"))
            .first()
        )
    finally:
        spark.stop()

    assert silver_count == 3
    assert late_count == 1
    assert audit_count == 3
    assert audit_totals["sum(rows_read)"] == 6
    assert audit_totals["sum(within_batch_duplicates)"] == 1
    assert audit_totals["sum(replayed_rows)"] == 1
    assert funnel.eligible_sessions == 2
    assert funnel.purchase_sessions == 1
    assert funnel.conversion_rate == 0.5


def test_inventory_stream_updates_latest_health_snapshot(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    spark = create_spark_session(load_settings("test", root))
    source = tmp_path / "inventory-source"
    source.mkdir()
    timestamp = datetime(2026, 1, 10, 12, tzinfo=UTC)
    _write_events(
        source / "batch-001.json",
        [
            _inventory_event("INV1", "P1", timestamp, 5),
            _inventory_event("INV2", "P2", timestamp, 0),
        ],
    )
    _write_events(
        source / "batch-002.json",
        [_inventory_event("INV3", "P1", timestamp + timedelta(hours=1), 0)],
    )
    config = StreamingQueryConfig(
        dataset_name="inventory_events",
        source_path=source,
        checkpoint_path=tmp_path / "checkpoints" / "inventory",
        silver_root=tmp_path / "silver",
        gold_root=tmp_path / "gold",
        quarantine_root=tmp_path / "quarantine",
        rules_path=root / "configs" / "data_quality_rules.yml",
        stream_name="inventory-test-stream",
        max_files_per_trigger=1,
    )
    try:
        _run_available_now(spark, config)
        health = (
            spark.read.format("delta")
            .load(str(tmp_path / "gold" / "streaming_inventory_health"))
            .first()
        )
        event_count = (
            spark.read.format("delta").load(str(tmp_path / "silver" / "inventory_events")).count()
        )
        audit_count = (
            spark.read.format("delta")
            .load(str(tmp_path / "silver" / "_stream_batch_audit"))
            .count()
        )
    finally:
        spark.stop()

    assert event_count == 3
    assert audit_count == 2
    assert health.product_observations == 2
    assert health.stockout_observations == 2
    assert health.stockout_rate == 1.0
