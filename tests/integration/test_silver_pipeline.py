import os
from datetime import datetime
from pathlib import Path

import pytest

from retail_lakehouse.config import load_settings
from retail_lakehouse.pipelines import run_silver_dataset
from retail_lakehouse.spark import create_spark_session
from retail_lakehouse.transformations import build_scd2_history

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.name == "nt", reason="Native Windows Spark requires winutils.exe"),
]


def _customer(customer_id: str, email: str) -> dict[str, str]:
    return {
        "customer_id": customer_id,
        "first_name": "Ada",
        "last_name": "Lovelace",
        "email": email,
        "phone": "+1-555-0100",
        "date_of_birth": "1985-01-01",
        "gender": "F",
        "address": "1 Lakehouse Lane",
        "city": "Seattle",
        "state": "WA",
        "country": "US",
        "postal_code": "98101",
        "loyalty_tier": "gold",
        "registration_date": "2025-01-01",
        "customer_status": "active",
        "created_at": "2025-01-01T00:00:00Z",
        "updated_at": "2026-01-01T00:00:00Z",
        "cdc_operation": "insert",
    }


def test_silver_quarantine_reconciliation_and_replay(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    settings = load_settings("test", project_root)
    spark = create_spark_session(settings)
    bronze_root = tmp_path / "bronze"
    silver_root = tmp_path / "silver"
    quarantine_root = tmp_path / "quarantine"
    good = _customer("C1", "ada@example.com")
    invalid = _customer("C2", "not-an-email")
    try:
        spark.createDataFrame([good, good, invalid]).write.format("delta").save(
            str(bronze_root / "customers")
        )
        first = run_silver_dataset(
            spark,
            dataset_name="customers",
            run_id="SILVER-TEST-1",
            bronze_root=bronze_root,
            silver_root=silver_root,
            quarantine_root=quarantine_root,
            rules_path=project_root / "configs" / "data_quality_rules.yml",
        )
        second = run_silver_dataset(
            spark,
            dataset_name="customers",
            run_id="SILVER-TEST-1",
            bronze_root=bronze_root,
            silver_root=silver_root,
            quarantine_root=quarantine_root,
            rules_path=project_root / "configs" / "data_quality_rules.yml",
        )
        silver_count = spark.read.format("delta").load(str(silver_root / "customers")).count()
        quarantine = spark.read.format("delta").load(str(quarantine_root / "customers"))
        metric_count = (
            spark.read.format("delta").load(str(silver_root / "_quality_results")).count()
        )
        failed_rule_ids = quarantine.select("_failed_rule_ids").first()[0]
    finally:
        spark.stop()

    assert first.bronze_rows == 3
    assert first.duplicates_removed == 1
    assert first.accepted_rows == 1
    assert first.quarantined_rows == 1
    assert second == first
    assert silver_count == 1
    assert failed_rule_ids == ["CUS-002"]
    assert metric_count == 2


def test_scd2_replay_and_late_arriving_change(tmp_path: Path) -> None:
    project_root = Path(__file__).resolve().parents[2]
    settings = load_settings("test", project_root)
    spark = create_spark_session(settings)
    try:
        initial = spark.createDataFrame(
            [
                ("C1", "BRONZE", datetime(2026, 1, 1)),
                ("C1", "GOLD", datetime(2026, 3, 1)),
            ],
            ["customer_id", "loyalty_tier", "updated_at"],
        )
        history = build_scd2_history(
            initial,
            business_keys=("customer_id",),
            tracked_columns=("loyalty_tier",),
            effective_timestamp="updated_at",
        )
        late_and_replay = spark.createDataFrame(
            [
                ("C1", "BRONZE", datetime(2026, 1, 1)),
                ("C1", "SILVER", datetime(2026, 2, 1)),
            ],
            ["customer_id", "loyalty_tier", "updated_at"],
        )
        rebuilt = build_scd2_history(
            late_and_replay,
            business_keys=("customer_id",),
            tracked_columns=("loyalty_tier",),
            effective_timestamp="updated_at",
            existing=history,
        )
        rows = (
            rebuilt.orderBy("valid_from")
            .select("loyalty_tier", "valid_from", "valid_to", "is_current")
            .collect()
        )
    finally:
        spark.stop()

    assert [row.loyalty_tier for row in rows] == ["BRONZE", "SILVER", "GOLD"]
    assert [row.is_current for row in rows] == [False, False, True]
    assert rows[0].valid_to < rows[1].valid_from
    assert rows[1].valid_to < rows[2].valid_from
