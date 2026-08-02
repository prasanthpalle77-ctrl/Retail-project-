"""Reconcile Bronze business keys with Silver and quarantine Delta tables."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from retail_lakehouse.config import load_settings
from retail_lakehouse.spark import create_spark_session
from retail_lakehouse.transformations import SILVER_SPECS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("run_id")
    parser.add_argument("--environment", default="prod")
    return parser.parse_args()


def _is_delta(spark: Any, path: Path) -> bool:
    from delta.tables import DeltaTable

    return bool(DeltaTable.isDeltaTable(spark, str(path.resolve())))


def main() -> int:
    args = parse_args()
    settings = load_settings(args.environment)
    bronze_root = Path(settings["storage"]["bronze"])
    silver_root = Path(settings["storage"]["silver"])
    quarantine_root = Path(settings["storage"]["quarantine"])
    spark = create_spark_session(settings)
    datasets = []
    try:
        for dataset_name, spec in SILVER_SPECS.items():
            bronze = spark.read.format("delta").load(str((bronze_root / dataset_name).resolve()))
            bronze_rows = bronze.count()
            unique_business_keys = bronze.select(*spec.business_keys).distinct().count()
            silver_rows = (
                spark.read.format("delta").load(str((silver_root / dataset_name).resolve())).count()
            )
            quarantine_path = quarantine_root / dataset_name
            quarantined_rows = 0
            if _is_delta(spark, quarantine_path):
                quarantined_rows = (
                    spark.read.format("delta")
                    .load(str(quarantine_path.resolve()))
                    .where(f"_quality_run_id = '{args.run_id}'")
                    .count()
                )
            duplicates_removed = bronze_rows - unique_business_keys
            reconciled = silver_rows + quarantined_rows + duplicates_removed
            datasets.append(
                {
                    "dataset_name": dataset_name,
                    "bronze_rows": bronze_rows,
                    "silver_rows": silver_rows,
                    "quarantined_rows": quarantined_rows,
                    "duplicates_removed": duplicates_removed,
                    "reconciled_rows": reconciled,
                    "valid": reconciled == bronze_rows,
                }
            )
        quality_results = spark.read.format("delta").load(
            str((silver_root / "_quality_results").resolve())
        )
        run_metrics = quality_results.where(f"run_id = '{args.run_id}'")
        failed_rules = [
            row.asDict()
            for row in run_metrics.where("rule_status = 'FAIL'")
            .select("dataset_name", "rule_id", "records_failed", "failure_percentage")
            .orderBy("dataset_name", "rule_id")
            .collect()
        ]
        metric_count = run_metrics.count()
    finally:
        spark.stop()

    totals = {
        key: sum(int(row[key]) for row in datasets)
        for key in (
            "bronze_rows",
            "silver_rows",
            "quarantined_rows",
            "duplicates_removed",
            "reconciled_rows",
        )
    }
    payload = {
        "run_id": args.run_id,
        "valid": all(bool(row["valid"]) for row in datasets),
        "totals": totals,
        "quality_metric_count": metric_count,
        "failed_rules": failed_rules,
        "datasets": datasets,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload["valid"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
