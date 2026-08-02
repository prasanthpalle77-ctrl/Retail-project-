"""Run configuration-driven Silver processing for one or every Bronze dataset."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from retail_lakehouse.config import load_settings
from retail_lakehouse.pipelines import run_silver_dataset
from retail_lakehouse.spark import create_spark_session
from retail_lakehouse.transformations import SILVER_SPECS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", choices=["all", *SILVER_SPECS], default="all")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--environment", default="prod")
    parser.add_argument("--rules", type=Path, default=Path("configs/data_quality_rules.yml"))
    parser.add_argument("--project-root", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = load_settings(args.environment, args.project_root)
    run_id = args.run_id or datetime.now(UTC).strftime("silver-%Y%m%dT%H%M%S%fZ")
    datasets = tuple(SILVER_SPECS) if args.dataset == "all" else (args.dataset,)
    spark = create_spark_session(settings)
    try:
        results = [
            asdict(
                run_silver_dataset(
                    spark,
                    dataset_name=dataset,
                    run_id=run_id,
                    bronze_root=Path(settings["storage"]["bronze"]),
                    silver_root=Path(settings["storage"]["silver"]),
                    quarantine_root=Path(settings["storage"]["quarantine"]),
                    rules_path=args.rules,
                )
            )
            for dataset in datasets
        ]
    finally:
        spark.stop()
    print(json.dumps({"run_id": run_id, "datasets": results}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    main()
