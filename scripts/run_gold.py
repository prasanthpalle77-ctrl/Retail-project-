"""Publish conformed Gold dimensions, facts, aggregates, and certified KPIs."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from retail_lakehouse.config import load_settings
from retail_lakehouse.pipelines import run_gold_pipeline
from retail_lakehouse.spark import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--environment", default="dev")
    parser.add_argument("--kpi-catalog", type=Path, default=Path("configs/kpi_definitions.yml"))
    parser.add_argument("--project-root", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = load_settings(args.environment, args.project_root)
    run_id = args.run_id or datetime.now(UTC).strftime("gold-%Y%m%dT%H%M%S%fZ")
    spark = create_spark_session(settings)
    try:
        result = run_gold_pipeline(
            spark,
            run_id=run_id,
            silver_root=Path(settings["storage"]["silver"]),
            gold_root=Path(settings["storage"]["gold"]),
            kpi_catalog_path=args.kpi_catalog,
        )
    finally:
        spark.stop()
    print(json.dumps(asdict(result), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    main()
