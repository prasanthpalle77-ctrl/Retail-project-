"""Compare generated source counts with the current Delta Bronze datasets."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from retail_lakehouse.config import load_settings
from retail_lakehouse.spark import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("generated_batch", type=Path)
    parser.add_argument("--bronze-root", type=Path, default=None)
    parser.add_argument("--environment", default="prod")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = json.loads((args.generated_batch / "generation_report.json").read_text("utf-8"))
    settings = load_settings(args.environment)
    bronze_root = args.bronze_root or Path(settings["storage"]["bronze"])
    spark = create_spark_session(settings)
    results = []
    try:
        for dataset_name, expected in sorted(report["record_counts"].items()):
            target = bronze_root / dataset_name
            actual = spark.read.format("delta").load(str(target.resolve())).count()
            results.append(
                {
                    "dataset_name": dataset_name,
                    "expected": expected,
                    "actual": actual,
                    "matches": expected == actual,
                }
            )
    finally:
        spark.stop()

    valid = all(result["matches"] for result in results)
    print(json.dumps({"valid": valid, "datasets": results}, indent=2, sort_keys=True))
    return 0 if valid else 1


if __name__ == "__main__":
    raise SystemExit(main())
