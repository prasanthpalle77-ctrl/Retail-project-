"""Load one Landing manifest into a local Delta Bronze table."""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict
from pathlib import Path

from retail_lakehouse.config import load_settings
from retail_lakehouse.ingestion.bronze import ingest_manifest_to_bronze
from retail_lakehouse.spark import create_spark_session


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--environment", default="dev")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = load_settings(args.environment)
    spark = create_spark_session(settings)
    try:
        result = ingest_manifest_to_bronze(
            spark,
            args.manifest,
            Path(settings["storage"]["bronze"]),
        )
        print(json.dumps(asdict(result), indent=2, sort_keys=True))
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
