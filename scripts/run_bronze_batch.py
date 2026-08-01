"""Load every manifest in a Landing batch into local Delta Bronze datasets."""

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
    parser.add_argument("batch_id")
    parser.add_argument("--manifest-root", type=Path, default=Path("data/landing/_manifests"))
    parser.add_argument("--bronze-root", type=Path, default=None)
    parser.add_argument("--environment", default="dev")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = load_settings(args.environment)
    bronze_root = args.bronze_root or Path(settings["storage"]["bronze"])
    manifests = sorted(args.manifest_root.glob(f"*/{args.batch_id}/*.manifest.json"))
    if not manifests:
        raise FileNotFoundError(f"No Landing manifests found for batch: {args.batch_id}")

    spark = create_spark_session(settings)
    try:
        results = [
            asdict(ingest_manifest_to_bronze(spark, manifest, bronze_root))
            for manifest in manifests
        ]
    finally:
        spark.stop()
    print(json.dumps({"batch_id": args.batch_id, "datasets": results}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
