"""Run a checkpointed customer-event or inventory file stream."""

from __future__ import annotations

import argparse
from pathlib import Path

from retail_lakehouse.config import load_settings
from retail_lakehouse.spark import create_spark_session
from retail_lakehouse.streaming import StreamingQueryConfig, start_file_stream


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", choices=("customer_events", "inventory_events"))
    parser.add_argument("source_path", type=Path)
    parser.add_argument("--environment", default="prod")
    parser.add_argument("--stream-name", default=None)
    parser.add_argument("--allowed-lateness-hours", type=int, default=None)
    parser.add_argument("--max-files-per-trigger", type=int, default=1)
    parser.add_argument("--continuous", action="store_true")
    parser.add_argument("--processing-time-seconds", type=int, default=5)
    parser.add_argument("--rules", type=Path, default=Path("configs/data_quality_rules.yml"))
    parser.add_argument("--project-root", type=Path, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    settings = load_settings(args.environment, args.project_root)
    lateness = args.allowed_lateness_hours
    if lateness is None:
        lateness = int(settings["pipeline"]["allowed_lateness_hours"])
    stream_name = args.stream_name or f"novaretail-{args.dataset}"
    config = StreamingQueryConfig(
        dataset_name=args.dataset,
        source_path=args.source_path,
        checkpoint_path=Path(settings["storage"]["checkpoints"]) / stream_name,
        bronze_root=Path(settings["storage"]["bronze"]),
        silver_root=Path(settings["storage"]["silver"]),
        gold_root=Path(settings["storage"]["gold"]),
        quarantine_root=Path(settings["storage"]["quarantine"]),
        rules_path=args.rules,
        stream_name=stream_name,
        allowed_lateness_hours=lateness,
        max_files_per_trigger=args.max_files_per_trigger,
        available_now=not args.continuous,
        processing_time_seconds=args.processing_time_seconds,
    )
    spark = create_spark_session(settings)
    query = start_file_stream(spark, config)
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    main()
