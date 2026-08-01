"""Structured Streaming query construction with durable checkpoints."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from retail_lakehouse.streaming.processor import process_stream_batch
from retail_lakehouse.streaming.specs import STREAMING_DATASETS, stream_schema_ddl
from retail_lakehouse.transformations.silver import SILVER_SPECS


@dataclass(frozen=True)
class StreamingQueryConfig:
    """Paths and operational controls for one file-backed event stream."""

    dataset_name: str
    source_path: Path
    checkpoint_path: Path
    bronze_root: Path
    silver_root: Path
    gold_root: Path
    quarantine_root: Path
    rules_path: Path
    stream_name: str | None = None
    allowed_lateness_hours: int = 24
    max_files_per_trigger: int = 1
    available_now: bool = True
    processing_time_seconds: int = 5

    def __post_init__(self) -> None:
        if self.dataset_name not in STREAMING_DATASETS:
            raise ValueError(f"Unsupported streaming dataset: {self.dataset_name}")
        if self.allowed_lateness_hours < 0:
            raise ValueError("allowed_lateness_hours cannot be negative.")
        if self.max_files_per_trigger < 1 or self.processing_time_seconds < 1:
            raise ValueError("Streaming trigger controls must be positive.")


def start_file_stream(spark: Any, config: StreamingQueryConfig) -> Any:
    """Start a checkpointed JSON file stream and return its StreamingQuery."""

    config.source_path.mkdir(parents=True, exist_ok=True)
    config.checkpoint_path.mkdir(parents=True, exist_ok=True)
    event_timestamp = SILVER_SPECS[config.dataset_name].event_timestamp
    stream = (
        spark.readStream.schema(stream_schema_ddl(config.dataset_name))
        .option("maxFilesPerTrigger", config.max_files_per_trigger)
        .json(str(config.source_path.resolve()))
        .withWatermark(event_timestamp, f"{config.allowed_lateness_hours} hours")
    )
    stream_name = config.stream_name or f"novaretail-{config.dataset_name}"

    def process(frame: Any, batch_id: int) -> None:
        process_stream_batch(
            spark,
            frame,
            batch_id,
            stream_name=stream_name,
            dataset_name=config.dataset_name,
            allowed_lateness_hours=config.allowed_lateness_hours,
            bronze_root=config.bronze_root,
            silver_root=config.silver_root,
            gold_root=config.gold_root,
            quarantine_root=config.quarantine_root,
            rules_path=config.rules_path,
        )

    writer = (
        stream.writeStream.queryName(stream_name)
        .option("checkpointLocation", str(config.checkpoint_path.resolve()))
        .foreachBatch(process)
    )
    if config.available_now:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime=f"{config.processing_time_seconds} seconds")
    return writer.start()
