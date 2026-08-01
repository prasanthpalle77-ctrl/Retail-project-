"""Replay-safe Delta Bronze ingestion for immutable Landing files."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast


class BronzeIngestionError(RuntimeError):
    """Raised when a Landing file cannot be committed to Bronze."""


@dataclass(frozen=True)
class BronzeIngestionResult:
    """Counts and target information for one Bronze file transaction."""

    dataset_name: str
    landing_file: str
    bronze_path: str
    rows_read: int
    corrupt_rows: int
    transaction_app_id: str
    transaction_version: int


def _load_manifest(path: Path) -> dict[str, Any]:
    payload = cast(dict[str, Any], json.loads(path.read_text(encoding="utf-8")))
    required = {
        "source_name",
        "landing_file_path",
        "checksum_sha256",
        "ingestion_timestamp",
        "pipeline_run_id",
        "batch_id",
    }
    missing = sorted(required - payload.keys())
    if missing:
        raise BronzeIngestionError(f"Landing manifest is missing fields: {', '.join(missing)}")
    return payload


def ingest_manifest_to_bronze(
    spark: Any,
    manifest_path: Path,
    bronze_root: Path,
) -> BronzeIngestionResult:
    """Parse one manifested file and append it idempotently to a Delta dataset.

    A transaction application ID is derived from the source and exact file
    checksum. Replaying the same application ID/version is ignored by Delta,
    while a genuinely changed file receives a distinct transaction identity.
    """

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise BronzeIngestionError("PySpark is required for Bronze ingestion.") from exc

    manifest = _load_manifest(manifest_path)
    landing_file = Path(manifest["landing_file_path"])
    if not landing_file.is_file():
        raise BronzeIngestionError(f"Landing file does not exist: {landing_file}")

    dataset_name = str(manifest["source_name"])
    suffix = landing_file.suffix.lower()
    if suffix == ".csv":
        frame = (
            spark.read.option("header", "true")
            .option("inferSchema", "false")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .csv(str(landing_file))
        )
    elif suffix in {".json", ".jsonl"}:
        frame = (
            spark.read.option("primitivesAsString", "true")
            .option("mode", "PERMISSIVE")
            .option("columnNameOfCorruptRecord", "_corrupt_record")
            .json(str(landing_file))
        )
    else:
        raise BronzeIngestionError(f"Bronze parser does not support: {suffix}")

    source_columns = sorted(frame.columns)
    if "_corrupt_record" not in frame.columns:
        frame = frame.withColumn("_corrupt_record", functions.lit(None).cast("string"))
    frame = (
        frame.withColumn(
            "_record_hash",
            functions.sha2(functions.to_json(functions.struct(*source_columns)), 256),
        )
        .withColumn("_source_system", functions.lit(manifest.get("source_system")))
        .withColumn("_source_file_path", functions.lit(str(landing_file)))
        .withColumn("_source_file_name", functions.lit(landing_file.name))
        .withColumn("_source_file_size", functions.lit(int(manifest["file_size"])))
        .withColumn("_source_file_modified_at", functions.lit(manifest["file_modified_at"]))
        .withColumn("_file_checksum", functions.lit(manifest["checksum_sha256"]))
        .withColumn(
            "_ingested_at", functions.to_timestamp(functions.lit(manifest["ingestion_timestamp"]))
        )
        .withColumn("_pipeline_run_id", functions.lit(manifest["pipeline_run_id"]))
        .withColumn("_batch_id", functions.lit(manifest["batch_id"]))
        .withColumn("_schema_version", functions.lit(1))
    )

    rows_read = frame.count()
    corrupt_rows = frame.where(functions.col("_corrupt_record").isNotNull()).count()
    target = (bronze_root / dataset_name).resolve()
    transaction_app_id = f"novaretail-bronze-{dataset_name}-{manifest['checksum_sha256']}"
    transaction_version = 1
    (
        frame.write.format("delta")
        .mode("append")
        .option("txnAppId", transaction_app_id)
        .option("txnVersion", transaction_version)
        .save(str(target))
    )
    return BronzeIngestionResult(
        dataset_name=dataset_name,
        landing_file=str(landing_file),
        bronze_path=str(target),
        rows_read=rows_read,
        corrupt_rows=corrupt_rows,
        transaction_app_id=transaction_app_id,
        transaction_version=transaction_version,
    )
