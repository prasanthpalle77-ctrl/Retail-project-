"""Deterministic foreachBatch processing for retail event streams."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from retail_lakehouse.quality import evaluate_quality, load_quality_rules
from retail_lakehouse.storage import (
    merge_current_state,
    merge_insert_or_update,
    synchronize_snapshot,
)
from retail_lakehouse.transformations.gold_aggregates import build_channel_funnel
from retail_lakehouse.transformations.silver import (
    SILVER_SPECS,
    deduplicate_latest,
    standardize_bronze,
)
from retail_lakehouse.utils.hashing import sha256_text


@dataclass(frozen=True)
class StreamBatchResult:
    """Reconciled counts for one Structured Streaming micro-batch."""

    stream_name: str
    dataset_name: str
    batch_id: int
    rows_read: int
    within_batch_duplicates: int
    late_rows: int
    quality_rejected_rows: int
    replayed_rows: int
    merged_rows: int
    maximum_event_timestamp: datetime | None
    watermark_cutoff: datetime | None


def _is_delta(spark: Any, path: Path) -> bool:
    try:
        from delta.tables import DeltaTable
    except ImportError as exc:
        raise RuntimeError("delta-spark is required for streaming processing.") from exc
    return bool(DeltaTable.isDeltaTable(spark, str(path.resolve())))


def _with_bronze_metadata(frame: Any, dataset_name: str, batch_id: int) -> Any:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for streaming processing.") from exc
    stream_metadata_columns = (
        "_stream_source_file_path",
        "_stream_source_file_name",
        "_stream_source_file_size",
        "_stream_source_file_modified_at",
    )
    source_columns = sorted(
        column for column in frame.columns if column not in stream_metadata_columns
    )
    return (
        frame.withColumn("_corrupt_record", functions.lit(None).cast("string"))
        .withColumn(
            "_record_hash",
            functions.sha2(functions.to_json(functions.struct(*source_columns)), 256),
        )
        .withColumn("_source_system", functions.lit(f"{dataset_name.upper()}_STREAM"))
        .withColumn("_source_file_path", functions.col("_stream_source_file_path"))
        .withColumn("_source_file_name", functions.col("_stream_source_file_name"))
        .withColumn("_source_file_size", functions.col("_stream_source_file_size"))
        .withColumn(
            "_source_file_modified_at",
            functions.col("_stream_source_file_modified_at").cast("string"),
        )
        .withColumn("_file_checksum", functions.lit(None).cast("string"))
        .withColumn("_ingested_at", functions.current_timestamp())
        .withColumn("_pipeline_run_id", functions.lit(f"stream-{dataset_name}"))
        .withColumn("_batch_id", functions.lit(str(batch_id)))
        .withColumn("_schema_version", functions.lit(1))
        .drop(*stream_metadata_columns)
    )


def _load_control(spark: Any, control_path: Path, stream_name: str) -> datetime | None:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for streaming control state.") from exc
    if not _is_delta(spark, control_path):
        return None
    rows = (
        spark.read.format("delta")
        .load(str(control_path.resolve()))
        .where(functions.col("stream_name") == stream_name)
        .select("maximum_event_timestamp")
        .limit(1)
        .collect()
    )
    return rows[0].maximum_event_timestamp if rows else None


def _split_exact_replays(
    spark: Any,
    accepted: Any,
    target_path: Path,
    business_keys: tuple[str, ...],
) -> tuple[Any, int]:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for streaming processing.") from exc
    if not _is_delta(spark, target_path):
        return accepted, 0
    existing = (
        spark.read.format("delta")
        .load(str(target_path.resolve()))
        .select(*business_keys, functions.col("_silver_record_hash").alias("_existing_record_hash"))
    )
    incoming = accepted.alias("incoming")
    stored = existing.alias("stored")
    condition = None
    for key in business_keys:
        comparison = functions.col(f"incoming.`{key}`").eqNullSafe(functions.col(f"stored.`{key}`"))
        condition = comparison if condition is None else condition & comparison
    compared = incoming.join(stored, condition, "left").select(
        "incoming.*", "stored._existing_record_hash"
    )
    replayed = compared.where(
        functions.col("_existing_record_hash") == functions.col("_silver_record_hash")
    ).count()
    return compared.where(
        functions.col("_existing_record_hash").isNull()
        | (functions.col("_existing_record_hash") != functions.col("_silver_record_hash"))
    ).drop("_existing_record_hash"), int(replayed)


def _quarantine(
    spark: Any,
    frame: Any,
    path: Path,
    *,
    dataset_name: str,
    reason: str,
) -> None:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for streaming quarantine.") from exc
    if frame.limit(1).count() == 0:
        return
    event_key = SILVER_SPECS[dataset_name].business_keys[0]
    quarantined = frame.withColumn(
        "_quarantine_id",
        functions.sha2(
            functions.concat_ws(
                "|",
                functions.lit(dataset_name),
                functions.col(event_key),
                functions.col("_silver_record_hash"),
                functions.lit(reason),
            ),
            256,
        ),
    )
    merge_insert_or_update(spark, quarantined, path, identifier="_quarantine_id")


def _persist_quality_metrics(spark: Any, metrics: tuple[Any, ...], silver_root: Path) -> None:
    if not metrics:
        return
    rows = []
    for metric in metrics:
        values = asdict(metric)
        values["quality_metric_id"] = sha256_text(
            f"{metric.run_id}|{metric.dataset_name}|{metric.rule_id}"
        )
        rows.append(values)
    merge_insert_or_update(
        spark,
        spark.createDataFrame(rows),
        silver_root / "_quality_results",
        identifier="quality_metric_id",
    )


def _refresh_customer_funnel(spark: Any, source: Any, gold_root: Path) -> None:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for streaming KPIs.") from exc
    events = source.withColumn(
        "event_date_key", functions.date_format("event_timestamp", "yyyyMMdd").cast("int")
    )
    funnel = build_channel_funnel(events)
    synchronize_snapshot(
        spark,
        funnel,
        gold_root / "streaming_channel_funnel",
        identifiers=("event_date_key", "device_type"),
    )


def _refresh_inventory_health(spark: Any, source: Any, gold_root: Path) -> None:
    try:
        from pyspark.sql import functions as functions
        from pyspark.sql.window import Window
    except ImportError as exc:
        raise RuntimeError("PySpark is required for streaming KPIs.") from exc
    window = Window.partitionBy("product_id", "store_id").orderBy(
        functions.col("event_timestamp").desc(),
        functions.col("inventory_event_id").desc(),
    )
    latest = (
        source.withColumn("_latest_rank", functions.row_number().over(window))
        .where("_latest_rank = 1")
        .withColumn(
            "event_date_key", functions.date_format("event_timestamp", "yyyyMMdd").cast("int")
        )
    )
    grouped = latest.groupBy("event_date_key", "store_id").agg(
        functions.count("inventory_event_id").alias("product_observations"),
        functions.sum((functions.col("quantity_on_hand") == 0).cast("long")).alias(
            "stockout_observations"
        ),
        functions.sum(
            (functions.col("quantity_on_hand") <= functions.col("reorder_level")).cast("long")
        ).alias("below_reorder_observations"),
        functions.sum("quantity_on_hand").cast("long").alias("total_quantity_on_hand"),
    )
    health = grouped.withColumn(
        "stockout_rate",
        functions.when(functions.col("product_observations") == 0, 0.0).otherwise(
            functions.col("stockout_observations") / functions.col("product_observations")
        ),
    )
    synchronize_snapshot(
        spark,
        health,
        gold_root / "streaming_inventory_health",
        identifiers=("event_date_key", "store_id"),
    )


def process_stream_batch(
    spark: Any,
    batch_frame: Any,
    batch_id: int,
    *,
    stream_name: str,
    dataset_name: str,
    allowed_lateness_hours: int,
    bronze_root: Path,
    silver_root: Path,
    gold_root: Path,
    quarantine_root: Path,
    rules_path: Path,
) -> StreamBatchResult:
    """Process one micro-batch with deterministic replay and reconciliation controls."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for streaming processing.") from exc
    if allowed_lateness_hours < 0:
        raise ValueError("allowed_lateness_hours cannot be negative.")
    spec = SILVER_SPECS[dataset_name]
    rows_read = int(batch_frame.count())
    bronze = _with_bronze_metadata(batch_frame, dataset_name, batch_id)
    bronze = bronze.withColumn(
        "_stream_record_id",
        functions.sha2(
            functions.concat_ws(
                "|",
                functions.lit(dataset_name),
                *[functions.col(key) for key in spec.business_keys],
                functions.col("_record_hash"),
            ),
            256,
        ),
    )
    merge_insert_or_update(
        spark,
        bronze.dropDuplicates(["_stream_record_id"]),
        bronze_root / f"{dataset_name}_streaming",
        identifier="_stream_record_id",
    )
    standardized = standardize_bronze(bronze.drop("_stream_record_id"), dataset_name)
    deduplicated = deduplicate_latest(standardized, dataset_name)
    deduplicated_rows = int(deduplicated.count())
    within_duplicates = rows_read - deduplicated_rows
    event_timestamp = spec.event_timestamp
    batch_maximum = (
        deduplicated.agg(functions.max(event_timestamp).alias("maximum")).first().maximum
    )
    control_path = silver_root / "_stream_control"
    previous_maximum = _load_control(spark, control_path, stream_name)
    watermark_cutoff = (
        previous_maximum - timedelta(hours=allowed_lateness_hours)
        if previous_maximum is not None
        else None
    )
    if watermark_cutoff is None:
        late = deduplicated.limit(0)
        on_time = deduplicated
    else:
        late = deduplicated.where(functions.col(event_timestamp) < watermark_cutoff)
        on_time = deduplicated.where(
            functions.col(event_timestamp).isNotNull()
            & (functions.col(event_timestamp) >= watermark_cutoff)
        )
    late = late.withColumn(
        "_failed_rule_ids", functions.array(functions.lit("STREAM_LATE_EVENT"))
    ).withColumn("_quality_status", functions.lit("QUARANTINED_LATE"))
    late_rows = int(late.count())
    _quarantine(
        spark,
        late,
        quarantine_root / "stream_late" / dataset_name,
        dataset_name=dataset_name,
        reason="STREAM_LATE_EVENT",
    )

    run_id = f"{stream_name}-batch-{batch_id}"
    rules = load_quality_rules(rules_path, dataset_name)
    evaluation = evaluate_quality(
        on_time,
        rules,
        run_id=run_id,
        pipeline_name="structured_streaming",
        dataset_name=dataset_name,
    )
    quality_rejected = int(evaluation.quarantined.count())
    _quarantine(
        spark,
        evaluation.quarantined,
        quarantine_root / "stream_quality" / dataset_name,
        dataset_name=dataset_name,
        reason="STREAM_QUALITY",
    )
    _persist_quality_metrics(spark, evaluation.metrics, silver_root)

    target_path = silver_root / dataset_name
    accepted = evaluation.accepted.drop("_failed_rule_ids")
    to_merge, replayed_rows = _split_exact_replays(spark, accepted, target_path, spec.business_keys)
    merged_rows = int(to_merge.count())
    merge_current_state(
        spark,
        to_merge,
        target_path,
        business_keys=spec.business_keys,
        event_timestamp=event_timestamp,
    )
    current = spark.read.format("delta").load(str(target_path.resolve()))
    if dataset_name == "customer_events":
        _refresh_customer_funnel(spark, current, gold_root)
    elif dataset_name == "inventory_events":
        _refresh_inventory_health(spark, current, gold_root)

    maximum = max(
        (value for value in (previous_maximum, batch_maximum) if value is not None),
        default=None,
    )
    checked_at = datetime.now(UTC)
    control = spark.createDataFrame(
        [
            (
                stream_name,
                dataset_name,
                maximum,
                allowed_lateness_hours,
                int(batch_id),
                checked_at,
            )
        ],
        schema=(
            "stream_name string, dataset_name string, maximum_event_timestamp timestamp, "
            "allowed_lateness_hours int, last_batch_id long, updated_at timestamp"
        ),
    )
    merge_insert_or_update(spark, control, control_path, identifier="stream_name")
    reconciled = within_duplicates + late_rows + quality_rejected + replayed_rows + merged_rows
    status = "PASS" if reconciled == rows_read else "FAIL"
    audit_id = sha256_text(f"{stream_name}|{batch_id}")
    audit = spark.createDataFrame(
        [
            (
                audit_id,
                stream_name,
                dataset_name,
                int(batch_id),
                rows_read,
                within_duplicates,
                late_rows,
                quality_rejected,
                replayed_rows,
                merged_rows,
                reconciled,
                status,
                maximum,
                watermark_cutoff,
                checked_at,
            )
        ],
        schema=(
            "stream_batch_audit_id string, stream_name string, dataset_name string, "
            "batch_id long, rows_read long, within_batch_duplicates long, late_rows long, "
            "quality_rejected_rows long, replayed_rows long, merged_rows long, "
            "reconciled_rows long, reconciliation_status string, "
            "maximum_event_timestamp timestamp, watermark_cutoff timestamp, "
            "processed_at timestamp"
        ),
    )
    merge_insert_or_update(
        spark,
        audit,
        silver_root / "_stream_batch_audit",
        identifier="stream_batch_audit_id",
    )
    if status != "PASS":
        raise RuntimeError(f"Streaming batch reconciliation failed for {stream_name}.")
    return StreamBatchResult(
        stream_name=stream_name,
        dataset_name=dataset_name,
        batch_id=int(batch_id),
        rows_read=rows_read,
        within_batch_duplicates=within_duplicates,
        late_rows=late_rows,
        quality_rejected_rows=quality_rejected,
        replayed_rows=replayed_rows,
        merged_rows=merged_rows,
        maximum_event_timestamp=maximum,
        watermark_cutoff=watermark_cutoff,
    )
