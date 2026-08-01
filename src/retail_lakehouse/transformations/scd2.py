"""Reusable, replay-safe SCD Type 2 history reconstruction."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def build_scd2_history(
    incoming: Any,
    *,
    business_keys: tuple[str, ...],
    tracked_columns: tuple[str, ...],
    effective_timestamp: str,
    existing: Any | None = None,
) -> Any:
    """Rebuild valid-time history, including late-arriving and replayed changes."""

    try:
        from pyspark.sql import functions as functions
        from pyspark.sql.window import Window
    except ImportError as exc:
        raise RuntimeError("PySpark is required for SCD Type 2 processing.") from exc

    required = set(business_keys) | set(tracked_columns) | {effective_timestamp}
    missing = sorted(required - set(incoming.columns))
    if missing:
        raise ValueError(f"Incoming SCD records are missing: {', '.join(missing)}")
    history_columns = {
        "surrogate_key",
        "valid_from",
        "valid_to",
        "is_current",
        "attribute_hash",
    }
    base_columns = [
        name
        for name in incoming.columns
        if not name.startswith("_scd_") and name not in history_columns
    ]

    def prepare(frame: Any, effective_column: Any, priority: int) -> Any:
        hash_value = functions.sha2(
            functions.to_json(functions.struct(*[functions.col(name) for name in tracked_columns])),
            256,
        )
        return (
            frame.withColumn("_scd_effective_at", effective_column.cast("timestamp"))
            .select(*base_columns, "_scd_effective_at")
            .withColumn("attribute_hash", hash_value)
            .withColumn("_scd_priority", functions.lit(priority))
        )

    versions = prepare(incoming, functions.col(effective_timestamp), 2)
    if existing is not None:
        existing_missing = sorted(set(base_columns) - set(existing.columns))
        if existing_missing or "valid_from" not in existing.columns:
            raise ValueError("Existing SCD history does not match the incoming base schema.")
        previous = prepare(existing, functions.col("valid_from"), 1)
        versions = previous.unionByName(versions)

    same_time = Window.partitionBy(*business_keys, "_scd_effective_at").orderBy(
        functions.col("_scd_priority").desc(), functions.col("attribute_hash").desc()
    )
    versions = versions.withColumn("_same_time_rank", functions.row_number().over(same_time)).where(
        functions.col("_same_time_rank") == 1
    )
    sequence = Window.partitionBy(*business_keys).orderBy("_scd_effective_at", "attribute_hash")
    versions = versions.withColumn(
        "_previous_hash", functions.lag("attribute_hash").over(sequence)
    ).where(
        functions.col("_previous_hash").isNull()
        | (functions.col("_previous_hash") != functions.col("attribute_hash"))
    )
    next_effective = functions.lead("_scd_effective_at").over(sequence)
    key_parts = [
        functions.coalesce(functions.col(name).cast("string"), functions.lit("<NULL>"))
        for name in business_keys
    ]
    return (
        versions.withColumn("valid_from", functions.col("_scd_effective_at"))
        .withColumn("_next_effective", next_effective)
        .withColumn(
            "valid_to",
            functions.when(
                functions.col("_next_effective").isNotNull(),
                functions.expr("_next_effective - INTERVAL 1 MICROSECOND"),
            ).cast("timestamp"),
        )
        .withColumn("is_current", functions.col("_next_effective").isNull())
        .withColumn(
            "surrogate_key",
            functions.sha2(
                functions.concat_ws("|", *key_parts, functions.col("valid_from").cast("string")),
                256,
            ),
        )
        .select(
            "surrogate_key", *base_columns, "attribute_hash", "valid_from", "valid_to", "is_current"
        )
    )


def write_scd2_history(history: Any, target_path: Path) -> None:
    """Materialize reconstructed history after severing reads from the target."""

    checkpointed = history.localCheckpoint(eager=True)
    checkpointed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
        str(target_path.resolve())
    )
