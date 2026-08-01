"""Reusable Delta Lake merge operations."""

from __future__ import annotations

from pathlib import Path
from typing import Any


def _delta_table(spark: Any, target_path: Path) -> Any | None:
    try:
        from delta.tables import DeltaTable
    except ImportError as exc:
        raise RuntimeError("delta-spark is required for Delta merge operations.") from exc
    resolved = str(target_path.resolve())
    return DeltaTable.forPath(spark, resolved) if DeltaTable.isDeltaTable(spark, resolved) else None


def merge_current_state(
    spark: Any,
    source: Any,
    target_path: Path,
    *,
    business_keys: tuple[str, ...],
    event_timestamp: str,
    operation_column: str | None = None,
) -> None:
    """Apply latest-event upserts/deletes without changing state on replay."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Delta merge operations.") from exc
    target_path = target_path.resolve()
    delta = _delta_table(spark, target_path)
    non_delete = source
    if operation_column and operation_column in source.columns:
        non_delete = source.where(
            functions.coalesce(
                functions.upper(functions.col(operation_column)), functions.lit("UPSERT")
            )
            != "DELETE"
        )
    if delta is None:
        non_delete.write.format("delta").mode("overwrite").save(str(target_path))
        return

    key_condition = " AND ".join(f"t.`{key}` <=> s.`{key}`" for key in business_keys)
    source_is_newer = (
        f"t.`{event_timestamp}` IS NULL OR s.`{event_timestamp}` > t.`{event_timestamp}` "
        f"OR (s.`{event_timestamp}` <=> t.`{event_timestamp}` AND "
        "NOT (s.`_silver_record_hash` <=> t.`_silver_record_hash`))"
    )
    builder = delta.alias("t").merge(source.alias("s"), key_condition)
    if operation_column and operation_column in source.columns:
        builder = (
            builder.whenMatchedDelete(
                condition=f"upper(s.`{operation_column}`) = 'DELETE' AND ({source_is_newer})"
            )
            .whenMatchedUpdateAll(
                condition=(
                    f"coalesce(upper(s.`{operation_column}`), 'UPSERT') <> 'DELETE' "
                    f"AND ({source_is_newer})"
                )
            )
            .whenNotMatchedInsertAll(
                condition=f"coalesce(upper(s.`{operation_column}`), 'UPSERT') <> 'DELETE'"
            )
        )
    else:
        builder = builder.whenMatchedUpdateAll(condition=source_is_newer).whenNotMatchedInsertAll()
    builder.execute()


def merge_insert_or_update(
    spark: Any,
    source: Any,
    target_path: Path,
    *,
    identifier: str,
) -> None:
    """Idempotently insert/update audit-style records by deterministic identifier."""

    target_path = target_path.resolve()
    delta = _delta_table(spark, target_path)
    if delta is None:
        source.write.format("delta").mode("overwrite").save(str(target_path))
        return
    delta.alias("t").merge(
        source.alias("s"), f"t.`{identifier}` = s.`{identifier}`"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


def synchronize_snapshot(
    spark: Any,
    source: Any,
    target_path: Path,
    *,
    identifiers: tuple[str, ...],
) -> None:
    """Synchronize a derived Delta snapshot using stable keys, including removals."""

    if not identifiers:
        raise ValueError("Snapshot synchronization requires at least one identifier.")
    target_path = target_path.resolve()
    delta = _delta_table(spark, target_path)
    if delta is None:
        source.write.format("delta").mode("overwrite").save(str(target_path))
        return
    condition = " AND ".join(f"t.`{name}` <=> s.`{name}`" for name in identifiers)
    (
        delta.alias("t")
        .merge(source.alias("s"), condition)
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .whenNotMatchedBySourceDelete()
        .execute()
    )
