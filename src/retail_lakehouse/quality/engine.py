"""Spark execution engine for row-level quality rules."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from retail_lakehouse.quality.models import QualityMetric, QualityRule


class DataQualityError(RuntimeError):
    """Raised when a configured fail-pipeline tolerance is exceeded."""


@dataclass(frozen=True)
class QualityEvaluation:
    """Accepted/quarantined frames plus rule-level metrics."""

    accepted: Any
    quarantined: Any
    metrics: tuple[QualityMetric, ...]


def evaluate_quality(
    frame: Any,
    rules: tuple[QualityRule, ...],
    *,
    run_id: str,
    pipeline_name: str,
    dataset_name: str,
    execution_timestamp: datetime | None = None,
) -> QualityEvaluation:
    """Evaluate every expression once and separate passing and failing records."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for quality evaluation.") from exc

    applicable = tuple(
        rule for rule in rules if rule.dataset_name == dataset_name and rule.is_active
    )
    if not applicable:
        accepted = frame.withColumn("_failed_rule_ids", functions.array().cast("array<string>"))
        return QualityEvaluation(accepted=accepted, quarantined=accepted.limit(0), metrics=())

    pass_columns = []
    failed_ids = []
    for rule in applicable:
        pass_column = f"_rule_{rule.rule_id.replace('-', '_')}_passed"
        passed = functions.coalesce(
            functions.expr(rule.rule_expression).cast("boolean"), functions.lit(False)
        )
        frame = frame.withColumn(pass_column, passed)
        pass_columns.append((rule, pass_column))
        failed_ids.append(functions.when(~functions.col(pass_column), functions.lit(rule.rule_id)))

    evaluated = frame.withColumn(
        "_failed_rule_ids",
        functions.filter(functions.array(*failed_ids), lambda item: item.isNotNull()),
    )
    aggregate_expressions = [functions.count(functions.lit(1)).alias("records_checked")]
    aggregate_expressions.extend(
        functions.sum(functions.when(~functions.col(name), 1).otherwise(0)).alias(name)
        for _, name in pass_columns
    )
    aggregate = evaluated.agg(*aggregate_expressions).collect()[0].asDict()
    checked = int(aggregate["records_checked"])
    executed_at = execution_timestamp or datetime.now(UTC)
    metrics = []
    blocking = []
    for rule, column_name in pass_columns:
        failed = int(aggregate[column_name] or 0)
        failure_rate = failed / checked if checked else 0.0
        metric = QualityMetric(
            run_id=run_id,
            pipeline_name=pipeline_name,
            dataset_name=dataset_name,
            rule_id=rule.rule_id,
            rule_name=rule.description,
            rule_status="PASS" if failure_rate <= rule.threshold else "FAIL",
            records_checked=checked,
            records_passed=checked - failed,
            records_failed=failed,
            failure_percentage=round(failure_rate * 100, 6),
            threshold=rule.threshold,
            severity=rule.severity,
            action=rule.action,
            execution_timestamp=executed_at,
        )
        metrics.append(metric)
        if metric.rule_status == "FAIL" and rule.action == "fail_pipeline":
            blocking.append(rule.rule_id)

    accepted = evaluated.where(functions.size("_failed_rule_ids") == 0).drop(
        *[name for _, name in pass_columns]
    )
    quarantined = (
        evaluated.where(functions.size("_failed_rule_ids") > 0)
        .withColumn("_quality_status", functions.lit("QUARANTINED"))
        .withColumn("_quality_run_id", functions.lit(run_id))
        .withColumn("_quality_checked_at", functions.lit(executed_at))
        .drop(*[name for _, name in pass_columns])
    )
    if blocking:
        raise DataQualityError(
            f"Pipeline blocked by quality rules for {dataset_name}: {', '.join(blocking)}"
        )
    return QualityEvaluation(accepted=accepted, quarantined=quarantined, metrics=tuple(metrics))
