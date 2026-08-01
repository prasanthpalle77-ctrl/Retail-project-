"""Typed contracts for quality rules and their execution results."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class QualityRule:
    """One executable row-level data quality expectation."""

    rule_id: str
    dataset_name: str
    column_name: str
    rule_type: str
    rule_expression: str
    severity: str
    threshold: float
    action: str
    is_active: bool
    description: str
    owner: str
    reference_dataset: str | None = None
    local_columns: tuple[str, ...] = ()
    reference_columns: tuple[str, ...] = ()
    reference_value_column: str | None = None


@dataclass(frozen=True)
class QualityMetric:
    """Auditable outcome for one rule in one pipeline run."""

    run_id: str
    pipeline_name: str
    dataset_name: str
    rule_id: str
    rule_name: str
    rule_status: str
    records_checked: int
    records_passed: int
    records_failed: int
    failure_percentage: float
    threshold: float
    severity: str
    action: str
    execution_timestamp: datetime
