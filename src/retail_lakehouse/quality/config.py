"""Load and validate the YAML data quality catalog."""

from __future__ import annotations

from dataclasses import MISSING
from pathlib import Path
from typing import Any

import yaml

from retail_lakehouse.quality.models import QualityRule

SUPPORTED_RULE_TYPES = {
    "accepted_values",
    "consistency",
    "not_null",
    "range",
    "reference_comparison",
    "reference_exists",
    "regex",
}
SUPPORTED_SEVERITIES = {"INFO", "WARNING", "ERROR", "CRITICAL"}
SUPPORTED_ACTIONS = {"observe", "quarantine", "fail_pipeline"}


class QualityConfigurationError(ValueError):
    """Raised when a quality catalog is unsafe or incomplete."""


def load_quality_rules(path: Path, dataset_name: str | None = None) -> tuple[QualityRule, ...]:
    """Return validated active rules, optionally filtered to one dataset."""

    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or not isinstance(payload.get("rules"), list):
        raise QualityConfigurationError("Quality configuration must contain a 'rules' list.")

    required = {
        name
        for name, field in QualityRule.__dataclass_fields__.items()
        if field.default is MISSING and field.default_factory is MISSING
    }
    rules: list[QualityRule] = []
    identifiers: set[str] = set()
    for index, raw in enumerate(payload["rules"]):
        if not isinstance(raw, dict):
            raise QualityConfigurationError(f"Rule at index {index} must be a mapping.")
        missing = sorted(required - raw.keys())
        if missing:
            raise QualityConfigurationError(
                f"Rule at index {index} is missing: {', '.join(missing)}"
            )
        rule = _to_rule(raw)
        _validate_rule(rule, identifiers)
        identifiers.add(rule.rule_id)
        if rule.is_active and (dataset_name is None or rule.dataset_name == dataset_name):
            rules.append(rule)
    return tuple(rules)


def _to_rule(raw: dict[str, Any]) -> QualityRule:
    return QualityRule(
        rule_id=str(raw["rule_id"]).strip(),
        dataset_name=str(raw["dataset_name"]).strip(),
        column_name=str(raw["column_name"]).strip(),
        rule_type=str(raw["rule_type"]).strip().lower(),
        rule_expression=str(raw["rule_expression"]).strip(),
        severity=str(raw["severity"]).strip().upper(),
        threshold=float(raw["threshold"]),
        action=str(raw["action"]).strip().lower(),
        is_active=bool(raw["is_active"]),
        description=str(raw["description"]).strip(),
        owner=str(raw["owner"]).strip(),
        reference_dataset=_optional_text(raw.get("reference_dataset")),
        local_columns=tuple(str(item) for item in raw.get("local_columns", ())),
        reference_columns=tuple(str(item) for item in raw.get("reference_columns", ())),
        reference_value_column=_optional_text(raw.get("reference_value_column")),
    )


def _optional_text(value: Any) -> str | None:
    return str(value).strip() if value is not None else None


def _validate_rule(rule: QualityRule, identifiers: set[str]) -> None:
    if not rule.rule_id or rule.rule_id in identifiers:
        raise QualityConfigurationError(f"Rule ID is empty or duplicated: {rule.rule_id!r}")
    if not rule.dataset_name or not rule.column_name or not rule.rule_expression:
        raise QualityConfigurationError(f"Rule {rule.rule_id} has an empty required value.")
    if rule.rule_type not in SUPPORTED_RULE_TYPES:
        raise QualityConfigurationError(
            f"Rule {rule.rule_id} has unsupported type: {rule.rule_type}"
        )
    if rule.severity not in SUPPORTED_SEVERITIES:
        raise QualityConfigurationError(
            f"Rule {rule.rule_id} has unsupported severity: {rule.severity}"
        )
    if rule.action not in SUPPORTED_ACTIONS:
        raise QualityConfigurationError(
            f"Rule {rule.rule_id} has unsupported action: {rule.action}"
        )
    if not 0 <= rule.threshold <= 1:
        raise QualityConfigurationError(f"Rule {rule.rule_id} threshold must be from 0 to 1.")
    if rule.rule_type.startswith("reference_"):
        if not rule.reference_dataset:
            raise QualityConfigurationError(f"Rule {rule.rule_id} requires reference_dataset.")
        if not rule.local_columns or len(rule.local_columns) != len(rule.reference_columns):
            raise QualityConfigurationError(
                f"Rule {rule.rule_id} requires equally sized local/reference columns."
            )
    if rule.rule_type == "reference_comparison" and not rule.reference_value_column:
        raise QualityConfigurationError(f"Rule {rule.rule_id} requires reference_value_column.")
