from pathlib import Path

import pytest

from retail_lakehouse.quality import QualityConfigurationError, load_quality_rules
from retail_lakehouse.transformations import SILVER_SPECS


def test_project_quality_catalog_covers_every_silver_dataset() -> None:
    project_root = Path(__file__).resolve().parents[2]
    rules = load_quality_rules(project_root / "configs" / "data_quality_rules.yml")

    assert {rule.dataset_name for rule in rules} == set(SILVER_SPECS)
    assert len({rule.rule_id for rule in rules}) == len(rules)
    assert all(0 <= rule.threshold <= 1 for rule in rules)
    assert {rule.rule_id for rule in rules if rule.reference_dataset} == {"ORD-005", "RET-003"}


def test_quality_catalog_can_filter_one_dataset() -> None:
    project_root = Path(__file__).resolve().parents[2]

    rules = load_quality_rules(project_root / "configs" / "data_quality_rules.yml", "customers")

    assert {rule.rule_id for rule in rules} == {"CUS-001", "CUS-002"}


def test_invalid_action_is_rejected(tmp_path: Path) -> None:
    catalog = tmp_path / "rules.yml"
    catalog.write_text(
        """rules:
  - rule_id: BAD-001
    dataset_name: orders
    column_name: order_id
    rule_type: not_null
    rule_expression: order_id IS NOT NULL
    severity: ERROR
    threshold: 0
    action: silently_drop
    is_active: true
    description: Invalid fixture.
    owner: test
""",
        encoding="utf-8",
    )

    with pytest.raises(QualityConfigurationError, match="unsupported action"):
        load_quality_rules(catalog)
