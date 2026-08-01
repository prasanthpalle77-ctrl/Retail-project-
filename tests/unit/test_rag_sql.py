from pathlib import Path

import pytest

from retail_lakehouse.rag.sql import (
    ApprovedSqlCatalog,
    SqlSafetyError,
    validate_read_only_sql,
)


def test_catalog_routes_metric_intents_and_executes_only_approved_sql() -> None:
    root = Path(__file__).resolve().parents[2]
    catalog = ApprovedSqlCatalog.load(root / "configs" / "rag_sql_templates.yml")
    template = catalog.match("Show me the latest conversion rate")
    captured: list[str] = []

    assert template is not None
    rows = catalog.execute(
        template, lambda sql: captured.append(sql) or [{"conversion_rate": 0.25}]
    )

    assert template.template_id == "conversion_rate"
    assert rows == ({"conversion_rate": 0.25},)
    assert captured == [template.sql]


@pytest.mark.parametrize(
    "sql, message",
    [
        ("DELETE FROM retail_kpi_daily LIMIT 1", "Only SELECT"),
        ("SELECT * FROM secrets LIMIT 1", "Unapproved tables"),
        ("SELECT * FROM retail_kpi_daily", "include a LIMIT"),
        ("SELECT * FROM retail_kpi_daily LIMIT 101", "cannot exceed"),
        ("SELECT * FROM retail_kpi_daily LIMIT 1; DROP TABLE x", "Multiple"),
        (
            "SELECT * FROM retail_kpi_daily UNION SELECT * FROM retail_kpi_daily LIMIT 1",
            "set operations",
        ),
        ("SELECT * FROM retail_kpi_daily -- bypass\nLIMIT 1", "comments"),
    ],
)
def test_sql_guard_rejects_unsafe_queries(sql: str, message: str) -> None:
    with pytest.raises(SqlSafetyError, match=message):
        validate_read_only_sql(sql, allowed_tables=frozenset({"retail_kpi_daily"}))


def test_policy_return_question_does_not_route_to_return_rate_sql() -> None:
    root = Path(__file__).resolve().parents[2]
    catalog = ApprovedSqlCatalog.load(root / "configs" / "rag_sql_templates.yml")

    assert catalog.match("What is the customer return policy?") is None
