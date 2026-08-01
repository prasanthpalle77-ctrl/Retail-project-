import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from retail_lakehouse.generation import (
    DatabricksScaleOptions,
    GenerationOptions,
    RetailDataGenerator,
    build_scale_statements,
)


def _file_hash(path: str) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _options(output_root: Path, include_invalid: bool = True) -> GenerationOptions:
    return GenerationOptions(
        output_root=output_root,
        seed=7,
        reference_time=datetime(2026, 1, 1, tzinfo=UTC),
        customer_count=5,
        product_count=5,
        store_count=2,
        supplier_count=2,
        order_count=6,
        include_invalid=include_invalid,
    )


def test_generation_is_deterministic(tmp_path: Path) -> None:
    first = RetailDataGenerator(_options(tmp_path / "first")).generate()
    second = RetailDataGenerator(_options(tmp_path / "second")).generate()

    assert first.record_counts == second.record_counts
    assert first.injected_issues == second.injected_issues
    assert {name: _file_hash(path) for name, path in first.files.items()} == {
        name: _file_hash(path) for name, path in second.files.items()
    }


def test_invalid_generation_documents_expected_defects(tmp_path: Path) -> None:
    report = RetailDataGenerator(_options(tmp_path)).generate()

    assert report.record_counts["customers"] == 6
    assert "orders: total mismatch" in report.injected_issues
    assert "customer_events: duplicate event ID" in report.injected_issues
    assert Path(report.output_directory, "generation_report.json").is_file()


def test_counts_must_be_positive(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="must be positive"):
        GenerationOptions(output_root=tmp_path, customer_count=0)


def test_databricks_scale_defaults_exceed_big_data_requirement() -> None:
    options = DatabricksScaleOptions()

    assert options.order_count == 5_000_000
    assert options.order_item_count == 10_000_000
    assert options.bronze_row_count == 26_601_000


def test_databricks_scale_sql_registers_layers_monitoring_and_kpis() -> None:
    options = DatabricksScaleOptions(
        catalog="retail_test",
        order_count=10,
        customer_count=5,
        product_count=4,
        store_count=2,
        inventory_event_count=3,
        batch_id="test_batch",
    )
    statements = dict(build_scale_statements(options))
    sql = "\n".join(statements.values())

    assert "`retail_test`.`bronze`.`orders`" in sql
    assert "`retail_test`.`silver`.`orders`" in sql
    assert "`retail_test`.`gold`.`fact_order_items`" in sql
    assert "`retail_test`.`gold`.`kpi_summary`" in sql
    assert "`retail_test`.`governance`.`data_arrival_status`" in sql
    assert "FROM range(20)" in statements["bronze:order_items"]


def test_databricks_scale_rejects_unsafe_catalog() -> None:
    with pytest.raises(ValueError, match="simple Unity Catalog identifier"):
        DatabricksScaleOptions(catalog="catalog; DROP TABLE x")
