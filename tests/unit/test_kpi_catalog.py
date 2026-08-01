from pathlib import Path

import pytest

from retail_lakehouse.analytics import KpiCatalogError, load_kpi_catalog


def test_project_catalog_contains_certified_retail_kpis() -> None:
    root = Path(__file__).resolve().parents[2]

    definitions = load_kpi_catalog(root / "configs" / "kpi_definitions.yml")

    assert {definition.name for definition in definitions} == {
        "average_order_value",
        "conversion_rate",
        "gross_sales",
        "net_revenue",
        "net_sales",
        "return_rate",
        "stockout_rate",
    }
    assert all(definition.certified_table for definition in definitions)


def test_catalog_version_is_required(tmp_path: Path) -> None:
    catalog = tmp_path / "kpis.yml"
    catalog.write_text("version: 2\nkpis: {}\n", encoding="utf-8")

    with pytest.raises(KpiCatalogError, match="version: 1"):
        load_kpi_catalog(catalog)
