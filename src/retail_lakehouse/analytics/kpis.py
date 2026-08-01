"""Load and validate the centrally governed KPI catalog."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class KpiDefinition:
    """Business definition and certified serving location for one KPI."""

    name: str
    display_name: str
    grain: tuple[str, ...]
    formula: str
    certified_table: str
    measure_column: str
    owner: str


class KpiCatalogError(ValueError):
    """Raised when KPI governance metadata is incomplete or inconsistent."""


def load_kpi_catalog(path: Path) -> tuple[KpiDefinition, ...]:
    """Load a versioned KPI YAML catalog and reject incomplete definitions."""

    payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("version") != 1:
        raise KpiCatalogError("KPI catalog must declare version: 1.")
    raw_kpis = payload.get("kpis")
    if not isinstance(raw_kpis, dict) or not raw_kpis:
        raise KpiCatalogError("KPI catalog must contain a non-empty 'kpis' mapping.")

    definitions = []
    required = {
        "display_name",
        "grain",
        "formula",
        "certified_table",
        "measure_column",
        "owner",
    }
    for name, raw in raw_kpis.items():
        if not isinstance(raw, dict):
            raise KpiCatalogError(f"KPI {name} must be a mapping.")
        missing = sorted(required - raw.keys())
        if missing:
            raise KpiCatalogError(f"KPI {name} is missing: {', '.join(missing)}")
        grain = raw["grain"]
        if not isinstance(grain, list) or not grain:
            raise KpiCatalogError(f"KPI {name} grain must be a non-empty list.")
        definition = _to_definition(str(name), raw, grain)
        if not all(
            (
                definition.name,
                definition.display_name,
                definition.formula,
                definition.certified_table,
                definition.measure_column,
                definition.owner,
            )
        ):
            raise KpiCatalogError(f"KPI {name} contains an empty required value.")
        definitions.append(definition)
    return tuple(definitions)


def _to_definition(name: str, raw: dict[str, Any], grain: list[Any]) -> KpiDefinition:
    return KpiDefinition(
        name=name.strip(),
        display_name=str(raw["display_name"]).strip(),
        grain=tuple(str(item).strip() for item in grain),
        formula=str(raw["formula"]).strip(),
        certified_table=str(raw["certified_table"]).strip(),
        measure_column=str(raw["measure_column"]).strip(),
        owner=str(raw["owner"]).strip(),
    )
