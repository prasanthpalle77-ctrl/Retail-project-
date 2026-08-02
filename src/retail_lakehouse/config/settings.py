"""Load environment-aware, non-secret NovaRetail YAML configuration."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml

SUPPORTED_ENVIRONMENTS = frozenset({"prod"})


class SettingsError(ValueError):
    """Raised when project configuration is missing or invalid."""


def find_project_root(start: Path | None = None) -> Path:
    """Return the closest parent containing ``pyproject.toml``.

    ``NOVARETAIL_PROJECT_ROOT`` takes precedence so deployed entry points can
    locate configuration without depending on the current working directory.
    """

    configured_root = os.getenv("NOVARETAIL_PROJECT_ROOT")
    if configured_root:
        root = Path(configured_root).expanduser().resolve()
        if not (root / "pyproject.toml").is_file():
            raise SettingsError(f"Invalid NOVARETAIL_PROJECT_ROOT: {root}")
        return root

    candidate = (start or Path.cwd()).resolve()
    for parent in (candidate, *candidate.parents):
        if (parent / "pyproject.toml").is_file():
            return parent
    raise SettingsError("Could not locate project root containing pyproject.toml")


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.is_file():
        raise SettingsError(f"Configuration file does not exist: {path}")
    with path.open(encoding="utf-8") as stream:
        payload = yaml.safe_load(stream) or {}
    if not isinstance(payload, dict):
        raise SettingsError(f"Configuration root must be a mapping: {path}")
    return payload


def load_settings(
    environment: str | None = None, project_root: Path | None = None
) -> dict[str, Any]:
    """Load the production configuration.

    Secrets are deliberately excluded. Callers obtain secrets from environment
    variables locally or a managed secret provider in Databricks.
    """

    selected_value = environment or os.getenv("NOVARETAIL_ENV") or "prod"
    selected = selected_value.lower()
    if selected not in SUPPORTED_ENVIRONMENTS:
        allowed = ", ".join(sorted(SUPPORTED_ENVIRONMENTS))
        raise SettingsError(f"Unsupported environment '{selected}'. Choose one of: {allowed}")

    root = project_root.resolve() if project_root else find_project_root()
    settings = _load_yaml(root / "configs" / "prod.yml")
    settings.setdefault("application", {})["environment"] = selected
    settings["project_root"] = str(root)
    return settings
