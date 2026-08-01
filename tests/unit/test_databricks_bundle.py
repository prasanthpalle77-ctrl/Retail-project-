from pathlib import Path

import pytest
import yaml

from scripts.evaluate_rag import resolve_project_root
from scripts.validate_bundle import BundleValidationError, validate_bundle

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_rag_project_root_uses_explicit_serverless_path(tmp_path: Path) -> None:
    configured = tmp_path / "bundle"

    assert resolve_project_root(configured, None) == configured.resolve()


def test_rag_project_root_uses_script_path_locally() -> None:
    script_path = PROJECT_ROOT / "scripts" / "evaluate_rag.py"

    assert resolve_project_root(None, str(script_path)) == PROJECT_ROOT


def test_project_bundle_has_all_targets_jobs_and_valid_task_graphs() -> None:
    report = validate_bundle(PROJECT_ROOT)

    assert report == {
        "bundle": "novaretail",
        "targets": ["dev", "prod", "staging"],
        "resource_files": 1,
        "jobs": 5,
        "tasks": 12,
    }


def test_bundle_validator_rejects_missing_task_file(tmp_path: Path) -> None:
    (tmp_path / "resources").mkdir()
    (tmp_path / "databricks.yml").write_text(
        """bundle:
  name: novaretail
include: [resources/*.yml]
targets:
  dev: {mode: development}
  staging: {mode: production}
  prod: {mode: production}
""",
        encoding="utf-8",
    )
    jobs = yaml.safe_load((PROJECT_ROOT / "resources" / "lakehouse_jobs.yml").read_text())
    (tmp_path / "resources" / "jobs.yml").write_text(yaml.safe_dump(jobs), encoding="utf-8")

    with pytest.raises(BundleValidationError, match="missing file"):
        validate_bundle(tmp_path)
