"""Offline structural validation for the NovaRetail Databricks bundle."""

from __future__ import annotations

import glob
from collections.abc import Mapping
from pathlib import Path
from typing import Any, cast

import yaml


class BundleValidationError(ValueError):
    """Raised when a local bundle invariant is violated."""


PROJECT_ROOT_ENTRYPOINTS = {
    "scripts/ask_databricks_rag.py",
    "scripts/evaluate_rag.py",
    "scripts/run_bronze_batch.py",
    "scripts/run_gold.py",
    "scripts/run_silver.py",
    "scripts/run_stream.py",
}


def _mapping(value: Any, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise BundleValidationError(f"{label} must be a mapping.")
    return cast(dict[str, Any], value)


def _load_yaml(path: Path) -> dict[str, Any]:
    return _mapping(yaml.safe_load(path.read_text(encoding="utf-8")), str(path))


def _resource_files(root: Path, bundle: Mapping[str, Any]) -> tuple[Path, ...]:
    paths: list[Path] = []
    for pattern in bundle.get("include", []):
        paths.extend(Path(item) for item in glob.glob(str(root / str(pattern))))
    if not paths:
        raise BundleValidationError("Bundle include patterns resolved to no resource files.")
    return tuple(sorted(paths))


def _validate_task_files(root: Path, jobs: Mapping[str, Any]) -> int:
    task_count = 0
    prefix = "${workspace.file_path}/"
    for job_key, raw_job in jobs.items():
        job = _mapping(raw_job, f"job {job_key}")
        tasks = job.get("tasks")
        if not isinstance(tasks, list) or not tasks:
            raise BundleValidationError(f"Job {job_key} must contain tasks.")
        keys = {str(_mapping(task, "task").get("task_key")) for task in tasks}
        dependencies: dict[str, set[str]] = {}
        for raw_task in tasks:
            task = _mapping(raw_task, f"task in {job_key}")
            key = str(task.get("task_key", ""))
            python_task = _mapping(task.get("spark_python_task"), f"task {key}")
            python_file = str(python_task.get("python_file", ""))
            if not python_file.startswith(prefix):
                raise BundleValidationError(f"Task {key} must use a synced workspace Python file.")
            local_python_file = root / python_file.removeprefix(prefix)
            if not local_python_file.is_file():
                raise BundleValidationError(f"Task {key} references missing file: {python_file}")
            if "raise SystemExit(main())" in local_python_file.read_text(encoding="utf-8"):
                raise BundleValidationError(
                    f"Task {key} entrypoint must return normally on serverless compute."
                )
            relative_python_file = python_file.removeprefix(prefix).replace("\\", "/")
            if relative_python_file in PROJECT_ROOT_ENTRYPOINTS:
                parameters = python_task.get("parameters", [])
                if not isinstance(parameters, list) or not all(
                    value in parameters for value in ("--project-root", "${workspace.file_path}")
                ):
                    raise BundleValidationError(
                        f"Task {key} must receive the synced bundle path as --project-root."
                    )
                if relative_python_file == "scripts/run_stream.py" and not all(
                    value in parameters
                    for value in (
                        "--rules",
                        "${workspace.file_path}/configs/data_quality_rules.yml",
                    )
                ):
                    raise BundleValidationError(
                        f"Streaming task {key} must receive the synced quality rules path."
                    )
            if task.get("environment_key") != "default":
                raise BundleValidationError(f"Task {key} must select the serverless environment.")
            dependency_keys = {
                str(_mapping(item, f"dependency for {key}").get("task_key"))
                for item in task.get("depends_on", [])
            }
            unknown = dependency_keys - keys
            if unknown:
                raise BundleValidationError(
                    f"Task {key} has unknown dependencies: {sorted(unknown)}"
                )
            dependencies[key] = dependency_keys
            task_count += 1
        _assert_acyclic(job_key, dependencies)
        environments = job.get("environments", [])
        defaults = [
            item
            for item in environments
            if isinstance(item, dict) and item.get("environment_key") == "default"
        ]
        if not defaults:
            raise BundleValidationError(f"Job {job_key} needs the default environment.")
        default_spec = _mapping(defaults[0].get("spec"), f"default environment in {job_key}")
        if "../dist/*.whl" not in default_spec.get("dependencies", []):
            raise BundleValidationError(f"Job {job_key} must install the built deployment wheel.")
    return task_count


def _assert_acyclic(job_key: str, dependencies: Mapping[str, set[str]]) -> None:
    remaining = {key: set(value) for key, value in dependencies.items()}
    while remaining:
        ready = {key for key, value in remaining.items() if not value}
        if not ready:
            raise BundleValidationError(f"Job {job_key} contains a task dependency cycle.")
        remaining = {key: value - ready for key, value in remaining.items() if key not in ready}


def _validate_serverless_compatibility(root: Path) -> None:
    unsupported = (".cache(", ".persist(", "input_file_name(")
    for path in (root / "src" / "retail_lakehouse").rglob("*.py"):
        source = path.read_text(encoding="utf-8")
        operation = next((item for item in unsupported if item in source), None)
        if operation is not None:
            relative = path.relative_to(root)
            raise BundleValidationError(
                f"Serverless deployment source {relative} uses unsupported {operation[:-1]}."
            )


def validate_bundle(root: Path) -> dict[str, Any]:
    _validate_serverless_compatibility(root)
    bundle = _load_yaml(root / "databricks.yml")
    bundle_metadata = _mapping(bundle.get("bundle"), "bundle")
    if bundle_metadata.get("name") != "novaretail":
        raise BundleValidationError("Bundle name must be novaretail.")
    targets = _mapping(bundle.get("targets"), "targets")
    if set(targets) != {"dev", "staging", "prod"}:
        raise BundleValidationError("Bundle targets must be dev, staging, and prod.")
    if _mapping(targets["dev"], "dev target").get("mode") != "development":
        raise BundleValidationError("Dev target must use development mode.")
    for target in ("staging", "prod"):
        if _mapping(targets[target], f"{target} target").get("mode") != "production":
            raise BundleValidationError(f"{target} target must use production mode.")

    jobs: dict[str, Any] = {}
    resource_files = _resource_files(root, bundle)
    for path in resource_files:
        resources = _mapping(_load_yaml(path).get("resources"), f"resources in {path}")
        for key, value in _mapping(resources.get("jobs", {}), f"jobs in {path}").items():
            if key in jobs:
                raise BundleValidationError(f"Duplicate job key: {key}")
            jobs[key] = value
    required_jobs = {
        "rag_copilot_query",
        "rag_evaluation",
        "retail_batch_pipeline",
        "retail_big_data_load",
        "retail_streaming_pipeline",
    }
    if set(jobs) != required_jobs:
        raise BundleValidationError(f"Expected jobs: {sorted(required_jobs)}")
    task_count = _validate_task_files(root, jobs)
    return {
        "bundle": bundle_metadata["name"],
        "targets": sorted(targets),
        "resource_files": len(resource_files),
        "jobs": len(jobs),
        "tasks": task_count,
    }


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    report = validate_bundle(root)
    print(
        f"Bundle {report['bundle']} validated: {report['jobs']} jobs, "
        f"{report['tasks']} tasks, targets={','.join(report['targets'])}."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
