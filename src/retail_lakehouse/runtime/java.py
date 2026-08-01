"""Discover and configure Java without requiring a system-wide installation."""

from __future__ import annotations

import os
import shutil
from pathlib import Path


def _java_executable(java_home: Path) -> Path:
    return java_home / "bin" / ("java.exe" if os.name == "nt" else "java")


def find_java_home(project_root: Path) -> Path | None:
    """Return a valid environment or project-local Java home."""

    if configured := os.getenv("JAVA_HOME"):
        candidate = Path(configured).expanduser().resolve()
        if _java_executable(candidate).is_file():
            return candidate

    runtime_root = project_root / ".runtime" / "java17"
    if runtime_root.is_dir():
        for candidate in sorted(path for path in runtime_root.iterdir() if path.is_dir()):
            if _java_executable(candidate).is_file():
                return candidate.resolve()

    if system_java := shutil.which("java"):
        resolved_java = Path(system_java).resolve()
        if resolved_java.is_file():
            return resolved_java.parent.parent
    return None


def configure_java(project_root: Path) -> Path:
    """Set process-local Java variables and return the selected Java home."""

    java_home = find_java_home(project_root)
    if java_home is None:
        raise RuntimeError(
            "Java 17 was not found. Set JAVA_HOME or install the project-local Temurin runtime."
        )
    os.environ["JAVA_HOME"] = str(java_home)
    os.environ["PATH"] = str(_java_executable(java_home).parent) + os.pathsep + os.environ["PATH"]
    return java_home
