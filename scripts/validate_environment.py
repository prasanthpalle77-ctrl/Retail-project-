"""Validate foundation prerequisites without importing optional Spark packages."""

from __future__ import annotations

import platform
import shutil
import subprocess  # nosec B404
import sys
from pathlib import Path

from retail_lakehouse.runtime import find_java_home


def command_version(command: str, args: list[str]) -> str | None:
    """Return the first version line or None when a command is unavailable."""

    executable = shutil.which(command)
    if not executable:
        return None
    # The executable is resolved with shutil.which and no shell is involved.
    result = subprocess.run(  # nosec B603
        [executable, *args], capture_output=True, check=False, text=True
    )
    output = (result.stdout or result.stderr).strip()
    return output.splitlines()[0] if output else None


def main() -> int:
    """Print prerequisite status and return nonzero only for foundation blockers."""

    root = Path(__file__).resolve().parents[1]
    required_files = [
        root / "pyproject.toml",
        root / "configs" / "dev.yml",
        root / "configs" / "source_config.yml",
        root / "configs" / "data_quality_rules.yml",
        root / "configs" / "kpi_definitions.yml",
    ]
    missing = [str(path.relative_to(root)) for path in required_files if not path.is_file()]

    print(f"Project root: {root}")
    print(f"Python: {platform.python_version()} ({sys.executable})")
    print(f"Git: {command_version('git', ['--version']) or 'NOT FOUND'}")
    java_home = find_java_home(root)
    java_version = command_version("java", ["-version"])
    if java_home and not java_version:
        java_command = (
            java_home / "bin" / ("java.exe" if platform.system() == "Windows" else "java")
        )
        # The executable path is validated by configure_java and no shell is involved.
        result = subprocess.run(  # nosec B603
            [str(java_command), "-version"], capture_output=True, check=False, text=True
        )
        java_version = (result.stdout or result.stderr).splitlines()[0]
    print(f"Java: {java_version or 'NOT FOUND - required for Spark'}")
    print(f"Java home: {java_home or 'NOT CONFIGURED'}")

    supported_python = (3, 11) <= sys.version_info[:2] < (3, 13)
    print(f"Supported Python: {'YES' if supported_python else 'NO'}")
    print(f"Required configuration: {'OK' if not missing else 'MISSING: ' + ', '.join(missing)}")

    if missing or not supported_python:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
