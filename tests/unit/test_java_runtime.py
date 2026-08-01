import os
from pathlib import Path

from retail_lakehouse.runtime.java import find_java_home


def test_project_runtime_is_found_when_present(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("JAVA_HOME", raising=False)
    java_home = tmp_path / ".runtime" / "java17" / "jdk-test"
    java_binary = java_home / "bin" / ("java.exe" if os.name == "nt" else "java")
    java_binary.parent.mkdir(parents=True)
    java_binary.touch()

    assert find_java_home(tmp_path) == java_home.resolve()


def test_missing_runtime_returns_none(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.delenv("JAVA_HOME", raising=False)
    monkeypatch.setenv("PATH", "")

    assert find_java_home(tmp_path) is None
