from datetime import UTC, datetime
from pathlib import Path

import pytest

from retail_lakehouse.ingestion.landing import (
    FileRegistry,
    LandingError,
    LandingStatus,
    sha256_file,
    stage_file,
)
from scripts.stage_landing import resolve_reported_source


def test_reported_cloud_source_is_constrained_to_batch_directory(tmp_path: Path) -> None:
    batch = tmp_path / "batch"
    batch.mkdir()
    source = batch / "orders.jsonl"
    source.write_text("{}\n", encoding="utf-8")

    resolved = resolve_reported_source(batch, r"C:\developer\data\batch\orders.jsonl")

    assert resolved == source


def _stage(source: Path, landing: Path, registry: FileRegistry, batch_id: str = "B1"):
    return stage_file(
        source,
        landing,
        registry,
        source_name="orders",
        source_system="TEST",
        batch_id=batch_id,
        run_id="RUN-1",
        ingested_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


def test_same_file_is_staged_exactly_once(tmp_path: Path) -> None:
    source = tmp_path / "source" / "orders.jsonl"
    source.parent.mkdir()
    source.write_text('{"order_id":"O1"}\n', encoding="utf-8")
    landing = tmp_path / "landing"
    registry = FileRegistry(landing / "_control" / "processed_files.json")

    first = _stage(source, landing, registry)
    second = _stage(source, landing, registry)

    assert first.status is LandingStatus.STAGED
    assert second.status is LandingStatus.ALREADY_STAGED
    assert registry.count() == 1
    assert sha256_file(Path(first.manifest.landing_file_path)) == sha256_file(source)
    assert Path(first.manifest_path).is_file()


def test_conflicting_content_cannot_overwrite_landing_target(tmp_path: Path) -> None:
    source = tmp_path / "orders.jsonl"
    source.write_text('{"order_id":"O1"}\n', encoding="utf-8")
    landing = tmp_path / "landing"
    registry = FileRegistry(landing / "_control" / "processed_files.json")
    _stage(source, landing, registry)

    source.write_text('{"order_id":"O2"}\n', encoding="utf-8")
    with pytest.raises(LandingError, match="different content"):
        _stage(source, landing, registry)


def test_empty_file_is_rejected(tmp_path: Path) -> None:
    source = tmp_path / "empty.csv"
    source.touch()
    registry = FileRegistry(tmp_path / "registry.json")

    with pytest.raises(LandingError, match="empty"):
        _stage(source, tmp_path / "landing", registry)
