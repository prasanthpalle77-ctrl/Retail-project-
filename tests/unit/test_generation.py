import hashlib
from datetime import UTC, datetime
from pathlib import Path

import pytest

from retail_lakehouse.generation import GenerationOptions, RetailDataGenerator


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
