import json
from pathlib import Path

import pytest

from retail_lakehouse.streaming import emit_json_microbatches


def test_simulator_emits_atomic_deterministic_chunks(tmp_path: Path) -> None:
    source = tmp_path / "events.jsonl"
    source.write_text(
        "".join(json.dumps({"event_id": index}) + "\n" for index in range(5)),
        encoding="utf-8",
    )
    output = tmp_path / "stream"

    first = emit_json_microbatches(source, output, records_per_file=2)
    second = emit_json_microbatches(source, output, records_per_file=2)

    assert first == second
    assert first.records_emitted == 5
    assert first.files_emitted == 3
    assert not list(output.glob("*.tmp"))


def test_simulator_rejects_non_object_json(tmp_path: Path) -> None:
    source = tmp_path / "events.jsonl"
    source.write_text("[1, 2, 3]\n", encoding="utf-8")

    with pytest.raises(ValueError, match="must contain an object"):
        emit_json_microbatches(source, tmp_path / "stream")
