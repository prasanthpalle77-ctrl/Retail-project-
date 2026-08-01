"""Deterministically split JSON Lines fixtures into atomic stream files."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class StreamSimulationResult:
    """Files and records emitted by one simulated stream run."""

    records_emitted: int
    files_emitted: int
    output_files: tuple[str, ...]


def emit_json_microbatches(
    source_path: Path,
    output_directory: Path,
    *,
    records_per_file: int = 10,
    interval_seconds: float = 0,
) -> StreamSimulationResult:
    """Validate JSONL records and atomically emit deterministic micro-batch files."""

    if records_per_file < 1:
        raise ValueError("records_per_file must be positive.")
    if not 0 <= interval_seconds <= 60:
        raise ValueError("interval_seconds must be between 0 and 60.")
    if not source_path.is_file():
        raise FileNotFoundError(f"Stream source does not exist: {source_path}")
    lines = [line.strip() for line in source_path.read_text(encoding="utf-8").splitlines()]
    lines = [line for line in lines if line]
    if not lines:
        raise ValueError("Stream source contains no JSON records.")
    for line_number, line in enumerate(lines, start=1):
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ValueError(f"Invalid JSON on line {line_number}.") from exc
        if not isinstance(value, dict):
            raise ValueError(f"JSON line {line_number} must contain an object.")

    output_directory.mkdir(parents=True, exist_ok=True)
    output_files = []
    chunks = [
        lines[index : index + records_per_file] for index in range(0, len(lines), records_per_file)
    ]
    for index, chunk in enumerate(chunks, start=1):
        target = output_directory / f"microbatch-{index:05d}.json"
        content = "\n".join(chunk) + "\n"
        if target.exists():
            if target.read_text(encoding="utf-8") != content:
                raise FileExistsError(f"Refusing to replace different stream data: {target}")
        else:
            temporary = output_directory / f".microbatch-{index:05d}.tmp"
            temporary.write_text(content, encoding="utf-8", newline="\n")
            temporary.replace(target)
        output_files.append(str(target))
        if interval_seconds and index < len(chunks):
            time.sleep(interval_seconds)
    return StreamSimulationResult(
        records_emitted=len(lines),
        files_emitted=len(output_files),
        output_files=tuple(output_files),
    )
