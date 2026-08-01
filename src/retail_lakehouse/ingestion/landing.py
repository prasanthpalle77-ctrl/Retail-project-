"""Immutable local Landing delivery with checksums, manifests, and replay protection."""

from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, cast

SUPPORTED_SUFFIXES = frozenset({".csv", ".json", ".jsonl", ".md", ".txt"})


class LandingError(ValueError):
    """Raised when a source file cannot be staged safely."""


class LandingStatus(StrEnum):
    """Result of an immutable Landing operation."""

    STAGED = "STAGED"
    ALREADY_STAGED = "ALREADY_STAGED"


@dataclass(frozen=True)
class LandingManifest:
    """File-level evidence captured when a source file reaches Landing."""

    file_identity: str
    source_name: str
    source_system: str
    source_file_name: str
    source_file_path: str
    landing_file_path: str
    file_size: int
    file_modified_at: str
    checksum_sha256: str
    ingestion_timestamp: str
    pipeline_run_id: str
    batch_id: str
    status: str = "RECEIVED"


@dataclass(frozen=True)
class LandingResult:
    """Result returned to orchestration for audit and downstream processing."""

    status: LandingStatus
    manifest: LandingManifest
    manifest_path: str


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """Calculate a file digest without loading the entire file into memory."""

    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with NamedTemporaryFile(
        "w", encoding="utf-8", newline="\n", dir=path.parent, delete=False, suffix=".tmp"
    ) as stream:
        json.dump(payload, stream, indent=2, sort_keys=True)
        stream.write("\n")
        temporary = Path(stream.name)
    temporary.replace(path)


class FileRegistry:
    """Atomic JSON registry used by local mode to identify delivered files."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def get(self, file_identity: str) -> LandingManifest | None:
        entry = self._load()["files"].get(file_identity)
        return LandingManifest(**entry) if entry else None

    def register(self, manifest: LandingManifest) -> None:
        payload = self._load()
        existing = payload["files"].get(manifest.file_identity)
        serialized = asdict(manifest)
        if existing and existing != serialized:
            raise LandingError(f"File identity collision: {manifest.file_identity}")
        payload["files"][manifest.file_identity] = serialized
        _atomic_json_write(self.path, payload)

    def count(self) -> int:
        return len(self._load()["files"])

    def _load(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"schema_version": 1, "files": {}}
        payload = cast(dict[str, Any], json.loads(self.path.read_text(encoding="utf-8")))
        if payload.get("schema_version") != 1 or not isinstance(payload.get("files"), dict):
            raise LandingError(f"Unsupported or corrupt registry: {self.path}")
        return payload


def _validate_source_file(path: Path) -> None:
    if not path.is_file():
        raise LandingError(f"Source file does not exist: {path}")
    if path.stat().st_size == 0:
        raise LandingError(f"Source file is empty: {path}")
    if path.suffix.lower() not in SUPPORTED_SUFFIXES:
        allowed = ", ".join(sorted(SUPPORTED_SUFFIXES))
        raise LandingError(f"Unsupported source format '{path.suffix}'. Allowed: {allowed}")


def stage_file(
    source_path: Path,
    landing_root: Path,
    registry: FileRegistry,
    *,
    source_name: str,
    source_system: str,
    batch_id: str,
    run_id: str,
    ingested_at: datetime | None = None,
) -> LandingResult:
    """Copy a file exactly once into immutable Landing and write its manifest."""

    source_path = source_path.resolve()
    _validate_source_file(source_path)
    checksum = sha256_file(source_path)
    file_identity = f"{source_name}:{checksum}"
    manifest_path = (
        landing_root / "_manifests" / source_name / batch_id / (source_path.name + ".manifest.json")
    )

    if existing := registry.get(file_identity):
        return LandingResult(
            status=LandingStatus.ALREADY_STAGED,
            manifest=existing,
            manifest_path=str(manifest_path),
        )

    landing_path = landing_root / source_name / f"batch_id={batch_id}" / source_path.name
    landing_path.parent.mkdir(parents=True, exist_ok=True)
    if landing_path.exists():
        existing_checksum = sha256_file(landing_path)
        if existing_checksum != checksum:
            raise LandingError(
                f"Landing target already exists with different content: {landing_path}"
            )
    else:
        temporary = landing_path.with_suffix(landing_path.suffix + ".tmp")
        shutil.copy2(source_path, temporary)
        temporary.replace(landing_path)

    timestamp = (ingested_at or datetime.now(UTC)).astimezone(UTC)
    manifest = LandingManifest(
        file_identity=file_identity,
        source_name=source_name,
        source_system=source_system,
        source_file_name=source_path.name,
        source_file_path=str(source_path),
        landing_file_path=landing_path.as_posix(),
        file_size=source_path.stat().st_size,
        file_modified_at=datetime.fromtimestamp(source_path.stat().st_mtime, UTC).isoformat(),
        checksum_sha256=checksum,
        ingestion_timestamp=timestamp.isoformat(),
        pipeline_run_id=run_id,
        batch_id=batch_id,
    )
    _atomic_json_write(manifest_path, asdict(manifest))
    registry.register(manifest)
    return LandingResult(
        status=LandingStatus.STAGED,
        manifest=manifest,
        manifest_path=str(manifest_path),
    )
