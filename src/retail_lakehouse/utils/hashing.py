"""Deterministic record canonicalization and hashing utilities."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any


def _normalize(value: Any) -> Any:
    if isinstance(value, datetime):
        timestamp = value if value.tzinfo else value.replace(tzinfo=UTC)
        return timestamp.astimezone(UTC).isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, Mapping):
        return {str(key): _normalize(value[key]) for key in sorted(value, key=str)}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_normalize(item) for item in value]
    return value


def canonical_record(record: Mapping[str, Any]) -> str:
    """Serialize a mapping consistently for comparisons and hash generation."""

    normalized = _normalize(record)
    return json.dumps(normalized, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def record_hash(record: Mapping[str, Any], algorithm: str = "sha256") -> str:
    """Return a stable hexadecimal digest of a record.

    SHA-256 is the platform default. The algorithm argument exists for migration
    and testing, not for accepting arbitrary user-controlled input.
    """

    try:
        digest = hashlib.new(algorithm)
    except ValueError as exc:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}") from exc
    digest.update(canonical_record(record).encode("utf-8"))
    return digest.hexdigest()


def sha256_text(value: str) -> str:
    """Return a stable SHA-256 identifier for an already-canonical string."""

    return hashlib.sha256(value.encode("utf-8")).hexdigest()
