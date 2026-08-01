"""Stable contracts shared by retrieval, analytics, API, and evaluation."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True)
class DocumentMetadata:
    document_id: str
    title: str
    version: str
    effective_date: str
    security_class: str
    source_uri: str


@dataclass(frozen=True)
class DocumentChunk:
    chunk_id: str
    document: DocumentMetadata
    section: str
    text: str
    content_hash: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class SearchResult:
    chunk: DocumentChunk
    score: float


@dataclass(frozen=True)
class Citation:
    citation_id: str
    title: str
    section: str
    source_uri: str
    evidence: str


@dataclass(frozen=True)
class CopilotResponse:
    answer: str
    route: str
    citations: tuple[Citation, ...] = ()
    sql: str | None = None
    rows: tuple[dict[str, Any], ...] = ()
    refused: bool = False
    refusal_reason: str | None = None
    audit_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
