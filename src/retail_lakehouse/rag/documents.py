"""Governed Markdown parsing and deterministic heading-aware chunking."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path
from typing import Any

import yaml

from retail_lakehouse.rag.guardrails import safe_evidence
from retail_lakehouse.rag.models import DocumentChunk, DocumentMetadata

_REQUIRED = {
    "document_id",
    "title",
    "version",
    "effective_date",
    "security_class",
    "source_uri",
}
_ALLOWED_SECURITY = {"public", "internal", "confidential"}


class DocumentError(ValueError):
    """Raised when governed document metadata or content is invalid."""


def parse_document(path: Path, *, max_words: int = 120) -> tuple[DocumentChunk, ...]:
    """Parse one Markdown document with YAML front matter into stable chunks."""

    raw = path.read_text(encoding="utf-8")
    if not raw.startswith("---\n") or "\n---\n" not in raw[4:]:
        raise DocumentError(f"{path.name} must start with YAML front matter.")
    front_matter, body = raw[4:].split("\n---\n", maxsplit=1)
    payload = yaml.safe_load(front_matter)
    if not isinstance(payload, dict):
        raise DocumentError(f"{path.name} front matter must be a mapping.")
    missing = sorted(_REQUIRED - payload.keys())
    if missing:
        raise DocumentError(f"{path.name} is missing metadata: {', '.join(missing)}")
    metadata = _metadata(payload)
    if metadata.security_class not in _ALLOWED_SECURITY:
        raise DocumentError(f"Unsupported security_class: {metadata.security_class}")
    return _chunk_sections(metadata, body, max_words=max_words)


def load_documents(directory: Path, *, max_words: int = 120) -> tuple[DocumentChunk, ...]:
    """Load governed Markdown documents in deterministic path order."""

    chunks: list[DocumentChunk] = []
    for path in sorted(directory.glob("*.md")):
        chunks.extend(parse_document(path, max_words=max_words))
    return tuple(chunks)


def _metadata(payload: dict[str, Any]) -> DocumentMetadata:
    values = {key: str(payload[key]).strip() for key in _REQUIRED}
    if not all(values.values()):
        raise DocumentError("Document metadata values cannot be empty.")
    return DocumentMetadata(**values)


def _chunk_sections(
    metadata: DocumentMetadata, body: str, *, max_words: int
) -> tuple[DocumentChunk, ...]:
    if max_words < 20:
        raise DocumentError("max_words must be at least 20.")
    sections: list[tuple[str, str]] = []
    heading = "Overview"
    lines: list[str] = []
    for line in body.splitlines():
        if match := re.match(r"^#{1,6}\s+(.+?)\s*$", line):
            if lines:
                sections.append((heading, "\n".join(lines).strip()))
            heading, lines = match.group(1), []
        else:
            lines.append(line)
    if lines:
        sections.append((heading, "\n".join(lines).strip()))

    chunks: list[DocumentChunk] = []
    for section, text in sections:
        words = text.split()
        for position in range(0, len(words), max_words):
            content = " ".join(words[position : position + max_words]).strip()
            if not safe_evidence(content):
                continue
            digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
            ordinal = position // max_words
            chunk_key = f"{metadata.document_id}|{metadata.version}|{section}|{ordinal}|{digest}"
            chunk_id = hashlib.sha256(chunk_key.encode("utf-8")).hexdigest()[:20]
            chunks.append(DocumentChunk(chunk_id, metadata, section, content, digest))
    if not chunks:
        raise DocumentError(f"Document {metadata.document_id} has no safe indexable content.")
    return tuple(chunks)
