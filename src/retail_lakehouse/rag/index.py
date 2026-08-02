"""Deterministic BM25 index for governed document retrieval."""

from __future__ import annotations

import json
import math
import re
from collections import Counter
from dataclasses import fields
from pathlib import Path

from retail_lakehouse.rag.models import (
    DocumentChunk,
    DocumentMetadata,
    SearchResult,
)

_TOKEN = re.compile(r"[a-z0-9]+")
_STOP_WORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "at",
        "be",
        "can",
        "do",
        "does",
        "for",
        "how",
        "i",
        "in",
        "is",
        "it",
        "me",
        "of",
        "on",
        "or",
        "should",
        "the",
        "to",
        "what",
        "when",
        "with",
    }
)


def tokenize(text: str) -> tuple[str, ...]:
    return tuple(token for token in _TOKEN.findall(text.lower()) if token not in _STOP_WORDS)


class LexicalIndex:
    """Persistable lexical index with metadata-based authorization filtering."""

    def __init__(self, chunks: tuple[DocumentChunk, ...]) -> None:
        self.chunks = chunks
        self._tokens = tuple(tokenize(f"{c.document.title} {c.section} {c.text}") for c in chunks)
        self._document_frequency = Counter(
            token for tokens in self._tokens for token in set(tokens)
        )
        self._average_length = (
            sum(len(tokens) for tokens in self._tokens) / len(self._tokens) if chunks else 0.0
        )

    def search(
        self,
        query: str,
        *,
        top_k: int = 3,
        allowed_security: frozenset[str] = frozenset({"public", "internal"}),
        minimum_score: float = 0.2,
    ) -> tuple[SearchResult, ...]:
        terms = tokenize(query)
        if not terms or top_k < 1:
            return ()
        results: list[SearchResult] = []
        for chunk, tokens in zip(self.chunks, self._tokens, strict=True):
            if chunk.document.security_class not in allowed_security:
                continue
            score = self._score(terms, tokens)
            if score >= minimum_score:
                results.append(SearchResult(chunk, round(score, 6)))
        results.sort(key=lambda result: (-result.score, result.chunk.chunk_id))
        return tuple(results[:top_k])

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"version": 1, "chunks": [chunk.to_dict() for chunk in self.chunks]}
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    @classmethod
    def load(cls, path: Path) -> LexicalIndex:
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload.get("version") != 1 or not isinstance(payload.get("chunks"), list):
            raise ValueError("Unsupported lexical index format.")
        chunks = []
        metadata_fields = {field.name for field in fields(DocumentMetadata)}
        for raw in payload["chunks"]:
            document = DocumentMetadata(
                **{key: value for key, value in raw["document"].items() if key in metadata_fields}
            )
            chunks.append(
                DocumentChunk(
                    chunk_id=raw["chunk_id"],
                    document=document,
                    section=raw["section"],
                    text=raw["text"],
                    content_hash=raw["content_hash"],
                )
            )
        return cls(tuple(chunks))

    def _score(self, query_terms: tuple[str, ...], tokens: tuple[str, ...]) -> float:
        if not tokens or not self.chunks:
            return 0.0
        frequencies = Counter(tokens)
        distinct_query_terms = set(query_terms)
        matched_terms = distinct_query_terms & frequencies.keys()
        if len(matched_terms) < min(2, len(distinct_query_terms)):
            return 0.0
        score = 0.0
        k1, b = 1.5, 0.75
        for term in distinct_query_terms:
            frequency = frequencies[term]
            if not frequency:
                continue
            document_frequency = self._document_frequency[term]
            inverse_frequency = math.log(
                1 + (len(self.chunks) - document_frequency + 0.5) / (document_frequency + 0.5)
            )
            normalization = frequency + k1 * (
                1 - b + b * len(tokens) / max(self._average_length, 1)
            )
            score += inverse_frequency * frequency * (k1 + 1) / normalization
        return score
