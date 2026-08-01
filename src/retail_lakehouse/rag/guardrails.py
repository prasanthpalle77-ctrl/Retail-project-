"""Input, evidence, and output controls for the deterministic copilot."""

from __future__ import annotations

import re

_INJECTION_PATTERNS = (
    re.compile(r"\bignore\s+(?:all\s+)?(?:previous|prior|system)\s+instructions?\b", re.I),
    re.compile(
        r"\b(?:reveal|show|print|dump)\s+(?:the\s+)?(?:system prompt|secrets?|api keys?)\b",
        re.I,
    ),
    re.compile(r"\byou are now\b", re.I),
    re.compile(r"\bdeveloper message\b", re.I),
)
_EMAIL = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.I)
_PHONE = re.compile(r"(?<!\w)(?:\+?\d[\d ()-]{7,}\d)(?!\w)")


def contains_prompt_injection(text: str) -> bool:
    """Return true for common attempts to replace trusted instructions."""

    return any(pattern.search(text) for pattern in _INJECTION_PATTERNS)


def redact_pii(text: str) -> str:
    """Remove common direct identifiers before audit persistence or display."""

    return _PHONE.sub("[REDACTED_PHONE]", _EMAIL.sub("[REDACTED_EMAIL]", text))


def safe_evidence(text: str) -> bool:
    """Treat retrieved text as data and exclude instruction-like passages."""

    return bool(text.strip()) and not contains_prompt_injection(text)
