"""Privacy-aware append-only audit events for copilot requests."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from retail_lakehouse.rag.guardrails import redact_pii
from retail_lakehouse.rag.models import CopilotResponse


class QueryAuditLog:
    def __init__(self, path: Path) -> None:
        self.path = path

    def record(
        self,
        question: str,
        response: CopilotResponse,
        *,
        latency_ms: float,
    ) -> str:
        audit_id = uuid.uuid4().hex
        event: dict[str, Any] = {
            "audit_id": audit_id,
            "timestamp_utc": datetime.now(UTC).isoformat(),
            "question": redact_pii(question),
            "route": response.route,
            "citation_ids": [item.citation_id for item in response.citations],
            "sql": response.sql,
            "row_count": len(response.rows),
            "refused": response.refused,
            "refusal_reason": response.refusal_reason,
            "latency_ms": round(latency_ms, 3),
            "model_usage": {"total_tokens": 0},
            "provider": "deterministic_local",
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(event, sort_keys=True) + "\n")
        return audit_id
