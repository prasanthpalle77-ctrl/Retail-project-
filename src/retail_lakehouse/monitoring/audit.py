"""In-memory audit model shared by local and Databricks persistence layers."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from uuid import uuid4


class RunStatus(StrEnum):
    """Allowed lifecycle states for a pipeline execution."""

    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


def utc_now() -> datetime:
    """Return an aware UTC timestamp."""

    return datetime.now(UTC)


@dataclass
class PipelineRunAudit:
    """Portable audit record populated by every pipeline entry point."""

    pipeline_name: str
    source_name: str
    run_id: str = field(default_factory=lambda: str(uuid4()))
    status: RunStatus = RunStatus.RUNNING
    started_at: datetime = field(default_factory=utc_now)
    completed_at: datetime | None = None
    rows_read: int = 0
    rows_inserted: int = 0
    rows_updated: int = 0
    rows_deleted: int = 0
    rows_rejected: int = 0
    error_type: str | None = None
    error_message: str | None = None

    def complete(self) -> None:
        """Mark a running audit record as successfully completed."""

        self._assert_running()
        self.status = RunStatus.SUCCEEDED
        self.completed_at = utc_now()

    def fail(self, error: BaseException) -> None:
        """Mark a running audit record as failed using sanitized error fields."""

        self._assert_running()
        self.status = RunStatus.FAILED
        self.completed_at = utc_now()
        self.error_type = type(error).__name__
        self.error_message = str(error)

    def _assert_running(self) -> None:
        if self.status is not RunStatus.RUNNING:
            raise RuntimeError(f"Run {self.run_id} is already terminal: {self.status}")

    def as_dict(self) -> dict[str, Any]:
        """Return a serialization-friendly representation."""

        result = asdict(self)
        result["status"] = self.status.value
        result["started_at"] = self.started_at.isoformat()
        result["completed_at"] = self.completed_at.isoformat() if self.completed_at else None
        return result
