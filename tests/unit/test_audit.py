import pytest

from retail_lakehouse.monitoring.audit import PipelineRunAudit, RunStatus


def test_successful_run_has_terminal_timestamp() -> None:
    audit = PipelineRunAudit(pipeline_name="bronze_orders", source_name="orders")
    audit.rows_read = 10
    audit.rows_inserted = 10

    audit.complete()
    payload = audit.as_dict()

    assert payload["status"] == RunStatus.SUCCEEDED.value
    assert payload["completed_at"] is not None
    assert payload["rows_inserted"] == 10


def test_terminal_run_cannot_be_completed_twice() -> None:
    audit = PipelineRunAudit(pipeline_name="bronze_orders", source_name="orders")
    audit.complete()

    with pytest.raises(RuntimeError, match="already terminal"):
        audit.complete()
