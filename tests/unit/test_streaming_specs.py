import pytest

from retail_lakehouse.streaming import STREAMING_DATASETS, StreamingQueryConfig, stream_schema_ddl


def test_streaming_schemas_have_event_keys_and_timestamps() -> None:
    assert set(STREAMING_DATASETS) == {"customer_events", "inventory_events"}
    assert "`event_id` string" in stream_schema_ddl("customer_events")
    assert "`event_timestamp` timestamp" in stream_schema_ddl("customer_events")
    assert "`inventory_event_id` string" in stream_schema_ddl("inventory_events")


def test_streaming_config_rejects_invalid_controls(tmp_path: object) -> None:
    with pytest.raises(ValueError, match="cannot be negative"):
        StreamingQueryConfig(
            dataset_name="customer_events",
            source_path=tmp_path / "source",
            checkpoint_path=tmp_path / "checkpoint",
            silver_root=tmp_path / "silver",
            gold_root=tmp_path / "gold",
            quarantine_root=tmp_path / "quarantine",
            rules_path=tmp_path / "rules.yml",
            allowed_lateness_hours=-1,
        )
