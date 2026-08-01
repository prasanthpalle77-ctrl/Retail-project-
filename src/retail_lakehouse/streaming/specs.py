"""Canonical streaming source contracts derived from Silver schemas."""

from retail_lakehouse.transformations.silver import SILVER_SPECS

STREAMING_DATASETS = ("customer_events", "inventory_events")


def stream_schema_ddl(dataset_name: str) -> str:
    """Return an explicit Spark DDL schema for one supported event stream."""

    if dataset_name not in STREAMING_DATASETS:
        raise ValueError(f"Unsupported streaming dataset: {dataset_name}")
    return ", ".join(
        f"`{name}` {data_type}" for name, data_type in SILVER_SPECS[dataset_name].columns
    )
