"""Typed canonical projections and deterministic business-key deduplication."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class SilverDatasetSpec:
    """Schema and ordering contract for one canonical Silver dataset."""

    business_keys: tuple[str, ...]
    event_timestamp: str
    columns: tuple[tuple[str, str], ...]
    uppercase_columns: tuple[str, ...] = ()


CUSTOMER_COLUMNS = (
    ("customer_id", "string"),
    ("first_name", "string"),
    ("last_name", "string"),
    ("email", "string"),
    ("phone", "string"),
    ("date_of_birth", "date"),
    ("gender", "string"),
    ("address", "string"),
    ("city", "string"),
    ("state", "string"),
    ("country", "string"),
    ("postal_code", "string"),
    ("loyalty_tier", "string"),
    ("registration_date", "date"),
    ("customer_status", "string"),
    ("created_at", "timestamp"),
    ("updated_at", "timestamp"),
    ("cdc_operation", "string"),
)
PRODUCT_COLUMNS = (
    ("product_id", "string"),
    ("sku", "string"),
    ("product_name", "string"),
    ("category", "string"),
    ("subcategory", "string"),
    ("brand", "string"),
    ("supplier_id", "string"),
    ("unit_cost", "decimal(18,2)"),
    ("list_price", "decimal(18,2)"),
    ("product_status", "string"),
    ("launch_date", "date"),
    ("created_at", "timestamp"),
    ("updated_at", "timestamp"),
    ("cdc_operation", "string"),
)
STORE_COLUMNS = (
    ("store_id", "string"),
    ("store_name", "string"),
    ("store_type", "string"),
    ("city", "string"),
    ("state", "string"),
    ("country", "string"),
    ("region", "string"),
    ("opening_date", "date"),
    ("store_status", "string"),
    ("manager_id", "string"),
    ("created_at", "timestamp"),
    ("updated_at", "timestamp"),
    ("cdc_operation", "string"),
)
SUPPLIER_COLUMNS = (
    ("supplier_id", "string"),
    ("supplier_name", "string"),
    ("contact_email", "string"),
    ("country", "string"),
    ("supplier_rating", "decimal(5,2)"),
    ("lead_time_days", "int"),
    ("supplier_status", "string"),
    ("created_at", "timestamp"),
    ("updated_at", "timestamp"),
    ("cdc_operation", "string"),
)
ORDER_COLUMNS = (
    ("order_id", "string"),
    ("customer_id", "string"),
    ("store_id", "string"),
    ("channel", "string"),
    ("order_status", "string"),
    ("order_timestamp", "timestamp"),
    ("currency", "string"),
    ("payment_method", "string"),
    ("shipping_amount", "decimal(18,2)"),
    ("tax_amount", "decimal(18,2)"),
    ("discount_amount", "decimal(18,2)"),
    ("gross_amount", "decimal(18,2)"),
    ("net_amount", "decimal(18,2)"),
    ("created_at", "timestamp"),
    ("updated_at", "timestamp"),
    ("cdc_operation", "string"),
)
ORDER_ITEM_COLUMNS = (
    ("order_item_id", "string"),
    ("order_id", "string"),
    ("product_id", "string"),
    ("quantity", "int"),
    ("unit_price", "decimal(18,2)"),
    ("discount_amount", "decimal(18,2)"),
    ("tax_amount", "decimal(18,2)"),
    ("line_amount", "decimal(18,2)"),
    ("created_at", "timestamp"),
    ("updated_at", "timestamp"),
)
PAYMENT_COLUMNS = (
    ("payment_id", "string"),
    ("order_id", "string"),
    ("payment_status", "string"),
    ("payment_method", "string"),
    ("payment_amount", "decimal(18,2)"),
    ("transaction_reference", "string"),
    ("payment_timestamp", "timestamp"),
    ("created_at", "timestamp"),
    ("updated_at", "timestamp"),
)
INVENTORY_COLUMNS = (
    ("inventory_event_id", "string"),
    ("product_id", "string"),
    ("store_id", "string"),
    ("event_type", "string"),
    ("quantity_change", "int"),
    ("quantity_on_hand", "int"),
    ("reorder_level", "int"),
    ("event_timestamp", "timestamp"),
    ("source_system", "string"),
)
RETURN_COLUMNS = (
    ("return_id", "string"),
    ("order_id", "string"),
    ("order_item_id", "string"),
    ("customer_id", "string"),
    ("product_id", "string"),
    ("return_reason", "string"),
    ("return_status", "string"),
    ("return_quantity", "int"),
    ("refund_amount", "decimal(18,2)"),
    ("return_timestamp", "timestamp"),
    ("processed_timestamp", "timestamp"),
)
PROMOTION_COLUMNS = (
    ("promotion_id", "string"),
    ("promotion_name", "string"),
    ("promotion_type", "string"),
    ("start_date", "date"),
    ("end_date", "date"),
    ("discount_percentage", "decimal(5,2)"),
    ("discount_amount", "decimal(18,2)"),
    ("category", "string"),
    ("product_id", "string"),
    ("minimum_order_value", "decimal(18,2)"),
    ("promotion_status", "string"),
)
SHIPMENT_COLUMNS = (
    ("shipment_id", "string"),
    ("order_id", "string"),
    ("carrier", "string"),
    ("shipment_status", "string"),
    ("shipped_timestamp", "timestamp"),
    ("expected_delivery_timestamp", "timestamp"),
    ("delivered_timestamp", "timestamp"),
    ("shipping_cost", "decimal(18,2)"),
    ("tracking_number", "string"),
)
EVENT_COLUMNS = (
    ("event_id", "string"),
    ("session_id", "string"),
    ("customer_id", "string"),
    ("event_type", "string"),
    ("product_id", "string"),
    ("event_timestamp", "timestamp"),
    ("page_url", "string"),
    ("device_type", "string"),
    ("browser", "string"),
    ("campaign_id", "string"),
    ("source", "string"),
    ("ingestion_timestamp", "timestamp"),
)

SILVER_SPECS: dict[str, SilverDatasetSpec] = {
    "customers": SilverDatasetSpec(
        ("customer_id",),
        "updated_at",
        CUSTOMER_COLUMNS,
        ("gender", "country", "loyalty_tier", "customer_status", "cdc_operation"),
    ),
    "products": SilverDatasetSpec(
        ("product_id",), "updated_at", PRODUCT_COLUMNS, ("product_status", "cdc_operation")
    ),
    "stores": SilverDatasetSpec(
        ("store_id",),
        "updated_at",
        STORE_COLUMNS,
        ("store_type", "country", "region", "store_status", "cdc_operation"),
    ),
    "suppliers": SilverDatasetSpec(
        ("supplier_id",),
        "updated_at",
        SUPPLIER_COLUMNS,
        ("country", "supplier_status", "cdc_operation"),
    ),
    "orders": SilverDatasetSpec(
        ("order_id",),
        "updated_at",
        ORDER_COLUMNS,
        ("channel", "order_status", "currency", "payment_method", "cdc_operation"),
    ),
    "order_items": SilverDatasetSpec(("order_item_id",), "updated_at", ORDER_ITEM_COLUMNS),
    "payments": SilverDatasetSpec(
        ("payment_id",), "updated_at", PAYMENT_COLUMNS, ("payment_status", "payment_method")
    ),
    "inventory_events": SilverDatasetSpec(
        ("inventory_event_id",),
        "event_timestamp",
        INVENTORY_COLUMNS,
        ("event_type", "source_system"),
    ),
    "returns": SilverDatasetSpec(
        ("return_id",), "return_timestamp", RETURN_COLUMNS, ("return_reason", "return_status")
    ),
    "promotions": SilverDatasetSpec(
        ("promotion_id",), "end_date", PROMOTION_COLUMNS, ("promotion_type", "promotion_status")
    ),
    "shipments": SilverDatasetSpec(
        ("shipment_id",), "delivered_timestamp", SHIPMENT_COLUMNS, ("carrier", "shipment_status")
    ),
    "customer_events": SilverDatasetSpec(
        ("event_id",),
        "event_timestamp",
        EVENT_COLUMNS,
        ("event_type", "device_type", "browser", "source"),
    ),
}


def standardize_bronze(frame: Any, dataset_name: str) -> Any:
    """Project raw strings into the canonical schema while retaining lineage metadata."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Silver standardization.") from exc
    if dataset_name not in SILVER_SPECS:
        raise ValueError(f"No Silver schema registered for dataset: {dataset_name}")
    spec = SILVER_SPECS[dataset_name]
    missing = [name for name, _ in spec.columns if name not in frame.columns]
    if missing:
        raise ValueError(f"Bronze {dataset_name} is missing columns: {', '.join(missing)}")

    projections = []
    for name, data_type in spec.columns:
        value = (
            functions.trim(functions.col(name)) if data_type == "string" else functions.col(name)
        )
        if name in spec.uppercase_columns:
            value = functions.upper(value)
        projections.append(value.cast(data_type).alias(name))
    canonical_names = {name for name, _ in spec.columns}
    metadata_names = [
        name for name in frame.columns if name.startswith("_") and name not in canonical_names
    ]
    metadata = [functions.col(name) for name in metadata_names]
    standardized = frame.select(*projections, *metadata)
    hash_columns = [functions.col(name) for name, _ in spec.columns]
    return standardized.withColumn(
        "_silver_record_hash",
        functions.sha2(functions.to_json(functions.struct(*hash_columns)), 256),
    ).withColumn("_silver_processed_at", functions.current_timestamp())


def deduplicate_latest(frame: Any, dataset_name: str) -> Any:
    """Keep the latest deterministic record for each configured business key."""

    try:
        from pyspark.sql import functions as functions
        from pyspark.sql.window import Window
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Silver deduplication.") from exc
    spec = SILVER_SPECS[dataset_name]
    ordering = [functions.col(spec.event_timestamp).desc_nulls_last()]
    if "_ingested_at" in frame.columns:
        ordering.append(functions.col("_ingested_at").desc_nulls_last())
    ordering.append(functions.col("_silver_record_hash").desc())
    window = Window.partitionBy(*spec.business_keys).orderBy(*ordering)
    return (
        frame.withColumn("_dedup_rank", functions.row_number().over(window))
        .where(functions.col("_dedup_rank") == 1)
        .drop("_dedup_rank")
    )
