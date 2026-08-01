"""Construct conformed Gold facts from certified Silver tables."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from retail_lakehouse.analytics.keys import UNKNOWN_BUSINESS_KEY, dimension_key
from retail_lakehouse.transformations.gold_dimensions import resolve_scd_key


@dataclass(frozen=True)
class GoldDimensions:
    """Conformed dimensions required while constructing facts."""

    customer: Any
    product: Any
    store: Any
    supplier: Any
    promotion: Any


def _fact_key(functions: Any, fact_name: str, column_name: str) -> Any:
    return functions.sha2(
        functions.concat_ws("|", functions.lit(fact_name), functions.col(column_name)), 256
    )


def _date_key(functions: Any, timestamp_column: str) -> Any:
    return functions.coalesce(
        functions.date_format(timestamp_column, "yyyyMMdd").cast("int"), functions.lit(0)
    )


def _channel_key(functions: Any, channel_column: str) -> Any:
    pairs = []
    for code in ("STORE", "WEB", "MOBILE", "MARKETPLACE"):
        pairs.extend([functions.lit(code), functions.lit(dimension_key("dim_channel", code))])
    mapping = functions.create_map(*pairs)
    return functions.coalesce(
        mapping[functions.col(channel_column)],
        functions.lit(dimension_key("dim_channel", UNKNOWN_BUSINESS_KEY)),
    )


def _resolve_order_dimensions(frame: Any, dimensions: GoldDimensions) -> Any:
    resolved = resolve_scd_key(
        frame,
        dimensions.customer,
        source_business_key="customer_id",
        dimension_business_key="customer_id",
        event_timestamp="order_timestamp",
        output_key="customer_key",
        dimension_name="dim_customer",
    )
    resolved = resolve_scd_key(
        resolved,
        dimensions.store,
        source_business_key="store_id",
        dimension_business_key="store_id",
        event_timestamp="order_timestamp",
        output_key="store_key",
        dimension_name="dim_store",
    )
    return resolved


def build_fact_sales(orders: Any, dimensions: GoldDimensions) -> Any:
    """Build the order-grain certified sales fact."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold facts.") from exc
    resolved = _resolve_order_dimensions(orders, dimensions)
    return resolved.select(
        _fact_key(functions, "fact_sales", "order_id").alias("sales_key"),
        "order_id",
        "customer_key",
        "store_key",
        _channel_key(functions, "channel").alias("channel_key"),
        _date_key(functions, "order_timestamp").alias("order_date_key"),
        "order_timestamp",
        "channel",
        "order_status",
        "currency",
        "payment_method",
        "gross_amount",
        "discount_amount",
        "tax_amount",
        "shipping_amount",
        "net_amount",
        functions.lit(1).cast("long").alias("order_count"),
    )


def _order_item_base(order_items: Any, orders: Any, products: Any) -> Any:
    return (
        order_items.alias("item")
        .join(orders.alias("orders"), "order_id", "inner")
        .join(products.select("product_id", "supplier_id").alias("product"), "product_id", "left")
        .select(
            "item.*",
            "orders.customer_id",
            "orders.store_id",
            "orders.channel",
            "orders.order_status",
            "orders.order_timestamp",
            "orders.currency",
            "product.supplier_id",
        )
    )


def _attribute_promotion(frame: Any, promotion_dimension: Any) -> Any:
    try:
        from pyspark.sql import functions as functions
        from pyspark.sql.window import Window
    except ImportError as exc:
        raise RuntimeError("PySpark is required for promotion attribution.") from exc
    promotions = promotion_dimension.where(
        ~promotion_dimension.promotion_id.isin("__UNKNOWN__", "__NOT_APPLICABLE__")
    ).alias("promotion")
    fact = frame.alias("fact")
    condition = (
        (functions.col("fact.product_id") == functions.col("promotion.product_id"))
        & (functions.to_date("fact.order_timestamp") >= functions.col("promotion.start_date"))
        & (functions.to_date("fact.order_timestamp") <= functions.col("promotion.end_date"))
    )
    candidates = fact.join(promotions, condition, "left").select(
        "fact.*",
        functions.col("promotion.promotion_key").alias("_candidate_promotion_key"),
        functions.col("promotion.discount_percentage").alias("_candidate_discount"),
        functions.col("promotion.promotion_id").alias("_candidate_promotion_id"),
    )
    window = Window.partitionBy("order_item_id").orderBy(
        functions.col("_candidate_discount").desc_nulls_last(),
        functions.col("_candidate_promotion_id").asc_nulls_last(),
    )
    return (
        candidates.withColumn("_promotion_rank", functions.row_number().over(window))
        .where("_promotion_rank = 1")
        .withColumn(
            "promotion_key",
            functions.coalesce(
                "_candidate_promotion_key",
                functions.lit(dimension_key("dim_promotion", "__NOT_APPLICABLE__")),
            ),
        )
        .drop(
            "_candidate_promotion_key",
            "_candidate_discount",
            "_candidate_promotion_id",
            "_promotion_rank",
        )
    )


def build_fact_order_items(
    order_items: Any,
    orders: Any,
    products: Any,
    dimensions: GoldDimensions,
) -> Any:
    """Build atomic sales lines for orders that passed canonical order controls."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold facts.") from exc
    resolved = _resolve_order_dimensions(
        _order_item_base(order_items, orders, products), dimensions
    )
    for dimension, source_key, business_key, output_key, name in (
        (dimensions.product, "product_id", "product_id", "product_key", "dim_product"),
        (dimensions.supplier, "supplier_id", "supplier_id", "supplier_key", "dim_supplier"),
    ):
        resolved = resolve_scd_key(
            resolved,
            dimension,
            source_business_key=source_key,
            dimension_business_key=business_key,
            event_timestamp="order_timestamp",
            output_key=output_key,
            dimension_name=name,
        )
    resolved = _attribute_promotion(resolved, dimensions.promotion)
    return resolved.select(
        _fact_key(functions, "fact_order_items", "order_item_id").alias("order_item_key"),
        "order_item_id",
        "order_id",
        "customer_key",
        "product_key",
        "store_key",
        "supplier_key",
        "promotion_key",
        _channel_key(functions, "channel").alias("channel_key"),
        _date_key(functions, "order_timestamp").alias("order_date_key"),
        "order_timestamp",
        "product_id",
        "supplier_id",
        "channel",
        "currency",
        "quantity",
        "unit_price",
        (functions.col("unit_price") * functions.col("quantity"))
        .cast("decimal(18,2)")
        .alias("gross_line_amount"),
        "discount_amount",
        "tax_amount",
        "line_amount",
    )


def _join_order_context(frame: Any, orders: Any, foreign_key: str = "order_id") -> Any:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold facts.") from exc
    fact_columns = [
        functions.col(f"fact.`{name}`")
        for name in frame.columns
        if name not in {"customer_id", "store_id", "channel", "order_timestamp"}
    ]
    customer = (
        functions.coalesce("fact.customer_id", "orders.customer_id")
        if "customer_id" in frame.columns
        else functions.col("orders.customer_id")
    )
    return (
        frame.alias("fact")
        .join(orders.alias("orders"), foreign_key, "left")
        .select(
            *fact_columns,
            customer.alias("customer_id"),
            functions.col("orders.store_id").alias("store_id"),
            functions.col("orders.channel").alias("channel"),
            functions.col("orders.order_timestamp").alias("order_timestamp"),
        )
    )


def build_fact_payments(payments: Any, orders: Any, dimensions: GoldDimensions) -> Any:
    """Build one row per accepted payment, retaining unmatched orders with unknown keys."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold facts.") from exc
    base = _join_order_context(payments, orders).withColumn(
        "dimension_timestamp", functions.coalesce("order_timestamp", "payment_timestamp")
    )
    base = base.withColumn("order_timestamp", functions.col("dimension_timestamp"))
    resolved = _resolve_order_dimensions(base, dimensions)
    return resolved.select(
        _fact_key(functions, "fact_payments", "payment_id").alias("payment_key"),
        "payment_id",
        "order_id",
        "customer_key",
        "store_key",
        _channel_key(functions, "channel").alias("channel_key"),
        _date_key(functions, "payment_timestamp").alias("payment_date_key"),
        "payment_timestamp",
        "payment_status",
        "payment_method",
        "payment_amount",
        "transaction_reference",
    )


def build_fact_returns(returns: Any, orders: Any, dimensions: GoldDimensions) -> Any:
    """Build one row per accepted return."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold facts.") from exc
    base = _join_order_context(returns, orders).withColumn(
        "dimension_timestamp", functions.coalesce("order_timestamp", "return_timestamp")
    )
    base = base.withColumn("order_timestamp", functions.col("dimension_timestamp"))
    resolved = _resolve_order_dimensions(base, dimensions)
    resolved = resolve_scd_key(
        resolved,
        dimensions.product,
        source_business_key="product_id",
        dimension_business_key="product_id",
        event_timestamp="return_timestamp",
        output_key="product_key",
        dimension_name="dim_product",
    )
    return resolved.select(
        _fact_key(functions, "fact_returns", "return_id").alias("return_key"),
        "return_id",
        "order_id",
        "order_item_id",
        "customer_key",
        "product_key",
        "store_key",
        _channel_key(functions, "channel").alias("channel_key"),
        _date_key(functions, "return_timestamp").alias("return_date_key"),
        "return_timestamp",
        "return_reason",
        "return_status",
        "return_quantity",
        "refund_amount",
    )


def build_fact_inventory_movements(inventory: Any, dimensions: GoldDimensions) -> Any:
    """Build event-grain inventory movements."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold facts.") from exc
    resolved = resolve_scd_key(
        inventory,
        dimensions.product,
        source_business_key="product_id",
        dimension_business_key="product_id",
        event_timestamp="event_timestamp",
        output_key="product_key",
        dimension_name="dim_product",
    )
    resolved = resolve_scd_key(
        resolved,
        dimensions.store,
        source_business_key="store_id",
        dimension_business_key="store_id",
        event_timestamp="event_timestamp",
        output_key="store_key",
        dimension_name="dim_store",
    )
    return resolved.select(
        _fact_key(functions, "fact_inventory_movements", "inventory_event_id").alias(
            "inventory_movement_key"
        ),
        "inventory_event_id",
        "product_key",
        "store_key",
        _date_key(functions, "event_timestamp").alias("event_date_key"),
        "event_timestamp",
        "event_type",
        "quantity_change",
        "quantity_on_hand",
        "reorder_level",
        "source_system",
    )


def build_fact_inventory_snapshot(inventory_movements: Any) -> Any:
    """Keep the latest product-store inventory observation."""

    try:
        from pyspark.sql import functions as functions
        from pyspark.sql.window import Window
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold facts.") from exc
    window = Window.partitionBy("product_key", "store_key").orderBy(
        functions.col("event_timestamp").desc(), functions.col("inventory_event_id").desc()
    )
    return (
        inventory_movements.withColumn("_latest_rank", functions.row_number().over(window))
        .where("_latest_rank = 1")
        .select(
            functions.sha2(functions.concat_ws("|", "product_key", "store_key"), 256).alias(
                "inventory_snapshot_key"
            ),
            "product_key",
            "store_key",
            "event_date_key",
            functions.col("event_timestamp").alias("snapshot_timestamp"),
            "quantity_on_hand",
            "reorder_level",
            (functions.col("quantity_on_hand") == 0).alias("is_stockout"),
            (functions.col("quantity_on_hand") <= functions.col("reorder_level")).alias(
                "is_below_reorder_level"
            ),
        )
    )


def build_fact_shipments(shipments: Any, orders: Any, dimensions: GoldDimensions) -> Any:
    """Build shipment-grain fulfillment facts."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold facts.") from exc
    base = _join_order_context(shipments, orders).withColumn(
        "dimension_timestamp", functions.coalesce("order_timestamp", "shipped_timestamp")
    )
    base = base.withColumn("order_timestamp", functions.col("dimension_timestamp"))
    resolved = _resolve_order_dimensions(base, dimensions)
    return resolved.select(
        _fact_key(functions, "fact_shipments", "shipment_id").alias("shipment_key"),
        "shipment_id",
        "order_id",
        "customer_key",
        "store_key",
        _channel_key(functions, "channel").alias("channel_key"),
        _date_key(functions, "shipped_timestamp").alias("shipped_date_key"),
        "shipped_timestamp",
        "expected_delivery_timestamp",
        "delivered_timestamp",
        "carrier",
        "shipment_status",
        "shipping_cost",
        functions.datediff("delivered_timestamp", "shipped_timestamp").alias("delivery_days"),
        functions.datediff("delivered_timestamp", "expected_delivery_timestamp").alias(
            "delivery_variance_days"
        ),
    )


def build_fact_customer_events(events: Any, dimensions: GoldDimensions) -> Any:
    """Build event-grain digital behavior facts."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold facts.") from exc
    resolved = resolve_scd_key(
        events,
        dimensions.customer,
        source_business_key="customer_id",
        dimension_business_key="customer_id",
        event_timestamp="event_timestamp",
        output_key="customer_key",
        dimension_name="dim_customer",
    )
    resolved = resolve_scd_key(
        resolved,
        dimensions.product,
        source_business_key="product_id",
        dimension_business_key="product_id",
        event_timestamp="event_timestamp",
        output_key="product_key",
        dimension_name="dim_product",
    )
    return resolved.select(
        _fact_key(functions, "fact_customer_events", "event_id").alias("customer_event_key"),
        "event_id",
        "session_id",
        "customer_key",
        "product_key",
        _date_key(functions, "event_timestamp").alias("event_date_key"),
        "event_timestamp",
        "event_type",
        "device_type",
        "browser",
        "campaign_id",
        "source",
    )
