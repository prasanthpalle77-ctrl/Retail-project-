"""Certified Gold aggregates and retail KPI calculations."""

from __future__ import annotations

from typing import Any


def _safe_divide(functions: Any, numerator: Any, denominator: Any) -> Any:
    return functions.when(denominator == 0, functions.lit(0.0)).otherwise(
        numerator.cast("double") / denominator
    )


def build_daily_store_sales(fact_sales: Any) -> Any:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold aggregates.") from exc
    return fact_sales.groupBy("order_date_key", "store_key", "channel_key").agg(
        functions.countDistinct("order_id").alias("order_count"),
        functions.sum("gross_amount").alias("gross_sales"),
        functions.sum("discount_amount").alias("discounts"),
        functions.sum("net_amount").alias("order_revenue"),
    )


def build_daily_product_sales(fact_items: Any, fact_returns: Any) -> Any:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold aggregates.") from exc
    sold = fact_items.groupBy("order_date_key", "product_key").agg(
        functions.sum("quantity").cast("long").alias("sold_units"),
        functions.sum("gross_line_amount").alias("gross_product_sales"),
        functions.sum("discount_amount").alias("product_discounts"),
        functions.sum("line_amount").alias("product_revenue"),
    )
    returned = (
        fact_returns.groupBy("return_date_key", "product_key")
        .agg(
            functions.sum("return_quantity").cast("long").alias("returned_units"),
            functions.sum("refund_amount").alias("refund_amount"),
        )
        .withColumnRenamed("return_date_key", "order_date_key")
    )
    combined = sold.join(returned, ["order_date_key", "product_key"], "full").fillna(
        {"sold_units": 0, "returned_units": 0}
    )
    return combined.withColumn(
        "return_rate",
        _safe_divide(functions, functions.col("returned_units"), functions.col("sold_units")),
    )


def build_channel_funnel(fact_events: Any) -> Any:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold aggregates.") from exc
    grouped = fact_events.groupBy("event_date_key", "device_type").agg(
        functions.countDistinct("session_id").alias("eligible_sessions"),
        functions.countDistinct(
            functions.when(functions.col("event_type") == "PRODUCT_VIEW", "session_id")
        ).alias("view_sessions"),
        functions.countDistinct(
            functions.when(functions.col("event_type") == "ADD_TO_CART", "session_id")
        ).alias("cart_sessions"),
        functions.countDistinct(
            functions.when(functions.col("event_type") == "PURCHASE", "session_id")
        ).alias("purchase_sessions"),
    )
    return grouped.withColumn(
        "conversion_rate",
        _safe_divide(
            functions,
            functions.col("purchase_sessions"),
            functions.col("eligible_sessions"),
        ),
    )


def build_inventory_health(snapshot: Any) -> Any:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold aggregates.") from exc
    grouped = snapshot.groupBy("event_date_key", "store_key").agg(
        functions.count("inventory_snapshot_key").alias("product_observations"),
        functions.sum(functions.col("is_stockout").cast("long")).alias("stockout_observations"),
        functions.sum(functions.col("is_below_reorder_level").cast("long")).alias(
            "below_reorder_observations"
        ),
        functions.sum("quantity_on_hand").cast("long").alias("total_quantity_on_hand"),
    )
    return grouped.withColumn(
        "stockout_rate",
        _safe_divide(
            functions,
            functions.col("stockout_observations"),
            functions.col("product_observations"),
        ),
    )


def build_supplier_performance(fact_items: Any) -> Any:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold aggregates.") from exc
    return fact_items.groupBy("order_date_key", "supplier_key").agg(
        functions.countDistinct("product_key").alias("products_sold"),
        functions.sum("quantity").cast("long").alias("units_sold"),
        functions.sum("line_amount").alias("supplier_revenue"),
    )


def build_promotion_performance(fact_items: Any) -> Any:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold aggregates.") from exc
    return fact_items.groupBy("order_date_key", "promotion_key").agg(
        functions.countDistinct("order_id").alias("attributed_orders"),
        functions.sum("quantity").cast("long").alias("attributed_units"),
        functions.sum("gross_line_amount").alias("attributed_gross_sales"),
        functions.sum("discount_amount").alias("attributed_discounts"),
        functions.sum("line_amount").alias("attributed_revenue"),
    )


def build_retail_kpi_daily(fact_sales: Any, fact_returns: Any) -> Any:
    """Calculate certified finance KPIs at date and channel grain."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold KPIs.") from exc
    sales = fact_sales.groupBy("order_date_key", "channel_key").agg(
        functions.countDistinct("order_id").alias("distinct_orders"),
        functions.sum("gross_amount").alias("gross_sales"),
        functions.sum("discount_amount").alias("discounts"),
        functions.sum("tax_amount").alias("tax_revenue"),
        functions.sum("shipping_amount").alias("shipping_revenue"),
    )
    refunds = (
        fact_returns.groupBy("return_date_key", "channel_key")
        .agg(functions.sum("refund_amount").alias("refunds"))
        .withColumnRenamed("return_date_key", "order_date_key")
    )
    combined = sales.join(refunds, ["order_date_key", "channel_key"], "full").fillna(
        {"distinct_orders": 0}
    )
    zero = functions.lit(0).cast("decimal(18,2)")
    for column in (
        "gross_sales",
        "discounts",
        "tax_revenue",
        "shipping_revenue",
        "refunds",
    ):
        combined = combined.withColumn(column, functions.coalesce(column, zero))
    combined = combined.withColumn(
        "net_sales",
        functions.col("gross_sales") - functions.col("discounts") - functions.col("refunds"),
    ).withColumn(
        "net_revenue",
        functions.col("gross_sales")
        + functions.col("tax_revenue")
        + functions.col("shipping_revenue")
        - functions.col("discounts")
        - functions.col("refunds"),
    )
    return combined.withColumn(
        "average_order_value",
        _safe_divide(functions, functions.col("net_sales"), functions.col("distinct_orders")),
    )


def build_retail_kpi_periodic(kpi_daily: Any) -> Any:
    """Roll certified daily finance KPIs to calendar month and channel."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold KPIs.") from exc
    grouped = (
        kpi_daily.withColumn(
            "year_month", functions.substring(functions.col("order_date_key").cast("string"), 1, 6)
        )
        .groupBy("year_month", "channel_key")
        .agg(
            functions.sum("distinct_orders").alias("distinct_orders"),
            functions.sum("gross_sales").alias("gross_sales"),
            functions.sum("discounts").alias("discounts"),
            functions.sum("tax_revenue").alias("tax_revenue"),
            functions.sum("shipping_revenue").alias("shipping_revenue"),
            functions.sum("refunds").alias("refunds"),
            functions.sum("net_sales").alias("net_sales"),
            functions.sum("net_revenue").alias("net_revenue"),
        )
    )
    return grouped.withColumn(
        "average_order_value",
        _safe_divide(functions, functions.col("net_sales"), functions.col("distinct_orders")),
    )
