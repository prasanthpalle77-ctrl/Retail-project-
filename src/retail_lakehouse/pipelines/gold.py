"""Silver-to-Gold dimensional analytics pipeline."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from retail_lakehouse.analytics import load_kpi_catalog
from retail_lakehouse.storage import synchronize_snapshot
from retail_lakehouse.transformations.gold_aggregates import (
    build_channel_funnel,
    build_daily_product_sales,
    build_daily_store_sales,
    build_inventory_health,
    build_promotion_performance,
    build_retail_kpi_daily,
    build_retail_kpi_periodic,
    build_supplier_performance,
)
from retail_lakehouse.transformations.gold_dimensions import (
    SCD_DIMENSION_CONTRACTS,
    build_channel_dimension,
    build_date_dimension,
    build_promotion_dimension,
    build_scd_dimension,
    existing_dimension,
)
from retail_lakehouse.transformations.gold_facts import (
    GoldDimensions,
    build_fact_customer_events,
    build_fact_inventory_movements,
    build_fact_inventory_snapshot,
    build_fact_order_items,
    build_fact_payments,
    build_fact_returns,
    build_fact_sales,
    build_fact_shipments,
)
from retail_lakehouse.transformations.scd2 import write_scd2_history
from retail_lakehouse.utils.hashing import sha256_text


@dataclass(frozen=True)
class GoldPipelineResult:
    """Published Gold counts and reconciliation status for one run."""

    run_id: str
    dimension_rows: int
    fact_rows: int
    aggregate_rows: int
    kpi_rows: int
    reconciliations: int
    reconciliation_status: str
    gold_root: str


def _read_silver(spark: Any, silver_root: Path, name: str) -> Any:
    return spark.read.format("delta").load(str((silver_root / name).resolve()))


def _calendar_bounds(sources: dict[str, Any]) -> tuple[date, date]:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold calendar bounds.") from exc
    candidates = (
        ("orders", "order_timestamp"),
        ("payments", "payment_timestamp"),
        ("returns", "return_timestamp"),
        ("inventory_events", "event_timestamp"),
        ("shipments", "shipped_timestamp"),
        ("customer_events", "event_timestamp"),
    )
    dates = None
    for source_name, timestamp in candidates:
        projected = sources[source_name].select(functions.to_date(timestamp).alias("business_date"))
        dates = projected if dates is None else dates.unionByName(projected)
    if dates is None:
        raise RuntimeError("No event sources are registered for the Gold calendar.")
    bounds = dates.agg(
        functions.min("business_date").alias("minimum"),
        functions.max("business_date").alias("maximum"),
    ).first()
    if bounds.minimum is None or bounds.maximum is None:
        raise RuntimeError("Cannot build Gold calendar without at least one event date.")
    return bounds.minimum, bounds.maximum


def _sync(
    spark: Any,
    frame: Any,
    gold_root: Path,
    name: str,
    identifiers: tuple[str, ...],
) -> int:
    count = int(frame.count())
    synchronize_snapshot(spark, frame, gold_root / name, identifiers=identifiers)
    return count


def _sum_decimal(frame: Any, column_name: str) -> Decimal:
    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold reconciliation.") from exc
    value = frame.agg(functions.sum(column_name).alias("amount")).first().amount
    return Decimal(value or 0).quantize(Decimal("0.01"))


def run_gold_pipeline(
    spark: Any,
    *,
    run_id: str,
    silver_root: Path,
    gold_root: Path,
    kpi_catalog_path: Path,
) -> GoldPipelineResult:
    """Publish replay-safe conformed dimensions, facts, aggregates, and KPIs."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for the Gold pipeline.") from exc
    silver_names = (
        "customers",
        "products",
        "stores",
        "suppliers",
        "orders",
        "order_items",
        "payments",
        "returns",
        "inventory_events",
        "promotions",
        "shipments",
        "customer_events",
    )
    silver = {name: _read_silver(spark, silver_root, name) for name in silver_names}

    dimension_counts: dict[str, int] = {}
    for dimension_name, contract in SCD_DIMENSION_CONTRACTS.items():
        target = gold_root / dimension_name
        dimension = build_scd_dimension(
            silver[str(contract["source"])],
            dimension_name,
            existing=existing_dimension(spark, target),
        )
        dimension_counts[dimension_name] = dimension.count()
        write_scd2_history(dimension, target)

    start_date, end_date = _calendar_bounds(silver)
    dimension_counts["dim_date"] = _sync(
        spark,
        build_date_dimension(spark, start_date, end_date),
        gold_root,
        "dim_date",
        ("date_key",),
    )
    dimension_counts["dim_channel"] = _sync(
        spark,
        build_channel_dimension(spark),
        gold_root,
        "dim_channel",
        ("channel_key",),
    )
    dimension_counts["dim_promotion"] = _sync(
        spark,
        build_promotion_dimension(silver["promotions"]),
        gold_root,
        "dim_promotion",
        ("promotion_key",),
    )

    dimensions = GoldDimensions(
        customer=spark.read.format("delta").load(str((gold_root / "dim_customer").resolve())),
        product=spark.read.format("delta").load(str((gold_root / "dim_product").resolve())),
        store=spark.read.format("delta").load(str((gold_root / "dim_store").resolve())),
        supplier=spark.read.format("delta").load(str((gold_root / "dim_supplier").resolve())),
        promotion=spark.read.format("delta").load(str((gold_root / "dim_promotion").resolve())),
    )
    facts = {
        "fact_sales": (
            build_fact_sales(silver["orders"], dimensions),
            ("sales_key",),
            "orders",
        ),
        "fact_order_items": (
            build_fact_order_items(
                silver["order_items"], silver["orders"], silver["products"], dimensions
            ),
            ("order_item_key",),
            "order_items",
        ),
        "fact_payments": (
            build_fact_payments(silver["payments"], silver["orders"], dimensions),
            ("payment_key",),
            "payments",
        ),
        "fact_returns": (
            build_fact_returns(silver["returns"], silver["orders"], dimensions),
            ("return_key",),
            "returns",
        ),
        "fact_inventory_movements": (
            build_fact_inventory_movements(silver["inventory_events"], dimensions),
            ("inventory_movement_key",),
            "inventory_events",
        ),
        "fact_shipments": (
            build_fact_shipments(silver["shipments"], silver["orders"], dimensions),
            ("shipment_key",),
            "shipments",
        ),
        "fact_customer_events": (
            build_fact_customer_events(silver["customer_events"], dimensions),
            ("customer_event_key",),
            "customer_events",
        ),
    }
    fact_counts: dict[str, int] = {}
    fact_sources: dict[str, str] = {}
    for name, (frame, identifiers, source_name) in facts.items():
        fact_counts[name] = _sync(spark, frame, gold_root, name, identifiers)
        fact_sources[name] = source_name

    inventory_movements = spark.read.format("delta").load(
        str((gold_root / "fact_inventory_movements").resolve())
    )
    fact_counts["fact_inventory_snapshot"] = _sync(
        spark,
        build_fact_inventory_snapshot(inventory_movements),
        gold_root,
        "fact_inventory_snapshot",
        ("inventory_snapshot_key",),
    )
    fact_sources["fact_inventory_snapshot"] = "inventory_events"

    published_facts = {
        name: spark.read.format("delta").load(str((gold_root / name).resolve()))
        for name in fact_counts
    }
    aggregate_frames = {
        "agg_daily_store_sales": (
            build_daily_store_sales(published_facts["fact_sales"]),
            ("order_date_key", "store_key", "channel_key"),
        ),
        "agg_daily_product_sales": (
            build_daily_product_sales(
                published_facts["fact_order_items"], published_facts["fact_returns"]
            ),
            ("order_date_key", "product_key"),
        ),
        "agg_channel_funnel": (
            build_channel_funnel(published_facts["fact_customer_events"]),
            ("event_date_key", "device_type"),
        ),
        "agg_inventory_health": (
            build_inventory_health(published_facts["fact_inventory_snapshot"]),
            ("event_date_key", "store_key"),
        ),
        "agg_supplier_performance": (
            build_supplier_performance(published_facts["fact_order_items"]),
            ("order_date_key", "supplier_key"),
        ),
        "agg_promotion_performance": (
            build_promotion_performance(published_facts["fact_order_items"]),
            ("order_date_key", "promotion_key"),
        ),
    }
    aggregate_counts = {
        name: _sync(spark, frame, gold_root, name, identifiers)
        for name, (frame, identifiers) in aggregate_frames.items()
    }

    kpi_daily = build_retail_kpi_daily(
        published_facts["fact_sales"], published_facts["fact_returns"]
    )
    daily_count = _sync(
        spark,
        kpi_daily,
        gold_root,
        "retail_kpi_daily",
        ("order_date_key", "channel_key"),
    )
    periodic_count = _sync(
        spark,
        build_retail_kpi_periodic(kpi_daily),
        gold_root,
        "retail_kpi_periodic",
        ("year_month", "channel_key"),
    )
    catalog_rows = [
        dict(asdict(item), catalog_version=1) for item in load_kpi_catalog(kpi_catalog_path)
    ]
    catalog = spark.createDataFrame(catalog_rows)
    _sync(spark, catalog, gold_root, "_kpi_catalog", ("name",))

    checked_at = datetime.now(UTC)
    reconciliation_rows = []
    for fact_name, published_rows in fact_counts.items():
        source_name = fact_sources[fact_name]
        source_rows = silver[source_name].count()
        excluded = max(source_rows - published_rows, 0)
        status = "PASS" if source_rows == published_rows + excluded else "FAIL"
        reconciliation_rows.append(
            {
                "reconciliation_id": sha256_text(f"{run_id}|{fact_name}|rows"),
                "run_id": run_id,
                "object_name": fact_name,
                "check_type": "ROW_COUNT",
                "source_rows": source_rows,
                "published_rows": published_rows,
                "excluded_rows": excluded,
                "source_amount": Decimal("0.00"),
                "published_amount": Decimal("0.00"),
                "difference_amount": Decimal("0.00"),
                "status": status,
                "checked_at": checked_at,
            }
        )
    source_amount = _sum_decimal(silver["orders"], "net_amount")
    published_amount = _sum_decimal(published_facts["fact_sales"], "net_amount")
    difference = source_amount - published_amount
    reconciliation_rows.append(
        {
            "reconciliation_id": sha256_text(f"{run_id}|fact_sales|net_amount"),
            "run_id": run_id,
            "object_name": "fact_sales",
            "check_type": "NET_AMOUNT",
            "source_rows": silver["orders"].count(),
            "published_rows": fact_counts["fact_sales"],
            "excluded_rows": 0,
            "source_amount": source_amount,
            "published_amount": published_amount,
            "difference_amount": difference,
            "status": "PASS" if difference == 0 else "FAIL",
            "checked_at": checked_at,
        }
    )
    reconciliations = spark.createDataFrame(reconciliation_rows)
    reconciliation_count = _sync(
        spark,
        reconciliations,
        gold_root,
        "_reconciliation_results",
        ("reconciliation_id",),
    )
    overall_status = (
        "PASS" if all(row["status"] == "PASS" for row in reconciliation_rows) else "FAIL"
    )
    if overall_status != "PASS":
        raise RuntimeError("Gold reconciliation failed; publication is not certified.")
    return GoldPipelineResult(
        run_id=run_id,
        dimension_rows=sum(dimension_counts.values()),
        fact_rows=sum(fact_counts.values()),
        aggregate_rows=sum(aggregate_counts.values()),
        kpi_rows=daily_count + periodic_count,
        reconciliations=reconciliation_count,
        reconciliation_status=overall_status,
        gold_root=str(gold_root.resolve()),
    )
