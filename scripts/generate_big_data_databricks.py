"""Create and register the distributed NovaRetail big-data model in Databricks."""

from __future__ import annotations

import argparse
import json
import time

from retail_lakehouse.generation import DatabricksScaleOptions, build_scale_statements


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", default="novaretail_prod")
    parser.add_argument("--orders", type=int, default=5_000_000)
    parser.add_argument("--items-per-order", type=int, default=2)
    parser.add_argument("--customers", type=int, default=500_000)
    parser.add_argument("--products", type=int, default=100_000)
    parser.add_argument("--stores", type=int, default=1_000)
    parser.add_argument("--inventory-events", type=int, default=1_000_000)
    parser.add_argument("--batch-id", default="bigdata_v1")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    options = DatabricksScaleOptions(
        catalog=args.catalog,
        order_count=args.orders,
        items_per_order=args.items_per_order,
        customer_count=args.customers,
        product_count=args.products,
        store_count=args.stores,
        inventory_event_count=args.inventory_events,
        batch_id=args.batch_id,
    )
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError("PySpark is required for distributed data generation.") from exc

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    started = time.perf_counter()
    completed = []
    for label, statement in build_scale_statements(options):
        step_started = time.perf_counter()
        spark.sql(statement).collect()
        elapsed = round(time.perf_counter() - step_started, 3)
        completed.append({"step": label, "elapsed_seconds": elapsed})
        print(json.dumps(completed[-1]))

    report = {
        "catalog": options.catalog,
        "batch_id": options.batch_id,
        "orders": options.order_count,
        "order_items": options.order_item_count,
        "bronze_rows": options.bronze_row_count,
        "steps": len(completed),
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "arrival_table": f"{options.catalog}.governance.data_arrival_status",
        "kpi_table": f"{options.catalog}.gold.kpi_summary",
    }
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
