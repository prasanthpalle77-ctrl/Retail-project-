import os
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path

import pytest

from retail_lakehouse.analytics.keys import (
    NOT_APPLICABLE_BUSINESS_KEY,
    UNKNOWN_BUSINESS_KEY,
    dimension_key,
)
from retail_lakehouse.config import load_settings
from retail_lakehouse.spark import create_spark_session
from retail_lakehouse.storage import synchronize_snapshot
from retail_lakehouse.transformations.gold_aggregates import build_retail_kpi_daily
from retail_lakehouse.transformations.gold_dimensions import build_scd_dimension
from retail_lakehouse.transformations.gold_facts import GoldDimensions, build_fact_sales

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(os.name == "nt", reason="Native Windows Spark requires winutils.exe"),
]


def _customer() -> dict[str, object]:
    return {
        "customer_id": "C1",
        "first_name": "Ada",
        "last_name": "Lovelace",
        "email": "ada@example.com",
        "phone": "+1 555 0100",
        "city": "Seattle",
        "state": "WA",
        "country": "US",
        "loyalty_tier": "GOLD",
        "customer_status": "ACTIVE",
        "registration_date": date(2025, 1, 1),
        "updated_at": datetime(2025, 1, 1),
    }


def _store() -> dict[str, object]:
    return {
        "store_id": "S1",
        "store_name": "Seattle",
        "store_type": "MALL",
        "city": "Seattle",
        "state": "WA",
        "country": "US",
        "region": "WEST",
        "opening_date": date(2020, 1, 1),
        "store_status": "ACTIVE",
        "updated_at": datetime(2020, 1, 1),
    }


def _order(order_id: str, customer_id: str, gross: str, discount: str) -> dict[str, object]:
    gross_amount = Decimal(gross)
    discount_amount = Decimal(discount)
    tax = (gross_amount - discount_amount) * Decimal("0.08")
    shipping = Decimal("0") if gross_amount >= 50 else Decimal("5")
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "store_id": None,
        "channel": "WEB",
        "order_status": "COMPLETED",
        "order_timestamp": datetime(2026, 1, 1, 12, 0),
        "currency": "USD",
        "payment_method": "CARD",
        "gross_amount": gross_amount,
        "discount_amount": discount_amount,
        "tax_amount": tax,
        "shipping_amount": shipping,
        "net_amount": gross_amount - discount_amount + tax + shipping,
    }


def test_gold_dimension_resolution_and_finance_kpis(tmp_path: Path) -> None:
    root = Path(__file__).resolve().parents[2]
    spark = create_spark_session(load_settings("test", root))
    try:
        customer = build_scd_dimension(spark.createDataFrame([_customer()]), "dim_customer")
        store = build_scd_dimension(spark.createDataFrame([_store()]), "dim_store")
        dimensions = GoldDimensions(
            customer=customer,
            product=customer,
            store=store,
            supplier=customer,
            promotion=customer,
        )
        orders = spark.createDataFrame(
            [
                _order("O1", "C1", "100.00", "10.00"),
                _order("O2", "MISSING", "50.00", "0.00"),
            ]
        )
        sales = build_fact_sales(orders, dimensions).cache()
        empty_returns = sales.limit(0).selectExpr(
            "order_date_key AS return_date_key",
            "channel_key",
            "CAST(NULL AS DECIMAL(18,2)) AS refund_amount",
        )
        kpi = build_retail_kpi_daily(sales, empty_returns).first()
        customer_dimension_count = customer.count()
        keys = {
            row.order_id: row
            for row in sales.select("order_id", "customer_key", "store_key").collect()
        }
        masked_email = customer.where("customer_id = 'C1'").select("masked_email").first()[0]

        synchronize_snapshot(
            spark,
            sales,
            tmp_path / "fact_sales",
            identifiers=("sales_key",),
        )
        synchronize_snapshot(
            spark,
            sales.where("order_id = 'O1'"),
            tmp_path / "fact_sales",
            identifiers=("sales_key",),
        )
        replay_count = spark.read.format("delta").load(str(tmp_path / "fact_sales")).count()
    finally:
        spark.stop()

    assert customer_dimension_count == 3
    assert masked_email == "a***@example.com"
    assert keys["O2"].customer_key == dimension_key("dim_customer", UNKNOWN_BUSINESS_KEY)
    assert keys["O1"].store_key == dimension_key("dim_store", NOT_APPLICABLE_BUSINESS_KEY)
    assert kpi.gross_sales == Decimal("150.00")
    assert kpi.net_sales == Decimal("140.00")
    assert kpi.net_revenue == Decimal("151.20")
    assert kpi.average_order_value == 70.0
    assert replay_count == 1
