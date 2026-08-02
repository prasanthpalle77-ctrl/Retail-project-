"""Distributed Databricks SQL generation for the portfolio-scale retail dataset."""

from __future__ import annotations

import re
from dataclasses import dataclass

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class DatabricksScaleOptions:
    """Cardinality controls for deterministic distributed data generation."""

    catalog: str = "novaretail_prod"
    order_count: int = 5_000_000
    items_per_order: int = 2
    customer_count: int = 500_000
    product_count: int = 100_000
    store_count: int = 1_000
    inventory_event_count: int = 1_000_000
    batch_id: str = "bigdata_v1"

    def __post_init__(self) -> None:
        if not _IDENTIFIER.fullmatch(self.catalog):
            raise ValueError("Catalog must be a simple Unity Catalog identifier.")
        counts = {
            "order_count": self.order_count,
            "items_per_order": self.items_per_order,
            "customer_count": self.customer_count,
            "product_count": self.product_count,
            "store_count": self.store_count,
            "inventory_event_count": self.inventory_event_count,
        }
        invalid = [name for name, value in counts.items() if value < 1]
        if invalid:
            raise ValueError(f"Scale counts must be positive: {', '.join(invalid)}")
        if not re.fullmatch(r"[A-Za-z0-9_-]+", self.batch_id):
            raise ValueError("Batch ID may contain only letters, numbers, underscores, and dashes.")

    @property
    def order_item_count(self) -> int:
        return self.order_count * self.items_per_order

    @property
    def customer_event_count(self) -> int:
        return self.order_count

    @property
    def payment_count(self) -> int:
        return self.order_count

    @property
    def bronze_row_count(self) -> int:
        return (
            self.customer_count
            + self.product_count
            + self.store_count
            + self.order_count
            + self.order_item_count
            + self.payment_count
            + self.customer_event_count
            + self.inventory_event_count
        )


def _qualified(catalog: str, schema: str, name: str) -> str:
    return f"`{catalog}`.`{schema}`.`{name}`"


def build_scale_statements(options: DatabricksScaleOptions) -> tuple[tuple[str, str], ...]:
    """Return ordered, replay-safe SQL statements for the large retail model."""

    catalog = options.catalog
    batch_id = options.batch_id
    orders = options.order_count
    items = options.order_item_count
    customers = options.customer_count
    products = options.product_count
    stores = options.store_count
    inventory = options.inventory_event_count

    def bronze(name: str) -> str:
        return _qualified(catalog, "bronze", name)

    def silver(name: str) -> str:
        return _qualified(catalog, "silver", name)

    def gold(name: str) -> str:
        return _qualified(catalog, "gold", name)

    def governance(name: str) -> str:
        return _qualified(catalog, "governance", name)

    statements: list[tuple[str, str]] = []
    for schema in ("bronze", "silver", "gold", "governance"):
        statements.append(
            (f"schema:{schema}", f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
        )

    statements.extend(
        [
            (
                "bronze:customers",
                f"""
CREATE OR REPLACE TABLE {bronze("customers")} USING DELTA AS
SELECT
  id AS customer_number,
  concat('C', lpad(cast(id AS STRING), 9, '0')) AS customer_id,
  concat('customer', cast(id AS STRING), '@example.com') AS email,
  CASE pmod(id, 4) WHEN 0 THEN 'BRONZE' WHEN 1 THEN 'SILVER'
       WHEN 2 THEN 'GOLD' ELSE 'PLATINUM' END AS loyalty_tier,
  CASE pmod(id, 5) WHEN 0 THEN 'NORTH' WHEN 1 THEN 'SOUTH'
       WHEN 2 THEN 'EAST' WHEN 3 THEN 'WEST' ELSE 'CENTRAL' END AS region,
  date_add(DATE '2020-01-01', cast(pmod(id, 1460) AS INT)) AS registration_date,
  '{batch_id}' AS _batch_id,
  current_timestamp() AS _ingested_at
FROM range({customers})
""".strip(),  # nosec B608
            ),
            (
                "bronze:products",
                f"""
CREATE OR REPLACE TABLE {bronze("products")} USING DELTA AS
SELECT
  id AS product_number,
  concat('P', lpad(cast(id AS STRING), 7, '0')) AS product_id,
  concat('Product ', cast(id AS STRING)) AS product_name,
  concat('Category ', cast(pmod(id, 25) AS STRING)) AS category,
  concat('Brand ', cast(pmod(id, 200) AS STRING)) AS brand,
  cast(5 + pmod(id * 17, 49500) / 100.0 AS DECIMAL(18,2)) AS list_price,
  concat('SUP', lpad(cast(pmod(id, 2000) AS STRING), 5, '0')) AS supplier_id,
  '{batch_id}' AS _batch_id,
  current_timestamp() AS _ingested_at
FROM range({products})
""".strip(),  # nosec B608
            ),
            (
                "bronze:stores",
                f"""
CREATE OR REPLACE TABLE {bronze("stores")} USING DELTA AS
SELECT
  id AS store_number,
  concat('S', lpad(cast(id AS STRING), 5, '0')) AS store_id,
  concat('Store ', cast(id AS STRING)) AS store_name,
  CASE pmod(id, 5) WHEN 0 THEN 'NORTH' WHEN 1 THEN 'SOUTH'
       WHEN 2 THEN 'EAST' WHEN 3 THEN 'WEST' ELSE 'CENTRAL' END AS region,
  CASE pmod(id, 3) WHEN 0 THEN 'MALL' WHEN 1 THEN 'STREET' ELSE 'OUTLET' END AS store_type,
  '{batch_id}' AS _batch_id,
  current_timestamp() AS _ingested_at
FROM range({stores})
""".strip(),  # nosec B608
            ),
            (
                "bronze:orders",
                f"""
CREATE OR REPLACE TABLE {bronze("orders")} USING DELTA AS
WITH base AS (
  SELECT
    id AS order_number,
    concat('O', lpad(cast(id AS STRING), 10, '0')) AS order_id,
    concat('C', lpad(cast(pmod(id * 17, {customers}) AS STRING), 9, '0')) AS customer_id,
    concat('S', lpad(cast(pmod(id * 13, {stores}) AS STRING), 5, '0')) AS store_id,
    CASE pmod(id, 3) WHEN 0 THEN 'STORE' WHEN 1 THEN 'WEB' ELSE 'MOBILE' END AS channel,
    CASE WHEN pmod(id, 50) = 0 THEN 'CANCELLED'
         WHEN pmod(id, 20) = 0 THEN 'RETURNED' ELSE 'COMPLETED' END AS order_status,
    date_add(DATE '2024-01-01', cast(pmod(id, 730) AS INT)) AS order_date,
    cast(20 + pmod(id * 29, 48000) / 100.0 AS DECIMAL(18,2)) AS gross_amount
  FROM range({orders})
), priced AS (
  SELECT *, cast(gross_amount * CASE WHEN pmod(order_number, 10) = 0 THEN 0.10 ELSE 0 END
    AS DECIMAL(18,2)) AS discount_amount FROM base
)
SELECT *,
  cast((gross_amount - discount_amount) * 0.08 AS DECIMAL(18,2)) AS tax_amount,
  cast(CASE WHEN channel = 'STORE' THEN 0 ELSE 5 END AS DECIMAL(18,2)) AS shipping_amount,
  cast(gross_amount - discount_amount
       + (gross_amount - discount_amount) * 0.08
       + CASE WHEN channel = 'STORE' THEN 0 ELSE 5 END AS DECIMAL(18,2)) AS net_amount,
  '{batch_id}' AS _batch_id,
  current_timestamp() AS _ingested_at
FROM priced
""".strip(),  # nosec B608
            ),
            (
                "bronze:order_items",
                f"""
CREATE OR REPLACE TABLE {bronze("order_items")} USING DELTA AS
WITH base AS (
  SELECT
    id AS order_item_number,
    pmod(id, {orders}) AS order_number,
    concat('OI', lpad(cast(id AS STRING), 11, '0')) AS order_item_id,
    concat('O', lpad(cast(pmod(id, {orders}) AS STRING), 10, '0')) AS order_id,
    cast(floor(id / {orders}) + 1 AS INT) AS line_number,
    concat('P', lpad(cast(pmod(id * 13, 10000) AS STRING), 7, '0')) AS product_id,
    concat('C', lpad(cast(pmod(pmod(id, {orders}) * 17, {customers}) AS STRING),
      9, '0')) AS customer_id,
    concat('S', lpad(cast(pmod(pmod(id, {orders}) * 13, {stores}) AS STRING),
      5, '0')) AS store_id,
    CASE pmod(pmod(id, {orders}), 3) WHEN 0 THEN 'STORE'
         WHEN 1 THEN 'WEB' ELSE 'MOBILE' END AS channel,
    date_add(DATE '2024-01-01', cast(pmod(pmod(id, {orders}), 730) AS INT)) AS order_date,
    cast(1 + pmod(id, 5) AS INT) AS quantity,
    cast(5 + pmod(id * 17, 49500) / 100.0 AS DECIMAL(18,2)) AS unit_price
  FROM range({items})
)
SELECT *,
  cast(quantity * unit_price AS DECIMAL(18,2)) AS gross_line_amount,
  cast(quantity * unit_price * CASE WHEN pmod(order_item_number, 10) = 0 THEN 0.10 ELSE 0 END
    AS DECIMAL(18,2)) AS discount_amount,
  cast(CASE WHEN pmod(order_item_number, 20) = 0 THEN quantity ELSE 0 END AS INT)
    AS returned_units,
  '{batch_id}' AS _batch_id,
  current_timestamp() AS _ingested_at
FROM base
""".strip(),  # nosec B608
            ),
            (
                "bronze:payments",
                f"""
CREATE OR REPLACE TABLE {bronze("payments")} USING DELTA AS
SELECT
  id AS payment_number,
  concat('PAY', lpad(cast(id AS STRING), 10, '0')) AS payment_id,
  concat('O', lpad(cast(id AS STRING), 10, '0')) AS order_id,
  CASE pmod(id, 4) WHEN 0 THEN 'CARD' WHEN 1 THEN 'WALLET'
       WHEN 2 THEN 'BANK_TRANSFER' ELSE 'CASH' END AS payment_method,
  CASE WHEN pmod(id, 100) = 0 THEN 'REFUNDED' ELSE 'CAPTURED' END AS payment_status,
  cast(20 + pmod(id * 29, 48000) / 100.0 AS DECIMAL(18,2)) AS payment_amount,
  date_add(DATE '2024-01-01', cast(pmod(id, 730) AS INT)) AS payment_date,
  '{batch_id}' AS _batch_id,
  current_timestamp() AS _ingested_at
FROM range({orders})
""".strip(),  # nosec B608
            ),
            (
                "bronze:customer_events",
                f"""
CREATE OR REPLACE TABLE {bronze("customer_events")} USING DELTA AS
SELECT
  id AS event_number,
  concat('EV', lpad(cast(id AS STRING), 10, '0')) AS event_id,
  concat('SESSION', lpad(cast(id AS STRING), 10, '0')) AS session_id,
  concat('C', lpad(cast(pmod(id * 17, {customers}) AS STRING), 9, '0')) AS customer_id,
  concat('P', lpad(cast(pmod(id * 13, 10000) AS STRING), 7, '0')) AS product_id,
  CASE pmod(id, 4) WHEN 0 THEN 'PURCHASE' WHEN 1 THEN 'ADD_TO_CART'
       WHEN 2 THEN 'PRODUCT_VIEW' ELSE 'SEARCH' END AS event_type,
  CASE pmod(id, 4) WHEN 0 THEN 'MOBILE' WHEN 1 THEN 'DESKTOP'
       WHEN 2 THEN 'TABLET' ELSE 'OTHER' END AS device_type,
  date_add(DATE '2024-01-01', cast(pmod(id, 730) AS INT)) AS event_date,
  '{batch_id}' AS _batch_id,
  current_timestamp() AS _ingested_at
FROM range({orders})
""".strip(),  # nosec B608
            ),
            (
                "bronze:inventory_events",
                f"""
CREATE OR REPLACE TABLE {bronze("inventory_events")} USING DELTA AS
SELECT
  id AS inventory_event_number,
  concat('INV', lpad(cast(id AS STRING), 10, '0')) AS inventory_event_id,
  concat('P', lpad(cast(pmod(id * 13, 10000) AS STRING), 7, '0')) AS product_id,
  concat('S', lpad(cast(pmod(id * 7, {stores}) AS STRING), 5, '0')) AS store_id,
  date_add(DATE '2024-01-01', cast(pmod(id, 730) AS INT)) AS event_date,
  cast(pmod(id * 19, 101) AS INT) AS quantity_on_hand,
  cast(10 + pmod(id, 20) AS INT) AS reorder_level,
  '{batch_id}' AS _batch_id,
  current_timestamp() AS _ingested_at
FROM range({inventory})
""".strip(),  # nosec B608
            ),
        ]
    )

    for name in (
        "customers",
        "products",
        "stores",
        "orders",
        "order_items",
        "payments",
        "customer_events",
        "inventory_events",
    ):
        statements.append(
            (
                f"silver:{name}",
                f"CREATE OR REPLACE VIEW {silver(name)} AS SELECT * FROM {bronze(name)}",  # nosec B608
            )
        )

    statements.extend(
        [
            (
                "gold:dim_customer",
                f"""
CREATE OR REPLACE TABLE {gold("dim_customer")} USING DELTA AS
SELECT customer_number AS customer_key, customer_id, email, loyalty_tier, region,
       registration_date FROM {silver("customers")}
""".strip(),  # nosec B608
            ),
            (
                "gold:dim_product",
                f"""
CREATE OR REPLACE TABLE {gold("dim_product")} USING DELTA AS
SELECT product_number AS product_key, product_id, product_name, category, brand,
       list_price, supplier_id FROM {silver("products")}
""".strip(),  # nosec B608
            ),
            (
                "gold:dim_store",
                f"""
CREATE OR REPLACE TABLE {gold("dim_store")} USING DELTA AS
SELECT store_number AS store_key, store_id, store_name, region, store_type
FROM {silver("stores")}
""".strip(),  # nosec B608
            ),
            (
                "gold:dim_date",
                f"""
CREATE OR REPLACE TABLE {gold("dim_date")} USING DELTA AS
SELECT cast(date_format(calendar_date, 'yyyyMMdd') AS INT) AS date_key,
       calendar_date, year(calendar_date) AS year, month(calendar_date) AS month,
       quarter(calendar_date) AS quarter, dayofweek(calendar_date) AS day_of_week
FROM (SELECT date_add(DATE '2024-01-01', cast(id AS INT)) AS calendar_date FROM range(730))
""".strip(),  # nosec B608
            ),
            (
                "gold:dim_channel",
                f"""
CREATE OR REPLACE TABLE {gold("dim_channel")} USING DELTA AS
SELECT * FROM VALUES (0, 'STORE'), (1, 'WEB'), (2, 'MOBILE') AS channel(channel_key, channel)
""".strip(),  # nosec B608
            ),
            (
                "gold:fact_sales",
                f"""
CREATE OR REPLACE TABLE {gold("fact_sales")} USING DELTA AS
SELECT order_number AS sales_key, order_id, customer_id, store_id,
       cast(date_format(order_date, 'yyyyMMdd') AS INT) AS order_date_key,
       channel, order_status, gross_amount, discount_amount, tax_amount,
       shipping_amount, net_amount, 1L AS order_count
FROM {silver("orders")}
""".strip(),  # nosec B608
            ),
            (
                "gold:fact_order_items",
                f"""
CREATE OR REPLACE TABLE {gold("fact_order_items")} USING DELTA AS
SELECT order_item_number AS order_item_key, order_item_id, order_id, line_number,
       product_id, customer_id, store_id,
       cast(date_format(order_date, 'yyyyMMdd') AS INT) AS order_date_key,
       channel, quantity, unit_price, gross_line_amount, discount_amount, returned_units
FROM {silver("order_items")}
""".strip(),  # nosec B608
            ),
            (
                "gold:fact_payments",
                f"""
CREATE OR REPLACE TABLE {gold("fact_payments")} USING DELTA AS
SELECT payment_number AS payment_key, payment_id, order_id, payment_method,
       payment_status, payment_amount,
       cast(date_format(payment_date, 'yyyyMMdd') AS INT) AS payment_date_key
FROM {silver("payments")}
""".strip(),  # nosec B608
            ),
            (
                "gold:fact_customer_events",
                f"""
CREATE OR REPLACE TABLE {gold("fact_customer_events")} USING DELTA AS
SELECT event_number AS customer_event_key, event_id, session_id, customer_id, product_id,
       event_type, device_type, cast(date_format(event_date, 'yyyyMMdd') AS INT) AS event_date_key
FROM {silver("customer_events")}
""".strip(),  # nosec B608
            ),
            (
                "gold:fact_inventory_movements",
                f"""
CREATE OR REPLACE TABLE {gold("fact_inventory_movements")} USING DELTA AS
SELECT inventory_event_number AS inventory_movement_key, inventory_event_id,
       product_id, store_id, cast(date_format(event_date, 'yyyyMMdd') AS INT) AS event_date_key,
       quantity_on_hand, reorder_level
FROM {silver("inventory_events")}
""".strip(),  # nosec B608
            ),
            (
                "gold:retail_kpi_daily",
                f"""
CREATE OR REPLACE TABLE {gold("retail_kpi_daily")} USING DELTA AS
SELECT order_date_key AS date_key, channel,
       cast(sum(gross_amount) AS DECIMAL(24,2)) AS gross_sales,
       cast(sum(gross_amount - discount_amount) AS DECIMAL(24,2)) AS net_sales,
       cast(sum(net_amount) AS DECIMAL(24,2)) AS net_revenue,
       cast(sum(gross_amount - discount_amount) / count(DISTINCT order_id)
         AS DECIMAL(18,2)) AS average_order_value,
       count(DISTINCT order_id) AS distinct_orders
FROM {gold("fact_sales")}
WHERE order_status <> 'CANCELLED'
GROUP BY order_date_key, channel
""".strip(),  # nosec B608
            ),
            (
                "gold:agg_daily_product_sales",
                f"""
CREATE OR REPLACE TABLE {gold("agg_daily_product_sales")} USING DELTA AS
SELECT order_date_key AS date_key, product_id AS product_key,
       sum(quantity) AS units_sold, sum(returned_units) AS returned_units,
       cast(CASE WHEN sum(quantity) = 0 THEN 0
         ELSE sum(returned_units) / sum(quantity) END AS DECIMAL(12,6)) AS return_rate
FROM {gold("fact_order_items")}
GROUP BY order_date_key, product_id
""".strip(),  # nosec B608
            ),
            (
                "gold:agg_channel_funnel",
                f"""
CREATE OR REPLACE TABLE {gold("agg_channel_funnel")} USING DELTA AS
SELECT event_date_key AS date_key, device_type,
       count(DISTINCT session_id) AS sessions,
       count(DISTINCT CASE WHEN event_type = 'PURCHASE' THEN session_id END)
         AS purchasing_sessions,
       cast(count(DISTINCT CASE WHEN event_type = 'PURCHASE' THEN session_id END)
         / count(DISTINCT session_id) AS DECIMAL(12,6)) AS conversion_rate
FROM {gold("fact_customer_events")}
GROUP BY event_date_key, device_type
""".strip(),  # nosec B608
            ),
            (
                "gold:agg_inventory_health",
                f"""
CREATE OR REPLACE TABLE {gold("agg_inventory_health")} USING DELTA AS
SELECT event_date_key AS date_key, store_id AS store_key,
       count(*) AS observations,
       sum(CASE WHEN quantity_on_hand = 0 THEN 1 ELSE 0 END) AS stockout_observations,
       cast(sum(CASE WHEN quantity_on_hand = 0 THEN 1 ELSE 0 END) / count(*)
         AS DECIMAL(12,6)) AS stockout_rate
FROM {gold("fact_inventory_movements")}
GROUP BY event_date_key, store_id
""".strip(),  # nosec B608
            ),
            (
                "gold:kpi_summary",
                f"""
CREATE OR REPLACE TABLE {gold("kpi_summary")} USING DELTA AS
SELECT 'total_orders' AS metric_name, cast(count(*) AS DOUBLE) AS metric_value,
       'Number of order records available for analysis.' AS business_use,
       current_timestamp() AS calculated_at FROM {gold("fact_sales")}
UNION ALL
SELECT 'net_revenue', cast(sum(net_amount) AS DOUBLE),
       'Revenue after discounts plus tax and shipping.', current_timestamp()
FROM {gold("fact_sales")} WHERE order_status <> 'CANCELLED'
UNION ALL
SELECT 'average_order_value', cast(avg(net_amount) AS DOUBLE),
       'Average revenue per non-cancelled order.', current_timestamp()
FROM {gold("fact_sales")} WHERE order_status <> 'CANCELLED'
UNION ALL
SELECT 'conversion_rate', cast(avg(conversion_rate) AS DOUBLE),
       'Share of eligible sessions that completed a purchase.', current_timestamp()
FROM {gold("agg_channel_funnel")}
UNION ALL
SELECT 'stockout_rate', cast(avg(stockout_rate) AS DOUBLE),
       'Share of inventory observations with zero units on hand.', current_timestamp()
FROM {gold("agg_inventory_health")}
UNION ALL
SELECT 'return_rate', cast(avg(return_rate) AS DOUBLE),
       'Share of sold units represented by returned units.', current_timestamp()
FROM {gold("agg_daily_product_sales")}
""".strip(),  # nosec B608
            ),
            (
                "governance:data_arrival_status",
                f"""
CREATE OR REPLACE TABLE {governance("data_arrival_status")} USING DELTA AS
WITH checks AS (
  SELECT 'bronze' AS layer, 'orders' AS object_name, {orders}L AS expected_rows,
         count(*) AS actual_rows FROM {bronze("orders")}
  UNION ALL SELECT 'bronze', 'order_items', {items}L, count(*) FROM {bronze("order_items")}
  UNION ALL SELECT 'bronze', 'customer_events', {orders}L, count(*)
    FROM {bronze("customer_events")}
  UNION ALL SELECT 'silver', 'orders', {orders}L, count(*) FROM {silver("orders")}
  UNION ALL SELECT 'silver', 'order_items', {items}L, count(*) FROM {silver("order_items")}
  UNION ALL SELECT 'gold', 'fact_sales', {orders}L, count(*) FROM {gold("fact_sales")}
  UNION ALL SELECT 'gold', 'fact_order_items', {items}L, count(*)
    FROM {gold("fact_order_items")}
)
SELECT layer, object_name, expected_rows, actual_rows,
       CASE WHEN actual_rows = expected_rows THEN 'ARRIVED' ELSE 'MISMATCH' END AS arrival_status,
       '{batch_id}' AS batch_id, current_timestamp() AS checked_at
FROM checks
""".strip(),  # nosec B608
            ),
            (
                "governance:pipeline_runs_table",
                f"""
CREATE TABLE IF NOT EXISTS {governance("pipeline_runs")} (
  run_id STRING, batch_id STRING, status STRING, order_count BIGINT,
  order_item_count BIGINT, bronze_row_count BIGINT, completed_at TIMESTAMP
) USING DELTA
""".strip(),  # nosec B608
            ),
            (
                "governance:pipeline_run_record",
                f"""
INSERT INTO {governance("pipeline_runs")}
SELECT uuid(), '{batch_id}', 'SUCCESS', {orders}L, {items}L,
       {options.bronze_row_count}L, current_timestamp()
""".strip(),
            ),
        ]
    )
    return tuple(statements)
