# Production data, KPIs, and RAG operations

## Where the data is stored

All registered data is in the Unity Catalog catalog `novaretail_prod`.

| Layer | Catalog Explorer location | Purpose |
|---|---|---|
| Bronze | `novaretail_prod.bronze` | Raw customers, products, stores, orders, items, payments, events, and inventory |
| Silver | `novaretail_prod.silver` | Standardized and governed business data |
| Gold | `novaretail_prod.gold` | Facts, dimensions, aggregates, and certified KPIs |
| Governance | `novaretail_prod.governance` | Data-arrival checks and pipeline history |

The production scale is 5,000,000 orders, 10,000,000 order items, and 26,601,000 Bronze rows. In Databricks, open **Catalog**, choose `novaretail_prod`, and select the required schema and table.

## How to trigger the data load

Open **Jobs & Pipelines**, select **novaretail-prod-big-data-load**, and click **Run now**. The production defaults are `order_count = 5000000` and `batch_id = production_v1`.

The workflow is manual and has no schedule. It generates the rows with distributed Spark operations inside Databricks.

## How to confirm data arrived

Run this query in the Databricks SQL Editor:

```sql
SELECT *
FROM novaretail_prod.governance.data_arrival_status
ORDER BY layer, object_name;
```

Every critical row must show `arrival_status = 'ARRIVED'`, and `actual_rows` must equal `expected_rows`. Load history is available in `novaretail_prod.governance.pipeline_runs`.

Independent count checks:

```sql
SELECT count(*) FROM novaretail_prod.bronze.orders;
SELECT count(*) FROM novaretail_prod.bronze.order_items;
SELECT count(*) FROM novaretail_prod.gold.fact_sales;
SELECT count(*) FROM novaretail_prod.gold.fact_order_items;
```

The expected counts are 5,000,000 orders and 10,000,000 order items in both the source and Gold facts.

## KPIs and their business use

| KPI | Meaning | Business use |
|---|---|---|
| Gross sales | Order value before discounts | Measures demand and top-line selling activity |
| Net sales | Gross sales minus discounts | Shows performance after commercial incentives |
| Net revenue | Net sales plus tax and shipping | Supports finance reporting and channel comparison |
| Average order value | Net sales divided by distinct orders | Helps increase customer basket value |
| Return rate | Returned units divided by sold units | Identifies product, fulfillment, and expectation issues |
| Conversion rate | Purchasing sessions divided by eligible sessions | Measures the digital shopping funnel |
| Stockout rate | Zero-stock observations divided by inventory observations | Identifies lost-sales and replenishment risk |

Certified daily metrics are in `novaretail_prod.gold.retail_kpi_daily`; the current summary is in `novaretail_prod.gold.kpi_summary`. Product returns, funnel conversion, and inventory health are in the `agg_*` Gold tables.

## Why RAG is used

RAG gives the Copilot governed evidence before it answers:

1. Document retrieval answers policy and operating questions with citations.
2. Approved read-only SQL answers numerical questions from certified Gold tables.

This prevents invented policies and prevents calculations from unapproved raw data. The system enforces prompt-injection checks, table allowlists, read-only SQL, row limits, citations, and refusal when evidence is insufficient.

Open the [NovaRetail Copilot](https://novaretail-copilot-7474648027961612.aws.databricksapps.com) and ask questions such as:

- `Has the data arrived?`
- `Show all KPIs and their meanings.`
- `What are the latest net sales and average order value by channel?`
- `What is the latest stockout rate?`
- `What is the return window for an unopened item?`

The production workflow **novaretail-prod-rag-copilot-query** provides the same governed answer, citations, approved SQL, and result rows in a job task output.

## Example queries

```sql
SELECT * FROM novaretail_prod.gold.kpi_summary ORDER BY metric_name;
```

```sql
SELECT date_key, channel, net_sales, net_revenue, average_order_value
FROM novaretail_prod.gold.retail_kpi_daily
ORDER BY date_key DESC, channel
LIMIT 30;
```

```sql
SELECT *
FROM novaretail_prod.gold.agg_inventory_health
ORDER BY stockout_rate DESC
LIMIT 30;
```
