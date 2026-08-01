# Big data, arrival monitoring, KPIs, and RAG operations

## Where the data is stored

The portfolio-scale load creates registered Unity Catalog objects in catalog `novaretail_dev`.

| Layer | Catalog Explorer location | Purpose |
|---|---|---|
| Bronze | `novaretail_dev.bronze` | Raw generated customers, products, stores, orders, items, payments, events, and inventory |
| Silver | `novaretail_dev.silver` | Queryable standardized views over the governed Bronze contracts |
| Gold | `novaretail_dev.gold` | Materialized dimensions, facts, aggregates, and certified KPI tables |
| Governance | `novaretail_dev.governance` | Data-arrival checks and load-run history |

The main scale is 5,000,000 orders and 10,000,000 order items. Bronze contains 26,601,000 rows across eight source objects before counting the queryable Silver layer and materialized Gold model.

In the Databricks UI, open **Catalog**, select `novaretail_dev`, and then select `bronze`, `silver`, `gold`, or `governance`.

## How to trigger the load

Open **Workflows**, select **novaretail-dev-big-data-load**, and click **Run now**. Its default parameters are:

- `order_count = 5000000`
- `batch_id = bigdata_v1`

The job is manual and has no schedule, so it cannot consume compute unexpectedly. It creates data with distributed Spark `range` queries inside Databricks; it does not generate millions of records on the laptop.

## How to know data arrived

The most direct check is:

```sql
SELECT *
FROM novaretail_dev.governance.data_arrival_status
ORDER BY layer, object_name;
```

Every row must show `arrival_status = 'ARRIVED'`, and `actual_rows` must equal `expected_rows`.

Load history is available with:

```sql
SELECT *
FROM novaretail_dev.governance.pipeline_runs
ORDER BY completed_at DESC;
```

Useful independent count checks are:

```sql
SELECT count(*) FROM novaretail_dev.bronze.orders;
SELECT count(*) FROM novaretail_dev.bronze.order_items;
SELECT count(*) FROM novaretail_dev.gold.fact_sales;
SELECT count(*) FROM novaretail_dev.gold.fact_order_items;
```

Expected results are 5,000,000 orders and 10,000,000 order items in both the source and analytical facts.

## KPIs and why they matter

| KPI | Meaning | Business use |
|---|---|---|
| Gross sales | Order value before discounts | Measures demand and top-line selling activity |
| Net sales | Gross sales minus discounts | Shows selling performance after commercial incentives |
| Net revenue | Net sales plus tax and shipping | Supports finance reporting and channel comparison |
| Average order value | Net sales divided by distinct orders | Helps merchandising and marketing increase basket value |
| Return rate | Returned units divided by sold units | Finds product-quality, fulfillment, and customer-expectation issues |
| Conversion rate | Purchasing sessions divided by eligible sessions | Measures effectiveness of the digital shopping funnel |
| Stockout rate | Zero-stock observations divided by inventory observations | Identifies lost-sales risk and replenishment problems |

The daily certified metrics are in `novaretail_dev.gold.retail_kpi_daily`. A compact current summary is in `novaretail_dev.gold.kpi_summary`. Product returns, funnel conversion, and inventory health are in the three `agg_*` Gold tables.

## Why RAG is implemented

Traditional dashboards answer predefined numerical questions, but users also ask policy and operational questions in natural language. RAG combines two governed evidence routes:

1. Document retrieval answers questions about return policy, inventory procedures, data-quality recovery, and KPI definitions with citations.
2. Approved SQL retrieval answers numerical questions only from certified Gold KPI tables.

This prevents the assistant from inventing policies or calculating business metrics from unapproved raw data. Prompt-injection checks, table allowlists, read-only SQL validation, row limits, citations, and refusal behavior are enforced.

## Where RAG can be used

- Store support: “What is the return window?”
- Inventory operations: “What should we do when stock reaches zero?”
- Data engineering support: “How do I recover a failed quality pipeline?”
- Executives and analysts: “What are the latest net sales and average order value by channel?”
- E-commerce teams: “What is the conversion rate by device?”
- Merchandising: “Which products have the highest return rate?”

To run it, open **Workflows**, select **novaretail-dev-rag-copilot-query**, click **Run now with different parameters**, enter the question, and run the job. The task output contains the answer, citations, approved SQL, and result rows. The RAG job never modifies a table.

Useful governed questions include:

- `Has the data arrived?`
- `Show all KPIs and their meanings`
- `What are the latest net sales and average order value by channel?`
- `What is the latest stockout rate?`

## Example SQL queries

```sql
SELECT * FROM novaretail_dev.gold.kpi_summary ORDER BY metric_name;
```

```sql
SELECT date_key, channel, net_sales, net_revenue, average_order_value
FROM novaretail_dev.gold.retail_kpi_daily
ORDER BY date_key DESC, channel
LIMIT 30;
```

```sql
SELECT *
FROM novaretail_dev.gold.agg_inventory_health
ORDER BY stockout_rate DESC
LIMIT 30;
```
