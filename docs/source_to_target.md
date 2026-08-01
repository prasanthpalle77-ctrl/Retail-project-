# Initial Source-to-Target Map

This mapping defines dataset-level lineage. Column-level mappings will be added with the executable schemas in the ingestion milestone.

| Source | Landing path | Bronze | Silver | Primary Gold targets |
|---|---|---|---|---|
| customers | `landing/customers` | `bronze_customers` | `silver_customers` | `dim_customer` |
| products | `landing/products` | `bronze_products` | `silver_products` | `dim_product` |
| stores | `landing/stores` | `bronze_stores` | `silver_stores` | `dim_store` |
| suppliers | `landing/suppliers` | `bronze_suppliers` | `silver_suppliers` | `dim_supplier` |
| orders | `landing/orders` | `bronze_orders` | `silver_orders` | `fact_sales` |
| order_items | `landing/order_items` | `bronze_order_items` | `silver_order_items` | `fact_order_items` |
| payments | `landing/payments` | `bronze_payments` | `silver_payments` | `fact_payments` |
| returns | `landing/returns` | `bronze_returns` | `silver_returns` | `fact_returns` |
| inventory | `landing/inventory` | `bronze_inventory_events` | `silver_inventory_movements` | `fact_inventory_movements`, `fact_inventory_snapshot` |
| promotions | `landing/promotions` | `bronze_promotions` | `silver_promotions` | `dim_promotion`, promotion aggregates |
| shipments | `landing/shipments` | `bronze_shipments` | `silver_shipments` | `fact_shipments` |
| customer events | `landing/customer_events` | `bronze_customer_events` | `silver_customer_events` | `fact_customer_events`, funnel aggregates |
| business documents | `documents/` | document manifest | normalized chunks | vector index and RAG citations |

## Common ingestion metadata

Every Bronze record includes:

- `_source_system`
- `_source_file_path`
- `_source_file_name`
- `_source_file_size`
- `_source_file_modified_at`
- `_ingested_at`
- `_pipeline_run_id`
- `_batch_id`
- `_schema_version`
- `_record_hash`
- `_rescued_data`

Every Silver and Gold record carries enough upstream keys and run metadata to trace it to the Bronze evidence and pipeline execution.
