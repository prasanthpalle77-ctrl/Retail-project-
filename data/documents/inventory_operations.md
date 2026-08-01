---
document_id: RUNBOOK-INVENTORY-001
title: Store Inventory Replenishment Runbook
version: "1.0"
effective_date: "2026-01-01"
security_class: internal
source_uri: knowledge://operations/inventory
---
# Reorder workflow

When available stock reaches or falls below the product-store reorder point, the replenishment service creates a proposed purchase order. The inventory planner reviews supplier lead time, open orders, safety stock, and promotion demand before approval.

# Stockout response

For a zero on-hand balance, store operations first confirm that receipts, transfers, and cycle counts are posted. The planner then checks nearby-store transfer options and supplier availability. Priority products are escalated when the expected stockout duration exceeds one business day.

# Inventory controls

Every manual adjustment requires a reason code and operator identifier. Negative on-hand quantities are quarantined from certified inventory reporting and investigated against shipment, receipt, sale, return, and adjustment events.
