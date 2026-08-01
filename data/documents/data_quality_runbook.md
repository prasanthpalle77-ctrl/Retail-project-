---
document_id: RUNBOOK-DQ-001
title: Retail Data Quality Incident Runbook
version: "1.0"
effective_date: "2026-01-01"
security_class: internal
source_uri: knowledge://operations/data-quality
---
# Triage

Start with the pipeline audit record, failed rule identifiers, affected dataset, run identifier, and failure percentage. Determine whether the issue is schema drift, a duplicate, a broken reference, an invalid status, a reconciliation variance, or a late event.

# Recovery

Correct the source or governed rule and replay the immutable Landing batch. Bronze ingestion is checksum-aware, Silver writes are idempotent, and failed records remain in quarantine with their rule identifiers. Do not edit certified Gold tables manually.

# Closure

Reconcile source, accepted, quarantined, and published counts. Record the root cause, corrective action, affected business dates, rerun identifier, and evidence that downstream KPIs were republished successfully.
