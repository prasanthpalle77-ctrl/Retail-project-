# Operations and recovery runbook

## Routine checks

For every job run, verify task status, duration, input batch or stream path, audit run identifiers, accepted and quarantined counts, reconciliation status, Gold publication counts, and RAG evaluation pass rate. Alert on a failed task, a critical quality-rule breach, a reconciliation mismatch, unexpected zero input, repeated late events, or a material duration increase.

The batch job is intentionally serialized with `max_concurrent_runs: 1`. Streaming checkpoints are isolated by stream name. Audit data, quality results, quarantine records, Delta transaction identifiers, and GitHub/Databricks run URLs form the operational evidence chain.

## Failed batch task

1. Stop downstream retries until the first failing task and its run identifier are known.
2. Preserve the immutable incoming and Landing files, manifests, logs, and quarantine output.
3. Classify the failure as source delivery, schema, quality, reference data, compute, permissions, or code.
4. Correct the source or code through Git; never edit Bronze, Silver, or Gold files manually.
5. Redeploy the corrected commit to development and rerun the same batch identifier. Landing checksums, Bronze transaction IDs, Silver merges, and Gold snapshot synchronization provide replay safety.
6. Confirm source-to-Landing, Bronze-to-Silver/quarantine, and Silver-to-Gold reconciliation before promotion.

## Streaming restart and checkpoint recovery

For an ordinary task failure, retain the checkpoint and rerun the available-now job. If a checkpoint is corrupt, copy it to an incident-preservation path, document the last committed offsets and affected files, choose a new checkpoint name, and replay the immutable source chunks. Confirm stable event identifiers prevent duplicate business facts before retiring the old checkpoint.

Never delete a checkpoint merely to make a job start. That destroys recovery evidence and can cause duplicate processing.

## Data rollback

Delta history is the first recovery source. Inspect table history and validate the proposed version in development before using `RESTORE TABLE` or rebuilding a table from immutable lower layers. Because this project stores Delta paths in a governed Volume, register a temporary table over the affected path when time-travel SQL is required.

Gold can be rebuilt from Silver; Silver can be rebuilt from Bronze plus quarantine rules; Bronze can be replayed from Landing. Prefer deterministic rebuilding over manual row correction.

## Deployment rollback

1. Identify the last healthy Git commit and its successful bundle deployment.
2. Revert the faulty change in Git through a new reviewed commit.
3. Run local CI, `bundle validate`, and `bundle plan` for the target.
4. Deploy the corrective commit; do not rewrite published Git history.
5. Rerun only the affected jobs or batches and attach reconciliation evidence.

Bundle state is scoped by bundle name, target, workspace, and root path. Do not change the production `root_path` during rollback, because that would create a second deployment identity instead of updating the existing resources.

## Disaster recovery

Recovery requires the Git repository, immutable source/Landing data, Delta tables or backups, streaming checkpoints, Unity Catalog grants, secret-scope definitions without secret values, and deployment/audit history. Test restoration in an isolated catalog at least quarterly. A successful exercise proves that one batch, one stream, certified Gold KPIs, and the RAG evaluation can be reproduced from documented inputs.

## Incident closure

Record timeline, impact, affected datasets and business dates, root cause, corrective and preventive actions, recovered versions, Git commit, deployment and job URLs, reconciliations, and approver. Remove temporary access and recovery copies according to retention policy after closure.
