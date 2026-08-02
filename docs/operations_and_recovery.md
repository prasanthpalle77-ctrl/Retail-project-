# Production operations and recovery

## Routine checks

For every run, verify task status, duration, run identifier, accepted and quarantined counts, reconciliation status, Gold publication counts, and data-arrival status. The production jobs use concurrency limits, finite timeouts, isolated streaming checkpoints, and manual triggers.

## Failed batch task

1. Identify the first failing task and run identifier.
2. Preserve incoming files, manifests, logs, and quarantine output.
3. Classify the cause as delivery, schema, quality, reference data, compute, permissions, or code.
4. Correct the source or code through Git; never edit Bronze, Silver, or Gold data files manually.
5. Deploy the corrective production commit and rerun the same batch identifier.
6. Confirm source, accepted, quarantined, and published counts before closing the incident.

## Streaming recovery

For an ordinary failure, retain the checkpoint and rerun the available-now workflow. If a checkpoint is corrupt, preserve a copy with the incident evidence, record the last committed offsets and affected files, choose a new checkpoint, and replay immutable source chunks.

Never delete a checkpoint merely to make a workflow start; doing so destroys recovery evidence and can cause duplicate processing.

## Data and deployment rollback

Use Delta history as the first data recovery source. Gold can be rebuilt from Silver, Silver from Bronze plus governed quality rules, and Bronze from immutable Landing data. Prefer deterministic rebuilding over manual row correction.

For a code rollback, revert the faulty change through a new Git commit, validate and plan the production bundle, deploy it, and rerun only the affected workflows. Do not rewrite published Git history or change the production bundle `root_path`.

## Incident closure

Record the timeline, impact, affected datasets and business dates, root cause, corrective and preventive actions, recovered versions, Git commit, deployment and rerun URLs, reconciliations, and approver. Confirm downstream KPIs were republished successfully before closure.
