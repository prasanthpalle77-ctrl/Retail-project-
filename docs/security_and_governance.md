# Security and governance guide

## Identity and deployment

Personal OAuth is acceptable for attended development. Staging and production deployments use a dedicated Databricks service principal and GitHub workload identity federation. GitHub receives permission to mint a short-lived OIDC token; no Databricks password, personal access token, or client secret is stored in Git or required by the deployment workflow.

Protect the GitHub `staging` and `prod` environments with reviewers and branch restrictions. Grant the service principal only the workspace and Unity Catalog privileges needed to manage this bundle. Separate the deployment identity from analyst and application identities.

## Unity Catalog model

Each environment has a separate catalog: `novaretail_dev`, `novaretail_staging`, or `novaretail_prod`. Schemas are `bronze`, `silver`, `gold`, `governance`, `rag`, and `platform`; the managed `platform.data` Volume stores files, Delta paths, documents, checkpoints, and quarantine output.

Recommended grants:

- Data engineers: use catalog, use schemas, read/write Volume, and modify Bronze/Silver operational objects.
- Analytics engineers: use catalog/schema and select on Gold; no Bronze PII access by default.
- RAG runtime: select only on approved Gold serving tables and read only authorized document paths.
- Analysts: select on Gold and execute approved copilot endpoints; no mutation privilege.
- Deployment service principal: create/manage bundle Jobs and the target namespaces, without account-admin rights.

Apply grants through workspace-approved infrastructure automation after replacing group placeholders. Review effective privileges before production deployment and quarterly thereafter.

## Data controls

Synthetic data is the repository default. Real customer identifiers must be classified, minimized, and masked before use. Quarantine retains failed records and therefore inherits the highest classification of its source. Restrict Volume browsing and audit access accordingly.

Documents carry document ID, version, effective date, security class, and source URI. Retrieval filters by an authorization context fixed by the service owner; API callers cannot self-elevate. Instruction-like retrieved content is excluded, PII is redacted from audit questions, and unknown questions refuse rather than improvise.

Numerical RAG responses come only from executed approved SQL. SQL is single-statement, read-only, allowlisted by table, row-limited, comment-free, and protected from set-operation and mutation bypasses. Return the executed SQL and evidence source with every metric answer.

## Secrets

Local secrets belong in an ignored `.env` or an authenticated CLI profile. Workspace secrets belong in Databricks secret scopes or a cloud secret manager. GitHub deployment uses OIDC environment variables `DATABRICKS_HOST` and `DATABRICKS_CLIENT_ID`; neither is a password, but environment changes must still be reviewed.

If any credential appears in Git, logs, screenshots, chat, or artifacts, revoke or rotate it immediately, remove it from active configuration, inspect access history, and follow the incident process. Deleting a later Git commit alone does not make an exposed secret safe.

## Audit and retention

Retain deployment identity, commit, target, plan, resource changes, job runs, pipeline audits, quality results, quarantines, RAG questions after redaction, citations, SQL, latency, safety decisions, and incident evidence according to organizational policy. Limit access, encrypt at rest and in transit, and test that retention deletion does not remove active recovery checkpoints.
