# Production security and governance

## Identity

Interactive administration uses authenticated Databricks OAuth. Automated GitHub deployment uses workload identity federation with the protected `prod` environment. No Databricks password, personal access token, or client secret is stored in Git.

The deployment identity, data engineers, analysts, and Copilot service principal have separate privileges. Grant each identity only the permissions required for its role.

## Unity Catalog

The single governed catalog is `novaretail_prod`. Its schemas are `bronze`, `silver`, `gold`, `governance`, `rag`, and `platform`; the `platform.data` managed Volume stores production files, Delta paths, documents, checkpoints, and quarantine output.

- Data engineers manage Bronze and Silver operational objects.
- Analytics users select from Gold without default Bronze access.
- The Copilot selects only approved Gold and governance tables.
- The deployment identity manages the production bundle without account-admin rights.

## Data and RAG controls

Quarantined records inherit the highest classification of their source. Restrict Volume browsing and audit access accordingly. Documents carry identifiers, versions, effective dates, security classes, and source URIs.

Numerical Copilot responses come only from executed approved SQL. SQL is single-statement, read-only, table-allowlisted, row-limited, and returned with its evidence. Unknown or unauthorized questions are refused.

## Secrets and audit

Local secrets belong in an ignored `.env` or authenticated CLI profile; workspace secrets belong in a Databricks secret scope or cloud secret manager. Revoke any credential exposed in Git, logs, screenshots, chat, or artifacts.

Retain deployment identity, commit, plan, job runs, quality results, quarantines, RAG questions after redaction, citations, SQL, latency, safety decisions, and incident evidence according to organizational policy.
