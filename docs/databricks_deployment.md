# Production Databricks deployment

NovaRetail has one Databricks Declarative Automation Bundle target: `prod`. It deploys four manual production workflows: batch medallion processing, available-now streams, the distributed 5-million-order load, and governed RAG queries.

Bundle source is synchronized as workspace files and the Python package is installed as a wheel on serverless compute. Business logic remains in `src/retail_lakehouse` instead of notebooks.

## Cost boundary

`bundle validate`, `bundle plan`, and `bundle deploy` do not run the production jobs. Compute usage begins when a workflow is explicitly started. The workflows have no schedules, finite timeouts, and concurrency limits. Databricks Free Edition is subject to platform quotas; paid workspaces charge according to their serverless and cloud usage.

## Deploy from a workstation

Authenticate the current Databricks CLI with personal OAuth. Never store a password, personal access token, or client secret in Git.

```powershell
python scripts\validate_environment.py
python scripts\validate_bundle.py
databricks bundle validate --target prod
databricks bundle plan --target prod
databricks bundle deploy --target prod
databricks bundle summary --target prod
```

Review `bundle plan` before deployment. Keep the production `root_path` unchanged so subsequent deployments update the same managed resources.

## Run production workflows

```powershell
databricks bundle run --target prod retail_big_data_load
databricks bundle run --target prod rag_copilot_query
databricks bundle run --target prod retail_batch_pipeline
databricks bundle run --target prod retail_streaming_pipeline
```

Batch inputs and streaming chunks belong under `/Volumes/novaretail_prod/platform/data/`. Available-now streams process the files currently present, commit their checkpoints, and terminate.

## GitHub deployment

The **Databricks Production Deployment** GitHub Actions workflow is manually dispatched. Configure the protected `prod` GitHub environment with `DATABRICKS_HOST` and `DATABRICKS_CLIENT_ID`, and configure Databricks workload identity federation for the GitHub identity. No client secret is required.

Record the Git commit, CLI version, plan output, deployment URL, resource summary, operator, and approval for every production change. Store no tokens or secrets in the evidence.
