# Databricks deployment guide

NovaRetail uses a Databricks Declarative Automation Bundle (formerly Asset Bundles) to deploy the same tested Python code to development, staging, and production. The bundle contains three Lakeflow Jobs: the batch medallion pipeline, checkpointed available-now streams, and the offline RAG evaluation.

The configuration follows the current Databricks bundle lifecycle: validate, plan, deploy, and run. Bundle source is synchronized as workspace files, the Python package is built as a wheel, and serverless environment version 3 installs that wheel for every task. No notebook contains business logic.

Official references: [Declarative Automation Bundles](https://docs.databricks.com/aws/en/dev-tools/bundles/), [bundle configuration](https://docs.databricks.com/aws/en/dev-tools/bundles/settings), [bundle commands](https://docs.databricks.com/aws/en/dev-tools/cli/bundle-commands), and [GitHub Actions integration](https://docs.databricks.com/aws/en/dev-tools/ci-cd/github).

## Cost boundary

Repository code, local structural validation, GitHub CI, and bundle planning do not create Databricks compute. `bundle deploy` creates or updates workspace job definitions but does not run the jobs. Compute usage starts only when a job is explicitly run. The jobs have no schedules, use maximum-concurrency limits, and use finite timeouts. Databricks Free Edition may cover learning-scale execution within its quotas; paid workspaces charge according to their serverless and cloud usage. Check the target workspace pricing and budget policies before running batch or streaming workloads.

## One-time workspace setup

1. Create or select a Databricks workspace with Unity Catalog and serverless Jobs enabled.
2. Install the current Databricks CLI and authenticate interactively for personal development. Do not place a password or personal access token in this repository.
3. Ensure the deployment identity can create the target catalog, schemas, managed Volume, and Jobs. Production should use a service principal.
4. For each GitHub environment (`dev`, `staging`, and `prod`), add repository environment variables `DATABRICKS_HOST` and `DATABRICKS_CLIENT_ID`.
5. Configure a Databricks workload-identity federation policy whose subject matches the GitHub environment. The deployment workflow uses `github-oidc`; no client secret is stored in GitHub.
6. Add required reviewers to the `staging` and `prod` GitHub environments.

Databricks currently recommends workload identity federation for automated deployments because it exchanges GitHub's short-lived OIDC identity instead of managing a long-lived Databricks secret. See [workload identity federation](https://docs.databricks.com/aws/en/dev-tools/auth/oauth-federation-provider).

## Validate and deploy from a workstation

Run the offline repository contract first:

```powershell
python scripts\validate_bundle.py
```

After configuring a Databricks CLI profile or unified-authentication environment variables:

```powershell
databricks bundle validate --target dev
databricks bundle plan --target dev
databricks bundle deploy --target dev
databricks bundle summary --target dev
```

`plan` previews remote changes. Deploy development first, run smoke tests, then promote the same Git commit to staging and production. Never use `--force` merely to bypass a branch or ownership error; investigate the mismatch.

## Load and run a development batch

Generate a small batch locally, then upload the complete generated batch directory to:

```text
/Volumes/novaretail_dev/platform/data/incoming/batch_id=<batch-id>/
```

The directory must contain `generation_report.json` and every source file referenced by the report. The cloud staging entrypoint falls back to matching filenames inside that uploaded directory, so local source paths embedded in the report are not trusted in the workspace.

Run the job with the uploaded batch identifier:

```powershell
databricks bundle run --target dev retail_batch_pipeline --params batch_id=<batch-id>
databricks bundle run --target dev rag_evaluation
```

For streaming, upload JSONL chunks to the `streaming_input/customer_events` and `streaming_input/inventory_events` directories under the same Volume root, then run:

```powershell
databricks bundle run --target dev retail_streaming_pipeline
```

The stream uses available-now triggers, processes the files currently present, commits checkpoints, and terminates. This is safer for demonstrations and controls cost. Continuous processing remains available through the underlying script but is intentionally not configured in the bundle.

## GitHub deployment

Open Actions, select **Databricks Bundle Deployment**, choose the target and `plan`, and run the workflow. Review the plan, then dispatch `deploy`. The optional smoke test runs only the no-data RAG evaluation. Batch and streaming jobs must be started separately after their input files are ready.

The manual workflow does not run when code is pushed. This prevents an unreviewed commit from consuming workspace compute or changing production resources.

## Promotion evidence

For every promotion, record the Git commit, bundle target, CLI version, plan output, deployment run URL, deployed resource summary, smoke-test run URL, operator identity, approval, and rollback commit. Store no tokens or secrets in the evidence.
