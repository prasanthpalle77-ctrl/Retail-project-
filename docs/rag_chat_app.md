# NovaRetail Copilot Chat App

The Streamlit application in `apps/retail_copilot` provides a chat-style interface for the
governed RAG system. It keeps conversation history in the browser session and shows certified
rows, citations, and approved SQL in expandable sections.

Live development app:

`https://novaretail-copilot-7474648027961612.aws.databricksapps.com`

## Evidence and permissions

The application has read-only access to six certified Unity Catalog tables and `CAN_USE` on the
Serverless Starter Warehouse. Databricks creates a dedicated service principal for the app. No
personal token or GitHub password is stored in the application.

The two evidence routes remain unchanged:

- Governed Markdown documents answer policy and operations questions.
- Pre-approved, read-only SQL templates answer data-arrival and Gold KPI questions.

## Deploy

Build the wheel and assemble the deployment directory:

```powershell
python -m pip wheel . --no-deps --wheel-dir dist
python scripts\stage_copilot_app.py --output C:\tmp\novaretail-copilot-app
```

Create the app once:

```powershell
databricks apps create novaretail-copilot `
  --json @apps/retail_copilot/resources.dev.json `
  --profile novaretail-dev
```

Upload and deploy:

```powershell
databricks sync C:\tmp\novaretail-copilot-app `
  /Workspace/Users/prasanthpalle077@gmail.com/novaretail-copilot `
  --profile novaretail-dev

databricks apps deploy novaretail-copilot `
  --source-code-path /Workspace/Users/prasanthpalle077@gmail.com/novaretail-copilot `
  --profile novaretail-dev
```

Free Edition can stop an app after 24 hours. Open **Databricks Apps**, select
`novaretail-copilot`, and click **Start** to restart it.
