# Production NovaRetail Copilot

The Streamlit app in `apps/retail_copilot` provides a governed chat interface with conversation history, certified result rows, citations, and approved SQL.

Open it here: [NovaRetail Copilot](https://novaretail-copilot-7474648027961612.aws.databricksapps.com)

The Databricks app service principal has `CAN_USE` on the SQL warehouse and read-only access to approved `novaretail_prod.gold` and `novaretail_prod.governance` tables. It has no table mutation permission.

## Deploy the app

```powershell
python -m pip wheel . --no-deps --wheel-dir dist
python scripts\stage_copilot_app.py --output C:\tmp\novaretail-copilot-prod
databricks apps create-update novaretail-copilot `
  --json @apps/retail_copilot/resources.prod.json
databricks sync --full C:\tmp\novaretail-copilot-prod `
  /Workspace/Users/prasanthpalle077@gmail.com/novaretail-copilot
databricks apps start novaretail-copilot
```

Databricks Free Edition can stop app compute after its allowed active period. If the link is unavailable, open **Compute > Apps**, select `novaretail-copilot`, and click **Start**.
