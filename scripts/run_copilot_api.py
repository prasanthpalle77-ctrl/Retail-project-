"""Run the optional local FastAPI copilot service."""

from __future__ import annotations

from pathlib import Path

from retail_lakehouse.rag import ApprovedSqlCatalog, LexicalIndex, RetailCopilot, load_documents
from retail_lakehouse.rag.api import create_app
from retail_lakehouse.rag.audit import QueryAuditLog

ROOT = Path(__file__).resolve().parents[1]
COPILOT = RetailCopilot(
    LexicalIndex(load_documents(ROOT / "data" / "documents")),
    ApprovedSqlCatalog.load(ROOT / "configs" / "rag_sql_templates.yml"),
    audit_log=QueryAuditLog(ROOT / "logs" / "rag_queries.jsonl"),
)
app = create_app(COPILOT)


if __name__ == "__main__":
    try:
        import uvicorn
    except ImportError as error:
        raise SystemExit("Install RAG dependencies with: pip install -e .[rag]") from error
    uvicorn.run(app, host="127.0.0.1", port=8000)
