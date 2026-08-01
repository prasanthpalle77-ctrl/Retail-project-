"""Ask the local document copilot from a terminal."""

from __future__ import annotations

import argparse
from pathlib import Path

from retail_lakehouse.rag import ApprovedSqlCatalog, LexicalIndex, RetailCopilot, load_documents
from retail_lakehouse.rag.audit import QueryAuditLog


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("question")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    copilot = RetailCopilot(
        LexicalIndex(load_documents(root / "data" / "documents")),
        ApprovedSqlCatalog.load(root / "configs" / "rag_sql_templates.yml"),
        audit_log=QueryAuditLog(root / "logs" / "rag_queries.jsonl"),
    )
    response = copilot.ask(args.question)
    print(response.answer)
    for citation in response.citations:
        print(
            f"[{citation.citation_id}] {citation.title} / {citation.section}: {citation.source_uri}"
        )


if __name__ == "__main__":
    main()
