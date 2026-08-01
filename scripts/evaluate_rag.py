"""Run the offline governed RAG evaluation set."""

from __future__ import annotations

import json
from pathlib import Path

from retail_lakehouse.rag import ApprovedSqlCatalog, LexicalIndex, RetailCopilot, load_documents
from retail_lakehouse.rag.evaluation import evaluate


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    copilot = RetailCopilot(
        LexicalIndex(load_documents(root / "data" / "documents")),
        ApprovedSqlCatalog.load(root / "configs" / "rag_sql_templates.yml"),
    )
    report = evaluate(copilot, root / "configs" / "rag_evaluation.yml")
    print(json.dumps(report, indent=2))
    if report["pass_rate"] != 1.0:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
