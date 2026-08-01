"""Run the offline governed RAG evaluation set."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from retail_lakehouse.rag import ApprovedSqlCatalog, LexicalIndex, RetailCopilot, load_documents
from retail_lakehouse.rag.evaluation import evaluate


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--project-root", type=Path, default=None)
    return parser.parse_args()


def resolve_project_root(configured: Path | None, script_path: str | None) -> Path:
    """Resolve the repository root in local and Databricks serverless execution."""
    if configured is not None:
        return configured.resolve()
    if script_path is not None:
        return Path(script_path).resolve().parents[1]
    return Path.cwd().resolve()


def main() -> None:
    args = parse_args()
    script_path = globals().get("__file__")
    root = resolve_project_root(args.project_root, str(script_path) if script_path else None)
    copilot = RetailCopilot(
        LexicalIndex(load_documents(root / "data" / "documents")),
        ApprovedSqlCatalog.load(root / "configs" / "rag_sql_templates.yml"),
    )
    report = evaluate(copilot, root / "configs" / "rag_evaluation.yml")
    print(json.dumps(report, indent=2))
    if report["pass_rate"] != 1.0:
        raise RuntimeError("RAG evaluation did not achieve a 100% pass rate.")


if __name__ == "__main__":
    main()
