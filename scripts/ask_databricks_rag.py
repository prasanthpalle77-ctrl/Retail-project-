"""Ask the governed NovaRetail copilot using live Databricks Gold tables."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from retail_lakehouse.rag import ApprovedSqlCatalog, LexicalIndex, RetailCopilot, load_documents


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--question", required=True)
    parser.add_argument("--catalog", default="novaretail_dev")
    parser.add_argument("--project-root", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError("PySpark is required for live Gold RAG queries.") from exc

    spark = SparkSession.getActiveSession() or SparkSession.builder.getOrCreate()
    spark.sql(f"USE CATALOG `{args.catalog}`")
    spark.sql("USE SCHEMA `gold`")

    def execute_sql(sql: str) -> Sequence[Mapping[str, Any]]:
        return [row.asDict(recursive=True) for row in spark.sql(sql).collect()]

    copilot = RetailCopilot(
        LexicalIndex(load_documents(args.project_root / "data" / "documents")),
        ApprovedSqlCatalog.load(args.project_root / "configs" / "rag_sql_templates.yml"),
        sql_executor=execute_sql,
    )
    response = copilot.ask(args.question)
    print(json.dumps(response.to_dict(), indent=2, default=str))


if __name__ == "__main__":
    main()
