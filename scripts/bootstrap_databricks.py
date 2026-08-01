"""Create the governed Unity Catalog namespaces and managed data volume."""

from __future__ import annotations

import argparse
import re
from collections.abc import Sequence
from typing import Any

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
DEFAULT_SCHEMAS = ("bronze", "silver", "gold", "governance", "rag", "platform")


def quote_identifier(value: str) -> str:
    """Allow only portable identifiers before adding SQL identifier quoting."""

    if not _IDENTIFIER.fullmatch(value):
        raise ValueError(f"Unsafe Unity Catalog identifier: {value!r}")
    return f"`{value}`"


def build_bootstrap_statements(
    catalog: str,
    *,
    schemas: Sequence[str] = DEFAULT_SCHEMAS,
    volume_schema: str = "platform",
    volume_name: str = "data",
) -> tuple[str, ...]:
    """Build idempotent DDL without accepting arbitrary SQL fragments."""

    catalog_sql = quote_identifier(catalog)
    statements = [f"CREATE CATALOG IF NOT EXISTS {catalog_sql}"]
    statements.extend(
        f"CREATE SCHEMA IF NOT EXISTS {catalog_sql}.{quote_identifier(schema)}"
        for schema in schemas
    )
    statements.append(
        "CREATE VOLUME IF NOT EXISTS "
        f"{catalog_sql}.{quote_identifier(volume_schema)}.{quote_identifier(volume_name)}"
    )
    return tuple(statements)


def execute_bootstrap(spark: Any, catalog: str) -> tuple[str, ...]:
    statements = build_bootstrap_statements(catalog)
    for statement in statements:
        spark.sql(statement)
    return statements


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        from pyspark.sql import SparkSession
    except ImportError as error:
        raise RuntimeError(
            "This entrypoint must run on Databricks or with PySpark installed."
        ) from error
    spark = SparkSession.builder.appName("NovaRetailBootstrap").getOrCreate()
    statements = execute_bootstrap(spark, args.catalog)
    print(f"Applied {len(statements)} idempotent Unity Catalog statements for {args.catalog}.")
    return 0


if __name__ == "__main__":
    main()
