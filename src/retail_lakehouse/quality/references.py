"""Enrich a dataset with configuration-driven cross-table rule outcomes."""

from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Any

from retail_lakehouse.quality.models import QualityRule


def enrich_reference_rules(
    spark: Any,
    frame: Any,
    rules: tuple[QualityRule, ...],
    *,
    silver_root: Path,
) -> tuple[Any, tuple[QualityRule, ...]]:
    """Join canonical reference tables and rewrite reference rules to pass flags."""

    try:
        from delta.tables import DeltaTable
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark and delta-spark are required for reference rules.") from exc

    enriched = frame
    executable = []
    for rule in rules:
        if not rule.rule_type.startswith("reference_"):
            executable.append(rule)
            continue
        if rule.reference_dataset is None:
            raise ValueError(f"Reference dataset is missing for {rule.rule_id}.")
        target = (silver_root / rule.reference_dataset).resolve()
        if not DeltaTable.isDeltaTable(spark, str(target)):
            raise RuntimeError(
                f"Reference table for {rule.rule_id} does not exist: {target}. "
                "Run upstream Silver datasets first."
            )

        token = rule.rule_id.replace("-", "_")
        key_aliases = [
            f"_reference_{token}_key_{index}" for index in range(len(rule.local_columns))
        ]
        reference = spark.read.format("delta").load(str(target))
        projections = [
            functions.col(reference_name).alias(alias)
            for reference_name, alias in zip(rule.reference_columns, key_aliases, strict=True)
        ]
        marker = f"_reference_{token}_marker"
        projections.append(functions.lit(True).alias(marker))
        value_column = f"_reference_{token}_value"
        if rule.reference_value_column:
            projections.append(functions.col(rule.reference_value_column).alias(value_column))
        reference = reference.select(*projections).dropDuplicates(key_aliases)
        left = enriched.alias("source")
        right = reference.alias("reference")
        join_condition = None
        for local_name, alias in zip(rule.local_columns, key_aliases, strict=True):
            comparison = functions.col(f"source.`{local_name}`").eqNullSafe(
                functions.col(f"reference.`{alias}`")
            )
            join_condition = comparison if join_condition is None else join_condition & comparison
        enriched = left.join(right, join_condition, "left").select(
            "source.*", *[functions.col(f"reference.`{name}`") for name in reference.columns]
        )

        pass_column = f"_reference_{token}_passed"
        if rule.rule_type == "reference_exists":
            passed = functions.coalesce(functions.col(marker), functions.lit(False))
        else:
            expression = rule.rule_expression.replace("_reference_value", f"`{value_column}`")
            passed = functions.col(marker).isNotNull() & functions.coalesce(
                functions.expr(expression).cast("boolean"), functions.lit(False)
            )
        enriched = enriched.withColumn(pass_column, passed).drop(*key_aliases, marker, value_column)
        executable.append(replace(rule, rule_expression=f"`{pass_column}`"))
    return enriched, tuple(executable)
