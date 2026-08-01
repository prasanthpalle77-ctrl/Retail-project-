"""Bronze-to-Silver orchestration with quarantine and audit persistence."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from retail_lakehouse.quality import enrich_reference_rules, evaluate_quality, load_quality_rules
from retail_lakehouse.storage import merge_current_state, merge_insert_or_update
from retail_lakehouse.transformations.silver import (
    SILVER_SPECS,
    deduplicate_latest,
    standardize_bronze,
)
from retail_lakehouse.utils.hashing import sha256_text


@dataclass(frozen=True)
class SilverPipelineResult:
    """Reconciliation counts for one canonical dataset run."""

    dataset_name: str
    run_id: str
    bronze_rows: int
    standardized_rows: int
    duplicates_removed: int
    accepted_rows: int
    quarantined_rows: int
    rules_evaluated: int
    silver_path: str
    quarantine_path: str


def run_silver_dataset(
    spark: Any,
    *,
    dataset_name: str,
    run_id: str,
    bronze_root: Path,
    silver_root: Path,
    quarantine_root: Path,
    rules_path: Path,
) -> SilverPipelineResult:
    """Standardize, deduplicate, validate, quarantine, and merge one Bronze table."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for the Silver pipeline.") from exc
    if dataset_name not in SILVER_SPECS:
        raise ValueError(f"Unknown Silver dataset: {dataset_name}")

    bronze_path = (bronze_root / dataset_name).resolve()
    bronze = spark.read.format("delta").load(str(bronze_path))
    bronze_rows = bronze.count()
    standardized = standardize_bronze(bronze, dataset_name)
    standardized_rows = standardized.count()
    deduplicated = deduplicate_latest(standardized, dataset_name).cache()
    deduplicated_rows = deduplicated.count()
    rules = load_quality_rules(rules_path, dataset_name)
    deduplicated, rules = enrich_reference_rules(
        spark, deduplicated, rules, silver_root=silver_root
    )
    evaluation = evaluate_quality(
        deduplicated,
        rules,
        run_id=run_id,
        pipeline_name="bronze_to_silver",
        dataset_name=dataset_name,
    )
    reference_columns = [name for name in deduplicated.columns if name.startswith("_reference_")]
    accepted = evaluation.accepted.drop(*reference_columns).cache()
    quarantined = evaluation.quarantined.drop(*reference_columns).cache()
    accepted_rows = accepted.count()
    quarantined_rows = quarantined.count()
    if accepted_rows + quarantined_rows != deduplicated_rows:
        raise RuntimeError(f"Silver reconciliation failed for {dataset_name}.")

    spec = SILVER_SPECS[dataset_name]
    silver_path = (silver_root / dataset_name).resolve()
    merge_current_state(
        spark,
        accepted.drop("_failed_rule_ids"),
        silver_path,
        business_keys=spec.business_keys,
        event_timestamp=spec.event_timestamp,
        operation_column="cdc_operation" if "cdc_operation" in accepted.columns else None,
    )

    quarantine_path = (quarantine_root / dataset_name).resolve()
    if quarantined_rows:
        quarantine = quarantined.withColumn(
            "_quarantine_id",
            functions.sha2(
                functions.concat_ws(
                    "|",
                    functions.lit(dataset_name),
                    *[
                        functions.coalesce(functions.col(key), functions.lit("<NULL>"))
                        for key in spec.business_keys
                    ],
                    functions.array_join("_failed_rule_ids", ","),
                    functions.col("_silver_record_hash"),
                ),
                256,
            ),
        )
        merge_insert_or_update(spark, quarantine, quarantine_path, identifier="_quarantine_id")

    if evaluation.metrics:
        metric_rows = []
        for metric in evaluation.metrics:
            values = asdict(metric)
            values["quality_metric_id"] = sha256_text(
                f"{metric.run_id}|{metric.dataset_name}|{metric.rule_id}"
            )
            metric_rows.append(values)
        metrics = spark.createDataFrame(metric_rows)
        merge_insert_or_update(
            spark,
            metrics,
            (silver_root / "_quality_results").resolve(),
            identifier="quality_metric_id",
        )

    accepted.unpersist()
    quarantined.unpersist()
    deduplicated.unpersist()
    return SilverPipelineResult(
        dataset_name=dataset_name,
        run_id=run_id,
        bronze_rows=bronze_rows,
        standardized_rows=standardized_rows,
        duplicates_removed=standardized_rows - deduplicated_rows,
        accepted_rows=accepted_rows,
        quarantined_rows=quarantined_rows,
        rules_evaluated=len(evaluation.metrics),
        silver_path=str(silver_path),
        quarantine_path=str(quarantine_path),
    )
