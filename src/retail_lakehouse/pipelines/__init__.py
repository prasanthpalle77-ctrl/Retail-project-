"""End-to-end medallion pipeline entry points."""

from retail_lakehouse.pipelines.gold import GoldPipelineResult, run_gold_pipeline
from retail_lakehouse.pipelines.silver import SilverPipelineResult, run_silver_dataset

__all__ = [
    "GoldPipelineResult",
    "SilverPipelineResult",
    "run_gold_pipeline",
    "run_silver_dataset",
]
