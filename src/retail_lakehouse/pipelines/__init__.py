"""End-to-end medallion pipeline entry points."""

from retail_lakehouse.pipelines.silver import SilverPipelineResult, run_silver_dataset

__all__ = ["SilverPipelineResult", "run_silver_dataset"]
