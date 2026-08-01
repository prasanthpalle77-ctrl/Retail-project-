"""Create local or managed Spark sessions from NovaRetail configuration."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from retail_lakehouse.runtime import configure_java


def create_spark_session(settings: dict[str, Any]) -> Any:
    """Build a Spark session and enable Delta extensions in local mode.

    Imports are lazy so documentation, configuration, and non-Spark unit tests
    remain usable before Java and PySpark are installed.
    """

    project_root = Path(settings["project_root"])
    configure_java(project_root)

    try:
        from pyspark.sql import SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "PySpark is not installed. Install requirements.txt and Java 17 before running Spark."
        ) from exc

    spark_settings = settings["spark"]
    builder = SparkSession.builder.appName(spark_settings["app_name"])
    if master := spark_settings.get("master"):
        builder = builder.master(str(master))
    builder = builder.config(
        "spark.sql.shuffle.partitions", str(spark_settings.get("shuffle_partitions", 4))
    )

    if spark_settings.get("enable_delta", False):
        try:
            from delta import configure_spark_with_delta_pip
        except ImportError as exc:
            raise RuntimeError("Delta Lake support requires the delta-spark package.") from exc
        builder = builder.config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
        ).config(
            "spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        return configure_spark_with_delta_pip(builder).getOrCreate()

    return builder.getOrCreate()
