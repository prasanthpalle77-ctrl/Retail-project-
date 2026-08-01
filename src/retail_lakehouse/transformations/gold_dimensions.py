"""Gold dimension construction and point-in-time surrogate-key resolution."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from retail_lakehouse.analytics.keys import (
    NOT_APPLICABLE_BUSINESS_KEY,
    UNKNOWN_BUSINESS_KEY,
    dimension_key,
)
from retail_lakehouse.transformations.scd2 import build_scd2_history

SCD_DIMENSION_CONTRACTS: dict[str, dict[str, Any]] = {
    "dim_customer": {
        "source": "customers",
        "business_key": "customer_id",
        "effective_timestamp": "updated_at",
        "tracked_columns": (
            "first_name",
            "last_name",
            "masked_email",
            "masked_phone",
            "city",
            "state",
            "country",
            "loyalty_tier",
            "customer_status",
            "registration_date",
        ),
    },
    "dim_product": {
        "source": "products",
        "business_key": "product_id",
        "effective_timestamp": "updated_at",
        "tracked_columns": (
            "sku",
            "product_name",
            "category",
            "subcategory",
            "brand",
            "supplier_id",
            "unit_cost",
            "list_price",
            "product_status",
            "launch_date",
        ),
    },
    "dim_store": {
        "source": "stores",
        "business_key": "store_id",
        "effective_timestamp": "updated_at",
        "tracked_columns": (
            "store_name",
            "store_type",
            "city",
            "state",
            "country",
            "region",
            "opening_date",
            "store_status",
        ),
    },
    "dim_supplier": {
        "source": "suppliers",
        "business_key": "supplier_id",
        "effective_timestamp": "updated_at",
        "tracked_columns": (
            "supplier_name",
            "country",
            "supplier_rating",
            "lead_time_days",
            "supplier_status",
        ),
    },
}


def project_dimension_source(frame: Any, dimension_name: str) -> Any:
    """Select governed business attributes and mask customer contact details."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold dimensions.") from exc
    if dimension_name not in SCD_DIMENSION_CONTRACTS:
        raise ValueError(f"Unknown SCD dimension: {dimension_name}")
    contract = SCD_DIMENSION_CONTRACTS[dimension_name]
    columns = [contract["business_key"], *contract["tracked_columns"], "updated_at"]
    if dimension_name == "dim_customer":
        return (
            frame.withColumn(
                "masked_email",
                functions.regexp_replace("email", r"(^.).*(@.*$)", "$1***$2"),
            )
            .withColumn(
                "masked_phone",
                functions.concat(
                    functions.lit("***-***-"),
                    functions.right(
                        functions.regexp_replace("phone", r"[^0-9]", ""), functions.lit(4)
                    ),
                ),
            )
            .select(*columns)
        )
    return frame.select(*columns)


def build_scd_dimension(
    source: Any,
    dimension_name: str,
    *,
    existing: Any | None = None,
) -> Any:
    """Build one SCD2 dimension and append explicit special members."""

    contract = SCD_DIMENSION_CONTRACTS[dimension_name]
    business_key = str(contract["business_key"])
    previous = None
    if existing is not None:
        previous = existing.where(
            ~existing[business_key].isin(UNKNOWN_BUSINESS_KEY, NOT_APPLICABLE_BUSINESS_KEY)
        )
    history = build_scd2_history(
        project_dimension_source(source, dimension_name),
        business_keys=(business_key,),
        tracked_columns=tuple(contract["tracked_columns"]),
        effective_timestamp=str(contract["effective_timestamp"]),
        existing=previous,
    )
    return add_special_members(history, dimension_name, business_key, "surrogate_key")


def add_special_members(
    frame: Any,
    dimension_name: str,
    business_key_column: str,
    surrogate_key_column: str,
) -> Any:
    """Append Unknown and Not Applicable rows using the exact target schema."""

    try:
        from pyspark.sql.types import (
            BooleanType,
            DateType,
            DecimalType,
            DoubleType,
            FloatType,
            IntegerType,
            LongType,
            ShortType,
            TimestampType,
        )
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold dimensions.") from exc

    rows = []
    for business_value, label in (
        (UNKNOWN_BUSINESS_KEY, "Unknown"),
        (NOT_APPLICABLE_BUSINESS_KEY, "Not Applicable"),
    ):
        values = []
        for field in frame.schema.fields:
            if field.name == surrogate_key_column:
                value: Any = dimension_key(dimension_name, business_value)
            elif field.name == business_key_column:
                value = business_value
            elif field.name == "attribute_hash":
                value = dimension_key(dimension_name, f"{business_value}|attributes")
            elif field.name == "valid_from":
                value = datetime(1900, 1, 1)
            elif field.name == "valid_to":
                value = None
            elif field.name == "is_current" or isinstance(field.dataType, BooleanType):
                value = True
            elif isinstance(field.dataType, TimestampType):
                value = datetime(1900, 1, 1)
            elif isinstance(field.dataType, DateType):
                value = date(1900, 1, 1)
            elif isinstance(field.dataType, DecimalType):
                value = Decimal("0")
            elif isinstance(
                field.dataType, (IntegerType, LongType, ShortType, FloatType, DoubleType)
            ):
                value = 0
            else:
                value = label
            values.append(value)
        rows.append(tuple(values))
    special = frame.sparkSession.createDataFrame(rows, frame.schema)
    return frame.unionByName(special)


def build_channel_dimension(spark: Any) -> Any:
    """Return the governed sales-channel mini-dimension."""

    channels = (
        ("STORE", "Physical Store"),
        ("WEB", "Web"),
        ("MOBILE", "Mobile Application"),
        ("MARKETPLACE", "Marketplace"),
        (UNKNOWN_BUSINESS_KEY, "Unknown"),
        (NOT_APPLICABLE_BUSINESS_KEY, "Not Applicable"),
    )
    return spark.createDataFrame(
        [(dimension_key("dim_channel", code), code, name) for code, name in channels],
        ["channel_key", "channel_code", "channel_name"],
    )


def build_promotion_dimension(promotions: Any) -> Any:
    """Build a deterministic Type 1 promotion dimension."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold dimensions.") from exc
    dimension = promotions.select(
        functions.sha2(
            functions.concat_ws("|", functions.lit("dim_promotion"), "promotion_id"), 256
        ).alias("promotion_key"),
        "promotion_id",
        "promotion_name",
        "promotion_type",
        "start_date",
        "end_date",
        "discount_percentage",
        "discount_amount",
        "category",
        "product_id",
        "minimum_order_value",
        "promotion_status",
    )
    return add_special_members(dimension, "dim_promotion", "promotion_id", "promotion_key")


def build_date_dimension(spark: Any, start_date: date, end_date: date) -> Any:
    """Generate a complete calendar dimension plus explicit special members."""

    try:
        from pyspark.sql import functions as functions
        from pyspark.sql.types import (
            BooleanType,
            DateType,
            IntegerType,
            StringType,
            StructField,
            StructType,
        )
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold dimensions.") from exc
    if end_date < start_date:
        raise ValueError("Date dimension end must not precede start.")
    dates = spark.sql(
        "SELECT explode(sequence("
        f"to_date('{start_date.isoformat()}'), to_date('{end_date.isoformat()}'), "
        "interval 1 day)) AS full_date"
    )
    calendar = dates.select(
        functions.date_format("full_date", "yyyyMMdd").cast("int").alias("date_key"),
        "full_date",
        functions.year("full_date").alias("year"),
        functions.quarter("full_date").alias("quarter"),
        functions.month("full_date").alias("month"),
        functions.date_format("full_date", "MMMM").alias("month_name"),
        functions.weekofyear("full_date").alias("week_of_year"),
        functions.dayofmonth("full_date").alias("day_of_month"),
        functions.date_format("full_date", "EEEE").alias("day_name"),
        (functions.dayofweek("full_date").isin(1, 7)).alias("is_weekend"),
    )
    special_schema = StructType(
        [
            StructField("date_key", IntegerType(), False),
            StructField("full_date", DateType(), True),
            StructField("year", IntegerType(), True),
            StructField("quarter", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("month_name", StringType(), True),
            StructField("week_of_year", IntegerType(), True),
            StructField("day_of_month", IntegerType(), True),
            StructField("day_name", StringType(), True),
            StructField("is_weekend", BooleanType(), True),
        ]
    )
    special = spark.createDataFrame(
        [
            (0, None, None, None, None, "Unknown", None, None, "Unknown", None),
            (-1, None, None, None, None, "Not Applicable", None, None, "Not Applicable", None),
        ],
        special_schema,
    )
    return calendar.unionByName(special)


def resolve_scd_key(
    frame: Any,
    dimension: Any,
    *,
    source_business_key: str,
    dimension_business_key: str,
    event_timestamp: str,
    output_key: str,
    dimension_name: str,
) -> Any:
    """Resolve the dimension version effective at a fact event timestamp."""

    try:
        from pyspark.sql import functions as functions
    except ImportError as exc:
        raise RuntimeError("PySpark is required for Gold key resolution.") from exc
    fact = frame.alias("fact")
    history = dimension.where(
        ~dimension[dimension_business_key].isin(UNKNOWN_BUSINESS_KEY, NOT_APPLICABLE_BUSINESS_KEY)
    ).alias("dimension")
    condition = (
        (
            functions.col(f"fact.`{source_business_key}`")
            == functions.col(f"dimension.`{dimension_business_key}`")
        )
        & (functions.col(f"fact.`{event_timestamp}`") >= functions.col("dimension.valid_from"))
        & (
            functions.col("dimension.valid_to").isNull()
            | (functions.col(f"fact.`{event_timestamp}`") <= functions.col("dimension.valid_to"))
        )
    )
    joined = fact.join(history, condition, "left").select(
        "fact.*", functions.col("dimension.surrogate_key").alias("_resolved_dimension_key")
    )
    return joined.withColumn(
        output_key,
        functions.when(
            functions.col(source_business_key).isNull(),
            functions.lit(dimension_key(dimension_name, NOT_APPLICABLE_BUSINESS_KEY)),
        ).otherwise(
            functions.coalesce(
                functions.col("_resolved_dimension_key"),
                functions.lit(dimension_key(dimension_name, UNKNOWN_BUSINESS_KEY)),
            )
        ),
    ).drop("_resolved_dimension_key")


def existing_dimension(spark: Any, path: Path) -> Any | None:
    """Load an existing Delta dimension when it is available."""

    try:
        from delta.tables import DeltaTable
    except ImportError as exc:
        raise RuntimeError("delta-spark is required for Gold dimensions.") from exc
    resolved = str(path.resolve())
    return (
        spark.read.format("delta").load(resolved)
        if DeltaTable.isDeltaTable(spark, resolved)
        else None
    )
