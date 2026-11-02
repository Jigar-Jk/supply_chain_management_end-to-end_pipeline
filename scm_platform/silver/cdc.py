"""
Debezium CDC: decode change events from Bronze; real-time stock position (inventory_current).

Part of the Silver layer; see scm_platform/transform_silver.py for the overview.
"""

from typing import Any, List, Sequence, Tuple

# Guard PySpark imports for local test execution and cluster portability
try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import (
        coalesce,
        col,
        concat_ws,
        current_timestamp,
        expr,
        from_json,
        lit,
        md5,
        row_number,
        when,
    )
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        LongType,
        StringType,
        StructField,
        StructType,
    )
    from pyspark.sql.window import Window
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    DataFrame = Any  # type: ignore
    SparkSession = Any  # type: ignore

from scm_platform.silver.quality import (
    QualityRule,
    _fmt,
    _missing_key_rule,
    apply_quality_rules,
    split_latest_versions,
)


# ---------------------------------------------------------------------------
# CDC: decode Debezium change events from Bronze into source rows
# ---------------------------------------------------------------------------

def _image_schema(fields) -> Any:
    return StructType([StructField(name, dtype, True) for name, dtype in fields])


# Row images as Debezium encodes them: DATE = days since 1970-01-01, DATETIME2 = epoch millis,
# DECIMAL = double (connector setting decimal.handling.mode=double).
ORDER_LINE_IMAGE_FIELDS = [
    ("order_id", StringType()), ("order_line_number", IntegerType()), ("customer_id", StringType()),
    ("product_id", StringType()), ("warehouse_id", StringType()), ("order_date", IntegerType()),
    ("promised_delivery_date", IntegerType()), ("ordered_quantity", IntegerType()),
    ("shipped_quantity", IntegerType()), ("unit_price", DoubleType()), ("order_status", StringType()),
    ("updated_at", LongType()),
]
INVENTORY_IMAGE_FIELDS = [
    ("warehouse_id", StringType()), ("product_id", StringType()), ("quantity_on_hand", IntegerType()),
    ("quantity_reserved", IntegerType()), ("safety_stock", IntegerType()), ("reorder_point", IntegerType()),
    ("updated_at", LongType()),
]
CDC_COLUMNS = ("_op", "_is_deleted", "_source_position")


def decode_cdc_rows(
    df_cdc: DataFrame,
    image_fields,
    date_columns: Sequence[str] = (),
    timestamp_columns: Sequence[str] = (),
) -> DataFrame:
    """One row per Bronze CDC event: the source row as typed columns, plus _op, _is_deleted, _source_position.

    The row comes from the `after` image, or from `before` for deletes (a delete has no after),
    so a deleted row keeps its last known values and can be soft-deleted downstream.
    Dates are decoded from days-since-epoch to 'yyyy-MM-dd' strings and timestamps from epoch
    millis to TIMESTAMP, matching what the file feeds deliver.
    """
    image = when(col("op") == "d", col("before")).otherwise(col("after"))
    audit = ["_ingest_timestamp"] if "_ingest_timestamp" in df_cdc.columns else []
    decoded = df_cdc.select(
        from_json(image, _image_schema(image_fields)).alias("_img"),
        col("op").alias("_op"),
        (col("op") == "d").alias("_is_deleted"),
        col("source_position").alias("_source_position"),
        *audit,
    ).select("_img.*", *CDC_COLUMNS, *audit)
    for c in date_columns:
        decoded = decoded.withColumn(c, expr(f"cast(date_add(DATE'1970-01-01', {c}) AS STRING)"))
    for c in timestamp_columns:
        decoded = decoded.withColumn(c, expr(f"timestamp_millis({c})"))
    return decoded


def decode_order_line_cdc(df_orders_cdc: DataFrame) -> DataFrame:
    """bronze.orders_cdc -> order-line rows shaped like the bronze.orders file feed (+ CDC columns)."""
    return decode_cdc_rows(df_orders_cdc, ORDER_LINE_IMAGE_FIELDS,
                           date_columns=("order_date", "promised_delivery_date"), timestamp_columns=("updated_at",))


def decode_inventory_cdc(df_inventory_cdc: DataFrame) -> DataFrame:
    """bronze.inventory_cdc -> current stock rows (+ CDC columns)."""
    return decode_cdc_rows(df_inventory_cdc, INVENTORY_IMAGE_FIELDS, timestamp_columns=("updated_at",))


def drop_cdc_redeliveries(df: DataFrame, key_col: str) -> DataFrame:
    """Collapse events re-sent by Debezium (same key AND same source position).

    At-least-once delivery is a transport property, not bad data: the copies are identical,
    every copy stays in append-only Bronze, and none is quarantined.
    """
    return df.dropDuplicates([key_col, "_source_position"])


def latest_cdc_rows(decoded: DataFrame, key_cols: Sequence[str]) -> DataFrame:
    """Latest decoded CDC row per key (highest _source_position), deleted rows included.

    The pure "apply" step without dimensions or quality rules: what the source table looks like
    according to the change stream. Used to reconcile Silver against the source database.
    """
    key = concat_ws("_", *[col(c).cast("string") for c in key_cols])
    window = Window.partitionBy(key).orderBy(col("_source_position").desc())
    return (drop_cdc_redeliveries(decoded.withColumn("_key", key), "_key")
            .withColumn("_rn", row_number().over(window))
            .filter(col("_rn") == 1).drop("_rn", "_key"))

# ---------------------------------------------------------------------------
# Real-time stock position from inventory CDC
# ---------------------------------------------------------------------------

INVENTORY_CURRENT_WORKING_COLUMNS = ("_grain_key", "_key_max_fresh", *CDC_COLUMNS)


def inventory_current_rules() -> List[QualityRule]:
    """Quality-gate rules for live stock rows from inventory CDC (order = priority)."""
    return [
        _missing_key_rule("RULE_INVENTORY_WAREHOUSE_ID_MISSING", "warehouse_id"),
        _missing_key_rule("RULE_INVENTORY_PRODUCT_ID_MISSING", "product_id"),
        QualityRule("RULE_QTY_ON_HAND_NON_NEGATIVE", "quantity_on_hand IS NULL OR quantity_on_hand < 0",
                    f"concat('quantity_on_hand must be >= 0, got ', {_fmt('quantity_on_hand')})"),
        QualityRule("RULE_QTY_RESERVED_INVALID", "quantity_reserved < 0 OR quantity_reserved > quantity_on_hand",
                    f"concat('quantity_reserved ', {_fmt('quantity_reserved')}, "
                    f"' outside 0..quantity_on_hand ', {_fmt('quantity_on_hand')})"),
    ]


def transform_inventory_current(
    df_inventory_cdc_rows: DataFrame,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Latest stock position per warehouse x product from decoded inventory CDC (decode_inventory_cdc).

    Complements the nightly WMS reconciliation (fact_inventory_snapshot, the system of record):
    this is the intraday view the OMS maintains as orders reserve and ship stock.
    Same CDC semantics as fact_order: highest source_position wins, redeliveries collapse,
    a delete soft-deletes. Quality rules: inventory_current_rules().
    """
    has_key = (col("warehouse_id").isNotNull() & (col("warehouse_id") != "")
               & col("product_id").isNotNull() & (col("product_id") != ""))
    prepared = drop_cdc_redeliveries(
        df_inventory_cdc_rows.withColumn(
            "_grain_key", when(has_key, concat_ws("_", col("warehouse_id"), col("product_id")))),
        "_grain_key",
    )
    kept, quar_duplicates = split_latest_versions(
        prepared, "_grain_key", "_source_position", "SQLSERVER_OMS", batch_id,
        key_label="warehouse/product", payload_exclude=INVENTORY_CURRENT_WORKING_COLUMNS,
    )
    valid, quar_rules = apply_quality_rules(
        kept, inventory_current_rules(), "SQLSERVER_OMS", batch_id, "warehouse_id",
        payload_exclude=INVENTORY_CURRENT_WORKING_COLUMNS,
    )
    current = valid.select(
        md5(concat_ws("_", col("warehouse_id"), col("product_id"))).alias("inventory_current_sk"),
        col("warehouse_id"),
        col("product_id"),
        col("quantity_on_hand").cast(IntegerType()).alias("quantity_on_hand"),
        col("quantity_reserved").cast(IntegerType()).alias("quantity_reserved"),
        (col("quantity_on_hand") - coalesce(col("quantity_reserved"), lit(0))).cast(IntegerType())
        .alias("quantity_available"),
        col("safety_stock").cast(IntegerType()).alias("safety_stock"),
        col("reorder_point").cast(IntegerType()).alias("reorder_point"),
        col("updated_at"),
        col("_is_deleted").alias("is_deleted"),
        col("_source_position").alias("source_position"),
        current_timestamp().alias("created_at"),
    )
    return current, quar_duplicates.unionByName(quar_rules)
