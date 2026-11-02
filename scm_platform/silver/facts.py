"""
Galaxy schema facts (shipment with GPS ping, order, inventory snapshot) and the weather table.

Part of the Silver layer; see scm_platform/transform_silver.py for the overview.
"""

from typing import Any, List, Optional, Tuple

# Guard PySpark imports for local test execution and cluster portability
try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import (
        coalesce,
        col,
        concat_ws,
        current_timestamp,
        expr,
        lit,
        md5,
        row_number,
        when,
    )
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        TimestampType,
    )
    from pyspark.sql.window import Window
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    DataFrame = Any  # type: ignore
    SparkSession = Any  # type: ignore

from scm_platform.silver.cdc import CDC_COLUMNS, drop_cdc_redeliveries
from scm_platform.silver.quality import (
    QualityRule,
    SCHEMA_DRIFT_RULE,
    _fmt,
    _missing_key_rule,
    apply_quality_rules,
    deduplicate_dataframe,
    payload_json,
    split_duplicates,
    split_latest_versions,
)
from scm_platform.silver.scd import lookup_dimension_sk


# ---------------------------------------------------------------------------
# 5. Galaxy Schema Fact Tables Transformations
# ---------------------------------------------------------------------------

# Bronze GPS columns that are transport/audit metadata, not part of the event payload
GPS_NON_PAYLOAD_COLUMNS = (
    "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "event_ts",
    "_source_system", "_rescued_data",
)


def gps_rules() -> List[QualityRule]:
    """Quality-gate rules for GPS/RFID events from bronze.gps_events (order = priority)."""
    return [
        _missing_key_rule("RULE_GPS_EVENT_ID_MISSING", "event_id"),
        _missing_key_rule("RULE_GPS_TRUCK_ID_MISSING", "truck_id"),
        _missing_key_rule("RULE_GPS_SHIPMENT_ID_MISSING", "shipment_id"),
        QualityRule("RULE_GPS_TIMESTAMP_INVALID", "event_ts IS NULL",
                    f"concat('Event timestamp missing or unparseable: ', {_fmt('ts')})"),
        QualityRule("RULE_GPS_COORDINATES",
                    "lat IS NULL OR lon IS NULL OR NOT (lat BETWEEN -90 AND 90 AND lon BETWEEN -180 AND 180)",
                    f"concat('Coordinates missing or out of range: (', {_fmt('lat')}, ', ', {_fmt('lon')}, ')')"),
        QualityRule("RULE_GPS_SPEED_IMPLAUSIBLE", "speed_kmh < 0 OR speed_kmh > 160",
                    f"concat('speed_kmh must be 0-160, got ', {_fmt('speed_kmh')})"),
    ]


def transform_gps_events(
    df_bronze_gps: DataFrame,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Validate Bronze GPS/RFID events: (clean_events, quarantine).

    Re-delivered events (same event_id) that slipped past the Bronze watermark dedup are
    collapsed here, not quarantined: at-least-once delivery is a transport property, not bad
    data, and every copy remains in append-only Bronze. Unparseable payloads never reach this
    table; they are in bronze.gps_events_dlq. Quality rules: gps_rules().
    """
    deduped, _redeliveries = split_duplicates(
        df_bronze_gps, key_col="event_id", tie_breaker_col="_ingest_timestamp",
    )
    return apply_quality_rules(
        deduped, gps_rules(), "GPS_STREAM", batch_id, "event_id",
        payload_exclude=GPS_NON_PAYLOAD_COLUMNS,
    )


def latest_ping_per_shipment(df_gps_clean: DataFrame) -> DataFrame:
    """Most recent clean ping per shipment: shipment_id, truck_id, last_lat, last_lon, last_ping_ts."""
    window = Window.partitionBy("shipment_id").orderBy(col("event_ts").desc(), col("event_id").desc())
    return (
        df_gps_clean.withColumn("_rn", row_number().over(window))
        .filter(col("_rn") == 1)
        .select(
            col("shipment_id").alias("_ping_shipment_id"),
            col("truck_id"),
            col("lat").alias("last_lat"),
            col("lon").alias("last_lon"),
            col("event_ts").alias("last_ping_ts"),
        )
    )


SHIPMENT_WORKING_COLUMNS = (
    "_dispatch_ts", "_planned_ts", "_actual_ts", "_updated_ts", "_key_max_fresh",
    "carrier_sk", "route_sk", "origin_warehouse_sk",
)

SHIPMENT_STATUSES = ("DISPATCHED", "IN_TRANSIT", "DELIVERED", "EXCEPTION")


def shipment_rules(has_warehouse_dim: bool) -> List[QualityRule]:
    """Quality-gate rules for shipments, evaluated after dimension lookups (order = priority)."""
    statuses = ", ".join(f"'{x}'" for x in SHIPMENT_STATUSES)
    rules = [
        SCHEMA_DRIFT_RULE,
        _missing_key_rule("RULE_SHIPMENT_ID_MISSING", "shipment_id"),
        QualityRule("RULE_TIMESTAMP_PARSE", "_dispatch_ts IS NULL OR _planned_ts IS NULL",
                    f"concat('Unparseable dispatch or planned delivery timestamp: ', {_fmt('dispatch_time_utc')}, "
                    f"' / ', {_fmt('planned_delivery_time_utc')})"),
        QualityRule("RULE_CHRONOLOGICAL_DISPATCH", "_planned_ts <= _dispatch_ts",
                    f"concat('planned_delivery_time ', {_fmt('planned_delivery_time_utc')}, "
                    f"' is not after dispatch_time ', {_fmt('dispatch_time_utc')})"),
        QualityRule("RULE_ACTUAL_BEFORE_DISPATCH", "_actual_ts <= _dispatch_ts",
                    f"concat('actual_delivery_time ', {_fmt('actual_delivery_time_utc')}, "
                    f"' is not after dispatch_time ', {_fmt('dispatch_time_utc')})",
                    requires=("actual_delivery_time_utc",)),
        QualityRule("RULE_SHIPMENT_STATUS_INVALID", f"shipment_status IS NULL OR shipment_status NOT IN ({statuses})",
                    f"concat('shipment_status must be one of {', '.join(SHIPMENT_STATUSES)}, got ', "
                    f"{_fmt('shipment_status')})",
                    requires=("shipment_status",)),
        QualityRule("RULE_DELIVERED_WITHOUT_TIMESTAMP", "shipment_status = 'DELIVERED' AND _actual_ts IS NULL",
                    "'shipment_status is DELIVERED but actual_delivery_time is missing or unparseable'",
                    requires=("shipment_status", "actual_delivery_time_utc")),
        QualityRule("RULE_SHIPPED_EXCEEDS_ORDERED", "shipped_quantity < 0 OR shipped_quantity > ordered_quantity",
                    f"concat('shipped_quantity ', {_fmt('shipped_quantity')}, "
                    f"' outside 0..ordered_quantity ', {_fmt('ordered_quantity')})",
                    requires=("shipped_quantity", "ordered_quantity")),
        QualityRule("RULE_MANDATORY_FIELDS", "data_type IS NULL", "'Missing mandatory field data_type'",
                    requires=("data_type",)),
        QualityRule("RULE_REF_INTEGRITY_CARRIER", "carrier_sk IS NULL",
                    f"concat('Referential integrity failure: carrier_id ', {_fmt('carrier_id')}, "
                    "' does not exist in silver.dim_carrier')"),
        QualityRule("RULE_REF_INTEGRITY_ROUTE", "route_sk IS NULL",
                    f"concat('Referential integrity failure: route_id ', {_fmt('route_id')}, "
                    "' does not exist in silver.dim_route')"),
    ]
    if has_warehouse_dim:
        rules.append(QualityRule(
            "RULE_REF_INTEGRITY_WAREHOUSE", "warehouse_id IS NOT NULL AND origin_warehouse_sk IS NULL",
            f"concat('Referential integrity failure: warehouse_id ', {_fmt('warehouse_id')}, "
            "' not found in silver.dim_warehouse')",
            requires=("warehouse_id",),
        ))
    return rules


def transform_fact_shipment(
    df_bronze_shipments: DataFrame,
    df_dim_carrier: DataFrame,
    df_dim_route: DataFrame,
    df_dim_date: Optional[DataFrame] = None,
    batch_id: str = "batch_default",
    df_dim_warehouse: Optional[DataFrame] = None,
    df_last_ping: Optional[DataFrame] = None,
) -> Tuple[DataFrame, DataFrame]:
    """Transform Bronze shipments into silver.fact_shipment.

    df_last_ping (latest_ping_per_shipment over clean GPS events) adds the truck's latest
    position: truck_id, last_lat, last_lon, last_ping_ts (NULL when no ping has been received).

    Grain: one row per shipment leg (single-leg shipments today: leg_sequence = 1).
    Source: carrier shipments generated from shipped/delivered orders (bronze.shipments).

    Versions vs duplicates (split_latest_versions): a shipment is updated as it moves
    (IN_TRANSIT -> DELIVERED); the latest updated_at wins, rows tied with it are RULE_DUPLICATE_KEY.

    KPI columns (delivered shipments; NULL while in transit):
    - delay_minutes = actual_delivery_time - planned_delivery_time (carrier's promised ETA)
    - is_on_time    = actual_delivery_time <= planned_delivery_time
    - is_in_full    = shipped_quantity >= ordered_quantity
    Quality rules: shipment_rules().
    """
    cols = df_bronze_shipments.columns

    def ts(name):
        return expr(f"try_to_timestamp(cast({name} as string))") if name in cols else lit(None).cast(TimestampType())

    prepared = (
        df_bronze_shipments
        .withColumn("_dispatch_ts", ts("dispatch_time_utc"))
        .withColumn("_planned_ts", ts("planned_delivery_time_utc"))
        .withColumn("_actual_ts", ts("actual_delivery_time_utc"))
        .withColumn("_updated_ts", ts("updated_at") if "updated_at" in cols else col("_dispatch_ts"))
    )
    kept, quar_duplicates = split_latest_versions(
        prepared, "shipment_id", "_updated_ts", "CARRIER_API", batch_id,
        payload_exclude=SHIPMENT_WORKING_COLUMNS,
    )

    # Dimension keys as of the dispatch time (point-in-time; Type 1 dims have one version)
    with_carrier = lookup_dimension_sk(
        kept, df_dim_carrier, natural_key="carrier_id", sk_col="carrier_sk", fact_timestamp_col="_dispatch_ts"
    )
    with_route = lookup_dimension_sk(
        with_carrier, df_dim_route, natural_key="route_id", sk_col="route_sk", fact_timestamp_col="_dispatch_ts"
    )
    if df_dim_warehouse is not None and "warehouse_id" in cols:
        with_wh = lookup_dimension_sk(
            with_route,
            df_dim_warehouse.withColumnRenamed("warehouse_sk", "origin_warehouse_sk"),
            natural_key="warehouse_id",
            sk_col="origin_warehouse_sk",
            fact_timestamp_col="_dispatch_ts",
        )
    else:
        with_wh = with_route.withColumn("origin_warehouse_sk", lit(None).cast(StringType()))

    valid, quar_rules = apply_quality_rules(
        with_wh, shipment_rules(df_dim_warehouse is not None), "CARRIER_API", batch_id, "shipment_id",
        payload_exclude=SHIPMENT_WORKING_COLUMNS,
    )

    def optional(name, dtype, default=None):
        return col(name).cast(dtype) if name in cols else lit(default).cast(dtype)

    ordered = optional("ordered_quantity", IntegerType())
    shipped = optional("shipped_quantity", IntegerType())
    hours_between = lambda a, b: (col(b).cast("long") - col(a).cast("long")) / 3600.0  # noqa: E731

    fact_df = valid.select(
        md5(concat_ws("_", col("shipment_id"), lit("1"))).alias("shipment_leg_sk"),
        col("shipment_id"),
        lit(1).alias("leg_sequence"),
        col("carrier_sk"),
        col("route_sk"),
        col("origin_warehouse_sk"),
        expr("cast(date_format(_dispatch_ts, 'yyyyMMdd') as int)").alias("dispatch_date_sk"),
        expr("cast(date_format(_planned_ts, 'yyyyMMdd') as int)").alias("planned_delivery_date_sk"),
        col("_dispatch_ts").alias("dispatch_time"),
        col("_planned_ts").alias("planned_delivery_time"),
        hours_between("_dispatch_ts", "_planned_ts").alias("planned_transit_hours"),
        col("_actual_ts").alias("actual_delivery_time"),
        when(col("_actual_ts").isNotNull(), hours_between("_dispatch_ts", "_actual_ts")).alias("actual_transit_hours"),
        optional("shipment_status", StringType(), "DISPATCHED").alias("shipment_status"),
        col("carrier_id"),
        col("route_id"),
        optional("data_type", StringType()).alias("data_type"),
        optional("order_id", StringType()).alias("order_id"),
        optional("warehouse_id", StringType()).alias("warehouse_id"),
        ordered.alias("ordered_quantity"),
        shipped.alias("shipped_quantity"),
        when(col("_actual_ts").isNotNull(),
             ((col("_actual_ts").cast("long") - col("_planned_ts").cast("long")) / 60).cast(IntegerType())
             ).alias("delay_minutes"),
        when(col("_actual_ts").isNotNull(), col("_actual_ts") <= col("_planned_ts")).alias("is_on_time"),
        when(ordered.isNotNull() & shipped.isNotNull(), shipped >= ordered).alias("is_in_full"),
        optional("weather_risk", StringType()).alias("weather_risk"),
        col("_updated_ts").alias("updated_at"),
        current_timestamp().alias("created_at"),
    )

    ping_columns = ["truck_id", "last_lat", "last_lon", "last_ping_ts"]
    if df_last_ping is not None:
        fact_df = (
            fact_df.join(df_last_ping, fact_df["shipment_id"] == df_last_ping["_ping_shipment_id"], "left")
            .drop("_ping_shipment_id")
        )
    else:
        fact_df = fact_df.select(
            "*",
            lit(None).cast(StringType()).alias("truck_id"),
            lit(None).cast(DoubleType()).alias("last_lat"),
            lit(None).cast(DoubleType()).alias("last_lon"),
            lit(None).cast(TimestampType()).alias("last_ping_ts"),
        )
    ordered_columns = [c for c in fact_df.columns if c not in ping_columns + ["created_at"]]
    fact_df = fact_df.select(*ordered_columns, *ping_columns, "created_at")

    return fact_df, quar_duplicates.unionByName(quar_rules)



ORDER_STATUSES = ("CREATED", "PICKED", "SHIPPED", "DELIVERED", "CANCELLED")

ORDER_WORKING_COLUMNS = (
    "_grain_key", "_key_max_fresh", "_order_ts", "_updated_ts",
    "customer_sk", "product_sk", "origin_warehouse_sk", *CDC_COLUMNS,
)


def order_rules(has_warehouse_dim: bool) -> List[QualityRule]:
    """Quality-gate rules for order lines, evaluated after dimension lookups (order = priority)."""
    rules = [
        SCHEMA_DRIFT_RULE,
        _missing_key_rule("RULE_ORDER_ID_MISSING", "order_id"),
        QualityRule("RULE_ORDER_LINE_NUMBER_INVALID", "order_line_number IS NULL OR order_line_number <= 0",
                    f"concat('order_line_number must be > 0, got ', {_fmt('order_line_number')})"),
        QualityRule("RULE_ORDER_DATE_INVALID", "_order_ts IS NULL",
                    f"concat('order_date missing or unparseable: ', {_fmt('order_date')})"),
        QualityRule("RULE_ORDER_DATE_IN_FUTURE", "_order_ts > current_timestamp()",
                    f"concat('order_date is in the future: ', {_fmt('order_date')})"),
        QualityRule("RULE_ORDER_QUANTITY_POSITIVE", "ordered_quantity IS NULL OR ordered_quantity <= 0",
                    f"concat('ordered_quantity must be > 0, got ', {_fmt('ordered_quantity')})"),
        QualityRule("RULE_UNIT_PRICE_NON_NEGATIVE", "unit_price IS NULL OR unit_price < 0",
                    f"concat('unit_price must be >= 0, got ', {_fmt('unit_price')})"),
        QualityRule("RULE_PROMISED_BEFORE_ORDER",
                    "try_to_timestamp(cast(promised_delivery_date as string)) < _order_ts",
                    f"concat('promised_delivery_date ', {_fmt('promised_delivery_date')}, "
                    f"' is before order_date ', {_fmt('order_date')})",
                    requires=("promised_delivery_date",)),
        QualityRule("RULE_SHIPPED_EXCEEDS_ORDERED",
                    "shipped_quantity < 0 OR shipped_quantity > ordered_quantity",
                    f"concat('shipped_quantity ', {_fmt('shipped_quantity')}, "
                    f"' outside 0..ordered_quantity ', {_fmt('ordered_quantity')})",
                    requires=("shipped_quantity",)),
        QualityRule("RULE_ORDER_STATUS_INVALID",
                    f"order_status IS NULL OR order_status NOT IN ({', '.join(repr(x) for x in ORDER_STATUSES)})",
                    f"concat('order_status must be one of {', '.join(ORDER_STATUSES)}, got ', {_fmt('order_status')})",
                    requires=("order_status",)),
        QualityRule("RULE_REF_INTEGRITY_CUSTOMER", "customer_sk IS NULL",
                    f"concat('Referential integrity failure: customer_id ', {_fmt('customer_id')}, "
                    "' not found in silver.dim_customer')"),
        QualityRule("RULE_REF_INTEGRITY_PRODUCT", "product_sk IS NULL",
                    f"concat('Referential integrity failure: product_id ', {_fmt('product_id')}, "
                    "' not found in silver.dim_product')"),
    ]
    if has_warehouse_dim:
        rules.append(QualityRule(
            "RULE_REF_INTEGRITY_WAREHOUSE", "warehouse_id IS NOT NULL AND origin_warehouse_sk IS NULL",
            f"concat('Referential integrity failure: warehouse_id ', {_fmt('warehouse_id')}, "
            "' not found in silver.dim_warehouse')",
            requires=("warehouse_id",),
        ))
    return rules


def transform_fact_order(
    df_bronze_orders: DataFrame,
    df_dim_customer: DataFrame,
    df_dim_product: DataFrame,
    df_dim_warehouse: Optional[DataFrame] = None,
    df_dim_date: Optional[DataFrame] = None,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Transform Bronze order lines into silver.fact_order.

    Grain: one row per order line (order_id + order_line_number).
    Sources: the OMS order-line file feed (bronze.orders), or Debezium CDC events decoded with
    decode_order_line_cdc (bronze.orders_cdc, SQL Server OMS).

    Versions vs duplicates (split_latest_versions): the latest version wins; older versions are
    superseded history, rows tied with the winner are RULE_DUPLICATE_KEY.
    - File feed: "latest" = updated_at.
    - CDC: "latest" = source_position (log order), so a stale event replayed late can never win;
      events re-sent by Debezium (same position) are collapsed first (drop_cdc_redeliveries), and a
      delete as the latest event yields the row's last values with is_deleted = TRUE (soft delete).

    Dimension keys resolve point-in-time as of the order date. Quality rules: order_rules().
    """
    cols = df_bronze_orders.columns
    is_cdc = "_source_position" in cols
    fresh_col = "_source_position" if is_cdc else ("_updated_ts" if "updated_at" in cols else "_order_ts")
    has_key = (
        col("order_id").isNotNull() & (col("order_id") != "") & col("order_line_number").isNotNull()
    )
    prepared = (
        df_bronze_orders
        .withColumn("_order_ts", expr("try_to_timestamp(cast(order_date as string))"))
        .withColumn(
            "_updated_ts",
            expr("try_to_timestamp(cast(updated_at as string))") if "updated_at" in cols else col("_order_ts"),
        )
        .withColumn(
            "_grain_key",
            when(has_key, concat_ws("_", col("order_id"), col("order_line_number").cast("string"))),
        )
    )
    if is_cdc:
        prepared = drop_cdc_redeliveries(prepared, "_grain_key")
    kept, quar_duplicates = split_latest_versions(
        prepared, "_grain_key", fresh_col, "SQLSERVER_OMS", batch_id,
        key_label="order line", payload_exclude=ORDER_WORKING_COLUMNS,
    )

    # Resolve dimension surrogate keys as of the order date (SCD2 point-in-time)
    with_cust = lookup_dimension_sk(
        kept, df_dim_customer, natural_key="customer_id", sk_col="customer_sk", fact_timestamp_col="_order_ts"
    )
    with_prod = lookup_dimension_sk(
        with_cust, df_dim_product, natural_key="product_id", sk_col="product_sk", fact_timestamp_col="_order_ts"
    )
    if df_dim_warehouse is not None and "warehouse_id" in cols:
        with_wh = lookup_dimension_sk(
            with_prod,
            df_dim_warehouse.withColumnRenamed("warehouse_sk", "origin_warehouse_sk"),
            natural_key="warehouse_id",
            sk_col="origin_warehouse_sk",
            fact_timestamp_col="_order_ts",
        )
    else:
        with_wh = with_prod.withColumn("origin_warehouse_sk", lit(None).cast(StringType()))

    valid, quar_rules = apply_quality_rules(
        with_wh, order_rules(df_dim_warehouse is not None), "SQLSERVER_OMS", batch_id, "order_id",
        payload_exclude=ORDER_WORKING_COLUMNS,
    )

    def optional(name, dtype, default=None):
        return col(name).cast(dtype) if name in cols else lit(default).cast(dtype)

    fact_df = valid.select(
        md5(concat_ws("_", col("order_id"), col("order_line_number").cast("string"))).alias("order_line_sk"),
        col("order_id"),
        col("order_line_number").cast(IntegerType()).alias("order_line_number"),
        col("customer_sk"),
        col("product_sk"),
        col("origin_warehouse_sk"),
        expr("cast(date_format(_order_ts, 'yyyyMMdd') as int)").alias("order_date_sk"),
        (expr("cast(date_format(try_to_timestamp(cast(promised_delivery_date as string)), 'yyyyMMdd') as int)")
         if "promised_delivery_date" in cols else lit(None).cast(IntegerType())).alias("promised_delivery_date_sk"),
        col("ordered_quantity").cast(IntegerType()).alias("ordered_quantity"),
        optional("shipped_quantity", IntegerType()).alias("shipped_quantity"),
        col("unit_price").cast(DoubleType()).alias("unit_price"),
        (col("ordered_quantity") * col("unit_price")).cast(DoubleType()).alias("total_price"),
        optional("order_status", StringType(), "CREATED").alias("order_status"),
        col("customer_id"),
        col("product_id"),
        optional("warehouse_id", StringType()).alias("warehouse_id"),
        col("_updated_ts").alias("updated_at"),
        optional("_is_deleted", BooleanType(), False).alias("is_deleted"),
        optional("_source_position", StringType()).alias("source_position"),
        current_timestamp().alias("created_at"),
    )

    return fact_df, quar_duplicates.unionByName(quar_rules)



INVENTORY_WORKING_COLUMNS = (
    "_grain_key", "_snapshot_ts", "_recorded_ts", "warehouse_sk", "product_sk",
)


def inventory_rules() -> List[QualityRule]:
    """Quality-gate rules for WMS snapshot rows, evaluated after dimension lookups (order = priority)."""
    return [
        SCHEMA_DRIFT_RULE,
        _missing_key_rule("RULE_INVENTORY_WAREHOUSE_ID_MISSING", "warehouse_id"),
        _missing_key_rule("RULE_INVENTORY_PRODUCT_ID_MISSING", "product_id"),
        QualityRule("RULE_SNAPSHOT_DATE_INVALID", "_snapshot_ts IS NULL",
                    f"concat('snapshot_date missing or unparseable: ', {_fmt('snapshot_date')})"),
        QualityRule("RULE_SNAPSHOT_DATE_IN_FUTURE", "_snapshot_ts > current_timestamp()",
                    f"concat('snapshot_date is in the future: ', {_fmt('snapshot_date')})"),
        QualityRule("RULE_QTY_ON_HAND_NON_NEGATIVE", "quantity_on_hand IS NULL OR quantity_on_hand < 0",
                    f"concat('quantity_on_hand must be >= 0, got ', {_fmt('quantity_on_hand')})"),
        QualityRule("RULE_QTY_RESERVED_INVALID",
                    "quantity_reserved < 0 OR quantity_reserved > quantity_on_hand",
                    f"concat('quantity_reserved ', {_fmt('quantity_reserved')}, "
                    f"' outside 0..quantity_on_hand ', {_fmt('quantity_on_hand')})",
                    requires=("quantity_reserved",)),
        QualityRule("RULE_REF_INTEGRITY_WAREHOUSE", "warehouse_sk IS NULL",
                    f"concat('Referential integrity failure: warehouse_id ', {_fmt('warehouse_id')}, "
                    "' not found in silver.dim_warehouse')"),
        QualityRule("RULE_REF_INTEGRITY_PRODUCT", "product_sk IS NULL",
                    f"concat('Referential integrity failure: product_id ', {_fmt('product_id')}, "
                    "' not found in silver.dim_product')"),
    ]


def transform_fact_inventory_snapshot(
    df_bronze_inventory: DataFrame,
    df_dim_warehouse: DataFrame,
    df_dim_product: DataFrame,
    df_dim_date: Optional[DataFrame] = None,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Transform Bronze WMS reconciliation rows into silver.fact_inventory_snapshot.

    Grain: one row per warehouse, product, and snapshot date.
    Source Contract: WMS nightly stock reconciliation NDJSON file.

    Versions vs duplicates (split_latest_versions): when the same snapshot arrives twice, the
    later recorded_at wins (a re-sent corrected file); rows tied with it are RULE_DUPLICATE_KEY.

    Measures: quantity_available = on_hand - reserved; is_stockout = available <= 0 (stockout
    rate KPI); is_stockout_risk = available <= safety_stock. Quality rules: inventory_rules().
    """
    cols = df_bronze_inventory.columns
    fresh_col = "_recorded_ts" if "recorded_at" in cols else "_snapshot_ts"
    has_key = (
        col("warehouse_id").isNotNull() & (col("warehouse_id") != "")
        & col("product_id").isNotNull() & (col("product_id") != "")
        & col("snapshot_date").isNotNull()
    )
    prepared = (
        df_bronze_inventory
        .withColumn("_snapshot_ts", expr("try_to_timestamp(cast(snapshot_date as string))"))
        .withColumn(
            "_recorded_ts",
            expr("try_to_timestamp(cast(recorded_at as string))") if "recorded_at" in cols else col("_snapshot_ts"),
        )
        .withColumn(
            "_grain_key",
            when(has_key, concat_ws("_", col("warehouse_id"), col("product_id"), col("snapshot_date").cast("string"))),
        )
    )
    kept, quar_duplicates = split_latest_versions(
        prepared, "_grain_key", fresh_col, "WMS", batch_id,
        key_label="warehouse/product/snapshot_date", payload_exclude=INVENTORY_WORKING_COLUMNS,
    )

    # Resolve warehouse & product surrogate keys as of the snapshot date (SCD2 point-in-time)
    with_wh = lookup_dimension_sk(
        kept, df_dim_warehouse, natural_key="warehouse_id", sk_col="warehouse_sk", fact_timestamp_col="_snapshot_ts"
    )
    with_prod = lookup_dimension_sk(
        with_wh, df_dim_product, natural_key="product_id", sk_col="product_sk", fact_timestamp_col="_snapshot_ts"
    )

    valid, quar_rules = apply_quality_rules(
        with_prod, inventory_rules(), "WMS", batch_id, "warehouse_id",
        payload_exclude=INVENTORY_WORKING_COLUMNS,
    )

    def optional(name, default=None):
        return col(name).cast(DoubleType()) if name in cols else lit(default).cast(DoubleType())

    on_hand = col("quantity_on_hand").cast(DoubleType())
    reserved = coalesce(optional("quantity_reserved"), lit(0.0))
    available = on_hand - reserved
    safety = coalesce(optional("safety_stock"), lit(0.0))
    snapshot_date_sk = expr("cast(date_format(_snapshot_ts, 'yyyyMMdd') as int)")

    fact_df = valid.select(
        md5(concat_ws("_", col("warehouse_id"), col("product_id"), snapshot_date_sk.cast("string")))
        .alias("inventory_snapshot_sk"),
        col("warehouse_sk"),
        col("product_sk"),
        snapshot_date_sk.alias("snapshot_date_sk"),
        expr("to_date(_snapshot_ts)").alias("snapshot_date"),
        on_hand.alias("quantity_on_hand"),
        reserved.alias("quantity_reserved"),
        available.alias("quantity_available"),
        optional("reorder_point").alias("reorder_point"),
        safety.alias("safety_stock"),
        (available <= 0).alias("is_stockout"),
        (available <= safety).alias("is_stockout_risk"),
        col("warehouse_id"),
        col("product_id"),
        col("_recorded_ts").alias("recorded_at"),
        lit(False).alias("is_stale"),       # TRUE only for rows carried forward after an SLA miss (sla_checks)
        current_timestamp().alias("created_at"),
    )

    return fact_df, quar_duplicates.unionByName(quar_rules)


# ---------------------------------------------------------------------------
# 6. Auxiliary & Legacy Compatibility Transformations
# ---------------------------------------------------------------------------

def transform_weather(
    df_bronze: DataFrame,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Validate, clean, deduplicate and split weather into (silver_df, quarantine_df)."""
    deduped = deduplicate_dataframe(
        df_bronze, key_col="weather_record_id", business_freshness_col="forecast_time_utc"
    )
    with_ts = deduped.withColumn("parsed_forecast_time", expr("try_to_timestamp(forecast_time_utc)"))

    valid_cond = (
        col("weather_record_id").isNotNull() & (col("weather_record_id") != "") &
        col("latitude").isNotNull() & (col("latitude") >= -90.0) & (col("latitude") <= 90.0) &
        col("longitude").isNotNull() & (col("longitude") >= -180.0) & (col("longitude") <= 180.0) &
        col("parsed_forecast_time").isNotNull() &
        (col("precipitation_mm").isNull() | (col("precipitation_mm") >= 0)) &
        (col("wind_speed_10m_kmh").isNull() | (col("wind_speed_10m_kmh") >= 0)) &
        (col("visibility_m").isNull() | (col("visibility_m") >= 0)) &
        col("source").isNotNull()
    )

    silver_df = (
        with_ts.filter(valid_cond)
        .select(
            col("weather_record_id"),
            col("latitude"),
            col("longitude"),
            col("parsed_forecast_time").alias("forecast_time"),
            col("temperature_2m_c"),
            col("precipitation_mm"),
            col("wind_speed_10m_kmh"),
            col("visibility_m"),
            col("source"),
            current_timestamp().alias("created_at"),
        )
    )

    invalid_raw = with_ts.filter(~valid_cond)
    clean_cols = [c for c in df_bronze.columns if c not in ["_ingest_timestamp", "_source_file", "_rn"]]
    raw_payload_expr = payload_json(clean_cols)
    quarantine_df = invalid_raw.select(
        md5(concat_ws("_", lit("OPEN_METEO"), coalesce(col("weather_record_id"), lit("none")), raw_payload_expr)).alias("quarantine_id"),
        raw_payload_expr.alias("original_payload"),
        lit("Validation failure: invalid coordinates, unparseable timestamp, or negative weather metrics").alias("failure_reason"),
        lit("RULE_WEATHER_METRICS").alias("failed_rule"),
        lit(batch_id).alias("batch_id"),
        lit("OPEN_METEO").alias("source_system"),
        current_timestamp().alias("ingested_at"),
        lit("PENDING").alias("status"),
        col("weather_record_id").alias("source_record_id"),
        lit("weather").alias("source_table"),
    )

    return silver_df, quarantine_df
