"""
Conformed dimensions: date, route, carrier, customer, product, warehouse (rules and transforms).

Part of the Silver layer; see scm_platform/transform_silver.py for the overview.
"""

from datetime import datetime, timedelta
from typing import Any, Optional, Tuple

# Guard PySpark imports for local test execution and cluster portability
try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import (
        col,
        current_timestamp,
        expr,
        lit,
    )
    from pyspark.sql.types import (
        BooleanType,
        DateType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    DataFrame = Any  # type: ignore
    SparkSession = Any  # type: ignore

from scm_platform.silver.quality import (
    QualityRule,
    SCHEMA_DRIFT_RULE,
    _fmt,
    _missing_key_rule,
    apply_quality_rules,
    quarantine_duplicates,
    split_duplicates,
)
from scm_platform.silver.scd import apply_scd1_dimension, apply_scd2_dimension


# ---------------------------------------------------------------------------
# 4. Conformed Dimensions Transformations
# ---------------------------------------------------------------------------

def generate_dim_date(
    spark: SparkSession,
    start_date_str: str = "2020-01-01",
    end_date_str: str = "2027-12-31",
) -> DataFrame:
    """Generate conformed static calendar date dimension (Type 0)."""
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to generate dim_date.")

    start_d = datetime.strptime(start_date_str, "%Y-%m-%d").date()
    end_d = datetime.strptime(end_date_str, "%Y-%m-%d").date()

    rows = []
    curr = start_d
    while curr <= end_d:
        date_sk = int(curr.strftime("%Y%m%d"))
        qtr = (curr.month - 1) // 3 + 1
        dow = curr.weekday() + 1  # 1 = Monday, 7 = Sunday
        is_wknd = curr.weekday() in (5, 6)
        rows.append((
            date_sk,
            curr,
            curr.year,
            qtr,
            curr.month,
            curr.strftime("%B"),
            curr.day,
            dow,
            curr.strftime("%A"),
            is_wknd,
            False,  # is_holiday
        ))
        curr += timedelta(days=1)

    schema = StructType([
        StructField("date_sk", IntegerType(), False),
        StructField("calendar_date", DateType(), False),
        StructField("year", IntegerType(), False),
        StructField("quarter", IntegerType(), False),
        StructField("month", IntegerType(), False),
        StructField("month_name", StringType(), False),
        StructField("day_of_month", IntegerType(), False),
        StructField("day_of_week", IntegerType(), False),
        StructField("day_name", StringType(), False),
        StructField("is_weekend", BooleanType(), False),
        StructField("is_holiday", BooleanType(), False),
    ])

    return spark.createDataFrame(rows, schema=schema)


def _with_effective_from(df: DataFrame) -> DataFrame:
    """effective_from = ingest time when known, else now."""
    return df.withColumn(
        "effective_from",
        col("_ingest_timestamp") if "_ingest_timestamp" in df.columns else current_timestamp(),
    )



# Rule sets: the single source of truth for each dimension's quality gate (order = priority).
ROUTE_RULES = [
    SCHEMA_DRIFT_RULE,
    _missing_key_rule("RULE_ROUTE_ID_MISSING", "route_id"),
    QualityRule(
        "RULE_ROUTE_COORDINATES",
        "origin_longitude IS NULL OR origin_latitude IS NULL OR destination_longitude IS NULL "
        "OR destination_latitude IS NULL OR NOT (origin_longitude BETWEEN -180 AND 180 "
        "AND destination_longitude BETWEEN -180 AND 180 AND origin_latitude BETWEEN -90 AND 90 "
        "AND destination_latitude BETWEEN -90 AND 90)",
        f"concat('Coordinates missing or out of range: origin (', {_fmt('origin_latitude')}, ', ', "
        f"{_fmt('origin_longitude')}, ') destination (', {_fmt('destination_latitude')}, ', ', "
        f"{_fmt('destination_longitude')}, ')')",
    ),
    QualityRule("RULE_ROUTE_DISTANCE_NON_POSITIVE", "distance_meters IS NULL OR distance_meters <= 0",
                f"concat('distance_meters must be > 0, got ', {_fmt('distance_meters')})"),
    QualityRule("RULE_ROUTE_DURATION_NON_POSITIVE", "duration_seconds IS NULL OR duration_seconds <= 0",
                f"concat('duration_seconds must be > 0, got ', {_fmt('duration_seconds')})"),
    QualityRule("RULE_ROUTE_ATTRIBUTES_MISSING", "routing_profile IS NULL OR source IS NULL",
                "'Missing routing_profile or source'"),
]

CARRIER_RULES = [
    SCHEMA_DRIFT_RULE,
    _missing_key_rule("RULE_CARRIER_ID_EMPTY", "carrier_id"),
    QualityRule("RULE_CARRIER_OTIF_TARGET_RANGE", "contracted_otif_target NOT BETWEEN 0 AND 100",
                f"concat('contracted_otif_target must be 0-100, got ', {_fmt('contracted_otif_target')})",
                requires=("contracted_otif_target",)),
]

CUSTOMER_RULES = [
    SCHEMA_DRIFT_RULE,
    _missing_key_rule("RULE_CUSTOMER_ID_EMPTY", "customer_id"),
]

PRODUCT_RULES = [
    SCHEMA_DRIFT_RULE,
    _missing_key_rule("RULE_PRODUCT_ID_MISSING", "product_id"),
    QualityRule("RULE_PRODUCT_WEIGHT_NON_POSITIVE", "unit_weight_kg <= 0",
                f"concat('unit_weight_kg must be > 0, got ', {_fmt('unit_weight_kg')})"),
    QualityRule("RULE_PRODUCT_VOLUME_NON_POSITIVE", "unit_volume_cbm <= 0",
                f"concat('unit_volume_cbm must be > 0, got ', {_fmt('unit_volume_cbm')})"),
    QualityRule("RULE_PRODUCT_COST_NEGATIVE", "unit_cost < 0",
                f"concat('unit_cost must be >= 0, got ', {_fmt('unit_cost')})", requires=("unit_cost",)),
    QualityRule("RULE_PRODUCT_PRICE_NEGATIVE", "unit_price < 0",
                f"concat('unit_price must be >= 0, got ', {_fmt('unit_price')})", requires=("unit_price",)),
]

WAREHOUSE_RULES = [
    SCHEMA_DRIFT_RULE,
    _missing_key_rule("RULE_WAREHOUSE_ID_MISSING", "warehouse_id"),
    QualityRule(
        "RULE_WAREHOUSE_COORDINATES",
        "latitude IS NULL OR longitude IS NULL OR NOT (latitude BETWEEN -90 AND 90 AND longitude BETWEEN -180 AND 180)",
        f"concat('Coordinates missing or out of range: (', {_fmt('latitude')}, ', ', {_fmt('longitude')}, ')')",
    ),
]


def transform_dim_route(
    df_bronze_routes: DataFrame,
    df_existing_dim: Optional[DataFrame] = None,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Validate, clean, deduplicate and apply SCD Type 1 for silver.dim_route.

    Source: OpenRouteService / GIS road network data (bronze.routes).
    Type 1 per implementation guide §8.1: a route is derived from its origin/destination pair,
    so corrections overwrite in place. Quality rules: ROUTE_RULES.
    """
    deduped, duplicates = split_duplicates(df_bronze_routes, key_col="route_id")
    valid, quar = apply_quality_rules(deduped, ROUTE_RULES, "ORS", batch_id, "route_id")
    quar_routes = quarantine_duplicates(duplicates, "route_id", "ORS", batch_id).unionByName(quar)

    dim_route = apply_scd1_dimension(
        df_incoming=_with_effective_from(valid),
        df_existing=df_existing_dim,
        natural_key="route_id",
        effective_from_col="effective_from",
        sk_col="route_sk",
    )
    return dim_route, quar_routes


CARRIER_MASTER_COLUMNS = [
    "carrier_id", "carrier_name", "service_level", "transport_mode",
    "contracted_otif_target", "contact_email", "is_active",
]


def transform_dim_carrier(
    df_bronze_carriers: DataFrame,
    df_existing_dim: Optional[DataFrame] = None,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Validate and apply SCD Type 1 for silver.dim_carrier.

    Sources:
    - bronze.carriers (carrier contract master) when the frame has a carrier_name column;
    - otherwise falls back to deriving carriers from bronze.shipments carrier_id values.
    Type 1 per implementation guide §8.1: carrier changes are name/service-level corrections only.
    Quality rules: CARRIER_RULES.
    """
    if "carrier_name" in df_bronze_carriers.columns:
        deduped, duplicates = split_duplicates(df_bronze_carriers, key_col="carrier_id")
        valid, quar = apply_quality_rules(deduped, CARRIER_RULES, "CARRIER_MASTER", batch_id, "carrier_id")
        quar_carriers = quarantine_duplicates(duplicates, "carrier_id", "CARRIER_MASTER", batch_id).unionByName(quar)
        carriers = _with_effective_from(
            valid.select(*[c for c in CARRIER_MASTER_COLUMNS if c in valid.columns],
                         *([col("_ingest_timestamp")] if "_ingest_timestamp" in valid.columns else []))
        )
    else:
        # Fallback: carriers seen on shipments. Repeats of a carrier_id are expected here
        # (one per shipment), so they are collapsed rather than quarantined as duplicates.
        valid, quar_carriers = apply_quality_rules(
            df_bronze_carriers, CARRIER_RULES, "CARRIER_API", batch_id, "carrier_id"
        )
        carriers = (
            valid.select(col("carrier_id"))
            .distinct()
            .withColumn("carrier_name", expr("concat_ws(' ', split(carrier_id, '_')[1], 'Logistics')"))
            .withColumn("service_level", lit("STANDARD"))
            .withColumn("contact_email", expr("concat(lower(carrier_id), '@carrier-network.com')"))
            .withColumn("is_active", lit(True))
            .withColumn("effective_from", current_timestamp())
        )

    dim_carrier = apply_scd1_dimension(
        df_incoming=carriers,
        df_existing=df_existing_dim,
        natural_key="carrier_id",
        effective_from_col="effective_from",
        sk_col="carrier_sk",
    )
    return dim_carrier, quar_carriers


def transform_dim_customer(
    df_bronze_customers: DataFrame,
    df_existing_dim: Optional[DataFrame] = None,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Validate and apply SCD Type 2 for silver.dim_customer.

    Source Contract: SQL Server OMS customer entity data (CDC feed).
    Tracked SCD2 attributes: customer_name, customer_segment, country, city.
    Quality rules: CUSTOMER_RULES.
    """
    deduped, duplicates = split_duplicates(df_bronze_customers, key_col="customer_id")
    valid, quar = apply_quality_rules(deduped, CUSTOMER_RULES, "SQLSERVER_OMS", batch_id, "customer_id")
    quar_customers = quarantine_duplicates(duplicates, "customer_id", "SQLSERVER_OMS", batch_id).unionByName(quar)

    dim_customer = apply_scd2_dimension(
        df_incoming=_with_effective_from(valid),
        df_existing=df_existing_dim,
        natural_key="customer_id",
        tracked_cols=["customer_name", "customer_segment", "country", "city"],
        effective_from_col="effective_from",
        sk_col="customer_sk",
    )
    return dim_customer, quar_customers


def transform_dim_product(
    df_bronze_products: DataFrame,
    df_existing_dim: Optional[DataFrame] = None,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Validate and apply SCD Type 2 for silver.dim_product.

    Source Contract: SQL Server OMS / ERP item master (CDC feed).
    Tracked SCD2 attributes: product_name, category, unit_weight_kg, unit_volume_cbm,
    unit_cost, unit_price (cost/price are optional in the feed).
    Quality rules: PRODUCT_RULES.
    """
    deduped, duplicates = split_duplicates(df_bronze_products, key_col="product_id")
    valid, quar = apply_quality_rules(deduped, PRODUCT_RULES, "SQLSERVER_OMS", batch_id, "product_id")
    quar_products = quarantine_duplicates(duplicates, "product_id", "SQLSERVER_OMS", batch_id).unionByName(quar)

    dim_product = apply_scd2_dimension(
        df_incoming=_with_effective_from(valid),
        df_existing=df_existing_dim,
        natural_key="product_id",
        tracked_cols=["product_name", "category", "unit_weight_kg", "unit_volume_cbm", "unit_cost", "unit_price"],
        effective_from_col="effective_from",
        sk_col="product_sk",
    )
    return dim_product, quar_products


def transform_dim_warehouse(
    df_bronze_warehouses: DataFrame,
    df_existing_dim: Optional[DataFrame] = None,
    batch_id: str = "batch_default",
) -> Tuple[DataFrame, DataFrame]:
    """Validate and apply SCD Type 2 for silver.dim_warehouse.

    Source Contract: WMS warehouse master (batch extract).
    Tracked SCD2 attributes: warehouse_name, facility_type, city, country, latitude, longitude, capacity_sqm.
    Quality rules: WAREHOUSE_RULES.
    """
    deduped, duplicates = split_duplicates(df_bronze_warehouses, key_col="warehouse_id")
    valid, quar = apply_quality_rules(deduped, WAREHOUSE_RULES, "WMS", batch_id, "warehouse_id")
    quar_warehouses = quarantine_duplicates(duplicates, "warehouse_id", "WMS", batch_id).unionByName(quar)

    dim_warehouse = apply_scd2_dimension(
        df_incoming=_with_effective_from(valid),
        df_existing=df_existing_dim,
        natural_key="warehouse_id",
        tracked_cols=["warehouse_name", "facility_type", "city", "country", "latitude", "longitude", "capacity_sqm"],
        effective_from_col="effective_from",
        sk_col="warehouse_sk",
    )
    return dim_warehouse, quar_warehouses
