"""
ingest_bronze.py – Modular Bronze Layer Ingestion for Databricks / Delta Lake.

Ingests raw JSON/JSONL datasets into the Unity Catalog Bronze Delta tables:
- API / synthetic feeds: `bronze.routes`, `bronze.weather`, `bronze.shipments`
- Master data feeds: `bronze.warehouses`, `bronze.products`, `bronze.customers`, `bronze.carriers`
- Transaction feeds: `bronze.orders` (SQL Server OMS order lines),
  `bronze.inventory` (WMS nightly stock reconciliation NDJSON)

Design Features:
- Cloud Auto Loader compatibility via format("cloudFiles") and file format "json".
- Incremental file-based ingestion with dedicated checkpointing.
- Bad-record handling via Auto Loader rescue data column (`_rescued_data`).
- Standardized ingestion audit columns (`_ingest_timestamp`, `_source_file`).
- Explicit PySpark schemas matching Liquibase DDL and JSONL data contracts.
- Modular functions decoupled from cluster runtime to allow unit testing and mock verification.
"""

from dataclasses import dataclass
from typing import Any, Dict

# Explicit PySpark imports guarded for local and cluster portability
try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import col, current_timestamp
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        LongType,
        StringType,
        StructField,
        StructType,
    )
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    DataFrame = Any  # type: ignore
    SparkSession = Any  # type: ignore


# ---------------------------------------------------------------------------
# Data Contracts & Explicit StructType Schemas
# ---------------------------------------------------------------------------

def get_routes_schema() -> Any:
    """Return explicit PySpark schema for routes matching Liquibase bronze.routes.

    All fields are nullable: Bronze stores bad records verbatim so the Silver quality gate
    can quarantine them (a non-nullable field would reject the record before quarantine).
    """
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to build StructType schema.")
    return StructType([
        StructField("route_id", StringType(), True),
        StructField("origin_longitude", DoubleType(), True),
        StructField("origin_latitude", DoubleType(), True),
        StructField("destination_longitude", DoubleType(), True),
        StructField("destination_latitude", DoubleType(), True),
        StructField("distance_meters", DoubleType(), True),
        StructField("duration_seconds", DoubleType(), True),
        StructField("routing_profile", StringType(), True),
        StructField("source", StringType(), True),
    ])


def get_weather_schema() -> Any:
    """Return explicit PySpark schema for weather matching Liquibase bronze.weather."""
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to build StructType schema.")
    return StructType([
        StructField("weather_record_id", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("forecast_time_utc", StringType(), True),
        StructField("temperature_2m_c", DoubleType(), True),
        StructField("precipitation_mm", DoubleType(), True),
        StructField("wind_speed_10m_kmh", DoubleType(), True),
        StructField("visibility_m", DoubleType(), True),
        StructField("source", StringType(), True),
    ])


def get_shipments_schema() -> Any:
    """Return explicit PySpark schema for shipments matching Liquibase bronze.shipments."""
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to build StructType schema.")
    return StructType([
        StructField("shipment_id", StringType(), True),
        StructField("route_id", StringType(), True),
        StructField("carrier_id", StringType(), True),
        StructField("dispatch_time_utc", StringType(), True),
        StructField("planned_delivery_time_utc", StringType(), True),
        StructField("data_type", StringType(), True),
        # Added with order-linked shipments (migration 008); absent in the legacy feed
        StructField("order_id", StringType(), True),
        StructField("warehouse_id", StringType(), True),
        StructField("actual_delivery_time_utc", StringType(), True),
        StructField("shipment_status", StringType(), True),
        StructField("ordered_quantity", LongType(), True),
        StructField("shipped_quantity", LongType(), True),
        StructField("weather_risk", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])


# Master-data feeds. Fields are nullable: Bronze must accept bad records verbatim so the
# Silver quality gate can quarantine them, rather than failing the whole ingest.

def get_warehouses_schema() -> Any:
    """Return explicit PySpark schema for the WMS warehouse master feed (bronze.warehouses)."""
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to build StructType schema.")
    return StructType([
        StructField("warehouse_id", StringType(), True),
        StructField("warehouse_name", StringType(), True),
        StructField("facility_type", StringType(), True),
        StructField("city", StringType(), True),
        StructField("country", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("capacity_sqm", DoubleType(), True),
    ])


def get_products_schema() -> Any:
    """Return explicit PySpark schema for the ERP item master feed (bronze.products)."""
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to build StructType schema.")
    return StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("unit_weight_kg", DoubleType(), True),
        StructField("unit_volume_cbm", DoubleType(), True),
        StructField("unit_cost", DoubleType(), True),
        StructField("unit_price", DoubleType(), True),
    ])


def get_customers_schema() -> Any:
    """Return explicit PySpark schema for the OMS customer master feed (bronze.customers)."""
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to build StructType schema.")
    return StructType([
        StructField("customer_id", StringType(), True),
        StructField("customer_name", StringType(), True),
        StructField("customer_segment", StringType(), True),
        StructField("country", StringType(), True),
        StructField("city", StringType(), True),
    ])


def get_carriers_schema() -> Any:
    """Return explicit PySpark schema for the carrier contract master feed (bronze.carriers)."""
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to build StructType schema.")
    return StructType([
        StructField("carrier_id", StringType(), True),
        StructField("carrier_name", StringType(), True),
        StructField("service_level", StringType(), True),
        StructField("transport_mode", StringType(), True),
        StructField("contracted_otif_target", DoubleType(), True),
        StructField("contact_email", StringType(), True),
        StructField("is_active", BooleanType(), True),
    ])


def get_orders_schema() -> Any:
    """Return explicit PySpark schema for SQL Server OMS order lines (bronze.orders).

    File feed of the OMS order lines; Silver prefers bronze.orders_cdc (SQL Server CDC via
    Debezium) when that table exists. Dates are kept as
    strings in Bronze so malformed values are preserved and rejected in Silver.
    """
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to build StructType schema.")
    return StructType([
        StructField("order_id", StringType(), True),
        StructField("order_line_number", LongType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("warehouse_id", StringType(), True),
        StructField("order_date", StringType(), True),
        StructField("promised_delivery_date", StringType(), True),
        StructField("ordered_quantity", LongType(), True),
        StructField("shipped_quantity", LongType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("order_status", StringType(), True),
        StructField("updated_at", StringType(), True),
    ])


def get_inventory_schema() -> Any:
    """Return explicit PySpark schema for the WMS nightly stock reconciliation feed (bronze.inventory).

    NDJSON batch file, one file per night. Dates/timestamps stay strings in Bronze so malformed
    values are preserved and rejected in Silver.
    """
    if not PYSPARK_AVAILABLE:
        raise RuntimeError("PySpark is required to build StructType schema.")
    return StructType([
        StructField("warehouse_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("snapshot_date", StringType(), True),
        StructField("quantity_on_hand", LongType(), True),
        StructField("quantity_reserved", LongType(), True),
        StructField("safety_stock", LongType(), True),
        StructField("reorder_point", LongType(), True),
        StructField("recorded_at", StringType(), True),
    ])


DATASET_SCHEMAS = {
    "routes": get_routes_schema,
    "weather": get_weather_schema,
    "shipments": get_shipments_schema,
    "warehouses": get_warehouses_schema,
    "products": get_products_schema,
    "customers": get_customers_schema,
    "carriers": get_carriers_schema,
    "orders": get_orders_schema,
    "inventory": get_inventory_schema,
}


# ---------------------------------------------------------------------------
# Ingestion Configuration
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IngestionConfig:
    """Configuration contract for a Bronze ingestion feed."""
    feed_name: str
    landing_path: str
    checkpoint_path: str
    target_table: str
    catalog: str = "scm_platform_dev"
    schema: str = "bronze"

    @property
    def full_target_table_name(self) -> str:
        """Return 3-level Unity Catalog table identifier: catalog.schema.table."""
        return f"{self.catalog}.{self.schema}.{self.target_table}"


def get_default_config(
    feed_name: str,
    storage_account: str = "stscmplatformdev",
    catalog: str = "scm_platform_dev",
) -> IngestionConfig:
    """Generate default ADLS Gen2 paths for Databricks Auto Loader ingestion."""
    if feed_name not in DATASET_SCHEMAS:
        raise ValueError(f"Unknown feed_name '{feed_name}'. Valid feeds: {list(DATASET_SCHEMAS.keys())}")

    landing_path = f"abfss://landing@{storage_account}.dfs.core.windows.net/{feed_name}/"
    checkpoint_path = f"abfss://checkpoints@{storage_account}.dfs.core.windows.net/bronze/{feed_name}/"

    return IngestionConfig(
        feed_name=feed_name,
        landing_path=landing_path,
        checkpoint_path=checkpoint_path,
        target_table=feed_name,
        catalog=catalog,
        schema="bronze",
    )


# ---------------------------------------------------------------------------
# Transformation Logic (Unit-Testable on DataFrames)
# ---------------------------------------------------------------------------

def add_bronze_metadata(df: DataFrame, batch_id: str = "manual") -> DataFrame:
    """Add the standard Bronze audit columns (implementation guide §6).

    - _ingest_timestamp: when the record was processed
    - _source_file: the file it came from, from Auto Loader's _metadata.file_path
      (input_file_name() is deprecated on Databricks and rejected on Unity Catalog compute)
    - _batch_id: the job run that loaded it (the job passes {{job.run_id}})
    """
    try:
        from pyspark.sql.functions import col, current_timestamp, lit
        ts_expr, file_expr, batch_expr = current_timestamp(), col("_metadata.file_path"), lit(batch_id)
    except Exception:
        # No active SparkContext (unit tests with mocked DataFrames): column names stand in
        ts_expr, file_expr, batch_expr = "_ingest_timestamp", "_source_file", "_batch_id"

    return (df.withColumn("_ingest_timestamp", ts_expr)
              .withColumn("_source_file", file_expr)
              .withColumn("_batch_id", batch_expr))


# ---------------------------------------------------------------------------
# Stream Ingestion Builders
# ---------------------------------------------------------------------------

def build_autoloader_stream(
    spark: SparkSession,
    config: IngestionConfig,
    schema: Any,
    include_rescue_column: bool = True,
    batch_id: str = "manual",
) -> Any:
    """Build a streaming DataFrame reader using Databricks Auto Loader (cloudFiles).

    Configures:
    - format: cloudFiles
    - cloudFiles.format: json
    - cloudFiles.schemaLocation: checkpoint_path / schema
    - cloudFiles.schemaEvolutionMode: addNewColumns (or rescueDataColumn)
    - cloudFiles.schemaRescueDataColumn: _rescued_data
    """
    reader = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{config.checkpoint_path}/schema")
        .schema(schema)
    )

    if include_rescue_column:
        reader = (
            reader.option("cloudFiles.schemaEvolutionMode", "rescue")
            .option("cloudFiles.schemaRescueDataColumn", "_rescued_data")
        )

    raw_stream = reader.load(config.landing_path)
    return add_bronze_metadata(raw_stream, batch_id=batch_id)


def start_bronze_stream_write(
    stream_df: Any,
    config: IngestionConfig,
    trigger_available_now: bool = True,
) -> Any:
    """Start streaming writer appending records into Unity Catalog Bronze Delta table.

    Configures:
    - outputMode: append (preserves raw append-only Bronze guarantee)
    - format: delta
    - checkpointLocation: config.checkpoint_path
    - trigger: availableNow=True (for scheduled, cost-efficient micro-batch execution)
    """
    writer = (
        stream_df.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", f"{config.checkpoint_path}/data")
    )

    if trigger_available_now:
        # Cost-conscious: processes available files and terminates compute
        writer = writer.trigger(availableNow=True)

    return writer.toTable(config.full_target_table_name)


# ---------------------------------------------------------------------------
# Pipeline Orchestrator Entrypoint
# ---------------------------------------------------------------------------

def ingest_feed(
    spark: SparkSession,
    feed_name: str,
    config: IngestionConfig = None,
    trigger_available_now: bool = True,
    batch_id: str = "manual",
) -> Any:
    """Ingest a single feed into Bronze; batch_id (the job run id) is stamped on every row."""
    if config is None:
        config = get_default_config(feed_name)

    schema_fn = DATASET_SCHEMAS.get(feed_name)
    if schema_fn is None:
        raise ValueError(f"No schema defined for feed '{feed_name}'.")

    schema = schema_fn()
    stream_df = build_autoloader_stream(spark, config, schema, batch_id=batch_id)
    query = start_bronze_stream_write(
        stream_df, config, trigger_available_now=trigger_available_now
    )
    return query


def ingest_all(
    spark: SparkSession,
    storage_account: str = "stscmplatformdev",
    catalog: str = "scm_platform_dev",
    trigger_available_now: bool = True,
    batch_id: str = "manual",
) -> Dict[str, Any]:
    """Execute ingestion for all configured Bronze feeds sequentially."""
    queries = {}
    for feed_name in DATASET_SCHEMAS:
        config = get_default_config(
            feed_name, storage_account=storage_account, catalog=catalog
        )
        query = ingest_feed(
            spark, feed_name, config=config, trigger_available_now=trigger_available_now, batch_id=batch_id
        )
        queries[feed_name] = query
    return queries


def main() -> None:
    """CLI / Job entrypoint for Bronze ingestion."""
    import argparse

    parser = argparse.ArgumentParser(description="Ingest landing data into Unity Catalog Bronze tables.")
    parser.add_argument(
        "--storage-account",
        type=str,
        default="stscmplatformdev",
        help="ADLS Gen2 storage account name (default: stscmplatformdev)",
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default="scm_platform_dev",
        help="Unity Catalog catalog name (default: scm_platform_dev)",
    )
    parser.add_argument(
        "--feed",
        type=str,
        default="all",
        choices=["all", *DATASET_SCHEMAS],
        help="Specific feed to ingest or 'all' for all feeds (default: all)",
    )
    parser.add_argument(
        "--batch-id",
        type=str,
        default="manual",
        help="Job run id stamped on every Bronze row as _batch_id (the job passes {{job.run_id}})",
    )
    args, unknown = parser.parse_known_args()

    spark = SparkSession.builder.getOrCreate()

    if args.feed == "all":
        ingest_all(spark, storage_account=args.storage_account, catalog=args.catalog, batch_id=args.batch_id)
    else:
        config = get_default_config(args.feed, storage_account=args.storage_account, catalog=args.catalog)
        ingest_feed(spark, args.feed, config=config, batch_id=args.batch_id)


if __name__ == "__main__":
    main()
