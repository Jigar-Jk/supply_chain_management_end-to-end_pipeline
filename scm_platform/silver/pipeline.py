"""
Bronze-to-Silver orchestrator (run_silver_pipeline) and the job entry point.

Part of the Silver layer; see scm_platform/transform_silver.py for the overview.
"""

from typing import Any, Optional

# Guard PySpark imports for local test execution and cluster portability
try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import col
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    DataFrame = Any  # type: ignore
    SparkSession = Any  # type: ignore

from scm_platform.silver.cdc import decode_inventory_cdc, decode_order_line_cdc, transform_inventory_current
from scm_platform.silver.dimensions import (
    generate_dim_date,
    transform_dim_carrier,
    transform_dim_customer,
    transform_dim_product,
    transform_dim_route,
    transform_dim_warehouse,
)
from scm_platform.silver.facts import (
    latest_ping_per_shipment,
    transform_fact_inventory_snapshot,
    transform_fact_order,
    transform_fact_shipment,
    transform_gps_events,
    transform_weather,
)
from scm_platform.silver.quality import conform_to_contract, rejected_file_log_rows, schema_drift_report
from scm_platform.silver.writer import write_quarantine_table, write_silver_table


# ---------------------------------------------------------------------------
# 8. Pipeline Orchestrator Entrypoint
# ---------------------------------------------------------------------------

def _table_or_none(spark: SparkSession, table_name: str) -> Optional[DataFrame]:
    """Return the table as a DataFrame, or None when it does not exist yet."""
    return spark.table(table_name) if spark.catalog.tableExists(table_name) else None


def _rows_or_none(spark: SparkSession, table_name: str) -> Optional[DataFrame]:
    """Like _table_or_none, but an existing empty table also counts as absent.

    Migration 011 always creates the CDC Bronze tables; until the CDC stream has delivered its first
    event they are empty, and the file feed must stay the source. CDC Bronze is append-only, so once
    it has rows it stays the source.
    """
    df = _table_or_none(spark, table_name)
    return df if df is not None and df.head(1) else None


def run_silver_pipeline(
    spark: SparkSession,
    catalog: str = "scm_platform_dev",
    batch_id: str = "batch_manual",
) -> None:
    """Execute complete Bronze-to-Silver Galaxy Schema transformation pipeline.

    Dimensions are maintained first so facts resolve against this run's versions.
    fact_order and fact_inventory_snapshot run when their Bronze feeds (bronze.orders,
    bronze.inventory) and the dimensions they reference exist.
    """
    quarantine_table = f"{catalog}.quarantine.records"
    run_log_table = f"{catalog}.ops.pipeline_run_log"

    def file_feed(df, feed):
        """Apply agreed renames (COLUMN_ALIASES), then log this run's files rejected for schema drift."""
        if df is None:
            return None
        df = conform_to_contract(df, feed)
        current = df.filter(col("_batch_id") == batch_id) if "_batch_id" in df.columns else df
        rejected = rejected_file_log_rows(schema_drift_report(current), feed, batch_id)
        if spark.catalog.tableExists(run_log_table) and rejected.head(1):
            rejected.write.mode("append").saveAsTable(run_log_table)
        return df

    def load_dimension(name, transform, bronze_df, sk_col, feed):
        existing = _table_or_none(spark, f"{catalog}.silver.{name}")
        dim_df, quar_df = transform(bronze_df, existing, batch_id=batch_id)
        write_silver_table(dim_df, f"{catalog}.silver.{name}", key_col=sk_col, spark=spark)
        write_quarantine_table(quar_df, quarantine_table, spark=spark, source_feed=feed)
        return dim_df

    # 1. Read Bronze tables
    bronze_routes = file_feed(spark.table(f"{catalog}.bronze.routes"), "routes")
    bronze_shipments = file_feed(spark.table(f"{catalog}.bronze.shipments"), "shipments")
    bronze_weather = file_feed(spark.table(f"{catalog}.bronze.weather"), "weather")
    # Carrier master is preferred; fall back to carrier IDs seen on shipments.
    bronze_carriers = file_feed(_table_or_none(spark, f"{catalog}.bronze.carriers"), "carriers")
    if bronze_carriers is None:
        bronze_carriers = bronze_shipments

    # 2. Maintain Conformed Dimensions
    dim_route = load_dimension("dim_route", transform_dim_route, bronze_routes, "route_sk", "routes")
    dim_carrier = load_dimension("dim_carrier", transform_dim_carrier, bronze_carriers, "carrier_sk", "carriers")
    dims = {}
    for name, transform, bronze_name, sk_col in [
        ("dim_warehouse", transform_dim_warehouse, "warehouses", "warehouse_sk"),
        ("dim_product", transform_dim_product, "products", "product_sk"),
        ("dim_customer", transform_dim_customer, "customers", "customer_sk"),
    ]:
        bronze_df = file_feed(_table_or_none(spark, f"{catalog}.bronze.{bronze_name}"), bronze_name)
        if bronze_df is not None:
            dims[name] = load_dimension(name, transform, bronze_df, sk_col, bronze_name)

    dim_date = generate_dim_date(spark)
    write_silver_table(dim_date, f"{catalog}.silver.dim_date", key_col="date_sk", spark=spark)

    # 3. Transform and write Fact Order (needs customer + product dims; warehouse optional)
    # CDC (SQL Server via Debezium) is the order source when present; the file feed otherwise.
    bronze_orders_cdc = _rows_or_none(spark, f"{catalog}.bronze.orders_cdc")
    bronze_orders = (decode_order_line_cdc(bronze_orders_cdc) if bronze_orders_cdc is not None
                     else file_feed(_table_or_none(spark, f"{catalog}.bronze.orders"), "orders"))
    order_freshness = "source_position" if bronze_orders_cdc is not None else "updated_at"
    if bronze_orders is not None and "dim_customer" in dims and "dim_product" in dims:
        fact_order, quar_order = transform_fact_order(
            bronze_orders, dims["dim_customer"], dims["dim_product"], dims.get("dim_warehouse"), dim_date,
            batch_id=batch_id,
        )
        write_silver_table(
            fact_order, f"{catalog}.silver.fact_order", key_col="order_line_sk", spark=spark,
            freshness_col=order_freshness,
        )
        write_quarantine_table(quar_order, quarantine_table, spark=spark, source_feed="orders")

    bronze_inventory_cdc = _rows_or_none(spark, f"{catalog}.bronze.inventory_cdc")
    if bronze_inventory_cdc is not None:
        inventory_current, quar_inventory_current = transform_inventory_current(
            decode_inventory_cdc(bronze_inventory_cdc), batch_id=batch_id)
        write_silver_table(
            inventory_current, f"{catalog}.silver.inventory_current", key_col="inventory_current_sk",
            spark=spark, freshness_col="source_position",
        )
        write_quarantine_table(quar_inventory_current, quarantine_table, spark=spark, source_feed="inventory_cdc")

    # 4. Transform and write Fact Inventory Snapshot (WMS nightly reconciliation)
    bronze_inventory = file_feed(_table_or_none(spark, f"{catalog}.bronze.inventory"), "inventory")
    if bronze_inventory is not None and "dim_warehouse" in dims and "dim_product" in dims:
        fact_inventory, quar_inventory = transform_fact_inventory_snapshot(
            bronze_inventory, dims["dim_warehouse"], dims["dim_product"], dim_date, batch_id=batch_id
        )
        write_silver_table(
            fact_inventory, f"{catalog}.silver.fact_inventory_snapshot", key_col="inventory_snapshot_sk",
            spark=spark, freshness_col="recorded_at",
        )
        write_quarantine_table(quar_inventory, quarantine_table, spark=spark, source_feed="inventory")

    # 5. GPS/RFID events (streamed into bronze.gps_events) -> quarantine + latest ping per shipment
    last_ping = None
    bronze_gps = _table_or_none(spark, f"{catalog}.bronze.gps_events")
    if bronze_gps is not None:
        gps_clean, quar_gps = transform_gps_events(bronze_gps, batch_id=batch_id)
        write_quarantine_table(quar_gps, quarantine_table, spark=spark, source_feed="gps_events")
        last_ping = latest_ping_per_shipment(gps_clean)

    # 6. Transform and write Fact Shipment
    fact_shipment, quar_shipment = transform_fact_shipment(
        bronze_shipments, dim_carrier, dim_route, dim_date, batch_id=batch_id,
        df_dim_warehouse=dims.get("dim_warehouse"), df_last_ping=last_ping,
    )
    write_silver_table(
        fact_shipment, f"{catalog}.silver.fact_shipment", key_col="shipment_leg_sk", spark=spark, freshness_col="updated_at"
    )
    write_quarantine_table(quar_shipment, quarantine_table, spark=spark, source_feed="shipments")

    # 7. Transform weather observations (auxiliary mart)
    silver_weather, quar_weather = transform_weather(bronze_weather, batch_id=batch_id)
    write_silver_table(
        silver_weather, f"{catalog}.silver.weather", key_col="weather_record_id", spark=spark, freshness_col="forecast_time"
    )
    write_quarantine_table(quar_weather, quarantine_table, spark=spark, source_feed="weather")


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="Run Silver layer Galaxy Schema transformations.")
    parser.add_argument("--catalog", type=str, default="scm_platform_dev", help="Unity Catalog catalog name")
    parser.add_argument("--batch-id", type=str, default="batch_manual", help="Unique batch / job run identifier")
    args, _ = parser.parse_known_args()

    spark = SparkSession.builder.getOrCreate()
    run_silver_pipeline(spark, catalog=args.catalog, batch_id=args.batch_id)
