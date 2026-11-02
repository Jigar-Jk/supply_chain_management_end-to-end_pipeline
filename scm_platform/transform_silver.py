"""
transform_silver.py – Silver Layer Galaxy Schema Transformations & Quality Gate.

Implements the Supply Chain Control Tower Silver Layer complying with the project charter:
1. Three Fact Tables:
   - silver.fact_order: Grain is one row per order line (Source: SQL Server OMS).
   - silver.fact_shipment: Grain is one row per shipment leg (Source: Carrier tracking / dispatch).
   - silver.fact_inventory_snapshot: Grain is one row per warehouse, product, snapshot date (Source: WMS).
2. Six Conformed Dimensions:
   - silver.dim_customer (SCD Type 2)
   - silver.dim_product (SCD Type 2)
   - silver.dim_warehouse (SCD Type 2)
   - silver.dim_carrier (SCD Type 1 — guide §8.1)
   - silver.dim_route (SCD Type 1 — guide §8.1)
   - silver.dim_date (Static Type 0 calendar dimension)
3. SCD Type 2 Dimension Maintenance (customer, product, warehouse):
   - Surrogate key generation (hash of natural key + effective_from).
   - Historical preservation: attribute changes close existing current version (effective_to, is_current=False)
     and insert a new current version (effective_from, effective_to=None, is_current=True).
   - Stale-record protection: prevents older versions from overwriting newer active records.
   - Point-in-time and current-version fact-to-dimension surrogate key resolution.
4. Charter-Compliant Centralized Quarantine:
   - Dead-letter schema: quarantine_id, original_payload, failure_reason, failed_rule, batch_id, source_system, ingested_at, status.
   - Status flow support: PENDING -> REPROCESSED or REJECTED.
   - Idempotent quarantine merge on quarantine_id.
5. Deterministic Deduplication:
   - Business freshness ordering -> Ingest timestamp -> Stable canonical payload hash tie-breaking.

Code layout (scm_platform/silver/): quality (gate, quarantine, schema drift) · scd (SCD1/SCD2, key
lookup) · dimensions · cdc (Debezium decode, inventory_current) · facts · writer (MERGE) · pipeline.
This module re-exports their public names, so `from scm_platform import transform_silver as ts` and
the job entry point scm_platform.transform_silver:main keep working.
"""

from scm_platform.silver.quality import (  # noqa: F401
    COLUMN_ALIASES,
    DRIFT_REJECT_SHARE,
    FEED_SOURCE_SYSTEM,
    QUARANTINE_LIFECYCLE_COLUMNS,
    QualityRule,
    RESCUED_FIELDS,
    SCHEMA_DRIFT_RULE,
    VALID_QUARANTINE_STATUSES,
    apply_quality_rules,
    build_quarantine_record,
    conform_to_contract,
    deduplicate_dataframe,
    payload_json,
    quarantine_duplicates,
    rejected_file_log_rows,
    schema_drift_report,
    split_duplicates,
    split_latest_versions,
    tag_quarantine_feed,
    update_quarantine_status,
    utc_lit,
    PYSPARK_AVAILABLE,
)
from scm_platform.silver.scd import (  # noqa: F401
    apply_scd1_dimension,
    apply_scd2_dimension,
    lookup_dimension_sk,
)
from scm_platform.silver.dimensions import (  # noqa: F401
    CARRIER_MASTER_COLUMNS,
    CARRIER_RULES,
    CUSTOMER_RULES,
    PRODUCT_RULES,
    ROUTE_RULES,
    WAREHOUSE_RULES,
    generate_dim_date,
    transform_dim_carrier,
    transform_dim_customer,
    transform_dim_product,
    transform_dim_route,
    transform_dim_warehouse,
)
from scm_platform.silver.cdc import (  # noqa: F401
    CDC_COLUMNS,
    INVENTORY_CURRENT_WORKING_COLUMNS,
    INVENTORY_IMAGE_FIELDS,
    ORDER_LINE_IMAGE_FIELDS,
    decode_cdc_rows,
    decode_inventory_cdc,
    decode_order_line_cdc,
    drop_cdc_redeliveries,
    inventory_current_rules,
    latest_cdc_rows,
    transform_inventory_current,
)
from scm_platform.silver.facts import (  # noqa: F401
    GPS_NON_PAYLOAD_COLUMNS,
    INVENTORY_WORKING_COLUMNS,
    ORDER_STATUSES,
    ORDER_WORKING_COLUMNS,
    SHIPMENT_STATUSES,
    SHIPMENT_WORKING_COLUMNS,
    gps_rules,
    inventory_rules,
    latest_ping_per_shipment,
    order_rules,
    shipment_rules,
    transform_fact_inventory_snapshot,
    transform_fact_order,
    transform_fact_shipment,
    transform_gps_events,
    transform_weather,
)
from scm_platform.silver.writer import (  # noqa: F401
    merge_update_condition,
    write_quarantine_table,
    write_silver_table,
)
from scm_platform.silver.pipeline import (  # noqa: F401
    main,
    run_silver_pipeline,
)

__all__ = [
    "COLUMN_ALIASES",
    "DRIFT_REJECT_SHARE",
    "FEED_SOURCE_SYSTEM",
    "QUARANTINE_LIFECYCLE_COLUMNS",
    "QualityRule",
    "RESCUED_FIELDS",
    "SCHEMA_DRIFT_RULE",
    "VALID_QUARANTINE_STATUSES",
    "apply_quality_rules",
    "build_quarantine_record",
    "conform_to_contract",
    "deduplicate_dataframe",
    "payload_json",
    "quarantine_duplicates",
    "rejected_file_log_rows",
    "schema_drift_report",
    "split_duplicates",
    "split_latest_versions",
    "tag_quarantine_feed",
    "update_quarantine_status",
    "utc_lit",
    "apply_scd1_dimension",
    "apply_scd2_dimension",
    "lookup_dimension_sk",
    "CARRIER_MASTER_COLUMNS",
    "CARRIER_RULES",
    "CUSTOMER_RULES",
    "PRODUCT_RULES",
    "ROUTE_RULES",
    "WAREHOUSE_RULES",
    "generate_dim_date",
    "transform_dim_carrier",
    "transform_dim_customer",
    "transform_dim_product",
    "transform_dim_route",
    "transform_dim_warehouse",
    "CDC_COLUMNS",
    "INVENTORY_CURRENT_WORKING_COLUMNS",
    "INVENTORY_IMAGE_FIELDS",
    "ORDER_LINE_IMAGE_FIELDS",
    "decode_cdc_rows",
    "decode_inventory_cdc",
    "decode_order_line_cdc",
    "drop_cdc_redeliveries",
    "inventory_current_rules",
    "latest_cdc_rows",
    "transform_inventory_current",
    "GPS_NON_PAYLOAD_COLUMNS",
    "INVENTORY_WORKING_COLUMNS",
    "ORDER_STATUSES",
    "ORDER_WORKING_COLUMNS",
    "SHIPMENT_STATUSES",
    "SHIPMENT_WORKING_COLUMNS",
    "gps_rules",
    "inventory_rules",
    "latest_ping_per_shipment",
    "order_rules",
    "shipment_rules",
    "transform_fact_inventory_snapshot",
    "transform_fact_order",
    "transform_fact_shipment",
    "transform_gps_events",
    "transform_weather",
    "merge_update_condition",
    "write_quarantine_table",
    "write_silver_table",
    "main",
    "run_silver_pipeline",
]


if __name__ == "__main__":
    main()
