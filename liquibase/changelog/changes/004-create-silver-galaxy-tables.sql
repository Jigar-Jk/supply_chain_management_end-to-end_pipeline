-- liquibase formatted sql

-- ============================================================================
-- Changeset: 004-create-silver-galaxy-tables
-- Description: Implement Silver Galaxy Schema (3 Fact Tables, 6 Conformed Dimensions)
--              with SCD Type 2 on customer/product/warehouse, Type 1 on carrier/route,
--              and charter-compliant quarantine.
-- Author: scm-platform-team
-- Note: Manages Silver layer schema objects within Unity Catalog `silver` and `quarantine`.
-- ============================================================================

-- changeset scm-platform-team:quarantine-records-charter-v2
-- comment: Centralized quarantine table strictly complying with charter attributes and lifecycle status
CREATE TABLE IF NOT EXISTS quarantine.records (
    quarantine_id STRING NOT NULL COMMENT 'Deterministic unique identifier for the quarantined entry',
    original_payload STRING NOT NULL COMMENT 'Raw payload of the failing record in JSON string format',
    failure_reason STRING NOT NULL COMMENT 'Human-readable explanation of why the record was quarantined',
    failed_rule STRING NOT NULL COMMENT 'Standardized rule code violated (e.g. RULE_REF_INTEGRITY_ROUTE)',
    batch_id STRING NOT NULL COMMENT 'Identifier of the ingestion batch or pipeline run',
    source_system STRING NOT NULL COMMENT 'Originating source system (e.g. ORACLE_OMS, WMS, CARRIER_API, ORS)',
    ingested_at TIMESTAMP NOT NULL COMMENT 'Timestamp when record was quarantined',
    status STRING NOT NULL DEFAULT 'PENDING' COMMENT 'Quarantine lifecycle status: PENDING, REPROCESSED, REJECTED',
    CONSTRAINT pk_quarantine_records PRIMARY KEY (quarantine_id)
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Centralized quarantine dead-letter repository with lifecycle status tracking';


-- changeset scm-platform-team:dim-customer-v1
-- comment: Conformed customer dimension with SCD Type 2 versioning (Source: Oracle OMS / ERP)
CREATE TABLE IF NOT EXISTS silver.dim_customer (
    customer_sk STRING NOT NULL COMMENT 'Surrogate key (hash of customer_id + effective_from)',
    customer_id STRING NOT NULL COMMENT 'Natural business key from Oracle OMS',
    customer_name STRING COMMENT 'Customer legal or commercial entity name',
    customer_segment STRING COMMENT 'Market segment: ENTERPRISE, SMB, RETAIL',
    country STRING COMMENT 'ISO country code or name',
    city STRING COMMENT 'Primary operating city',
    effective_from TIMESTAMP NOT NULL COMMENT 'SCD2 validity start timestamp',
    effective_to TIMESTAMP COMMENT 'SCD2 validity end timestamp (NULL for active version)',
    is_current BOOLEAN NOT NULL COMMENT 'SCD2 active version flag (TRUE = current, FALSE = historical)',
    created_at TIMESTAMP NOT NULL COMMENT 'Silver materialization audit timestamp',
    CONSTRAINT pk_dim_customer PRIMARY KEY (customer_sk)
)
USING DELTA
COMMENT 'Conformed customer dimension supporting SCD Type 2 history';


-- changeset scm-platform-team:dim-product-v1
-- comment: Conformed product master dimension with SCD Type 2 versioning (Source: Oracle OMS / ERP)
CREATE TABLE IF NOT EXISTS silver.dim_product (
    product_sk STRING NOT NULL COMMENT 'Surrogate key (hash of product_id + effective_from)',
    product_id STRING NOT NULL COMMENT 'Natural business key (SKU code) from Oracle OMS',
    product_name STRING COMMENT 'Descriptive product title',
    category STRING COMMENT 'Merchandise or supply hierarchy category',
    unit_weight_kg DOUBLE COMMENT 'Standard unit weight in kilograms',
    unit_volume_cbm DOUBLE COMMENT 'Standard unit volume in cubic meters',
    effective_from TIMESTAMP NOT NULL COMMENT 'SCD2 validity start timestamp',
    effective_to TIMESTAMP COMMENT 'SCD2 validity end timestamp (NULL for active version)',
    is_current BOOLEAN NOT NULL COMMENT 'SCD2 active version flag (TRUE = current, FALSE = historical)',
    created_at TIMESTAMP NOT NULL COMMENT 'Silver materialization audit timestamp',
    CONSTRAINT pk_dim_product PRIMARY KEY (product_sk)
)
USING DELTA
COMMENT 'Conformed product dimension supporting SCD Type 2 history';


-- changeset scm-platform-team:dim-warehouse-v1
-- comment: Conformed warehouse / fulfillment center dimension with SCD Type 2 (Source: WMS)
CREATE TABLE IF NOT EXISTS silver.dim_warehouse (
    warehouse_sk STRING NOT NULL COMMENT 'Surrogate key (hash of warehouse_id + effective_from)',
    warehouse_id STRING NOT NULL COMMENT 'Natural business key from WMS',
    warehouse_name STRING COMMENT 'Distribution center / warehouse facility name',
    facility_type STRING COMMENT 'Facility classification: REGIONAL_DC, HUB, FULFILLMENT_CENTER',
    city STRING COMMENT 'Physical city location',
    country STRING COMMENT 'Physical country location',
    latitude DOUBLE COMMENT 'Validated geographic latitude coordinate',
    longitude DOUBLE COMMENT 'Validated geographic longitude coordinate',
    capacity_sqm DOUBLE COMMENT 'Total storage capacity in square meters',
    effective_from TIMESTAMP NOT NULL COMMENT 'SCD2 validity start timestamp',
    effective_to TIMESTAMP COMMENT 'SCD2 validity end timestamp (NULL for active version)',
    is_current BOOLEAN NOT NULL COMMENT 'SCD2 active version flag (TRUE = current, FALSE = historical)',
    created_at TIMESTAMP NOT NULL COMMENT 'Silver materialization audit timestamp',
    CONSTRAINT pk_dim_warehouse PRIMARY KEY (warehouse_sk)
)
USING DELTA
COMMENT 'Conformed warehouse dimension supporting SCD Type 2 history';


-- changeset scm-platform-team:dim-carrier-v1
-- comment: Conformed carrier dimension, SCD Type 1 per guide 8.1 (Source: Carrier Tracking & Telemetry)
CREATE TABLE IF NOT EXISTS silver.dim_carrier (
    carrier_sk STRING NOT NULL COMMENT 'Stable surrogate key (md5 of carrier_id); SCD Type 1',
    carrier_id STRING NOT NULL COMMENT 'Natural business key e.g. CARRIER_DHL, CARRIER_UPS',
    carrier_name STRING COMMENT 'Full carrier brand name',
    service_level STRING COMMENT 'Default SLA tier e.g. STANDARD, EXPRESS, FREIGHT_HEAVY',
    contact_email STRING COMMENT 'Carrier dispatch contact email',
    is_active BOOLEAN NOT NULL DEFAULT TRUE COMMENT 'Operational authorization indicator',
    effective_from TIMESTAMP NOT NULL COMMENT 'SCD2 validity start timestamp',
    effective_to TIMESTAMP COMMENT 'SCD2 validity end timestamp (NULL for active version)',
    is_current BOOLEAN NOT NULL COMMENT 'SCD2 active version flag (TRUE = current, FALSE = historical)',
    created_at TIMESTAMP NOT NULL COMMENT 'Silver materialization audit timestamp',
    CONSTRAINT pk_dim_carrier PRIMARY KEY (carrier_sk)
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Conformed carrier dimension (SCD Type 1, overwrite in place)';


-- changeset scm-platform-team:dim-route-v1
-- comment: Conformed route network dimension, SCD Type 1 per guide 8.1 (Source: OpenRouteService / GIS)
CREATE TABLE IF NOT EXISTS silver.dim_route (
    route_sk STRING NOT NULL COMMENT 'Stable surrogate key (md5 of route_id); SCD Type 1',
    route_id STRING NOT NULL COMMENT 'Natural unique route corridor identifier',
    origin_longitude DOUBLE NOT NULL COMMENT 'Origin longitude coordinate',
    origin_latitude DOUBLE NOT NULL COMMENT 'Origin latitude coordinate',
    destination_longitude DOUBLE NOT NULL COMMENT 'Destination longitude coordinate',
    destination_latitude DOUBLE NOT NULL COMMENT 'Destination latitude coordinate',
    distance_meters DOUBLE NOT NULL COMMENT 'Calculated road network distance in meters',
    duration_seconds DOUBLE NOT NULL COMMENT 'Expected driving duration in seconds',
    routing_profile STRING NOT NULL COMMENT 'Profile e.g. driving-hgv',
    source STRING NOT NULL COMMENT 'Geospatial service provider e.g. openrouteservice',
    effective_from TIMESTAMP NOT NULL COMMENT 'SCD2 validity start timestamp',
    effective_to TIMESTAMP COMMENT 'SCD2 validity end timestamp (NULL for active version)',
    is_current BOOLEAN NOT NULL COMMENT 'SCD2 active version flag (TRUE = current, FALSE = historical)',
    created_at TIMESTAMP NOT NULL COMMENT 'Silver materialization audit timestamp',
    CONSTRAINT pk_dim_route PRIMARY KEY (route_sk)
)
USING DELTA
COMMENT 'Conformed transportation route corridor dimension (SCD Type 1, overwrite in place)';


-- changeset scm-platform-team:dim-date-v1
-- comment: Conformed calendar date dimension (Static Type 0 dimension)
CREATE TABLE IF NOT EXISTS silver.dim_date (
    date_sk INT NOT NULL COMMENT 'Surrogate integer key formatted as YYYYMMDD (e.g. 20261012)',
    calendar_date DATE NOT NULL COMMENT 'Exact calendar date',
    year INT NOT NULL COMMENT 'Calendar year e.g. 2026',
    quarter INT NOT NULL COMMENT 'Calendar quarter (1 to 4)',
    month INT NOT NULL COMMENT 'Calendar month (1 to 12)',
    month_name STRING NOT NULL COMMENT 'Full month name e.g. October',
    day_of_month INT NOT NULL COMMENT 'Day of month (1 to 31)',
    day_of_week INT NOT NULL COMMENT 'Day of week (1 = Monday, 7 = Sunday)',
    day_name STRING NOT NULL COMMENT 'Day name e.g. Monday',
    is_weekend BOOLEAN NOT NULL COMMENT 'TRUE if Saturday or Sunday',
    is_holiday BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'TRUE if declared public holiday',
    CONSTRAINT pk_dim_date PRIMARY KEY (date_sk)
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Conformed static calendar date dimension for cross-fact time intelligence';


-- changeset scm-platform-team:fact-order-v1
-- comment: Grain: one row per order line. Source: Oracle OMS order data.
CREATE TABLE IF NOT EXISTS silver.fact_order (
    order_line_sk STRING NOT NULL COMMENT 'Deterministic surrogate key for order line',
    order_id STRING NOT NULL COMMENT 'Natural order identifier from Oracle OMS',
    order_line_number INT NOT NULL COMMENT 'Line item number within order',
    customer_sk STRING NOT NULL COMMENT 'Surrogate foreign key to silver.dim_customer',
    product_sk STRING NOT NULL COMMENT 'Surrogate foreign key to silver.dim_product',
    origin_warehouse_sk STRING COMMENT 'Surrogate foreign key to silver.dim_warehouse (fulfillment source)',
    order_date_sk INT NOT NULL COMMENT 'Surrogate foreign key to silver.dim_date (order placement)',
    promised_delivery_date_sk INT COMMENT 'Surrogate foreign key to silver.dim_date (promised delivery)',
    ordered_quantity INT NOT NULL COMMENT 'Ordered item quantity (positive integer)',
    unit_price DOUBLE NOT NULL COMMENT 'Unit sale price in base currency',
    total_price DOUBLE NOT NULL COMMENT 'Line extended total price (ordered_quantity * unit_price)',
    order_status STRING NOT NULL COMMENT 'Lifecycle state: PLACED, CONFIRMED, FULFILLED, CANCELLED',
    customer_id STRING NOT NULL COMMENT 'Natural customer identifier for operational tracing',
    product_id STRING NOT NULL COMMENT 'Natural product SKU identifier for operational tracing',
    warehouse_id STRING COMMENT 'Natural warehouse identifier for operational tracing',
    created_at TIMESTAMP NOT NULL COMMENT 'Silver materialization timestamp',
    CONSTRAINT pk_silver_fact_order PRIMARY KEY (order_line_sk),
    CONSTRAINT fk_fact_order_customer FOREIGN KEY (customer_sk) REFERENCES silver.dim_customer(customer_sk),
    CONSTRAINT fk_fact_order_product FOREIGN KEY (product_sk) REFERENCES silver.dim_product(product_sk),
    CONSTRAINT fk_fact_order_warehouse FOREIGN KEY (origin_warehouse_sk) REFERENCES silver.dim_warehouse(warehouse_sk),
    CONSTRAINT fk_fact_order_date FOREIGN KEY (order_date_sk) REFERENCES silver.dim_date(date_sk)
)
USING DELTA
COMMENT 'Silver order line transaction fact table from Oracle OMS';


-- changeset scm-platform-team:fact-shipment-v1
-- comment: Grain: one row per shipment leg. Source: Carrier tracking and shipment events.
CREATE TABLE IF NOT EXISTS silver.fact_shipment (
    shipment_leg_sk STRING NOT NULL COMMENT 'Deterministic surrogate key for shipment leg',
    shipment_id STRING NOT NULL COMMENT 'Natural shipment identifier',
    leg_sequence INT NOT NULL DEFAULT 1 COMMENT 'Leg sequence index (1 for direct point-to-point)',
    carrier_sk STRING NOT NULL COMMENT 'Surrogate foreign key to silver.dim_carrier',
    route_sk STRING NOT NULL COMMENT 'Surrogate foreign key to silver.dim_route',
    origin_warehouse_sk STRING COMMENT 'Surrogate foreign key to silver.dim_warehouse (origin DC if applicable)',
    dispatch_date_sk INT NOT NULL COMMENT 'Surrogate foreign key to silver.dim_date (dispatch date)',
    planned_delivery_date_sk INT NOT NULL COMMENT 'Surrogate foreign key to silver.dim_date (planned delivery date)',
    dispatch_time TIMESTAMP NOT NULL COMMENT 'Parsed dispatch timestamp in UTC',
    planned_delivery_time TIMESTAMP NOT NULL COMMENT 'Parsed planned delivery timestamp in UTC',
    planned_transit_hours DOUBLE NOT NULL COMMENT 'Calculated planned transit duration in hours',
    actual_delivery_time TIMESTAMP COMMENT 'Actual delivery timestamp in UTC (NULL if in transit)',
    actual_transit_hours DOUBLE COMMENT 'Actual transit duration in hours (NULL if in transit)',
    shipment_status STRING NOT NULL DEFAULT 'DISPATCHED' COMMENT 'Status: DISPATCHED, IN_TRANSIT, DELIVERED, EXCEPTION',
    carrier_id STRING NOT NULL COMMENT 'Natural carrier identifier for operational tracing',
    route_id STRING NOT NULL COMMENT 'Natural route corridor identifier for operational tracing',
    data_type STRING NOT NULL COMMENT 'Data classification e.g. synthetic, live',
    created_at TIMESTAMP NOT NULL COMMENT 'Silver materialization timestamp',
    CONSTRAINT pk_silver_fact_shipment PRIMARY KEY (shipment_leg_sk),
    CONSTRAINT fk_fact_shipment_carrier FOREIGN KEY (carrier_sk) REFERENCES silver.dim_carrier(carrier_sk),
    CONSTRAINT fk_fact_shipment_route FOREIGN KEY (route_sk) REFERENCES silver.dim_route(route_sk),
    CONSTRAINT fk_fact_shipment_dispatch_date FOREIGN KEY (dispatch_date_sk) REFERENCES silver.dim_date(date_sk),
    CONSTRAINT fk_fact_shipment_delivery_date FOREIGN KEY (planned_delivery_date_sk) REFERENCES silver.dim_date(date_sk)
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Silver shipment leg fact table from carrier tracking and dispatch events';


-- changeset scm-platform-team:fact-inventory-snapshot-v1
-- comment: Grain: one row per warehouse, product, and snapshot date. Source: WMS reconciliation data.
CREATE TABLE IF NOT EXISTS silver.fact_inventory_snapshot (
    inventory_snapshot_sk STRING NOT NULL COMMENT 'Deterministic surrogate key for inventory snapshot',
    warehouse_sk STRING NOT NULL COMMENT 'Surrogate foreign key to silver.dim_warehouse',
    product_sk STRING NOT NULL COMMENT 'Surrogate foreign key to silver.dim_product',
    snapshot_date_sk INT NOT NULL COMMENT 'Surrogate foreign key to silver.dim_date',
    snapshot_date DATE NOT NULL COMMENT 'Date of the inventory balance count',
    quantity_on_hand DOUBLE NOT NULL COMMENT 'Physical stock count currently on hand',
    quantity_reserved DOUBLE NOT NULL DEFAULT 0.0 COMMENT 'Stock allocated/reserved against open orders',
    quantity_available DOUBLE NOT NULL COMMENT 'Net stock available for fulfillment (on_hand - reserved)',
    reorder_point DOUBLE COMMENT 'Configured inventory replenishment reorder threshold',
    safety_stock DOUBLE COMMENT 'Configured minimum buffer safety stock level',
    is_stockout_risk BOOLEAN NOT NULL DEFAULT FALSE COMMENT 'Flag set TRUE when quantity_available <= safety_stock',
    warehouse_id STRING NOT NULL COMMENT 'Natural warehouse identifier for operational tracing',
    product_id STRING NOT NULL COMMENT 'Natural product SKU identifier for operational tracing',
    created_at TIMESTAMP NOT NULL COMMENT 'Silver materialization timestamp',
    CONSTRAINT pk_silver_fact_inventory PRIMARY KEY (inventory_snapshot_sk),
    CONSTRAINT fk_fact_inventory_warehouse FOREIGN KEY (warehouse_sk) REFERENCES silver.dim_warehouse(warehouse_sk),
    CONSTRAINT fk_fact_inventory_product FOREIGN KEY (product_sk) REFERENCES silver.dim_product(product_sk),
    CONSTRAINT fk_fact_inventory_date FOREIGN KEY (snapshot_date_sk) REFERENCES silver.dim_date(date_sk)
)
USING DELTA
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
COMMENT 'Silver daily inventory balance snapshot fact table from WMS reconciliation';
