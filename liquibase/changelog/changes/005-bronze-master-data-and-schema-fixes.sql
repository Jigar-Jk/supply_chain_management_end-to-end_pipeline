-- liquibase formatted sql

-- ============================================================================
-- Changeset: 005-bronze-master-data-and-schema-fixes
-- Description: 1. Bronze tables for the master-data feeds (warehouses, products,
--                 customers, carriers) that populate the conformed dimensions.
--              2. Fixes to the 001 Bronze tables so bad records can land and be
--                 quarantined: business columns become nullable, and the Auto Loader
--                 `_rescued_data` column is added (the stream writes it, the DDL lacked it).
--              3. Extra dimension attributes needed by Gold KPIs:
--                 dim_product.unit_cost / unit_price (inventory turnover, COGS) and
--                 dim_carrier.transport_mode / contracted_otif_target (carrier OTIF).
-- Author: scm-platform-team
-- ============================================================================

-- changeset scm-platform-team:bronze-warehouses-v1
-- comment: Raw append-only WMS warehouse master feed
CREATE TABLE IF NOT EXISTS bronze.warehouses (
    warehouse_id STRING COMMENT 'Natural warehouse key from WMS (nullable: bad records are quarantined in Silver)',
    warehouse_name STRING COMMENT 'Facility display name',
    facility_type STRING COMMENT 'REGIONAL_DC, HUB, FULFILLMENT_CENTER, CROSS_DOCK',
    city STRING COMMENT 'Physical city',
    country STRING COMMENT 'ISO country code',
    latitude DOUBLE COMMENT 'Latitude as received (not range-checked in Bronze)',
    longitude DOUBLE COMMENT 'Longitude as received (not range-checked in Bronze)',
    capacity_sqm DOUBLE COMMENT 'Storage capacity in square meters',
    _ingest_timestamp TIMESTAMP COMMENT 'UTC timestamp when record was ingested into Bronze',
    _source_file STRING COMMENT 'Source file path or ADLS blob URI',
    _rescued_data STRING COMMENT 'Auto Loader rescued data for fields not matching the schema'
)
USING DELTA
COMMENT 'Bronze raw append-only WMS warehouse master';

-- changeset scm-platform-team:bronze-products-v1
-- comment: Raw append-only ERP item master feed
CREATE TABLE IF NOT EXISTS bronze.products (
    product_id STRING COMMENT 'Natural SKU key (nullable: bad records are quarantined in Silver)',
    product_name STRING COMMENT 'Product title',
    category STRING COMMENT 'Product category',
    unit_weight_kg DOUBLE COMMENT 'Unit weight in kilograms as received',
    unit_volume_cbm DOUBLE COMMENT 'Unit volume in cubic meters as received',
    unit_cost DOUBLE COMMENT 'Standard unit cost (COGS basis)',
    unit_price DOUBLE COMMENT 'List unit sale price',
    _ingest_timestamp TIMESTAMP COMMENT 'UTC timestamp when record was ingested into Bronze',
    _source_file STRING COMMENT 'Source file path or ADLS blob URI',
    _rescued_data STRING COMMENT 'Auto Loader rescued data for fields not matching the schema'
)
USING DELTA
COMMENT 'Bronze raw append-only ERP item master';

-- changeset scm-platform-team:bronze-customers-v1
-- comment: Raw append-only OMS customer master feed
CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id STRING COMMENT 'Natural customer key (nullable: bad records are quarantined in Silver)',
    customer_name STRING COMMENT 'Customer entity name',
    customer_segment STRING COMMENT 'ENTERPRISE, SMB, RETAIL',
    country STRING COMMENT 'ISO country code',
    city STRING COMMENT 'Primary operating city',
    _ingest_timestamp TIMESTAMP COMMENT 'UTC timestamp when record was ingested into Bronze',
    _source_file STRING COMMENT 'Source file path or ADLS blob URI',
    _rescued_data STRING COMMENT 'Auto Loader rescued data for fields not matching the schema'
)
USING DELTA
COMMENT 'Bronze raw append-only OMS customer master';

-- changeset scm-platform-team:bronze-carriers-v1
-- comment: Raw append-only carrier contract master feed
CREATE TABLE IF NOT EXISTS bronze.carriers (
    carrier_id STRING COMMENT 'Natural carrier key (nullable: bad records are quarantined in Silver)',
    carrier_name STRING COMMENT 'Carrier display name',
    service_level STRING COMMENT 'STANDARD, EXPRESS, FREIGHT_HEAVY, ECONOMY',
    transport_mode STRING COMMENT 'ROAD, RAIL, INTERMODAL',
    contracted_otif_target DOUBLE COMMENT 'Contracted OTIF target percentage (0-100)',
    contact_email STRING COMMENT 'Dispatch contact email',
    is_active BOOLEAN COMMENT 'Operational authorization flag',
    _ingest_timestamp TIMESTAMP COMMENT 'UTC timestamp when record was ingested into Bronze',
    _source_file STRING COMMENT 'Source file path or ADLS blob URI',
    _rescued_data STRING COMMENT 'Auto Loader rescued data for fields not matching the schema'
)
USING DELTA
COMMENT 'Bronze raw append-only carrier contract master';

-- changeset scm-platform-team:bronze-001-tables-accept-bad-records
-- comment: Bronze must store every record verbatim; NOT NULL made one bad row fail the whole ingest
ALTER TABLE bronze.routes ALTER COLUMN route_id DROP NOT NULL;
ALTER TABLE bronze.routes ALTER COLUMN origin_longitude DROP NOT NULL;
ALTER TABLE bronze.routes ALTER COLUMN origin_latitude DROP NOT NULL;
ALTER TABLE bronze.routes ALTER COLUMN destination_longitude DROP NOT NULL;
ALTER TABLE bronze.routes ALTER COLUMN destination_latitude DROP NOT NULL;
ALTER TABLE bronze.routes ALTER COLUMN distance_meters DROP NOT NULL;
ALTER TABLE bronze.routes ALTER COLUMN duration_seconds DROP NOT NULL;
ALTER TABLE bronze.routes ALTER COLUMN routing_profile DROP NOT NULL;
ALTER TABLE bronze.routes ALTER COLUMN source DROP NOT NULL;
ALTER TABLE bronze.weather ALTER COLUMN weather_record_id DROP NOT NULL;
ALTER TABLE bronze.weather ALTER COLUMN latitude DROP NOT NULL;
ALTER TABLE bronze.weather ALTER COLUMN longitude DROP NOT NULL;
ALTER TABLE bronze.weather ALTER COLUMN forecast_time_utc DROP NOT NULL;
ALTER TABLE bronze.weather ALTER COLUMN source DROP NOT NULL;
ALTER TABLE bronze.shipments ALTER COLUMN shipment_id DROP NOT NULL;
ALTER TABLE bronze.shipments ALTER COLUMN route_id DROP NOT NULL;
ALTER TABLE bronze.shipments ALTER COLUMN carrier_id DROP NOT NULL;
ALTER TABLE bronze.shipments ALTER COLUMN dispatch_time_utc DROP NOT NULL;
ALTER TABLE bronze.shipments ALTER COLUMN planned_delivery_time_utc DROP NOT NULL;
ALTER TABLE bronze.shipments ALTER COLUMN data_type DROP NOT NULL;

-- changeset scm-platform-team:bronze-001-tables-rescued-data
-- comment: Auto Loader writes _rescued_data; the 001 tables lacked it, so appends would fail
ALTER TABLE bronze.routes ADD COLUMNS (_rescued_data STRING COMMENT 'Auto Loader rescued data for fields not matching the schema');
ALTER TABLE bronze.weather ADD COLUMNS (_rescued_data STRING COMMENT 'Auto Loader rescued data for fields not matching the schema');
ALTER TABLE bronze.shipments ADD COLUMNS (_rescued_data STRING COMMENT 'Auto Loader rescued data for fields not matching the schema');

-- changeset scm-platform-team:dim-product-cost-price-v2
-- comment: Unit cost/price drive COGS and inventory turnover in Gold (tracked as SCD2 attributes)
ALTER TABLE silver.dim_product ADD COLUMNS (
    unit_cost DOUBLE COMMENT 'Standard unit cost (COGS basis); SCD2-tracked',
    unit_price DOUBLE COMMENT 'List unit sale price; SCD2-tracked'
);

-- changeset scm-platform-team:dim-carrier-contract-attributes-v2
-- comment: Carrier contract attributes from the carrier master feed (SCD Type 1)
ALTER TABLE silver.dim_carrier ADD COLUMNS (
    transport_mode STRING COMMENT 'ROAD, RAIL, INTERMODAL',
    contracted_otif_target DOUBLE COMMENT 'Contracted OTIF target percentage (0-100)'
);
