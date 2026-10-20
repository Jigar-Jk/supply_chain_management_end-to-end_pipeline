-- liquibase formatted sql

-- ============================================================================
-- Changeset: 002-create-silver-tables
-- Description: Define cleansed, strongly-typed, deduplicated Silver Delta tables,
--              plus a centralized quarantine table for records failing quality gates.
-- Author: scm-platform-team
-- Note: The `silver` and `quarantine` schemas are provisioned by Terraform in Unity Catalog.
--       Liquibase manages table objects inside these schemas.
-- ============================================================================

-- changeset scm-platform-team:quarantine-records-v1
-- comment: Centralized quarantine table for records failing data quality validations
CREATE TABLE IF NOT EXISTS quarantine.invalid_records (
    quarantine_id STRING NOT NULL COMMENT 'Unique identifier for the quarantined entry',
    source_layer STRING NOT NULL COMMENT 'Origin layer where failure occurred e.g. bronze_to_silver',
    source_table STRING NOT NULL COMMENT 'Origin table name e.g. shipments, routes, weather',
    source_record_id STRING COMMENT 'Identifier of the offending record if parseable',
    raw_payload STRING NOT NULL COMMENT 'Full JSON or string payload of the failed record',
    failure_reason STRING NOT NULL COMMENT 'Description or rule violation causing quarantine',
    quarantine_timestamp TIMESTAMP NOT NULL COMMENT 'Timestamp when record was quarantined',
    reprocessed_flag BOOLEAN DEFAULT FALSE COMMENT 'Indicator if record was remediated and reprocessed'
)
USING DELTA
COMMENT 'Quarantine table for malformed records, nulls, and referential integrity violations'
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported');

-- changeset scm-platform-team:silver-routes-v1
-- comment: Cleaned and typed Silver routes table with primary key constraint
CREATE TABLE IF NOT EXISTS silver.routes (
    route_id STRING NOT NULL COMMENT 'Unique route identifier',
    origin_longitude DOUBLE NOT NULL COMMENT 'Origin longitude coordinate',
    origin_latitude DOUBLE NOT NULL COMMENT 'Origin latitude coordinate',
    destination_longitude DOUBLE NOT NULL COMMENT 'Destination longitude coordinate',
    destination_latitude DOUBLE NOT NULL COMMENT 'Destination latitude coordinate',
    distance_meters DOUBLE NOT NULL COMMENT 'Validated route distance in meters',
    duration_seconds DOUBLE NOT NULL COMMENT 'Validated route duration in seconds',
    routing_profile STRING NOT NULL COMMENT 'Cleaned routing profile e.g. driving-hgv',
    source STRING NOT NULL COMMENT 'Source API identifier',
    created_at TIMESTAMP NOT NULL COMMENT 'Silver materialization timestamp',
    CONSTRAINT pk_silver_routes PRIMARY KEY (route_id)
)
USING DELTA
COMMENT 'Silver cleansed and validated routes with PK constraint';

-- changeset scm-platform-team:silver-weather-v1
-- comment: Cleaned and typed Silver hourly weather table
CREATE TABLE IF NOT EXISTS silver.weather (
    weather_record_id STRING NOT NULL COMMENT 'Unique weather record identifier',
    latitude DOUBLE NOT NULL COMMENT 'Forecast latitude',
    longitude DOUBLE NOT NULL COMMENT 'Forecast longitude',
    forecast_time TIMESTAMP NOT NULL COMMENT 'Parsed TIMESTAMP in UTC',
    temperature_2m_c DOUBLE COMMENT 'Air temperature in Celsius',
    precipitation_mm DOUBLE COMMENT 'Precipitation in millimeters (>= 0)',
    wind_speed_10m_kmh DOUBLE COMMENT 'Wind speed in km/h (>= 0)',
    visibility_m DOUBLE COMMENT 'Visibility in meters (>= 0)',
    source STRING NOT NULL COMMENT 'Source API identifier',
    created_at TIMESTAMP NOT NULL COMMENT 'Silver materialization timestamp',
    CONSTRAINT pk_silver_weather PRIMARY KEY (weather_record_id)
)
USING DELTA
COMMENT 'Silver cleansed and typed hourly weather observations and forecasts';

-- changeset scm-platform-team:silver-shipments-v1
-- comment: Cleaned and typed Silver shipments table with parsed timestamps and route FK
CREATE TABLE IF NOT EXISTS silver.shipments (
    shipment_id STRING NOT NULL COMMENT 'Unique shipment identifier',
    route_id STRING NOT NULL COMMENT 'Foreign key to silver.routes',
    carrier_id STRING NOT NULL COMMENT 'Validated carrier identifier',
    dispatch_time TIMESTAMP NOT NULL COMMENT 'Parsed dispatch TIMESTAMP in UTC',
    planned_delivery_time TIMESTAMP NOT NULL COMMENT 'Parsed planned delivery TIMESTAMP in UTC',
    planned_transit_hours DOUBLE COMMENT 'Calculated planned transit duration in hours',
    data_type STRING NOT NULL COMMENT 'Data classification e.g. synthetic',
    created_at TIMESTAMP NOT NULL COMMENT 'Silver materialization timestamp',
    CONSTRAINT pk_silver_shipments PRIMARY KEY (shipment_id),
    CONSTRAINT fk_silver_shipments_route FOREIGN KEY (route_id) REFERENCES silver.routes(route_id)
)
USING DELTA
COMMENT 'Silver cleansed shipment dispatches with parsed timestamps and FK constraint';
