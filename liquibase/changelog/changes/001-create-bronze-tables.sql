-- liquibase formatted sql

-- ============================================================================
-- Changeset: 001-create-bronze-tables
-- Description: Define raw append-only Bronze Delta tables for routes, weather,
--              and shipments feeds, along with raw ingestion metadata.
-- Author: scm-platform-team
-- Note: The `bronze` schema is provisioned by Terraform in Unity Catalog.
--       Liquibase manages table objects inside this schema.
-- ============================================================================

-- changeset scm-platform-team:bronze-routes-v1
-- comment: Create raw bronze routes Delta table matching OpenRouteService normalized schema
CREATE TABLE IF NOT EXISTS bronze.routes (
    route_id STRING NOT NULL COMMENT 'Deterministic 16-char SHA-256 identifier',
    origin_longitude DOUBLE NOT NULL COMMENT 'Origin longitude coordinate',
    origin_latitude DOUBLE NOT NULL COMMENT 'Origin latitude coordinate',
    destination_longitude DOUBLE NOT NULL COMMENT 'Destination longitude coordinate',
    destination_latitude DOUBLE NOT NULL COMMENT 'Destination latitude coordinate',
    distance_meters DOUBLE NOT NULL COMMENT 'Calculated road route distance in meters',
    duration_seconds DOUBLE NOT NULL COMMENT 'Calculated road route duration in seconds',
    routing_profile STRING NOT NULL COMMENT 'Routing profile e.g. driving-hgv',
    source STRING NOT NULL COMMENT 'Source API or system identifier e.g. openrouteservice',
    _ingest_timestamp TIMESTAMP NOT NULL COMMENT 'UTC timestamp when record was ingested into Bronze',
    _source_file STRING COMMENT 'Source file path or ADLS blob URI'
)
USING DELTA
COMMENT 'Bronze raw append-only Delta table for route calculations';

-- changeset scm-platform-team:bronze-weather-v1
-- comment: Create raw bronze weather Delta table matching Open-Meteo hourly forecast schema
CREATE TABLE IF NOT EXISTS bronze.weather (
    weather_record_id STRING NOT NULL COMMENT 'Deterministic 16-char SHA-256 identifier',
    latitude DOUBLE NOT NULL COMMENT 'Weather station / forecast point latitude',
    longitude DOUBLE NOT NULL COMMENT 'Weather station / forecast point longitude',
    forecast_time_utc STRING NOT NULL COMMENT 'Hourly forecast target timestamp as ISO 8601 string',
    temperature_2m_c DOUBLE COMMENT 'Air temperature at 2 meters in Celsius',
    precipitation_mm DOUBLE COMMENT 'Hourly precipitation in millimeters',
    wind_speed_10m_kmh DOUBLE COMMENT 'Wind speed at 10 meters in km/h',
    visibility_m DOUBLE COMMENT 'Horizontal visibility in meters',
    source STRING NOT NULL COMMENT 'Source API or system identifier e.g. open-meteo',
    _ingest_timestamp TIMESTAMP NOT NULL COMMENT 'UTC timestamp when record was ingested into Bronze',
    _source_file STRING COMMENT 'Source file path or ADLS blob URI'
)
USING DELTA
COMMENT 'Bronze raw append-only Delta table for hourly weather forecasts';

-- changeset scm-platform-team:bronze-shipments-v1
-- comment: Create raw bronze shipments Delta table matching synthetic shipments schema
CREATE TABLE IF NOT EXISTS bronze.shipments (
    shipment_id STRING NOT NULL COMMENT 'Deterministic shipment identifier (shp_<hex>)',
    route_id STRING NOT NULL COMMENT 'Foreign route identifier referencing bronze.routes',
    carrier_id STRING NOT NULL COMMENT 'Assigned carrier identifier e.g. CARRIER_DHL',
    dispatch_time_utc STRING NOT NULL COMMENT 'Shipment departure timestamp in ISO 8601 UTC string',
    planned_delivery_time_utc STRING NOT NULL COMMENT 'Planned arrival timestamp in ISO 8601 UTC string',
    data_type STRING NOT NULL COMMENT 'Data classification tag e.g. synthetic or production',
    _ingest_timestamp TIMESTAMP NOT NULL COMMENT 'UTC timestamp when record was ingested into Bronze',
    _source_file STRING COMMENT 'Source file path or ADLS blob URI'
)
USING DELTA
COMMENT 'Bronze raw append-only Delta table for shipment dispatches';
