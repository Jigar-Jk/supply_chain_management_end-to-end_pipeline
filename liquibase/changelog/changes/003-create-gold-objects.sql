-- liquibase formatted sql

-- ============================================================================
-- Changeset: 003-create-gold-objects
-- Description: Define business-level Gold analytical views and summary marts
--              for logistics visibility and carrier performance.
-- Author: scm-platform-team
-- Note: The `gold` schema is provisioned by Terraform in Unity Catalog.
--       Liquibase manages views and table objects inside this schema.
-- ============================================================================

-- changeset scm-platform-team:gold-shipment-route-summary-v1
-- comment: Gold analytical view joining shipments with route distances and calculated transit times
CREATE OR REPLACE VIEW gold.shipment_route_summary AS
SELECT
    s.shipment_id,
    s.carrier_id,
    s.route_id,
    r.origin_longitude,
    r.origin_latitude,
    r.destination_longitude,
    r.destination_latitude,
    r.distance_meters,
    r.duration_seconds AS expected_driving_seconds,
    s.dispatch_time,
    s.planned_delivery_time,
    s.planned_transit_hours,
    s.data_type,
    CURRENT_TIMESTAMP() AS view_refreshed_at
FROM silver.shipments s
JOIN silver.routes r ON s.route_id = r.route_id;

-- changeset scm-platform-team:gold-carrier-performance-daily-v1
-- comment: Gold aggregate table summarizing daily shipment volume and planned transit metrics by carrier
CREATE TABLE IF NOT EXISTS gold.carrier_performance_daily (
    summary_date DATE NOT NULL COMMENT 'Date of dispatch in UTC',
    carrier_id STRING NOT NULL COMMENT 'Carrier identifier',
    total_dispatched_shipments BIGINT NOT NULL COMMENT 'Total count of shipments dispatched on this date',
    avg_planned_transit_hours DOUBLE COMMENT 'Average planned transit duration in hours',
    min_planned_transit_hours DOUBLE COMMENT 'Minimum planned transit duration in hours',
    max_planned_transit_hours DOUBLE COMMENT 'Maximum planned transit duration in hours',
    total_planned_distance_km DOUBLE COMMENT 'Total planned distance in kilometers across all routes',
    updated_at TIMESTAMP NOT NULL COMMENT 'Timestamp when metrics row was calculated/updated',
    CONSTRAINT pk_gold_carrier_perf PRIMARY KEY (summary_date, carrier_id)
)
USING DELTA
COMMENT 'Gold aggregate table summarizing daily dispatch volumes and transit metrics by carrier';
