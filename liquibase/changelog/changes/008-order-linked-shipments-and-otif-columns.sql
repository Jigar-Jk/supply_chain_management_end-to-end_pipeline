-- liquibase formatted sql

-- ============================================================================
-- Changeset: 008-order-linked-shipments-and-otif-columns
-- Description: Shipments are now generated from shipped/delivered orders and carry
--              delivery outcomes, so OTIF and delay can be computed:
--              1. New columns on bronze.shipments (raw, as received).
--              2. New columns on silver.fact_shipment: order/warehouse links,
--                 quantities and the KPI flags delay_minutes, is_on_time, is_in_full.
-- Author: scm-platform-team
-- ============================================================================

-- changeset scm-platform-team:bronze-shipments-order-linked-v2
-- comment: Order linkage and delivery outcome fields from the carrier feed
ALTER TABLE bronze.shipments ADD COLUMNS (
    order_id STRING COMMENT 'Order this shipment fulfils (Oracle OMS order_id)',
    warehouse_id STRING COMMENT 'Origin warehouse natural key',
    actual_delivery_time_utc STRING COMMENT 'Actual delivery timestamp as received (ISO 8601 UTC); NULL while in transit',
    shipment_status STRING COMMENT 'IN_TRANSIT or DELIVERED as received',
    ordered_quantity BIGINT COMMENT 'Total units ordered on the order',
    shipped_quantity BIGINT COMMENT 'Total units loaded on this shipment',
    weather_risk STRING COMMENT 'LOW, MODERATE, HIGH (Open-Meteo thresholds)',
    updated_at STRING COMMENT 'Carrier last-update timestamp (ISO 8601 UTC); freshness key'
);

-- changeset scm-platform-team:fact-shipment-otif-columns-v2
-- comment: OTIF / delay measures; NULL while the shipment is in transit
ALTER TABLE silver.fact_shipment ADD COLUMNS (
    order_id STRING COMMENT 'Order this shipment fulfils',
    warehouse_id STRING COMMENT 'Origin warehouse natural key for operational tracing',
    ordered_quantity INT COMMENT 'Total units ordered on the order',
    shipped_quantity INT COMMENT 'Total units shipped',
    delay_minutes INT COMMENT 'actual_delivery_time - planned_delivery_time in minutes; negative = early; NULL in transit',
    is_on_time BOOLEAN COMMENT 'actual_delivery_time <= planned_delivery_time (carrier promised ETA); NULL in transit',
    is_in_full BOOLEAN COMMENT 'shipped_quantity >= ordered_quantity',
    weather_risk STRING COMMENT 'LOW, MODERATE, HIGH',
    updated_at TIMESTAMP COMMENT 'Carrier last-update timestamp; MERGE never replaces a newer row with an older one'
);

-- changeset scm-platform-team:fact-shipment-status-comment-v2
-- comment: Align documented status domain with the feed
ALTER TABLE silver.fact_shipment ALTER COLUMN shipment_status
    COMMENT 'DISPATCHED (legacy feed without status), IN_TRANSIT, DELIVERED, EXCEPTION';
