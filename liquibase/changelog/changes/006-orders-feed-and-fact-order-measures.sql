-- liquibase formatted sql

-- ============================================================================
-- Changeset: 006-orders-feed-and-fact-order-measures
-- Description: 1. Bronze table for the OMS order lines (file feed, used when no CDC
--                 feed is present).
--              2. fact_order measures/attributes needed by Gold:
--                 shipped_quantity (fill rate), updated_at (CDC freshness / stale-update
--                 guard on MERGE).
-- Author: scm-platform-team
-- ============================================================================

-- changeset scm-platform-team:bronze-orders-v1
-- comment: Raw append-only Oracle OMS order lines (one row per order line per change)
CREATE TABLE IF NOT EXISTS bronze.orders (
    order_id STRING COMMENT 'Natural order identifier (nullable: bad records are quarantined in Silver)',
    order_line_number BIGINT COMMENT 'Line number within the order',
    customer_id STRING COMMENT 'Customer natural key',
    product_id STRING COMMENT 'Product / SKU natural key',
    warehouse_id STRING COMMENT 'Fulfilling warehouse natural key',
    order_date STRING COMMENT 'Order placement date as received (yyyy-MM-dd)',
    promised_delivery_date STRING COMMENT 'Promised delivery date as received (yyyy-MM-dd)',
    ordered_quantity BIGINT COMMENT 'Ordered quantity as received',
    shipped_quantity BIGINT COMMENT 'Shipped quantity as received',
    unit_price DOUBLE COMMENT 'Unit sale price as received',
    order_status STRING COMMENT 'CREATED, PICKED, SHIPPED, DELIVERED, CANCELLED',
    updated_at STRING COMMENT 'Source last-modified timestamp (ISO 8601 UTC); CDC freshness key',
    _ingest_timestamp TIMESTAMP COMMENT 'UTC timestamp when record was ingested into Bronze',
    _source_file STRING COMMENT 'Source file path or ADLS blob URI',
    _rescued_data STRING COMMENT 'Auto Loader rescued data for fields not matching the schema'
)
USING DELTA
COMMENT 'Bronze raw append-only Oracle OMS order lines';

-- changeset scm-platform-team:fact-order-shipped-updated-v2
-- comment: Fill-rate measure and CDC freshness timestamp on the order-line fact
ALTER TABLE silver.fact_order ADD COLUMNS (
    shipped_quantity INT COMMENT 'Quantity shipped so far (0 until SHIPPED); fill rate = shipped / ordered',
    updated_at TIMESTAMP COMMENT 'Source last-modified timestamp; MERGE never replaces a newer row with an older one'
);

-- changeset scm-platform-team:fact-order-status-comment-v2
-- comment: Align documented status domain with the OMS lifecycle
ALTER TABLE silver.fact_order ALTER COLUMN order_status
    COMMENT 'Lifecycle state: CREATED, PICKED, SHIPPED, DELIVERED, CANCELLED';
