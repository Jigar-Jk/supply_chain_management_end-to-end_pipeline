-- liquibase formatted sql

-- ============================================================================
-- Changeset: 007-wms-inventory-feed-and-stockout-flag
-- Description: 1. Bronze table for the WMS nightly stock reconciliation NDJSON feed
--                 (the batch-file ingestion pattern).
--              2. fact_inventory_snapshot columns needed by Gold:
--                 is_stockout (stockout-rate KPI: available <= 0) and recorded_at
--                 (export timestamp; a re-sent file replaces an older snapshot, and
--                 MERGE never replaces a newer snapshot with an older one).
-- Author: scm-platform-team
-- ============================================================================

-- changeset scm-platform-team:bronze-inventory-v1
-- comment: Raw append-only WMS nightly stock reconciliation rows (one NDJSON file per night)
CREATE TABLE IF NOT EXISTS bronze.inventory (
    warehouse_id STRING COMMENT 'Warehouse natural key (nullable: bad records are quarantined in Silver)',
    product_id STRING COMMENT 'Product / SKU natural key',
    snapshot_date STRING COMMENT 'Reconciliation date as received (yyyy-MM-dd)',
    quantity_on_hand BIGINT COMMENT 'Physical stock counted, as received',
    quantity_reserved BIGINT COMMENT 'Stock allocated to open orders, as received',
    safety_stock BIGINT COMMENT 'Configured safety stock level',
    reorder_point BIGINT COMMENT 'Configured reorder point',
    recorded_at STRING COMMENT 'WMS export timestamp (ISO 8601 UTC); freshness key for re-sent files',
    _ingest_timestamp TIMESTAMP COMMENT 'UTC timestamp when record was ingested into Bronze',
    _source_file STRING COMMENT 'Source file path or ADLS blob URI (one file per night)',
    _rescued_data STRING COMMENT 'Auto Loader rescued data for fields not matching the schema'
)
USING DELTA
COMMENT 'Bronze raw append-only WMS nightly stock reconciliation';

-- changeset scm-platform-team:fact-inventory-stockout-recorded-v2
-- comment: Stockout flag for the stockout-rate KPI and export timestamp for freshness
ALTER TABLE silver.fact_inventory_snapshot ADD COLUMNS (
    is_stockout BOOLEAN COMMENT 'TRUE when quantity_available <= 0; stockout rate = stockout SKUs / SKUs stocked',
    recorded_at TIMESTAMP COMMENT 'WMS export timestamp; a later export of the same snapshot replaces an earlier one'
);
