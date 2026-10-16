-- liquibase formatted sql

-- ============================================================================
-- Changeset: 012-cdc-silver-apply
-- Description: Silver side of CDC (bronze.orders_cdc / bronze.inventory_cdc):
--              1. fact_order gains is_deleted (soft delete: a deleted source row is kept for
--                 audit, Gold excludes it) and source_position (log order; the MERGE never
--                 replaces a row with an older position, so a stale event replayed late loses).
--              2. silver.inventory_current: real-time stock position per warehouse x product,
--                 maintained by the OMS and captured by CDC. The nightly WMS reconciliation
--                 (fact_inventory_snapshot) remains the system of record.
-- Author: scm-platform-team
-- ============================================================================

-- changeset scm-platform-team:fact-order-cdc-columns-v3
-- comment: Soft delete and CDC ordering key on the order-line fact
ALTER TABLE silver.fact_order ADD COLUMNS (
    is_deleted BOOLEAN COMMENT 'TRUE when the source row was deleted (CDC op d); kept for audit, excluded by Gold',
    source_position STRING COMMENT 'commit_lsn|change_lsn|serial of the applied change; MERGE only moves forward'
);

-- changeset scm-platform-team:silver-inventory-current-v1
-- comment: Latest stock position per warehouse x product from inventory CDC
CREATE TABLE IF NOT EXISTS silver.inventory_current (
    inventory_current_sk STRING NOT NULL COMMENT 'md5(warehouse_id, product_id)',
    warehouse_id STRING NOT NULL COMMENT 'Warehouse natural key',
    product_id STRING NOT NULL COMMENT 'Product / SKU natural key',
    quantity_on_hand INT COMMENT 'Units physically on hand',
    quantity_reserved INT COMMENT 'Units reserved for open orders',
    quantity_available INT COMMENT 'quantity_on_hand - quantity_reserved',
    safety_stock INT COMMENT 'Configured safety stock',
    reorder_point INT COMMENT 'Configured reorder point',
    updated_at TIMESTAMP COMMENT 'When the OMS last changed this row',
    is_deleted BOOLEAN COMMENT 'TRUE when the source row was deleted (CDC op d)',
    source_position STRING COMMENT 'commit_lsn|change_lsn|serial of the applied change; MERGE only moves forward',
    created_at TIMESTAMP COMMENT 'Silver materialization timestamp',
    CONSTRAINT pk_inventory_current PRIMARY KEY (inventory_current_sk)
)
USING DELTA
COMMENT 'Real-time stock position from OMS inventory CDC (intraday view; WMS nightly snapshot is the system of record)';
