-- liquibase formatted sql

-- ============================================================================
-- Changeset: 014-bronze-batch-id-and-stale-snapshots
-- Description: 1. _batch_id on the Auto Loader Bronze tables: the job run that loaded each row
--                 (implementation guide §6 audit columns; the job passes {{job.run_id}}).
--              2. fact_inventory_snapshot.is_stale: TRUE for rows carried forward from the last
--                 good night after the WMS file missed its SLA (sla_checks.py). Gold surfaces it
--                 as built_from_stale_data; the late file's MERGE replaces those rows.
-- Author: scm-platform-team
-- ============================================================================

-- changeset scm-platform-team:bronze-batch-id-v1
-- comment: Job run id on every file-fed Bronze table
ALTER TABLE bronze.routes     ADD COLUMNS (_batch_id STRING COMMENT 'Job run that loaded the row');
ALTER TABLE bronze.weather    ADD COLUMNS (_batch_id STRING COMMENT 'Job run that loaded the row');
ALTER TABLE bronze.shipments  ADD COLUMNS (_batch_id STRING COMMENT 'Job run that loaded the row');
ALTER TABLE bronze.warehouses ADD COLUMNS (_batch_id STRING COMMENT 'Job run that loaded the row');
ALTER TABLE bronze.products   ADD COLUMNS (_batch_id STRING COMMENT 'Job run that loaded the row');
ALTER TABLE bronze.customers  ADD COLUMNS (_batch_id STRING COMMENT 'Job run that loaded the row');
ALTER TABLE bronze.carriers   ADD COLUMNS (_batch_id STRING COMMENT 'Job run that loaded the row');
ALTER TABLE bronze.orders     ADD COLUMNS (_batch_id STRING COMMENT 'Job run that loaded the row');
ALTER TABLE bronze.inventory  ADD COLUMNS (_batch_id STRING COMMENT 'Job run that loaded the row');

-- changeset scm-platform-team:fact-inventory-is-stale-v3
-- comment: Last-known-good flag for SLA misses
ALTER TABLE silver.fact_inventory_snapshot ADD COLUMNS (
    is_stale BOOLEAN COMMENT 'TRUE when carried forward from the last good night because the WMS file missed its SLA'
);
