-- liquibase formatted sql

-- ============================================================================
-- Changeset: 015-streaming-sla-and-sqlserver-cdc-name
-- Description: 1. Streaming lag limits match the deployed schedule. scm_streaming drains Event Hubs
--                 every 30 minutes (--available-now, no cluster running 24/7), so a source can be
--                 up to ~30 min behind between runs. Limits: sqlserver_cdc 45 min, gps_stream 40 min
--                 (AMBER above, RED above 2x). A continuous deployment would set them back to 15 / 5.
--              2. The OMS source is SQL Server: the earlier source names are renamed to
--                 sqlserver_cdc and SQLSERVER_OMS, and
--                 table / column descriptions say SQL Server OMS.
-- 013 is not edited: changing an applied changeset breaks its checksum on every database that ran it.
-- Author: scm-platform-team
-- ============================================================================

-- changeset scm-platform-team:ops-sla-config-sqlserver-cdc-v1
-- comment: Rename the CDC source and set streaming lag limits for the 30-minute schedule
UPDATE ops.sla_config SET source_system = 'sqlserver_cdc', max_lag_minutes = 45 WHERE source_system = 'oracle_cdc';
UPDATE ops.sla_config SET max_lag_minutes = 40 WHERE source_system = 'gps_stream';
UPDATE ops.pipeline_run_log SET source_system = 'sqlserver_cdc' WHERE source_system = 'oracle_cdc';
ALTER TABLE ops.sla_config ALTER COLUMN source_system
    COMMENT 'wms_batch, carrier_api, sqlserver_cdc (SQL Server OMS via Debezium), gps_stream';

-- changeset scm-platform-team:sqlserver-oms-naming-v1
-- comment: Quarantine rows written before the rename, and the descriptions that named Oracle OMS
UPDATE quarantine.records SET source_system = 'SQLSERVER_OMS' WHERE source_system = 'ORACLE_OMS';
ALTER TABLE quarantine.records ALTER COLUMN source_system
    COMMENT 'Originating source system (e.g. SQLSERVER_OMS, WMS, CARRIER_API, ORS)';
ALTER TABLE silver.dim_customer ALTER COLUMN customer_id COMMENT 'Natural business key from SQL Server OMS';
ALTER TABLE silver.dim_product ALTER COLUMN product_id COMMENT 'Natural business key (SKU code) from SQL Server OMS';
ALTER TABLE silver.fact_order ALTER COLUMN order_id COMMENT 'Natural order identifier from SQL Server OMS';
COMMENT ON TABLE silver.fact_order IS 'Silver order line transaction fact table from SQL Server OMS';
COMMENT ON TABLE bronze.orders IS 'Bronze raw append-only SQL Server OMS order lines (file feed)';
ALTER TABLE bronze.shipments ALTER COLUMN order_id COMMENT 'Order this shipment fulfils (SQL Server OMS order_id)';
