-- liquibase formatted sql

-- ============================================================================
-- Changeset: 011-cdc-bronze-tables
-- Description: CDC ingestion pattern (SQL Server OMS -> Debezium -> Kafka / Event Hubs):
--              1. bronze.orders_cdc     one row per change event on dbo.order_lines
--              2. bronze.inventory_cdc  one row per change event on dbo.inventory
--              3. bronze.cdc_dlq        messages that are not Debezium events (kept verbatim)
-- Generic layout: Debezium envelope as columns, row images (before / after) as raw JSON text,
-- so Bronze never breaks when a source column is added. Silver applies each table's schema
-- and keeps the highest source_position per key.
-- Columns match scm_platform/stream_cdc_events.py exactly.
-- Author: scm-platform-team
-- ============================================================================

-- changeset scm-platform-team:bronze-orders-cdc-v1
-- comment: Change events for the OMS order_lines table
CREATE TABLE IF NOT EXISTS bronze.orders_cdc (
    record_key STRING COMMENT 'Debezium message key: the primary key as JSON {order_id, order_line_number}',
    op STRING COMMENT 'r = snapshot read, c = insert, u = update, d = delete',
    `before` STRING COMMENT 'Row before the change (JSON); NULL for r and c',
    `after` STRING COMMENT 'Row after the change (JSON); NULL for d. Dates are days since epoch, timestamps epoch millis',
    source_db STRING COMMENT 'Source database (scm_oms)',
    source_schema STRING COMMENT 'Source schema (dbo)',
    source_table STRING COMMENT 'Source table (order_lines)',
    commit_lsn STRING COMMENT 'SQL Server LSN of the committing transaction',
    change_lsn STRING COMMENT 'SQL Server LSN of this change within the transaction; NULL for snapshot rows',
    event_serial_no INT COMMENT 'Order of events sharing one change LSN',
    is_snapshot BOOLEAN COMMENT 'TRUE for rows from the initial snapshot',
    source_ts TIMESTAMP COMMENT 'When the change happened in SQL Server',
    captured_ts TIMESTAMP COMMENT 'When Debezium processed the change',
    kafka_topic STRING COMMENT 'Source topic / Event Hub',
    kafka_partition INT COMMENT 'Source partition',
    kafka_offset BIGINT COMMENT 'Source offset (with partition: exact replay position)',
    kafka_timestamp TIMESTAMP COMMENT 'Broker append time',
    source_position STRING COMMENT 'commit_lsn|change_lsn|serial: sortable log order; highest per key wins in Silver',
    _ingest_timestamp TIMESTAMP COMMENT 'When the event was written to Bronze',
    _source_system STRING COMMENT 'sqlserver_cdc'
)
USING DELTA
COMMENT 'Bronze CDC change events for OMS order lines (append-only)';

-- changeset scm-platform-team:bronze-inventory-cdc-v1
-- comment: Change events for the OMS inventory table (same layout)
CREATE TABLE IF NOT EXISTS bronze.inventory_cdc LIKE bronze.orders_cdc;
ALTER TABLE bronze.inventory_cdc SET TBLPROPERTIES ('comment' = 'Bronze CDC change events for OMS inventory (append-only)');

-- changeset scm-platform-team:bronze-cdc-dlq-v1
-- comment: Dead letter for CDC topic messages that are not Debezium events
CREATE TABLE IF NOT EXISTS bronze.cdc_dlq (
    record_key STRING COMMENT 'Message key as received',
    payload STRING COMMENT 'Message value exactly as received',
    kafka_topic STRING COMMENT 'Source topic (tells which table)',
    kafka_partition INT COMMENT 'Source partition',
    kafka_offset BIGINT COMMENT 'Source offset',
    kafka_timestamp TIMESTAMP COMMENT 'Broker append time',
    dlq_reason STRING COMMENT 'Why the message was dead-lettered, e.g. NOT_A_DEBEZIUM_EVENT',
    _ingest_timestamp TIMESTAMP COMMENT 'When the message was dead-lettered',
    _source_system STRING COMMENT 'sqlserver_cdc'
)
USING DELTA
COMMENT 'Dead-letter table for CDC messages that are not Debezium events';
