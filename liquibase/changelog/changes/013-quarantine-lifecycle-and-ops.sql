-- liquibase formatted sql

-- ============================================================================
-- Changeset: 013-quarantine-lifecycle-and-ops
-- Description: 1. Quarantine reprocessing lifecycle (implementation guide §7.4-7.6):
--                 which feed a row came from (so it can be replayed), the steward's correction
--                 (original_payload is never edited, for audit), retries and resolution.
--              2. ops schema tables every job references (guide §4.3):
--                 ops.sla_config        expected-by times, lag limits, quarantine SLA per source
--                 ops.pipeline_run_log  one row per job step: rows read / written / quarantined
-- Author: scm-platform-team
-- ============================================================================

-- changeset scm-platform-team:quarantine-lifecycle-v3
-- comment: Columns used by scm_platform/reprocess_quarantine.py
ALTER TABLE quarantine.records ADD COLUMNS (
    source_feed STRING COMMENT 'Feed / transform the row belongs to (orders, inventory, shipments, warehouses, ...); drives replay',
    corrected_payload STRING COMMENT 'Steward correction (JSON). Replayed instead of original_payload, which is never edited',
    retry_count INT COMMENT 'Number of reprocessing attempts',
    last_retry_at TIMESTAMP COMMENT 'Last reprocessing attempt',
    resolved_at TIMESTAMP COMMENT 'When the row reached REPROCESSED or REJECTED',
    resolved_by STRING COMMENT 'Steward who corrected it, or reprocess_quarantine for automatic outcomes',
    resolution_note STRING COMMENT 'Why it was closed: auto-healed, corrected, duplicate, superseded, SLA expiry'
);

-- changeset scm-platform-team:ops-sla-config-v1
-- comment: Per-source SLA settings read by the SLA checks and the reprocessing job
CREATE TABLE IF NOT EXISTS ops.sla_config (
    source_system STRING NOT NULL COMMENT 'wms_batch, carrier_api, oracle_cdc (SQL Server OMS), gps_stream',
    expected_by_time STRING COMMENT 'Daily deadline for batch sources, HH:MM UTC; NULL for continuous sources',
    max_lag_minutes INT COMMENT 'Allowed staleness before the SLA tile turns AMBER (RED at 2x)',
    quarantine_sla_days INT COMMENT 'PENDING quarantine rows older than this are REJECTED',
    CONSTRAINT pk_sla_config PRIMARY KEY (source_system)
)
USING DELTA
COMMENT 'Pipeline SLA configuration per source';

-- changeset scm-platform-team:ops-sla-config-seed-v1
-- comment: Guide §4.3 defaults
INSERT INTO ops.sla_config VALUES
    ('wms_batch',   '02:00', 120, 7),
    ('carrier_api', '01:00',  60, 7),
    ('oracle_cdc',  NULL,     15, 7),
    ('gps_stream',  NULL,      5, 7);

-- changeset scm-platform-team:ops-pipeline-run-log-v1
-- comment: One row per job step; feeds the pipeline SLA tile on the DQ dashboard
CREATE TABLE IF NOT EXISTS ops.pipeline_run_log (
    run_id STRING COMMENT 'Job run / batch id',
    source_system STRING COMMENT 'Source the step processed',
    step STRING COMMENT 'Step name, e.g. fact_order, reprocess_quarantine',
    layer STRING COMMENT 'bronze, silver, gold, quarantine',
    started_at TIMESTAMP,
    ended_at TIMESTAMP,
    rows_read BIGINT,
    rows_written BIGINT,
    rows_quarantined BIGINT,
    status STRING COMMENT 'SUCCESS, FAILED, SLA_MISS',
    error_message STRING
)
USING DELTA
COMMENT 'Pipeline run log (observability)';
