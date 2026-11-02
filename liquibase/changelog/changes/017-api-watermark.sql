-- liquibase formatted sql

-- ============================================================================
-- Changeset: 017-api-watermark
-- Description: ops.api_watermark - how far each REST source has read (scm_platform/api/watermark.py).
--              One row per source. The watermark is the newest change time delivered (the largest
--              updated_at landed), written only after every page of a run has landed, and it never
--              moves back. The next run asks the API for changes after it.
-- Author: scm-platform-team
-- ============================================================================

-- changeset scm-platform-team:ops-api-watermark-v1
-- comment: Incremental position of each REST source
CREATE TABLE IF NOT EXISTS ops.api_watermark (
    source STRING NOT NULL COMMENT 'REST source, e.g. carrier_shipments, weather_forecast',
    watermark STRING NOT NULL COMMENT 'Newest change delivered so far, ISO 8601 UTC; the next run asks for changes after it',
    rows_loaded BIGINT COMMENT 'Records landed by the run that wrote this watermark',
    run_id STRING COMMENT 'Job run that wrote it',
    updated_at TIMESTAMP COMMENT 'When it was written'
)
USING DELTA
COMMENT 'Incremental watermark per REST API source (one row per source)';
