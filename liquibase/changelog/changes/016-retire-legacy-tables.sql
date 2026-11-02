-- liquibase formatted sql

-- ============================================================================
-- Changeset: 016-retire-legacy-tables
-- Description: Drop the tables of the first design (002) that nothing writes or reads any more:
--              1. quarantine.invalid_records - replaced by quarantine.records (004, 013): charter
--                 schema with failed_rule, batch_id and the PENDING -> REPROCESSED / REJECTED lifecycle.
--              2. silver.routes, silver.shipments - the old Silver mirror tables, replaced by
--                 dim_route and fact_shipment (004). Their only reader, gold.shipment_route_summary,
--                 was dropped in 009.
--              silver.weather (also from 002) is still written by the pipeline and stays.
-- 002 is not edited: a fresh catalog still creates these tables in 002 and drops them here, so
-- every environment ends in the same state and keeps the same history.
-- Author: scm-platform-team
-- ============================================================================

-- changeset scm-platform-team:retire-quarantine-invalid-records
-- comment: Superseded by quarantine.records
DROP TABLE IF EXISTS quarantine.invalid_records;

-- changeset scm-platform-team:retire-silver-mirror-routes
-- comment: Superseded by silver.dim_route
DROP TABLE IF EXISTS silver.routes;

-- changeset scm-platform-team:retire-silver-mirror-shipments
-- comment: Superseded by silver.fact_shipment
DROP TABLE IF EXISTS silver.shipments;
