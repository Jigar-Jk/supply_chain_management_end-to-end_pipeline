-- liquibase formatted sql

-- ============================================================================
-- Changeset: 009-retire-legacy-gold-objects
-- Description: Retire the Gold objects from 003, which read the old Silver mirror tables
--              (silver.shipments, silver.routes) that the pipeline no longer writes.
--              They are replaced by the KPI views and mart in changelog/gold/*.sql,
--              deployed by the runOnChange changesets in db.changelog-master.xml.
--              003 itself is left untouched (already-applied migrations are not rewritten).
-- Author: scm-platform-team
-- ============================================================================

-- changeset scm-platform-team:retire-gold-shipment-route-summary
-- comment: Superseded by gold.vw_otif_daily / gold.vw_delivery_delay_risk over the galaxy schema
DROP VIEW IF EXISTS gold.shipment_route_summary;

-- changeset scm-platform-team:retire-gold-carrier-performance-daily
-- comment: Was never populated by any job; superseded by gold.mart_logistics_kpi_daily
DROP TABLE IF EXISTS gold.carrier_performance_daily;
