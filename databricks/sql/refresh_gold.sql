-- Rebuild the Gold KPI mart after Silver passed its quality tests (task refresh_gold in scm_daily_pipeline).
-- The vw_* KPI views are always live; only the materialized mart needs a refresh. The refresh is
-- transactional, so readers see the previous mart or the new one, never a half-built one.
REFRESH MATERIALIZED VIEW IDENTIFIER(:catalog || '.gold.mart_logistics_kpi_daily');
