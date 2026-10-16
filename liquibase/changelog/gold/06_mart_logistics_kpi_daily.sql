-- Gold reporting mart: one row per warehouse per day, all daily KPIs side by side.
-- A materialized view: its refresh is transactional, so readers never see a half-refreshed
-- mart (failure scenario "Gold refresh fails mid-run").
-- kpi_date is the event date of each KPI: delivery date (OTIF), order date (fill rate),
-- snapshot date (stockout rate, inventory turnover). A day without a given event has NULL
-- for that KPI rather than 0.
-- Warehouse attributes come from the current dim_warehouse version for filtering and Genie.
-- Reads Gold KPI views and Silver dimensions only.
CREATE OR REPLACE MATERIALIZED VIEW gold.mart_logistics_kpi_daily
COMMENT 'Control-tower daily KPI mart: one row per warehouse per day with OTIF, fill rate, stockout rate and inventory turnover'
AS
WITH spine AS (
  SELECT warehouse_id, kpi_date FROM gold.vw_otif_daily
  UNION
  SELECT warehouse_id, kpi_date FROM gold.vw_fill_rate_daily
  UNION
  SELECT warehouse_id, kpi_date FROM gold.vw_stockout_rate_daily
  UNION
  SELECT warehouse_id, kpi_date FROM gold.vw_inventory_turnover_daily
)
SELECT
  sp.warehouse_id,
  dw.warehouse_name,
  dw.city,
  dw.country,
  sp.kpi_date,
  o.shipments_delivered,
  o.otif_pct,
  o.on_time_pct,
  o.in_full_pct,
  f.lines_shipped,
  f.fill_rate_pct,
  s.skus_stocked,
  s.stockout_rate_pct,
  s.at_risk_pct,
  s.built_from_stale_data,
  t.inventory_turnover,
  t.days_of_inventory
FROM spine sp
LEFT JOIN silver.dim_warehouse dw
  ON dw.warehouse_id = sp.warehouse_id AND dw.is_current
LEFT JOIN gold.vw_otif_daily o
  ON o.warehouse_id = sp.warehouse_id AND o.kpi_date = sp.kpi_date
LEFT JOIN gold.vw_fill_rate_daily f
  ON f.warehouse_id = sp.warehouse_id AND f.kpi_date = sp.kpi_date
LEFT JOIN gold.vw_stockout_rate_daily s
  ON s.warehouse_id = sp.warehouse_id AND s.kpi_date = sp.kpi_date
LEFT JOIN gold.vw_inventory_turnover_daily t
  ON t.warehouse_id = sp.warehouse_id AND t.kpi_date = sp.kpi_date
