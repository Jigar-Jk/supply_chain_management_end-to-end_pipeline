-- Gold KPI 3: Stockout Rate % by warehouse and snapshot date (WMS nightly reconciliation).
-- Stockout rate % = SKUs with available quantity <= 0 / SKUs stocked x 100.
-- At-risk %       = SKUs with available quantity <= safety stock / SKUs stocked x 100.
-- built_from_stale_data = TRUE when the night's WMS file missed its SLA and Silver carried the
--                         last good snapshot forward (sla_checks.carry_forward_last_good).
-- Reads Silver only.
CREATE OR REPLACE VIEW gold.vw_stockout_rate_daily
COMMENT 'Stockout rate by warehouse and snapshot date: SKUs with available <= 0 / SKUs stocked x 100'
AS
SELECT
  w.warehouse_id,
  i.snapshot_date                                                          AS kpi_date,
  count(*)                                                                 AS skus_stocked,
  sum(CASE WHEN i.is_stockout THEN 1 ELSE 0 END)                           AS stockout_skus,
  sum(CASE WHEN i.is_stockout_risk THEN 1 ELSE 0 END)                      AS at_risk_skus,
  round(100.0 * sum(CASE WHEN i.is_stockout THEN 1 ELSE 0 END) / count(*), 2)      AS stockout_rate_pct,
  round(100.0 * sum(CASE WHEN i.is_stockout_risk THEN 1 ELSE 0 END) / count(*), 2) AS at_risk_pct,
  max(coalesce(i.is_stale, false))                                         AS built_from_stale_data
FROM silver.fact_inventory_snapshot i
JOIN silver.dim_warehouse w ON i.warehouse_sk = w.warehouse_sk
GROUP BY w.warehouse_id, i.snapshot_date
