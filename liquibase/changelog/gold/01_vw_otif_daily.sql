-- Gold KPI 1: OTIF % by origin warehouse and delivery date.
-- OTIF % = delivered shipments that were on time (actual delivery <= carrier promised ETA)
--          AND in full (shipped quantity >= ordered quantity) / delivered shipments x 100.
-- Reads Silver only. Shipments still in transit are excluded until delivered.
CREATE OR REPLACE VIEW gold.vw_otif_daily
COMMENT 'OTIF by origin warehouse and delivery date: on-time AND in-full delivered shipments / delivered shipments x 100'
AS
SELECT
  w.warehouse_id,
  to_date(s.actual_delivery_time)                                         AS kpi_date,
  count(*)                                                                AS shipments_delivered,
  sum(CASE WHEN s.is_on_time THEN 1 ELSE 0 END)                           AS on_time_shipments,
  sum(CASE WHEN s.is_in_full THEN 1 ELSE 0 END)                           AS in_full_shipments,
  sum(CASE WHEN s.is_on_time AND s.is_in_full THEN 1 ELSE 0 END)          AS otif_shipments,
  round(100.0 * sum(CASE WHEN s.is_on_time THEN 1 ELSE 0 END) / count(*), 2)                  AS on_time_pct,
  round(100.0 * sum(CASE WHEN s.is_in_full THEN 1 ELSE 0 END) / count(*), 2)                  AS in_full_pct,
  round(100.0 * sum(CASE WHEN s.is_on_time AND s.is_in_full THEN 1 ELSE 0 END) / count(*), 2) AS otif_pct
FROM silver.fact_shipment s
JOIN silver.dim_warehouse w ON s.origin_warehouse_sk = w.warehouse_sk
WHERE s.shipment_status = 'DELIVERED'
  AND s.actual_delivery_time IS NOT NULL
GROUP BY w.warehouse_id, to_date(s.actual_delivery_time)
