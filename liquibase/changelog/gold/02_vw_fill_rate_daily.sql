-- Gold KPI 2: Fill Rate % by fulfilling warehouse and order date.
-- Fill rate %      = units shipped / units ordered x 100, over order lines that have shipped
--                    (status SHIPPED or DELIVERED; open and cancelled lines are excluded).
-- Line fill rate % = lines shipped complete / lines shipped x 100.
-- Reads Silver only.
CREATE OR REPLACE VIEW gold.vw_fill_rate_daily
COMMENT 'Fill rate by fulfilling warehouse and order date: units shipped / units ordered x 100 over shipped order lines'
AS
SELECT
  w.warehouse_id,
  d.calendar_date                                                          AS kpi_date,
  count(*)                                                                 AS lines_shipped,
  sum(o.ordered_quantity)                                                  AS units_ordered,
  sum(o.shipped_quantity)                                                  AS units_shipped,
  round(100.0 * sum(o.shipped_quantity) / nullif(sum(o.ordered_quantity), 0), 2) AS fill_rate_pct,
  round(100.0 * sum(CASE WHEN o.shipped_quantity >= o.ordered_quantity THEN 1 ELSE 0 END) / count(*), 2)
                                                                           AS line_fill_rate_pct
FROM silver.fact_order o
JOIN silver.dim_warehouse w ON o.origin_warehouse_sk = w.warehouse_sk
JOIN silver.dim_date      d ON o.order_date_sk       = d.date_sk
WHERE o.order_status IN ('SHIPPED', 'DELIVERED')
  AND NOT coalesce(o.is_deleted, false)          -- CDC soft deletes are kept for audit, not counted
GROUP BY w.warehouse_id, d.calendar_date
