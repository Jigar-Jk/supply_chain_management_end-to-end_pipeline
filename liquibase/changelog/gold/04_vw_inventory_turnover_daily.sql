-- Gold KPI 4: Inventory Turnover by warehouse, for each WMS snapshot date.
-- Inventory turnover (annualised) = COGS over the trailing 30 days
--                                   / average inventory value over the snapshots in those 30 days
--                                   x 365 / 30
--   COGS            = shipped units x unit cost, dated by shipment dispatch (cost recognised when goods leave)
--   Inventory value = on-hand units x unit cost, summed over the warehouse's SKUs
--   Unit cost comes from the dim_product version valid on the order / snapshot date (SCD2).
-- A trailing window is used because single-day COGS against a single snapshot is too noisy.
-- Days of inventory = average inventory value / average daily COGS.
-- Reads Silver only.
CREATE OR REPLACE VIEW gold.vw_inventory_turnover_daily
COMMENT 'Annualised inventory turnover by warehouse and snapshot date: trailing-30-day COGS / average inventory value x 365/30'
AS
WITH cogs_daily AS (
  SELECT
    w.warehouse_id,
    to_date(s.dispatch_time)              AS cogs_date,
    sum(o.shipped_quantity * p.unit_cost) AS cogs
  FROM silver.fact_order o
  JOIN silver.fact_shipment s ON o.order_id = s.order_id
  JOIN silver.dim_product   p ON o.product_sk = p.product_sk
  JOIN silver.dim_warehouse w ON o.origin_warehouse_sk = w.warehouse_sk
  WHERE o.order_status IN ('SHIPPED', 'DELIVERED')
    AND NOT coalesce(o.is_deleted, false)          -- CDC soft deletes are kept for audit, not counted
  GROUP BY w.warehouse_id, to_date(s.dispatch_time)
),
inventory_value AS (
  SELECT
    w.warehouse_id,
    i.snapshot_date,
    sum(i.quantity_on_hand * p.unit_cost) AS inventory_value
  FROM silver.fact_inventory_snapshot i
  JOIN silver.dim_product   p ON i.product_sk   = p.product_sk
  JOIN silver.dim_warehouse w ON i.warehouse_sk = w.warehouse_sk
  GROUP BY w.warehouse_id, i.snapshot_date
),
cogs_30d AS (
  SELECT v.warehouse_id, v.snapshot_date, coalesce(sum(c.cogs), 0) AS cogs_30d
  FROM inventory_value v
  LEFT JOIN cogs_daily c
    ON  c.warehouse_id = v.warehouse_id
    AND c.cogs_date BETWEEN date_sub(v.snapshot_date, 29) AND v.snapshot_date
  GROUP BY v.warehouse_id, v.snapshot_date
),
inventory_30d AS (
  SELECT v.warehouse_id, v.snapshot_date, avg(h.inventory_value) AS avg_inventory_value_30d
  FROM inventory_value v
  JOIN inventory_value h
    ON  h.warehouse_id = v.warehouse_id
    AND h.snapshot_date BETWEEN date_sub(v.snapshot_date, 29) AND v.snapshot_date
  GROUP BY v.warehouse_id, v.snapshot_date
)
SELECT
  c.warehouse_id,
  c.snapshot_date                                                          AS kpi_date,
  round(c.cogs_30d, 2)                                                     AS cogs_30d,
  round(i.avg_inventory_value_30d, 2)                                      AS avg_inventory_value_30d,
  round(c.cogs_30d / nullif(i.avg_inventory_value_30d, 0) * 365.0 / 30.0, 2) AS inventory_turnover,
  round(i.avg_inventory_value_30d / nullif(c.cogs_30d / 30.0, 0), 1)        AS days_of_inventory
FROM cogs_30d c
JOIN inventory_30d i
  ON c.warehouse_id = i.warehouse_id AND c.snapshot_date = i.snapshot_date
