-- Gold KPI 1b: OTIF % by carrier and delivery date, next to the carrier's contracted OTIF target.
-- Same definition as vw_otif_daily (on time AND in full / delivered x 100), grouped by carrier instead
-- of origin warehouse, so carrier performance can be compared with what was agreed in the contract.
-- Carrier name and target are those of the carrier version the shipment was booked with (SCD2 key).
-- Reads Silver only, like every Gold view; business users read this view, never Silver.
CREATE OR REPLACE VIEW gold.vw_otif_by_carrier_daily
COMMENT 'OTIF by carrier and delivery date, with the contracted OTIF target: on-time AND in-full delivered shipments / delivered shipments x 100'
AS
SELECT
  c.carrier_id,
  c.carrier_name,
  c.contracted_otif_target,
  to_date(s.actual_delivery_time)                                         AS kpi_date,
  count(*)                                                                AS shipments_delivered,
  sum(CASE WHEN s.is_on_time THEN 1 ELSE 0 END)                           AS on_time_shipments,
  sum(CASE WHEN s.is_on_time AND s.is_in_full THEN 1 ELSE 0 END)          AS otif_shipments,
  round(100.0 * sum(CASE WHEN s.is_on_time AND s.is_in_full THEN 1 ELSE 0 END) / count(*), 2) AS otif_pct
FROM silver.fact_shipment s
JOIN silver.dim_carrier c ON s.carrier_sk = c.carrier_sk
WHERE s.shipment_status = 'DELIVERED'
  AND s.actual_delivery_time IS NOT NULL
GROUP BY c.carrier_id, c.carrier_name, c.contracted_otif_target, to_date(s.actual_delivery_time)
