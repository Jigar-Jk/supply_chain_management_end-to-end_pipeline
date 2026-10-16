-- Gold KPI 5: Delivery Delay Risk for every shipment still in flight.
-- delay_risk_score (0-1) = 0.35 x weather risk          (HIGH 1.0, MODERATE 0.5, LOW 0.1)
--                        + 0.35 x carrier unreliability (1 - on-time rate over the last 90 days of deliveries;
--                                                        0.8 assumed when a carrier has no history)
--                        + 0.20 x route distance band   (>800 km 1.0, >300 km 0.5, else 0.15)
--                        + 0.10 x GPS staleness         (latest ping older than 2 hours, or no ping at all)
-- The weights are a documented business assumption (implementation guide §9.2), not derived.
-- risk_band: HIGH >= 0.45, MEDIUM >= 0.25, else LOW (also an assumption).
--
-- GPS term: last_ping_ts comes from the GPS/RFID stream (silver.fact_shipment.last_ping_ts).
-- A truck that has never pinged counts as stale: its position is unknown, which is a risk in itself.
-- Staleness is measured against the current time, so this term is live, unlike the history terms.
--
-- The 90-day carrier history is anchored to the latest delivery in Silver (equal to "now" in
-- production), which keeps the score reproducible on historical or synthetic data.
-- Reads Silver only.
CREATE OR REPLACE VIEW gold.vw_delivery_delay_risk
COMMENT 'Delay risk score (0-1) and band for in-flight shipments: weather, carrier history, route distance, GPS staleness'
AS
WITH as_of AS (
  SELECT max(actual_delivery_time) AS latest_delivery FROM silver.fact_shipment
),
carrier_history AS (
  SELECT
    s.carrier_sk,
    avg(CASE WHEN s.is_on_time THEN 1.0 ELSE 0.0 END) AS hist_on_time_rate,
    count(*)                                          AS hist_deliveries
  FROM silver.fact_shipment s
  CROSS JOIN as_of a
  WHERE s.actual_delivery_time IS NOT NULL
    AND s.actual_delivery_time >= a.latest_delivery - INTERVAL 90 DAYS
  GROUP BY s.carrier_sk
),
scored AS (
  SELECT
    s.shipment_id,
    s.order_id,
    w.warehouse_id,
    c.carrier_id,
    c.carrier_name,
    s.dispatch_time,
    s.planned_delivery_time,
    s.weather_risk,
    round(r.distance_meters / 1000.0, 1)               AS distance_km,
    round(h.hist_on_time_rate, 4)                      AS carrier_hist_on_time_rate,
    s.truck_id,
    s.last_lat,
    s.last_lon,
    s.last_ping_ts,
    s.last_ping_ts IS NOT NULL                         AS gps_signal_available,
    CAST((unix_timestamp(current_timestamp()) - unix_timestamp(s.last_ping_ts)) / 60 AS INT)
                                                       AS minutes_since_last_ping,
    round(
        0.35 * CASE s.weather_risk WHEN 'HIGH' THEN 1.0 WHEN 'MODERATE' THEN 0.5 ELSE 0.1 END
      + 0.35 * (1 - coalesce(h.hist_on_time_rate, 0.8))
      + 0.20 * CASE WHEN r.distance_meters > 800000 THEN 1.0
                    WHEN r.distance_meters > 300000 THEN 0.5
                    ELSE 0.15 END
      + 0.10 * CASE WHEN s.last_ping_ts IS NULL
                      OR s.last_ping_ts < current_timestamp() - INTERVAL 2 HOURS THEN 1.0 ELSE 0.0 END
    , 3)                                               AS delay_risk_score
  FROM silver.fact_shipment s
  JOIN silver.dim_warehouse w ON s.origin_warehouse_sk = w.warehouse_sk
  JOIN silver.dim_carrier   c ON s.carrier_sk          = c.carrier_sk
  JOIN silver.dim_route     r ON s.route_sk            = r.route_sk
  LEFT JOIN carrier_history h ON s.carrier_sk          = h.carrier_sk
  WHERE s.shipment_status IN ('DISPATCHED', 'IN_TRANSIT')
)
SELECT
  *,
  CASE WHEN delay_risk_score >= 0.45 THEN 'HIGH'
       WHEN delay_risk_score >= 0.25 THEN 'MEDIUM'
       ELSE 'LOW' END                                  AS risk_band
FROM scored
