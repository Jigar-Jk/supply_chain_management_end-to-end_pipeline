-- Alert: early warning when a feed quarantines an unusual share of today's rows (guide §7.7: ~5 %).
-- One row per breaching feed, worst first; NO rows = everything normal.
--   >= 5 %  WARNING   someone should look (Gold still publishes)
--   >= 20 % CRITICAL  the Silver quality tests also stop Gold (databricks/sql/silver_quality_tests.sql)
-- Feeds with fewer than 100 rows today are skipped: 1 bad row out of 4 is noise, not a trend.
-- Deployed as a Databricks SQL query + alert per catalog (infra/alerts.tf); the quarantine_rate_alert
-- task of scm_daily_pipeline evaluates it after Silver and e-mails the subscribers.
WITH params AS (
  SELECT 5.0 AS warn_pct, 20.0 AS stop_pct, 100 AS min_rows
),
processed AS (
  SELECT source_feed, count(*) AS rows_processed
  FROM (
    SELECT 'orders'     AS source_feed, _ingest_timestamp FROM bronze.orders
    UNION ALL SELECT 'orders',     _ingest_timestamp FROM bronze.orders_cdc
    UNION ALL SELECT 'inventory',  _ingest_timestamp FROM bronze.inventory
    UNION ALL SELECT 'shipments',  _ingest_timestamp FROM bronze.shipments
    UNION ALL SELECT 'warehouses', _ingest_timestamp FROM bronze.warehouses
    UNION ALL SELECT 'products',   _ingest_timestamp FROM bronze.products
    UNION ALL SELECT 'customers',  _ingest_timestamp FROM bronze.customers
    UNION ALL SELECT 'carriers',   _ingest_timestamp FROM bronze.carriers
    UNION ALL SELECT 'routes',     _ingest_timestamp FROM bronze.routes
    UNION ALL SELECT 'gps_events', _ingest_timestamp FROM bronze.gps_events
  )
  WHERE to_date(_ingest_timestamp) = current_date()
  GROUP BY source_feed
),
quarantined AS (
  SELECT source_feed, failed_rule, count(*) AS rows_quarantined
  FROM quarantine.records
  WHERE to_date(ingested_at) = current_date()
  GROUP BY source_feed, failed_rule
),
per_feed AS (
  SELECT source_feed,
         sum(rows_quarantined)                 AS rows_quarantined,
         max_by(failed_rule, rows_quarantined) AS top_rule,
         max(rows_quarantined)                 AS top_rule_rows
  FROM quarantined
  GROUP BY source_feed
)
SELECT
  current_date()                                               AS day,
  p.source_feed,
  p.rows_processed,
  q.rows_quarantined,
  round(100.0 * q.rows_quarantined / p.rows_processed, 2)      AS quarantine_pct,
  q.top_rule,
  q.top_rule_rows,
  CASE WHEN 100.0 * q.rows_quarantined / p.rows_processed >= stop_pct THEN 'CRITICAL'
       ELSE 'WARNING' END                                      AS severity
FROM processed p
JOIN per_feed q ON q.source_feed = p.source_feed
CROSS JOIN params
WHERE p.rows_processed >= min_rows
  AND 100.0 * q.rows_quarantined / p.rows_processed >= warn_pct
ORDER BY quarantine_pct DESC, p.source_feed
