-- DQ tile 1: rows processed vs quarantined, per source feed per day (last 14 days).
-- quarantine_pct above ~5% is the alert threshold (guide §7.7).
WITH processed AS (
  SELECT 'orders'     AS source_feed, to_date(_ingest_timestamp) AS day, count(*) AS rows_processed FROM bronze.orders     GROUP BY to_date(_ingest_timestamp)
  UNION ALL
  SELECT 'inventory',  to_date(_ingest_timestamp), count(*) FROM bronze.inventory  GROUP BY to_date(_ingest_timestamp)
  UNION ALL
  SELECT 'shipments',  to_date(_ingest_timestamp), count(*) FROM bronze.shipments  GROUP BY to_date(_ingest_timestamp)
  UNION ALL
  SELECT 'warehouses', to_date(_ingest_timestamp), count(*) FROM bronze.warehouses GROUP BY to_date(_ingest_timestamp)
  UNION ALL
  SELECT 'products',   to_date(_ingest_timestamp), count(*) FROM bronze.products   GROUP BY to_date(_ingest_timestamp)
  UNION ALL
  SELECT 'customers',  to_date(_ingest_timestamp), count(*) FROM bronze.customers  GROUP BY to_date(_ingest_timestamp)
  UNION ALL
  SELECT 'carriers',   to_date(_ingest_timestamp), count(*) FROM bronze.carriers   GROUP BY to_date(_ingest_timestamp)
  UNION ALL
  SELECT 'routes',     to_date(_ingest_timestamp), count(*) FROM bronze.routes     GROUP BY to_date(_ingest_timestamp)
  UNION ALL
  SELECT 'gps_events', to_date(_ingest_timestamp), count(*) FROM bronze.gps_events GROUP BY to_date(_ingest_timestamp)
),
quarantined AS (
  SELECT source_feed, to_date(ingested_at) AS day, count(*) AS rows_quarantined
  FROM quarantine.records
  GROUP BY source_feed, to_date(ingested_at)
)
SELECT
  p.day,
  p.source_feed,
  p.rows_processed,
  coalesce(q.rows_quarantined, 0)                                            AS rows_quarantined,
  round(100.0 * coalesce(q.rows_quarantined, 0) / nullif(p.rows_processed, 0), 2) AS quarantine_pct
FROM processed p
LEFT JOIN quarantined q ON q.source_feed = p.source_feed AND q.day = p.day
WHERE p.day >= date_sub(current_date(), 14)
