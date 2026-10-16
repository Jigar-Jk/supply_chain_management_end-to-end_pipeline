-- DQ tile 3: reprocessing success rate = REPROCESSED / (REPROCESSED + REJECTED), per feed and overall.
-- PENDING rows are still open, so they are reported but not counted in the rate.
WITH by_feed AS (
  SELECT
    source_feed,
    sum(CASE WHEN status = 'REPROCESSED' THEN 1 ELSE 0 END) AS reprocessed,
    sum(CASE WHEN status = 'REJECTED'    THEN 1 ELSE 0 END) AS rejected,
    sum(CASE WHEN status = 'PENDING'     THEN 1 ELSE 0 END) AS still_pending
  FROM quarantine.records
  GROUP BY source_feed
),
with_total AS (
  SELECT * FROM by_feed
  UNION ALL
  SELECT 'ALL', sum(reprocessed), sum(rejected), sum(still_pending) FROM by_feed
)
SELECT
  source_feed,
  reprocessed,
  rejected,
  still_pending,
  round(100.0 * reprocessed / nullif(reprocessed + rejected, 0), 2) AS reprocessing_success_pct
FROM with_total
