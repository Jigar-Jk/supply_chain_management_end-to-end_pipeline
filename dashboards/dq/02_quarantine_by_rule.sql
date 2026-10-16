-- DQ tile 2: quarantine breakdown by feed, rule and lifecycle status (root-cause analysis).
SELECT
  source_feed,
  failed_rule,
  status,
  count(*)              AS rows,
  min(ingested_at)      AS oldest,
  max(retry_count)      AS max_retries
FROM quarantine.records
GROUP BY source_feed, failed_rule, status
