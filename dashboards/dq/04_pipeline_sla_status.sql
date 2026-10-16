-- DQ tile 4: pipeline SLA status per source (GREEN / AMBER / RED).
-- RED   : an SLA miss was logged in the last 24 hours, or no successful run within 2 x max_lag_minutes
-- AMBER : last successful run older than max_lag_minutes
-- GREEN : otherwise
WITH last_success AS (
  SELECT source_system, max(ended_at) AS last_success
  FROM ops.pipeline_run_log
  WHERE status = 'SUCCESS'
  GROUP BY source_system
),
recent_miss AS (
  SELECT source_system, max(ended_at) AS last_sla_miss, max_by(error_message, ended_at) AS miss_message
  FROM ops.pipeline_run_log
  WHERE status = 'SLA_MISS' AND ended_at >= current_timestamp() - INTERVAL 24 HOURS
  GROUP BY source_system
),
lag AS (
  SELECT
    c.source_system,
    c.expected_by_time,
    c.max_lag_minutes,
    l.last_success,
    m.last_sla_miss,
    m.miss_message,
    CAST((unix_timestamp(current_timestamp()) - unix_timestamp(l.last_success)) / 60 AS INT) AS minutes_since_success
  FROM ops.sla_config c
  LEFT JOIN last_success l ON l.source_system = c.source_system
  LEFT JOIN recent_miss  m ON m.source_system = c.source_system
)
SELECT
  source_system,
  expected_by_time,
  max_lag_minutes,
  last_success,
  minutes_since_success,
  last_sla_miss,
  miss_message,
  CASE WHEN last_sla_miss IS NOT NULL
         OR last_success IS NULL
         OR minutes_since_success > 2 * max_lag_minutes THEN 'RED'
       WHEN minutes_since_success > max_lag_minutes     THEN 'AMBER'
       ELSE 'GREEN' END                                  AS sla_status
FROM lag
