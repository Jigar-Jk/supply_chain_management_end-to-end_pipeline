-- Governance tile 1: who opened which of our tables in the last 7 days (system.access.audit).
-- Unity Catalog logs a getTable when someone looks a table up and a generateTemporaryTableCredential
-- when a cluster or warehouse fetches the files to actually read it - both count as "accessed".
-- Only our catalogs (scm_platform_dev / _qa / _prod); everything else in the workspace is left out.
-- Audit events land minutes to about an hour late, so a read done just now may not be here yet.
SELECT
  event_date,
  user_identity.email                    AS principal,
  request_params['full_name_arg']        AS table_name,
  count(*)                               AS events,
  max(event_time)                        AS last_seen
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name IN ('getTable', 'generateTemporaryTableCredential')
  AND request_params['full_name_arg'] LIKE 'scm_platform_%'
  AND event_date >= current_date() - INTERVAL 7 DAYS
  AND response.status_code = 200
GROUP BY event_date, user_identity.email, request_params['full_name_arg']
ORDER BY event_date DESC, events DESC
