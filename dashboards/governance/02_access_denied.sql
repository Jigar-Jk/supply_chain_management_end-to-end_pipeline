-- Governance tile 2: requests Unity Catalog refused (HTTP 403) in the last 7 days.
-- This is the proof that the grants in infra/governance.tf work: e.g. scm-analyst trying to read
-- silver.dim_customer shows up here with the PERMISSION_DENIED message, while its Gold reads succeed.
-- A sudden burst for one principal is worth a look (a broken job, or someone probing).
SELECT
  event_time,
  user_identity.email                    AS principal,
  action_name,
  coalesce(request_params['full_name_arg'],
           request_params['securable_full_name'],
           request_params['name'])       AS object_name,
  response.error_message                 AS error_message
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND response.status_code = 403
  AND event_date >= current_date() - INTERVAL 7 DAYS
ORDER BY event_time DESC
