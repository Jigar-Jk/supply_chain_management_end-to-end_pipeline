-- Governance tile 3: every GRANT / REVOKE on our catalogs in the last 30 days, and who made it.
-- Grants are owned by Terraform (infra/governance.tf, authoritative), so the expected principal here
-- is whoever ran terraform apply. A change by anyone else was made by hand in the UI or SQL - it
-- will be overwritten on the next apply, but it should still be explained.
SELECT
  event_time,
  user_identity.email                    AS changed_by,
  request_params['securable_type']       AS securable_type,
  request_params['securable_full_name']  AS securable,
  request_params['changes']              AS changes,
  response.status_code                   AS status_code
FROM system.access.audit
WHERE service_name = 'unityCatalog'
  AND action_name = 'updatePermissions'
  AND request_params['securable_full_name'] LIKE 'scm_platform_%'
  AND event_date >= current_date() - INTERVAL 30 DAYS
ORDER BY event_time DESC
