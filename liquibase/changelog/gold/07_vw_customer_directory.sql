-- Gold: customer directory for business users (current version of every customer).
-- customer_name is PII: shown only to members of the pii_readers workspace group, *** for everyone
-- else (the analyst included). Segment and location stay visible, so customers can still be counted
-- and grouped without knowing who they are.
-- Masked here, not in Silver: the SCD2 merge compares Silver values, and a masked value would look
-- like a name change every night. Analysts cannot read Silver at all (infra/governance.tf).
CREATE OR REPLACE VIEW gold.vw_customer_directory AS
SELECT
  customer_id,
  CASE WHEN is_member('pii_readers') THEN customer_name ELSE '***' END AS customer_name,
  customer_segment,
  country,
  city
FROM silver.dim_customer
WHERE is_current
