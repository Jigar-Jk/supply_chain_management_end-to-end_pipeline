-- Gold: carrier directory for business users (current version of every carrier).
-- contact_email is PII: members of the pii_readers workspace group see it in full; everyone else sees
-- only the domain (***@dhl-freight.de) - enough to know which company, not which person.
-- Masked here, not in Silver, for the same reason as vw_customer_directory.
CREATE OR REPLACE VIEW gold.vw_carrier_directory AS
SELECT
  carrier_id,
  carrier_name,
  service_level,
  transport_mode,
  contracted_otif_target,
  is_active,
  CASE WHEN is_member('pii_readers') THEN contact_email
       WHEN contact_email IS NULL THEN NULL
       ELSE concat('***@', substring_index(contact_email, '@', -1)) END AS contact_email
FROM silver.dim_carrier
WHERE is_current
