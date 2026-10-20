# Unity Catalog reads and writes the data lake through the access connector's managed identity.
resource "azurerm_role_assignment" "dbac_storage_contributor" {
  scope                = azurerm_storage_account.sa.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.dbac.identity[0].principal_id
}

# The person running Terraform uploads the landing files (az storage blob upload-batch --auth-mode login).
resource "azurerm_role_assignment" "deployer_storage_contributor" {
  scope                = azurerm_storage_account.sa.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

# ... and writes secrets into the RBAC-mode Key Vault.
resource "azurerm_role_assignment" "deployer_kv_secrets_officer" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}
