resource "azurerm_role_assignment" "dbac_storage_contributor" {
  scope                = azurerm_storage_account.sa.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.dbac.identity[0].principal_id
}
