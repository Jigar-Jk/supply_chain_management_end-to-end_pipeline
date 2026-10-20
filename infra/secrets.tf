# Secrets as code: the Event Hubs connection string is created by Azure and written straight into a
# Databricks secret scope; the streaming job references it as
# {{secrets/scm-platform/eventhub-connection-string}}. It never appears in a file or in the bundle.
# A copy is kept in Key Vault as the Azure-side record. (The value is also in the Terraform state,
# which is git-ignored and local.)
#
# Why a Databricks-managed scope and not a Key Vault-backed one: a Key Vault-backed scope needs the
# "AzureDatabricks" application to be granted access, which means looking it up in Entra ID - and a
# guest account (how this subscription is operated) may not read the directory. With the Directory
# Readers role, switch to keyvault_metadata { resource_id, dns_name } and grant that application
# "Key Vault Secrets User".

# Listen-only access: the GPS and CDC streams only read from Event Hubs.
resource "azurerm_eventhub_namespace_authorization_rule" "databricks_streams" {
  name                = "databricks-streams"
  namespace_name      = azurerm_eventhub_namespace.evhns.name
  resource_group_name = azurerm_resource_group.rg.name
  listen              = true
  send                = false
  manage              = false
}

resource "azurerm_key_vault_secret" "eventhub_connection_string" {
  name         = "eventhub-connection-string"
  value        = azurerm_eventhub_namespace_authorization_rule.databricks_streams.primary_connection_string
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.deployer_kv_secrets_officer]
}

resource "databricks_secret_scope" "scm_platform" {
  name = "scm-platform"
}

resource "databricks_secret" "eventhub_connection_string" {
  scope        = databricks_secret_scope.scm_platform.name
  key          = "eventhub-connection-string"
  string_value = azurerm_eventhub_namespace_authorization_rule.databricks_streams.primary_connection_string
}
