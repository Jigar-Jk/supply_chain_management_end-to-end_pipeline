# Values needed after each `terraform apply` (the workspace URL changes on every deploy).
output "databricks_workspace_url" {
  description = "Log in with: databricks auth login --host <this> --profile scm-azure"
  value       = "https://${azurerm_databricks_workspace.dbw.workspace_url}"
}

output "storage_account_name" {
  description = "Bundle variable storage_account"
  value       = azurerm_storage_account.sa.name
}

output "eventhub_namespace" {
  description = "Kafka bootstrap: <this>.servicebus.windows.net:9093"
  value       = azurerm_eventhub_namespace.evhns.name
}

output "resource_group" {
  description = "Check in the portal that this is gone after terraform destroy"
  value       = azurerm_resource_group.rg.name
}

output "catalogs" {
  value = [for c in databricks_catalog.env : c.name]
}

output "sql_warehouse_id" {
  description = "Bundle variable warehouse_id"
  value       = databricks_sql_endpoint.scm.id
}

output "key_vault_name" {
  value = azurerm_key_vault.kv.name
}
