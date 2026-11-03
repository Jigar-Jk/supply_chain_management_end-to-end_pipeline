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

output "quarantine_alert_ids" {
  description = "Bundle variable quarantine_alert_id, per environment"
  value       = { for env, alert in databricks_alert.quarantine_rate : env => alert.id }
}

output "carrier_api_url" {
  description = "Bundle variable carrier_api_url: where fetch_carrier_api calls the carrier API; 'disabled' skips it"
  # The mock API on Azure Container Apps (carrier_api.tf). scripts/up.ps1 passes "disabled" instead
  # when it cannot roll out the API's image (Docker not running), and shipments come from the file.
  value = "https://${azurerm_container_app.carrier_api.ingress[0].fqdn}"
}

output "carrier_api_deploy" {
  description = "Where scripts/up.ps1 pushes the carrier API image and which Container App it rolls it out to"
  value = {
    registry_name = azurerm_container_registry.acr.name
    login_server  = azurerm_container_registry.acr.login_server
    app_name      = azurerm_container_app.carrier_api.name
  }
}

output "wms_sftp" {
  description = "Where the WMS drops its nightly CSV: sftp <username>@<host>, password in Key Vault secret <password_secret>"
  value = {
    host            = "${azurerm_storage_account.wms_drop.name}.blob.core.windows.net"
    username        = "${azurerm_storage_account.wms_drop.name}.${azurerm_storage_account_local_user.wms.name}"
    folder          = "inbox"
    password_secret = azurerm_key_vault_secret.wms_sftp_password.name
  }
}

output "data_factory_name" {
  description = "Run the WMS copy by hand: az datafactory pipeline create-run --factory-name <this> --name copy_wms_inventory"
  value       = azurerm_data_factory.adf.name
}

output "pipeline_client_id" {
  description = "CD login: DATABRICKS_CLIENT_ID (GitHub secret, set by scripts/up.ps1)"
  value       = databricks_service_principal.pipeline.application_id
}

output "pipeline_client_secret" {
  description = "CD login: DATABRICKS_CLIENT_SECRET (GitHub secret, set by scripts/up.ps1)"
  value       = databricks_service_principal_secret.pipeline.secret
  sensitive   = true # never shown in plan / apply output
}
