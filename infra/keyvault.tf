data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "kv" {
  name                       = "kv-${var.project_name}-${var.environment}"
  location                   = var.location
  resource_group_name        = azurerm_resource_group.rg.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  soft_delete_retention_days = 7
  purge_protection_enabled   = false
  rbac_authorization_enabled = true

  tags = {
    project     = "supply-chain-control-tower"
    environment = var.environment
    managed_by  = "terraform"
  }
}
