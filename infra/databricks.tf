resource "azurerm_databricks_workspace" "dbw" {
  name                        = "dbw-${var.project_name}-${var.environment}"
  resource_group_name         = azurerm_resource_group.rg.name
  location                    = var.location
  sku                         = "premium"
  managed_resource_group_name = "rg-dbw-${var.project_name}-${var.environment}-managed"

  tags = {
    project     = "supply-chain-control-tower"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "azurerm_databricks_access_connector" "dbac" {
  name                = "dbac-${var.project_name}-${var.environment}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }

  tags = {
    project     = "supply-chain-control-tower"
    environment = var.environment
    managed_by  = "terraform"
  }
}
