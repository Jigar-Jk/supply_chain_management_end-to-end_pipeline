resource "azurerm_databricks_workspace" "dbw" {
  name                        = "dbw-${local.base}"
  resource_group_name         = azurerm_resource_group.rg.name
  location                    = var.location
  sku                         = "premium" # Unity Catalog needs Premium
  managed_resource_group_name = "rg-dbw-${local.base}-managed"
  tags                        = local.tags
}

resource "azurerm_databricks_access_connector" "dbac" {
  name                = "dbac-${local.base}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.location
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}
