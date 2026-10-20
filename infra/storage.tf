resource "azurerm_storage_account" "sa" {
  name                     = replace("st${var.project_name}${var.name_suffix}", "-", "")
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true
  tags                     = local.tags
}

resource "azurerm_storage_container" "containers" {
  for_each              = toset(["landing", "checkpoints", "rejected", "managed"])
  name                  = each.key
  storage_account_id    = azurerm_storage_account.sa.id
  container_access_type = "private"
}
