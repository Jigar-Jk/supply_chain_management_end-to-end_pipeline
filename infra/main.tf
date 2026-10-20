# One shared stack (resource group, storage, Event Hubs, Key Vault, Databricks workspace) serving all
# three environments; dev / qa / prod are separated by Unity Catalog catalog (see unity_catalog.tf).
locals {
  base = "${var.project_name}-${var.name_suffix}"
  tags = {
    project    = "supply-chain-control-tower"
    managed_by = "terraform"
  }
}

resource "azurerm_resource_group" "rg" {
  name     = "rg-${local.base}"
  location = var.location
  tags     = local.tags
}
