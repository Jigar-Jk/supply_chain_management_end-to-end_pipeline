resource "azurerm_resource_group" "rg" {
  name     = "rg-${var.project_name}-${var.environment}"
  location = var.location

  tags = {
    project     = "supply-chain-control-tower"
    environment = var.environment
    managed_by  = "terraform"
  }
}
