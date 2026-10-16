resource "azurerm_eventhub_namespace" "evhns" {
  name                = "evhns-${var.project_name}-${var.environment}"
  location            = var.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "Standard"
  capacity            = 1

  tags = {
    project     = "supply-chain-control-tower"
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "azurerm_eventhub" "gps_events" {
  name              = "gps-events"
  namespace_id      = azurerm_eventhub_namespace.evhns.id
  partition_count   = 8
  message_retention = 7
}
