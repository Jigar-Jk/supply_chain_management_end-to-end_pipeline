# Standard tier: the Kafka endpoint the GPS and CDC streams use (Basic has no Kafka support).
# Billed per hour while it exists, even idle, so destroy the stack after each working session.
resource "azurerm_eventhub_namespace" "evhns" {
  name                = "evhns-${local.base}"
  location            = var.location
  resource_group_name = azurerm_resource_group.rg.name
  sku                 = "Standard"
  capacity            = 1
  tags                = local.tags
}

resource "azurerm_eventhub" "gps_events" {
  name              = "gps-events"
  namespace_id      = azurerm_eventhub_namespace.evhns.id
  partition_count   = 8
  message_retention = 7
}
