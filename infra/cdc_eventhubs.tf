# SQL Server CDC through Event Hubs: Debezium (Kafka Connect, on the laptop next to SQL Server)
# publishes to these event hubs over the Kafka endpoint, and scm_streaming's stream_cdc drains them.
#
#   SQL Server (scm_oms) -> Debezium -> scm.scm_oms.dbo.order_lines / .inventory -> stream_cdc -> Bronze
#
# Event Hubs only creates topics a client asks for with Manage rights, which Connect does not get, so
# every topic is declared here:
#   - the two change topics, named as Debezium names them (topic.prefix "scm", database, schema, table);
#   - Kafka Connect's three internal topics. They must be compacted: Connect keeps only the latest
#     config, offset (Debezium's LSN bookmark) and status per key, and refuses a topic that is not.
# Debezium's schema history is a file in the Connect container (a volume), not a topic: it must be
# kept forever, and Event Hubs Standard keeps events at most 7 days.
# Standard allows 10 event hubs per namespace: gps-events + these 5.

locals {
  cdc_topics = {
    "scm.scm_oms.dbo.order_lines" = 4
    "scm.scm_oms.dbo.inventory"   = 4
  }
  connect_topics = {
    "connect_configs"  = 1 # Connect requires exactly one partition for its configs
    "connect_offsets"  = 4
    "connect_statuses" = 2
  }
}

resource "azurerm_eventhub" "cdc" {
  for_each          = local.cdc_topics
  name              = each.key
  namespace_id      = azurerm_eventhub_namespace.evhns.id
  partition_count   = each.value
  message_retention = 7 # the CDC change tables also keep 7 days (cdc/sqlserver/02_enable_cdc.sql)
}

resource "azurerm_eventhub" "connect" {
  for_each        = local.connect_topics
  name            = each.key
  namespace_id    = azurerm_eventhub_namespace.evhns.id
  partition_count = each.value

  retention_description {
    cleanup_policy                    = "Compact"
    tombstone_retention_time_in_hours = 24
  }
}

# Debezium's Connect worker reads its own internal topics and writes them and the change topics:
# Send and Listen on the namespace, not Manage. Kept in Key Vault only; scripts/cdc_azure.ps1 loads
# it into the Connect container's environment for the session.
resource "azurerm_eventhub_namespace_authorization_rule" "debezium_connect" {
  name                = "debezium-connect"
  namespace_name      = azurerm_eventhub_namespace.evhns.name
  resource_group_name = azurerm_resource_group.rg.name
  listen              = true
  send                = true
  manage              = false
}

resource "azurerm_key_vault_secret" "eventhub_debezium_connection_string" {
  name         = "eventhub-debezium-connection-string"
  value        = azurerm_eventhub_namespace_authorization_rule.debezium_connect.primary_connection_string
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.deployer_kv_secrets_officer]
}
