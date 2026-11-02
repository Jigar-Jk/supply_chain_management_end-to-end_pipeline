# Azure Data Factory: copies the WMS nightly stock CSV from the SFTP drop (wms_sftp.tf) into the data
# lake's landing/inventory/, where the daily pipeline's Auto Loader reads it. File transfer with no
# custom logic is what Data Factory is for: a built-in SFTP connector, a schedule, retries, and (if
# the WMS ran on-premises) a self-hosted integration runtime to reach it.
#
#   01:30 UTC  copy_wms_inventory: inbox/*.csv -> landing/inventory/, then inbox -> archive/
#   02:00      WMS file deadline (ops.sla_config)
#   02:30      scm_wms_sla_check   03:00  scm_daily_pipeline
#
# No passwords here: Data Factory reads the SFTP password from Key Vault, and writes to the data lake
# with its managed identity.

resource "azurerm_data_factory" "adf" {
  name                = "adf-${local.base}"
  location            = var.location
  resource_group_name = azurerm_resource_group.rg.name
  tags                = local.tags

  identity {
    type = "SystemAssigned"
  }
}

# What the managed identity may do: read secrets, and write to landing only (not the other containers)
resource "azurerm_role_assignment" "adf_kv_secrets_user" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

resource "azurerm_role_assignment" "adf_landing_writer" {
  scope                = azurerm_storage_container.containers["landing"].id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

# ------------------------------------------------------------------ connections (linked services)

resource "azurerm_data_factory_linked_service_key_vault" "kv" {
  name            = "ls_key_vault"
  data_factory_id = azurerm_data_factory.adf.id
  key_vault_id    = azurerm_key_vault.kv.id
}

resource "azurerm_data_factory_linked_service_sftp" "wms_drop" {
  name                = "ls_wms_sftp"
  data_factory_id     = azurerm_data_factory.adf.id
  authentication_type = "Basic"
  host                = "${azurerm_storage_account.wms_drop.name}.blob.core.windows.net"
  port                = 22
  username            = "${azurerm_storage_account.wms_drop.name}.${azurerm_storage_account_local_user.adf.name}"
  # Azure Storage's SFTP host keys are published per region; pin one with host_key_fingerprint in production
  skip_host_key_validation = true

  key_vault_password {
    linked_service_name = azurerm_data_factory_linked_service_key_vault.kv.name
    secret_name         = azurerm_key_vault_secret.adf_sftp_password.name
  }
}

resource "azurerm_data_factory_linked_service_azure_blob_storage" "data_lake" {
  name                 = "ls_data_lake"
  data_factory_id      = azurerm_data_factory.adf.id
  service_endpoint     = azurerm_storage_account.sa.primary_blob_endpoint
  use_managed_identity = true
}

# ------------------------------------------------------------------ datasets (where the files are)

resource "azurerm_data_factory_custom_dataset" "wms_inbox" {
  name            = "ds_wms_inbox"
  data_factory_id = azurerm_data_factory.adf.id
  type            = "Binary"
  linked_service {
    name = azurerm_data_factory_linked_service_sftp.wms_drop.name
  }
  type_properties_json = jsonencode({ location = { type = "SftpLocation", folderPath = "inbox" } })
}

resource "azurerm_data_factory_custom_dataset" "wms_archive" {
  name            = "ds_wms_archive"
  data_factory_id = azurerm_data_factory.adf.id
  type            = "Binary"
  linked_service {
    name = azurerm_data_factory_linked_service_sftp.wms_drop.name
  }
  type_properties_json = jsonencode({ location = { type = "SftpLocation", folderPath = "archive" } })
}

resource "azurerm_data_factory_custom_dataset" "landing_inventory" {
  name            = "ds_landing_inventory"
  data_factory_id = azurerm_data_factory.adf.id
  type            = "Binary"
  linked_service {
    name = azurerm_data_factory_linked_service_azure_blob_storage.data_lake.name
  }
  type_properties_json = jsonencode({
    location = { type = "AzureBlobStorageLocation", container = "landing", folderPath = "inventory" }
  })
}

# ------------------------------------------------------------------ pipeline and schedule

resource "azurerm_data_factory_pipeline" "copy_wms_inventory" {
  name            = "copy_wms_inventory"
  data_factory_id = azurerm_data_factory.adf.id
  description     = "WMS nightly stock CSV: SFTP drop inbox/ -> landing/inventory/, then archive/"
  activities_json = file("${path.module}/adf/copy_wms_inventory.activities.json")
  depends_on = [
    azurerm_data_factory_custom_dataset.wms_inbox,
    azurerm_data_factory_custom_dataset.wms_archive,
    azurerm_data_factory_custom_dataset.landing_inventory,
  ]
}

resource "azurerm_data_factory_trigger_schedule" "wms_nightly" {
  name            = "wms_nightly_0130_utc"
  data_factory_id = azurerm_data_factory.adf.id
  pipeline_name   = azurerm_data_factory_pipeline.copy_wms_inventory.name
  frequency       = "Day"
  interval        = 1
  time_zone       = "UTC"
  activated       = true

  schedule {
    hours   = [1]
    minutes = [30]
  }
}
