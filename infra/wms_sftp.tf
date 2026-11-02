# The WMS "letter box": an SFTP drop where the warehouse system leaves its nightly stock CSV, and
# from where Data Factory picks it up (adf.tf). The WMS gets access to this drop only, never to the
# data lake. A storage account of its own, because SFTP is switched on for a whole account and the
# data lake must not be reachable over SFTP.
#
#   inbox/     the WMS uploads inventory_<night>.csv here
#   archive/   Data Factory moves each file here once it has landed in the data lake
#
# Login: sftp <account>.<user>@<account>.blob.core.windows.net (port 22). Azure generates each
# user's password; both go straight into Key Vault. SFTP is billed per hour while enabled, so this
# lives and dies with the session stack like everything else.

resource "azurerm_storage_account" "wms_drop" {
  name                     = replace("st${var.project_name}wms${var.name_suffix}", "-", "")
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true # SFTP needs a hierarchical namespace
  sftp_enabled             = true
  local_user_enabled       = true
  tags                     = local.tags
}

resource "azurerm_storage_container" "wms_drop" {
  name                  = "wms-drop"
  storage_account_id    = azurerm_storage_account.wms_drop.id
  container_access_type = "private"
}

# The WMS may put files into the drop and see what is there; it cannot read them back or delete them.
resource "azurerm_storage_account_local_user" "wms" {
  name                 = "wms"
  storage_account_id   = azurerm_storage_account.wms_drop.id
  ssh_password_enabled = true
  home_directory       = azurerm_storage_container.wms_drop.name

  permission_scope {
    service       = "blob"
    resource_name = azurerm_storage_container.wms_drop.name
    permissions {
      create = true
      write  = true
      list   = true
    }
  }
}

# Data Factory reads the files, writes the archive copy and deletes the original from inbox/.
resource "azurerm_storage_account_local_user" "adf" {
  name                 = "adf"
  storage_account_id   = azurerm_storage_account.wms_drop.id
  ssh_password_enabled = true
  home_directory       = azurerm_storage_container.wms_drop.name

  permission_scope {
    service       = "blob"
    resource_name = azurerm_storage_container.wms_drop.name
    permissions {
      read   = true
      list   = true
      create = true
      write  = true
      delete = true
    }
  }
}

resource "azurerm_key_vault_secret" "wms_sftp_password" {
  name         = "wms-sftp-password"
  value        = azurerm_storage_account_local_user.wms.password
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.deployer_kv_secrets_officer]
}

resource "azurerm_key_vault_secret" "adf_sftp_password" {
  name         = "adf-sftp-password"
  value        = azurerm_storage_account_local_user.adf.password
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [azurerm_role_assignment.deployer_kv_secrets_officer]
}
