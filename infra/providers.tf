terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 4.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.50"
    }
  }
}

# Subscription: set ARM_SUBSCRIPTION_ID before plan/apply (azurerm 4.x requires it), e.g.
#   $env:ARM_SUBSCRIPTION_ID = (az account show --query id -o tsv)
provider "azurerm" {
  features {
    key_vault {
      # destroy -> apply cycles: purge instead of leaving a soft-deleted vault that blocks the name for 7 days
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = false
    }
  }
}

provider "databricks" {
  host                        = azurerm_databricks_workspace.dbw.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.dbw.id
}
