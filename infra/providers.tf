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
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

# Subscription: set ARM_SUBSCRIPTION_ID before plan/apply (azurerm 4.x requires it), e.g.
#   $env:ARM_SUBSCRIPTION_ID = (az account show --query id -o tsv)
provider "azurerm" {
  # Container Apps (carrier_api.tf) is not among the providers azurerm registers by itself. Listed
  # here, not as an azurerm_resource_provider_registration resource: that resource unregisters the
  # provider on destroy, which Azure refuses (409 InvalidUnregistration) and which stopped down.ps1.
  # A registration is a free subscription setting; it can stay.
  resource_providers_to_register = ["Microsoft.App"]

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
