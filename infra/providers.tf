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

provider "azurerm" {
  features {}
}

provider "databricks" {
  host                        = azurerm_databricks_workspace.dbw.workspace_url
  azure_workspace_resource_id = azurerm_databricks_workspace.dbw.id
}
