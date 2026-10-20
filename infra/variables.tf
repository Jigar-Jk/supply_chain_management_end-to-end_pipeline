variable "location" {
  description = "Azure region where resources will be created (check the subscription's allowed regions first)"
  type        = string
  default     = "Central India"
}

variable "project_name" {
  description = "Project name prefix for resource naming"
  type        = string
  default     = "scm-platform"
}

variable "name_suffix" {
  description = "Short suffix making the globally unique names (storage, Key Vault, Event Hubs) unique to this subscription"
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]{1,5}$", var.name_suffix))
    error_message = "name_suffix must be 1-5 lowercase letters or digits (it becomes part of the storage account name)."
  }
}

variable "environments" {
  description = "One Unity Catalog catalog per environment, all in the one shared workspace"
  type        = list(string)
  default     = ["dev", "qa", "prod"]
}

variable "serverless_sql_warehouse" {
  description = "Serverless SQL warehouse (recommended); false = classic PRO on VMs, if the region has no serverless"
  type        = bool
  default     = true
}
