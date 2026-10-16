variable "location" {
  description = "Azure region where resources will be created"
  type        = string
  default     = "Central India"
}

variable "environment" {
  description = "Deployment environment name"
  type        = string
  default     = "dev"
}

variable "project_name" {
  description = "Project name prefix for resource naming"
  type        = string
  default     = "scm-platform"
}
