resource "databricks_storage_credential" "dbac_cred" {
  name    = "cred-${var.project_name}-${var.environment}"
  comment = "Storage credential backed by Azure Databricks Access Connector"

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.dbac.id
  }
}

resource "databricks_external_location" "landing" {
  name            = "scm-landing"
  url             = "abfss://landing@stscmplatformdev.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.dbac_cred.id
  comment         = "External location for raw landing data"
}

resource "databricks_external_location" "checkpoints" {
  name            = "scm-checkpoints"
  url             = "abfss://checkpoints@stscmplatformdev.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.dbac_cred.id
  comment         = "External location for Spark Structured Streaming checkpoints"
}

resource "databricks_external_location" "rejected" {
  name            = "scm-rejected"
  url             = "abfss://rejected@stscmplatformdev.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.dbac_cred.id
  comment         = "External location for DLT/Auto Loader rejected records"
}

resource "databricks_external_location" "managed" {
  name            = "scm-managed"
  url             = "abfss://managed@stscmplatformdev.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.dbac_cred.id
  comment         = "External location for managed catalog storage"
  depends_on      = [azurerm_storage_container.containers]
}

import {
  to = databricks_storage_credential.dbac_cred
  id = "cred-scm-platform-dev"
}

import {
  to = databricks_external_location.landing
  id = "scm-landing"
}

import {
  to = databricks_external_location.checkpoints
  id = "scm-checkpoints"
}

import {
  to = databricks_external_location.rejected
  id = "scm-rejected"
}

resource "databricks_catalog" "project_catalog" {
  name         = "scm_platform_${var.environment}"
  comment      = "Catalog for SCM Logistics Platform"
  storage_root = "abfss://managed@stscmplatformdev.dfs.core.windows.net/"
  depends_on   = [databricks_external_location.managed]
}

resource "databricks_schema" "bronze" {
  catalog_name = databricks_catalog.project_catalog.name
  name         = "bronze"
  comment      = "Raw landing data schema"
}

resource "databricks_schema" "quarantine" {
  catalog_name = databricks_catalog.project_catalog.name
  name         = "quarantine"
  comment      = "Quarantine data schema"
}

resource "databricks_schema" "silver" {
  catalog_name = databricks_catalog.project_catalog.name
  name         = "silver"
  comment      = "Cleaned and filtered data schema"
}

resource "databricks_schema" "gold" {
  catalog_name = databricks_catalog.project_catalog.name
  name         = "gold"
  comment      = "Business-level aggregates schema"
}

resource "databricks_schema" "ops" {
  catalog_name = databricks_catalog.project_catalog.name
  name         = "ops"
  comment      = "Operational and audit data schema"
}
