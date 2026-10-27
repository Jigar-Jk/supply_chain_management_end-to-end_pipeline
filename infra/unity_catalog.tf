# Unity Catalog objects in the workspace's metastore. All three environments share one storage
# credential and the external locations; each environment gets its own catalog with its own storage
# folder (managed/<catalog>) and the five medallion schemas.
# force_destroy: `terraform destroy` removes catalogs / schemas even when they still hold tables,
# which the deploy -> run -> destroy cycle needs.

locals {
  sa_name  = azurerm_storage_account.sa.name
  catalogs = { for env in var.environments : env => "scm_platform_${env}" }
  schemas = {
    bronze     = "Raw landing data schema"
    silver     = "Cleaned and conformed galaxy schema"
    gold       = "Business-level KPI views and mart"
    quarantine = "Rows that failed a quality rule"
    ops        = "Operational and audit data (SLA config, run log, checkpoints)"
  }
  catalog_schemas = {
    for pair in setproduct(var.environments, keys(local.schemas)) : "${pair[0]}.${pair[1]}" => {
      env    = pair[0]
      schema = pair[1]
    }
  }
}

resource "databricks_storage_credential" "dbac_cred" {
  name          = "cred-${local.base}"
  comment       = "Storage credential backed by the Azure Databricks Access Connector"
  force_destroy = true

  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.dbac.id
  }

  depends_on = [azurerm_role_assignment.dbac_storage_contributor]
}

resource "databricks_external_location" "locations" {
  for_each = {
    landing     = "Raw landing files (one folder per feed)"
    checkpoints = "Auto Loader and Structured Streaming checkpoints"
    rejected    = "Rejected files"
    managed     = "Managed storage for the environment catalogs"
  }
  name            = "scm-${each.key}-${var.name_suffix}"
  url             = "abfss://${each.key}@${local.sa_name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.dbac_cred.id
  comment         = each.value
  force_destroy   = true
  depends_on      = [azurerm_storage_container.containers]
}

resource "databricks_catalog" "env" {
  for_each      = local.catalogs
  name          = each.value
  comment       = "SCM Logistics Platform - ${each.key} environment"
  storage_root  = "abfss://managed@${local.sa_name}.dfs.core.windows.net/${each.value}"
  force_destroy = true
  depends_on    = [databricks_external_location.locations]
}

resource "databricks_schema" "medallion" {
  for_each      = local.catalog_schemas
  catalog_name  = databricks_catalog.env[each.value.env].name
  name          = each.value.schema
  comment       = local.schemas[each.value.schema]
  force_destroy = true
}

# Structured Streaming checkpoints (committed Kafka offsets) of the scm_streaming job. Here, not in the
# bundle: in dev each developer and CD deploy their own copy of the bundle, and a volume declared in
# it would be created twice in the same catalog (RESOURCE_ALREADY_EXISTS).
resource "databricks_volume" "checkpoints" {
  for_each     = local.catalogs
  catalog_name = databricks_catalog.env[each.key].name
  schema_name  = databricks_schema.medallion["${each.key}.ops"].name
  name         = "checkpoints"
  volume_type  = "MANAGED"
  comment      = "Structured Streaming checkpoints (committed Kafka offsets) for the GPS and CDC streams"
}
