# Data governance: who may read or change which data (Unity Catalog), as code.
# Works with workspace-admin rights only: permissions are granted to service principals (robot
# identities) instead of account groups, because account groups need a Databricks account admin.

# The "analyst" persona for the demo: may read the Gold KPIs and nothing else.
resource "databricks_service_principal" "analyst" {
  display_name          = "scm-analyst"
  databricks_sql_access = true # may run queries on the SQL warehouse (dashboards, Genie)
  workspace_access      = true
}

# The identity the prod jobs will run as once CD exists: the only writer of prod.
resource "databricks_service_principal" "pipeline" {
  display_name          = "scm-pipeline"
  databricks_sql_access = true
  workspace_access      = true
  allow_cluster_create  = true # job clusters are created as the identity the job runs as
}

# ------------------------------------------------------------------ permissions (Unity Catalog grants)
# Reading a table needs all three: USE_CATALOG (enter the catalog) + USE_SCHEMA (enter the schema) +
# SELECT. Privileges granted on a schema cover every table in it, today's and future ones.
# databricks_grants is authoritative: each securable gets exactly the grants listed here, so access
# only changes through code review (a grant clicked in the UI is removed by the next apply).
# The deploying user owns the catalogs (owners hold every privilege), so needs no grant of its own.
# Gold views read Silver with their owner's rights, so the analyst needs no access to Silver.

resource "databricks_grants" "catalog" {
  for_each = local.catalogs
  catalog  = databricks_catalog.env[each.key].name

  grant {
    principal  = databricks_service_principal.pipeline.application_id
    privileges = ["ALL_PRIVILEGES"]
  }

  grant {
    principal  = databricks_service_principal.analyst.application_id
    privileges = ["USE_CATALOG"] # can enter the catalog; only gold is opened below
  }
}

resource "databricks_grants" "gold" {
  for_each = local.catalogs
  schema   = databricks_schema.medallion["${each.key}.gold"].id

  grant {
    principal  = databricks_service_principal.analyst.application_id
    privileges = ["USE_SCHEMA", "SELECT"] # read the KPI views and mart - nothing else, nowhere else
  }
}

# Running any query needs CAN_USE on the SQL warehouse (workspace permission, not Unity Catalog).
resource "databricks_permissions" "warehouse" {
  sql_endpoint_id = databricks_sql_endpoint.scm.id

  access_control {
    service_principal_name = databricks_service_principal.analyst.application_id
    permission_level       = "CAN_USE"
  }

  access_control {
    service_principal_name = databricks_service_principal.pipeline.application_id
    permission_level       = "CAN_USE"
  }
}

# Files outside the catalogs: Bronze reads the landing container, Auto Loader keeps its progress in
# checkpoints. Catalog grants do not reach external locations, so the pipeline gets them here.
# rejected and managed get no grant: no job reads or writes them directly.
resource "databricks_grants" "landing" {
  external_location = databricks_external_location.locations["landing"].id

  grant {
    principal  = databricks_service_principal.pipeline.application_id
    privileges = ["READ_FILES"]
  }
}

resource "databricks_grants" "checkpoints" {
  external_location = databricks_external_location.locations["checkpoints"].id

  grant {
    principal  = databricks_service_principal.pipeline.application_id
    privileges = ["READ_FILES", "WRITE_FILES"]
  }
}

# The streaming job reads the Event Hubs connection string from this scope.
resource "databricks_secret_acl" "pipeline_reads_secrets" {
  scope      = databricks_secret_scope.scm_platform.name
  principal  = databricks_service_principal.pipeline.application_id
  permission = "READ"
}

# CD logs in as the pipeline (OAuth machine-to-machine): client id = application_id, plus this secret.
# Recreated on every apply of a fresh stack; scripts/up.ps1 hands it to GitHub without printing it.
resource "databricks_service_principal_secret" "pipeline" {
  service_principal_id = databricks_service_principal.pipeline.id
}

# ------------------------------------------------------------------ PII access (masking)
# Members see real customer names / contact e-mails in the Gold customer view; everyone else sees ***.
# A workspace group: masking checks it with is_member() (workspace admins can create these; Unity
# Catalog grants, by contrast, need account groups - hence the service principals above).
# Created empty on purpose (safe default: nobody sees PII until added). Members are added by a workspace
# admin under Settings > Identity and access > Groups; the demo adds the presenter live.
resource "databricks_group" "pii_readers" {
  display_name = "pii_readers"
}
