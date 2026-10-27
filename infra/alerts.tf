# Quarantine-rate early warning (5 %), one SQL query + alert per environment catalog.
# The query (dashboards/alerts/quarantine_rate_alert.sql) returns only the feeds at or above 5 %, worst
# first, so the alert checks the first row; no rows means OK. The alert has no schedule of its own: the
# quarantine_rate_alert task of scm_daily_pipeline evaluates it right after Silver and e-mails the
# subscribers. Keep threshold.double_value in step with warn_pct in the SQL (tests/test_quarantine_alert.py).

resource "databricks_query" "quarantine_rate" {
  for_each     = local.catalogs
  display_name = "quarantine-rate-${each.key}"
  description  = "Feeds quarantining >= 5 % of today's rows in ${each.value} (DQ early warning)"
  warehouse_id = databricks_sql_endpoint.scm.id
  catalog      = each.value
  query_text   = file("${path.module}/../dashboards/alerts/quarantine_rate_alert.sql")
}

resource "databricks_alert" "quarantine_rate" {
  for_each       = local.catalogs
  display_name   = "Quarantine rate >= 5 % (${each.key})"
  query_id       = databricks_query.quarantine_rate[each.key].id
  custom_subject = "[${each.key}] A feed quarantined >= 5 % of today's rows"
  notify_on_ok   = false

  condition {
    op                 = "GREATER_THAN_OR_EQUAL"
    empty_result_state = "OK"

    operand {
      column {
        name = "quarantine_pct"
      }
    }

    threshold {
      value {
        double_value = 5
      }
    }
  }
}

# The quarantine_rate_alert task evaluates the alert as the job's identity: scm-pipeline under CD (and
# always in prod, run_as). The deploying user owns query and alert, so the pipeline needs CAN_RUN on both.
resource "databricks_permissions" "quarantine_rate_query" {
  for_each     = local.catalogs
  sql_query_id = databricks_query.quarantine_rate[each.key].id

  access_control {
    service_principal_name = databricks_service_principal.pipeline.application_id
    permission_level       = "CAN_RUN"
  }
}

resource "databricks_permissions" "quarantine_rate_alert" {
  for_each     = local.catalogs
  sql_alert_id = databricks_alert.quarantine_rate[each.key].id

  access_control {
    service_principal_name = databricks_service_principal.pipeline.application_id
    permission_level       = "CAN_RUN"
  }
}
