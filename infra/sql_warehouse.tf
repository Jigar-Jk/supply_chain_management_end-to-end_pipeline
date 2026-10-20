# SQL warehouse for the Silver quality tests, the Gold refresh, dashboards and Genie.
# Serverless: starts in seconds and does not use the subscription's VM quota. Auto-stops when idle.
# If serverless is not available in the region, set serverless_sql_warehouse = false (classic PRO,
# which runs on VMs in this subscription and counts against its vCPU quota).
resource "databricks_sql_endpoint" "scm" {
  name                      = "scm-warehouse"
  cluster_size              = "2X-Small"
  min_num_clusters          = 1
  max_num_clusters          = 1
  auto_stop_mins            = 10
  warehouse_type            = "PRO"
  enable_serverless_compute = var.serverless_sql_warehouse
}
