<#
.SYNOPSIS
  Point this PowerShell window at the running session's workspace, for the Databricks CLI and bundles.

.DESCRIPTION
  scripts/up.ps1 sets these only in its own window. In any other window, run this first; otherwise
  the CLI may use a stale default profile from ~/.databrickscfg (a destroyed workspace) and the bundle
  deploys with the REPLACE-WITH-... placeholders of databricks.yml.

  Sets, from the terraform outputs (nothing typed by hand, no secret read):
    DATABRICKS_HOST, DATABRICKS_AUTH_TYPE=azure-cli, DATABRICKS_CONFIG_FILE (empty, ignores ~/.databrickscfg)
    BUNDLE_VAR_warehouse_id, _storage_account, _kafka_bootstrap, _alert_email, _quarantine_alert_id

  Must be dot-sourced (note the leading ". "), so the variables stay in this window.

.EXAMPLE
  . .\scripts\session_env.ps1
  . .\scripts\session_env.ps1 -Target prod     # quarantine alert of the prod catalog
  databricks bundle deploy -t dev
#>
param([string]$Target = "dev")

if ($MyInvocation.InvocationName -ne ".") {
    Write-Warning "Run it dot-sourced so the settings stay in this window:  . .\scripts\session_env.ps1"
    return
}

$infra = Join-Path (Split-Path -Parent $PSScriptRoot) "infra"
$hostUrl = terraform "-chdir=$infra" output -raw databricks_workspace_url
if ($LASTEXITCODE -ne 0 -or -not $hostUrl) { Write-Warning "No terraform outputs - is a session running (scripts/up.ps1)?"; return }

$env:DATABRICKS_HOST = $hostUrl
$env:DATABRICKS_AUTH_TYPE = "azure-cli"
Remove-Item Env:DATABRICKS_CONFIG_PROFILE -ErrorAction SilentlyContinue
$env:DATABRICKS_CONFIG_FILE = Join-Path $env:TEMP "scm-session.databrickscfg"
if (-not (Test-Path $env:DATABRICKS_CONFIG_FILE)) { Set-Content -Path $env:DATABRICKS_CONFIG_FILE -Value "" -Encoding ascii }

$env:BUNDLE_VAR_warehouse_id = terraform "-chdir=$infra" output -raw sql_warehouse_id
$env:BUNDLE_VAR_storage_account = terraform "-chdir=$infra" output -raw storage_account_name
$env:BUNDLE_VAR_kafka_bootstrap = (terraform "-chdir=$infra" output -raw eventhub_namespace) + ".servicebus.windows.net:9093"
$env:BUNDLE_VAR_alert_email = az account show --query user.name -o tsv
$env:BUNDLE_VAR_quarantine_alert_id = (terraform "-chdir=$infra" output -json quarantine_alert_ids | ConvertFrom-Json).$Target
if (-not $env:BUNDLE_VAR_quarantine_alert_id) { Write-Warning "No quarantine alert for target '$Target'." }

Write-Host "This window now uses $hostUrl (target $Target):"
Get-ChildItem Env:BUNDLE_VAR_* | ForEach-Object { Write-Host ("    {0,-36} {1}" -f $_.Name, $_.Value) }
