<#
.SYNOPSIS
  SQL Server CDC into Azure: start Debezium against the session's Event Hubs and register the connector.

.DESCRIPTION
  1. Checks SQL Server (MSSQLSERVER) and SQL Server Agent run: CDC's capture job needs the Agent.
  2. Optionally (-LoadSource) reloads scm_oms from the generated data (data_generators/load_sqlserver_oms).
  3. Reads the Debezium Send+Listen connection string (infra/cdc_eventhubs.tf) from Key Vault into this
     process's environment only, never on screen or in a file, and starts the kafka-connect-azure
     container (local/streaming/docker-compose.yml, profile "azure") with it.
  4. Registers cdc/debezium/sqlserver-connector.azure.json and waits until the connector is RUNNING.
     Debezium snapshots both tables (op r), then streams every change (c/u/d) to
     scm.scm_oms.dbo.order_lines / .inventory.
  Then: python -m data_generators.simulate_oms_changes (live changes), and
  databricks bundle run -t dev scm_streaming (stream_cdc drains them into Bronze).

.EXAMPLE
  .\scripts\cdc_azure.ps1 -LoadSource     # fresh source, then Debezium
  .\scripts\cdc_azure.ps1                 # (re)start Debezium; it resumes from its saved LSN
  .\scripts\cdc_azure.ps1 -Stop           # stop the container (offsets stay in Event Hubs)
#>
param([switch]$LoadSource, [switch]$Stop)

$ErrorActionPreference = "Continue"
$repo = Split-Path -Parent $PSScriptRoot
$infra = Join-Path $repo "infra"
$compose = Join-Path $repo "local\streaming\docker-compose.yml"
$connectUrl = "http://localhost:8084"
$connector = "scm-oms-sqlserver"

function Step($text) { Write-Host ""; Write-Host "==> $text" -ForegroundColor Cyan }
function Check($what) { if ($LASTEXITCODE -ne 0) { throw "$what failed (exit code $LASTEXITCODE)." } }

if ($Stop) {
    docker compose -f $compose --profile azure stop kafka-connect-azure
    return
}

Step "Checking SQL Server and SQL Server Agent"
$services = Get-Service MSSQLSERVER, SQLSERVERAGENT -ErrorAction SilentlyContinue
$stopped = $services | Where-Object Status -ne "Running"
if (-not $services -or $stopped) {
    throw "Not running: $(($stopped.Name) -join ', '). In a PowerShell run as Administrator:  Start-Service MSSQLSERVER; Start-Service SQLSERVERAGENT"
}
Write-Host "    MSSQLSERVER and SQLSERVERAGENT running"

if ($LoadSource) {
    Step "Loading the OMS source (scm_oms) from the generated data"
    Push-Location $repo
    try { python -m data_generators.load_sqlserver_oms --reset; Check "load_sqlserver_oms" } finally { Pop-Location }
}

Step "Starting Debezium against Event Hubs"
$out = terraform "-chdir=$infra" output -json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or -not $out.eventhub_namespace) { throw "No terraform outputs - is the stack up (scripts/up.ps1)?" }
$env:EVENTHUB_BOOTSTRAP = "$($out.eventhub_namespace.value).servicebus.windows.net:9093"
# A user environment variable: a window opened before it was set does not have it yet
if (-not $env:DEBEZIUM_SQL_PASSWORD) {
    $env:DEBEZIUM_SQL_PASSWORD = [Environment]::GetEnvironmentVariable("DEBEZIUM_SQL_PASSWORD", "User")
}
if (-not $env:DEBEZIUM_SQL_PASSWORD) { throw "DEBEZIUM_SQL_PASSWORD is not set (see docs/cdc.md)." }
$env:EVENTHUB_DEBEZIUM_CONNECTION_STRING = az keyvault secret show --vault-name $out.key_vault_name.value `
    --name eventhub-debezium-connection-string --query value -o tsv 2>$null
try {
    if ($LASTEXITCODE -ne 0 -or -not $env:EVENTHUB_DEBEZIUM_CONNECTION_STRING) {
        throw "Could not read eventhub-debezium-connection-string from Key Vault (terraform apply done?)."
    }
    docker compose -f $compose --profile azure up -d --wait kafka-connect-azure
    Check "docker compose up kafka-connect-azure"
} finally {
    # The container has them now; this window does not need to keep them
    Remove-Item Env:EVENTHUB_DEBEZIUM_CONNECTION_STRING, Env:DEBEZIUM_SQL_PASSWORD -ErrorAction SilentlyContinue
}
Write-Host "    Kafka Connect up at $connectUrl ($($env:EVENTHUB_BOOTSTRAP))"

Step "Registering the connector $connector"
Invoke-RestMethod -Method Put -Uri "$connectUrl/connectors/$connector/config" -ContentType application/json `
    -InFile (Join-Path $repo "cdc\debezium\sqlserver-connector.azure.json") | Out-Null
for ($i = 1; ; $i++) {
    try { $status = Invoke-RestMethod "$connectUrl/connectors/$connector/status" } catch { $status = $null }
    $task = $status.tasks | Select-Object -First 1
    if ($status.connector.state -eq "RUNNING" -and $task.state -eq "RUNNING") { break }
    if ($task.state -eq "FAILED") { throw "Connector task FAILED: $(($task.trace -split "`n")[0])" }
    if ($i -ge 36) { throw "Connector not RUNNING after 3 minutes: $($status | ConvertTo-Json -Depth 4 -Compress)" }
    Start-Sleep -Seconds 5
}
Write-Host "    $connector RUNNING: snapshot, then live changes, to scm.scm_oms.dbo.order_lines / .inventory"
Write-Host "Next: python -m data_generators.simulate_oms_changes --events 2000 --tps 20" -ForegroundColor Yellow
Write-Host "      databricks bundle run -t dev scm_streaming      (in a window prepared with scripts/session_env.ps1)" -ForegroundColor Yellow
