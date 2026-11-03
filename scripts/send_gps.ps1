<#
.SYNOPSIS
  Send truck GPS pings to the live Event Hubs of the current Azure session.

.DESCRIPTION
  Reads the producer's Send-only connection string (infra/secrets.tf, gps-producer rule) from Key
  Vault into this process's EVENTHUB_CONNECTION_STRING - never on screen or in a file - and runs
  data_generators/produce_gps.py against <namespace>.servicebus.windows.net:9093 (Kafka endpoint).
  Then drain the stream into Bronze with: databricks bundle run -t dev scm_streaming

.EXAMPLE
  .\scripts\send_gps.ps1
  .\scripts\send_gps.ps1 -Events 50000 -Eps 2000
#>
param([int]$Events = 20000, [double]$Eps = 1000)

$ErrorActionPreference = "Continue"
$repo = Split-Path -Parent $PSScriptRoot
$infra = Join-Path $repo "infra"

$out = terraform "-chdir=$infra" output -json | ConvertFrom-Json
if ($LASTEXITCODE -ne 0 -or -not $out.eventhub_namespace) { throw "No terraform outputs - is the stack up (scripts/up.ps1)?" }
$bootstrap = "$($out.eventhub_namespace.value).servicebus.windows.net:9093"

$env:EVENTHUB_CONNECTION_STRING = az keyvault secret show --vault-name $out.key_vault_name.value `
    --name eventhub-producer-connection-string --query value -o tsv 2>$null
if ($LASTEXITCODE -ne 0 -or -not $env:EVENTHUB_CONNECTION_STRING) {
    throw "Could not read eventhub-producer-connection-string from Key Vault (terraform apply done?)."
}
try {
    Write-Host "==> Sending $Events GPS pings to $bootstrap (gps-events)" -ForegroundColor Cyan
    Push-Location $repo
    python -m data_generators.produce_gps --bootstrap $bootstrap --events $Events --eps $Eps
    if ($LASTEXITCODE -ne 0) { throw "produce_gps failed." }
}
finally {
    Pop-Location
    Remove-Item Env:EVENTHUB_CONNECTION_STRING -ErrorAction SilentlyContinue
}
Write-Host "Next: databricks bundle run -t dev scm_streaming" -ForegroundColor Yellow
