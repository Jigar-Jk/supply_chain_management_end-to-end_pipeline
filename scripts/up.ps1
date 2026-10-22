<#
.SYNOPSIS
  Start a working session on Azure: build the stack, create the tables, load fresh data, deploy the jobs.

.DESCRIPTION
  1. Safety checks: logged in to the Azure for Students subscription; the previous session's
     Databricks managed resource group is gone (otherwise the workspace name clashes).
  2. terraform apply (asks you to type "yes"; nothing is created without it).
  3. Reads the outputs (workspace URL, warehouse id, storage account) - nothing is copied by hand.
  4. Databricks CLI login reuses your az login (no browser).
  5. Creates the tables in the dev, qa and prod catalogs (scripts/apply_migrations.py).
  6. Generates fresh data (--reference-time now) and uploads it to the landing container.
  7. Deploys the bundle to the target (default dev).

  Billing starts at step 2. End every session with scripts/down.ps1.

.EXAMPLE
  .\scripts\up.ps1
  .\scripts\up.ps1 -SkipData      # keep the landing data already uploaded in this stack
#>
param(
    [string]$Target = "dev",
    [switch]$SkipData
)

# Not "Stop": in Windows PowerShell 5.1 that turns any stderr text of az/terraform into a crash.
# Every external command is checked by its exit code instead (Check / $LASTEXITCODE), and throw always stops.
$ErrorActionPreference = "Continue"
$repo = Split-Path -Parent $PSScriptRoot
$infra = Join-Path $repo "infra"
$tfvars = Join-Path $infra "envs\student.tfvars"

function Step($text) { Write-Host ""; Write-Host "==> $text" -ForegroundColor Cyan }
function Check($what) { if ($LASTEXITCODE -ne 0) { throw "$what failed (exit code $LASTEXITCODE). Stopped - fix the error above and run up.ps1 again." } }

# ------------------------------------------------------------------ 1. safety checks
Step "Checking the Azure login"
$account = az account show -o json 2>$null | ConvertFrom-Json
if (-not $account) { throw "Not logged in. Run: az login --tenant 75b3330d-6fde-4894-8d4c-b5541adbddad" }
if ($account.name -ne "Azure for Students") { throw "Active subscription is '$($account.name)', not 'Azure for Students'. Stopped before creating anything." }
Write-Host "    $($account.name) as $($account.user.name)"
$env:ARM_SUBSCRIPTION_ID = $account.id

$suffix = ([regex]::Match((Get-Content $tfvars -Raw), 'name_suffix\s*=\s*"([a-z0-9]+)"')).Groups[1].Value
$managedRg = "rg-dbw-scm-platform-$suffix-managed"
if ((az group exists -n $managedRg) -eq "true" -and (az group exists -n "rg-scm-platform-$suffix") -eq "false") {
    throw "$managedRg from the last session still exists (Azure deletes it ~20-30 min after the workspace). Wait and try again."
}

# ------------------------------------------------------------------ 2. infrastructure
Step "terraform apply (type 'yes' to build; billing starts)"
terraform "-chdir=$infra" init -input=false | Out-Null; Check "terraform init"
terraform "-chdir=$infra" apply "-var-file=$tfvars"; Check "terraform apply"

# ------------------------------------------------------------------ 3. outputs
$out = terraform "-chdir=$infra" output -json | ConvertFrom-Json; Check "terraform output"
$hostUrl = $out.databricks_workspace_url.value
$warehouse = $out.sql_warehouse_id.value
$storage = $out.storage_account_name.value
$kafka = "$($out.eventhub_namespace.value).servicebus.windows.net:9093"
$alertId = $out.quarantine_alert_ids.value.$Target           # the 5 % quarantine alert of this environment
if (-not $alertId) { throw "No quarantine alert for target '$Target' in the terraform outputs." }
Write-Host "    workspace $hostUrl | warehouse $warehouse | storage $storage | alert $alertId"

# ------------------------------------------------------------------ 4. Databricks login = your az login
$env:DATABRICKS_HOST = $hostUrl
$env:DATABRICKS_AUTH_TYPE = "azure-cli"
Remove-Item Env:DATABRICKS_CONFIG_PROFILE -ErrorAction SilentlyContinue

# ------------------------------------------------------------------ 5. tables
Step "Creating the tables in dev, qa and prod"
python (Join-Path $repo "scripts\apply_migrations.py") --host $hostUrl --warehouse-id $warehouse `
    --catalog scm_platform_dev --catalog scm_platform_qa --catalog scm_platform_prod
Check "migrations"

# ------------------------------------------------------------------ 6. fresh data
if (-not $SkipData) {
    Step "Generating fresh data"
    Push-Location $repo
    try {
        $now = (Get-Date).ToUniversalTime()
        $ref = $now.ToString("yyyy-MM-ddTHH:mm:ssZ")      # one 'today' for orders and shipments
        if (-not (Test-Path "data\warehouses\warehouses.jsonl")) { python -m data_generators.generate_master_data; Check "master data" }
        python -m data_generators.generate_orders --reference-time $ref; Check "orders"
        python -m data_generators.generate_shipments --reference-time $ref; Check "shipments"
        python -m data_generators.generate_inventory --last-night $now.ToString("yyyy-MM-dd"); Check "inventory"
    } finally { Pop-Location }

    Step "Uploading to the landing container"
    foreach ($feed in "routes", "weather", "shipments", "warehouses", "products", "customers", "carriers", "orders", "inventory") {
        for ($try = 1; ; $try++) {
            az storage blob upload-batch --account-name $storage --destination landing --destination-path $feed `
                --source (Join-Path $repo "data\$feed") --pattern "*.jsonl" --auth-mode login --overwrite -o none 2>$null
            if ($LASTEXITCODE -eq 0) { Write-Host "    uploaded $feed"; break }
            # A new role assignment can take a few minutes to become active
            if ($try -ge 10) { throw "Upload of $feed still failing after 10 tries." }
            Write-Host "    $feed not allowed yet (storage permission activating) - retry $try/10 in 30 s"
            Start-Sleep -Seconds 30
        }
    }
}

# ------------------------------------------------------------------ 7. jobs
Step "Deploying the bundle to $Target"
Push-Location $repo
try {
    databricks bundle deploy -t $Target "--var=warehouse_id=$warehouse" "--var=storage_account=$storage" `
        "--var=kafka_bootstrap=$kafka" "--var=alert_email=$($account.user.name)" "--var=quarantine_alert_id=$alertId"
    Check "bundle deploy"
} finally { Pop-Location }

Step "Ready"
Write-Host "    Workspace : $hostUrl"
Write-Host "    Run       : databricks bundle run -t $Target scm_daily_pipeline   (in this window)"
Write-Host "    In another window, first:  `$env:DATABRICKS_HOST='$hostUrl'; `$env:DATABRICKS_AUTH_TYPE='azure-cli'"
Write-Host "    Finish    : .\scripts\down.ps1   (billing continues until then)" -ForegroundColor Yellow
