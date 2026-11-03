<#
.SYNOPSIS
  Start a working session on Azure: build the stack, create the tables, load fresh data, deploy the jobs.

.DESCRIPTION
  1. Safety checks: logged in to the Azure for Students subscription; the previous session's
     Databricks managed resource group is gone (otherwise the workspace name clashes).
  2. terraform apply (asks you to type "yes"; nothing is created without it).
  3. Reads the outputs (workspace URL, warehouse id, storage account) - nothing is copied by hand,
     and stores them in GitHub for the CD workflow (gh CLI; the pipeline's OAuth secret goes from
     terraform straight to GitHub and is never shown).
  4. Databricks CLI login reuses your az login (no browser).
  5. Creates the tables in the dev, qa and prod catalogs (scripts/apply_migrations.py).
  6. Generates fresh data (--reference-time now) and uploads it to the landing container; the WMS
     stock CSV instead goes the way a warehouse system sends it: to the SFTP drop, from where a Data
     Factory run (started here, not waiting for its 01:30 schedule) copies it into landing.
  7. Deploys the bundle to the target (default dev).

  Billing starts at step 2. End every session with scripts/down.ps1.

.EXAMPLE
  .\scripts\up.ps1
  .\scripts\up.ps1 -SkipData      # keep the landing data already uploaded in this stack
  .\scripts\up.ps1 -SkipGitHub    # this session will not use CD
#>
param(
    [string]$Target = "dev",
    [switch]$SkipData,
    [switch]$SkipGitHub
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
# The JSON holds the CD secret too: drop it at once; step 3b reads it straight from terraform.
if (-not $out.pipeline_client_secret) { throw "terraform output has no pipeline_client_secret." }
$out.PSObject.Properties.Remove("pipeline_client_secret")
$hostUrl = $out.databricks_workspace_url.value
$warehouse = $out.sql_warehouse_id.value
$storage = $out.storage_account_name.value
$kafka = "$($out.eventhub_namespace.value).servicebus.windows.net:9093"
$alertId = $out.quarantine_alert_ids.value.$Target           # the 5 % quarantine alert of this environment
if (-not $alertId) { throw "No quarantine alert for target '$Target' in the terraform outputs." }
# Where the daily pipeline pulls shipments from the carrier API; "disabled": from the uploaded file
$carrierApi = if ($out.carrier_api_url) { $out.carrier_api_url.value } else { "disabled" }
# The API runs in Azure Container Apps, but its image (with today's shipments) is built here (step 6b):
# no Docker, or no data to put in it -> the API stays off for this session and the file is uploaded
$carrierDeploy = if ($out.carrier_api_deploy) { $out.carrier_api_deploy.value } else { $null }
if ($carrierApi -ne "disabled") {
    docker info *> $null
    if ($LASTEXITCODE -ne 0) {
        Write-Warning "Docker is not running - carrier API off this session (shipments come from the file). Start Docker Desktop and re-run to switch it on."
        $carrierApi = "disabled"
    } elseif ($SkipData -and -not (Test-Path (Join-Path $repo "data\shipments\shipments.jsonl"))) {
        Write-Warning "No data\shipments\shipments.jsonl for the carrier API image - carrier API off this session."
        $carrierApi = "disabled"
    }
}
$rg = $out.resource_group.value
# The WMS SFTP drop and the Data Factory that copies from it (infra/wms_sftp.tf, infra/adf.tf)
$wmsSftp = if ($out.wms_sftp) { $out.wms_sftp.value } else { $null }
$adf = if ($out.data_factory_name) { $out.data_factory_name.value } else { $null }
Write-Host "    workspace $hostUrl | warehouse $warehouse | storage $storage | alert $alertId"

# ------------------------------------------------------------------ 3b. values for CD (GitHub)
# The names are the ones .github/workflows/cd.yml reads (vars.X / secrets.X).
# Public repo: Actions logs are public and only secrets are masked there, so the workspace URL and
# your e-mail are secrets; plain ids stay readable variables. Not logged in to gh: the session still
# works, only CD will not.
Step "Storing this session's values in GitHub for CD"
$ghReady = $false
if ($SkipGitHub) { Write-Host "    skipped (-SkipGitHub)" }
elseif (-not (Get-Command gh -ErrorAction SilentlyContinue)) { Write-Warning "GitHub CLI not installed - CD values not set. The session works; CD will not." }
else {
    gh auth status *> $null
    if ($LASTEXITCODE -eq 0) { $ghReady = $true }
    else { Write-Warning "GitHub CLI not logged in (gh auth login) - CD values not set. The session works; CD will not." }
}
if ($ghReady) {
    Push-Location $repo
    try {
        gh secret set DATABRICKS_HOST --body $hostUrl; Check "gh secret set DATABRICKS_HOST"
        gh secret set ALERT_EMAIL --body $account.user.name; Check "gh secret set ALERT_EMAIL"
        # From terraform straight into gh: never on screen, in a variable or in a file. cmd's pipe, not
        # PowerShell's: Windows PowerShell 5.1 re-encodes piped text and adds a newline to it.
        cmd /c "terraform -chdir=`"$infra`" output -raw pipeline_client_secret | gh secret set DATABRICKS_CLIENT_SECRET"
        Check "gh secret set DATABRICKS_CLIENT_SECRET"
        $cdVars = [ordered]@{
            DATABRICKS_CLIENT_ID = $out.pipeline_client_id.value
            WAREHOUSE_ID         = $warehouse
            STORAGE_ACCOUNT      = $storage
            KAFKA_BOOTSTRAP      = $kafka
            CARRIER_API_URL      = $carrierApi
        }
        foreach ($name in $cdVars.Keys) { gh variable set $name --body $cdVars[$name]; Check "gh variable set $name" }
        foreach ($envName in "dev", "qa", "prod") {
            gh variable set QUARANTINE_ALERT_ID --env $envName --body $out.quarantine_alert_ids.value.$envName
            Check "gh variable set QUARANTINE_ALERT_ID ($envName)"
        }
        Write-Host "    secrets: DATABRICKS_HOST, DATABRICKS_CLIENT_SECRET, ALERT_EMAIL"
        Write-Host "    variables: $($cdVars.Keys -join ', '); QUARANTINE_ALERT_ID in dev, qa, prod"
    } finally { Pop-Location }
}

# ------------------------------------------------------------------ 4. Databricks login = your az login
$env:DATABRICKS_HOST = $hostUrl
$env:DATABRICKS_AUTH_TYPE = "azure-cli"
Remove-Item Env:DATABRICKS_CONFIG_PROFILE -ErrorAction SilentlyContinue
# An empty CLI config for this window: ~/.databrickscfg can hold a default_profile pointing at an
# earlier, already destroyed workspace, which the CLI would use instead of DATABRICKS_HOST.
$cliConfig = Join-Path $env:TEMP "scm-session.databrickscfg"
Set-Content -Path $cliConfig -Value "" -Encoding ascii
$env:DATABRICKS_CONFIG_FILE = $cliConfig

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
}

# ------------------------------------------------------------------ 6b. carrier API image
# Today's shipments go into the image, the image into the registry, and the Container App rolls it
# out; the pipeline then fetches shipments from https://<app>/v1/shipments instead of a file.
if ($carrierApi -ne "disabled") {
    Step "Carrier API: image with today's shipments -> $($carrierDeploy.login_server) -> $($carrierDeploy.app_name)"
    $image = "$($carrierDeploy.login_server)/carrier-api:$((Get-Date).ToUniversalTime().ToString('yyyyMMddHHmmss'))"
    az acr login --name $carrierDeploy.registry_name --only-show-errors; Check "az acr login"
    docker build -q -f (Join-Path $repo "services\carrier_api\Dockerfile") -t $image $repo; Check "docker build"
    docker push -q $image; Check "docker push"
    az extension show --name containerapp -o none 2>$null
    if ($LASTEXITCODE -ne 0) { az extension add --name containerapp --only-show-errors; Check "az extension add containerapp" }
    az containerapp update --name $carrierDeploy.app_name --resource-group $rg --image $image -o none --only-show-errors
    Check "az containerapp update"
    # Wait until the new revision answers with the shipments (the placeholder has no /health)
    for ($i = 1; ; $i++) {
        try { $health = Invoke-RestMethod "$carrierApi/health" -TimeoutSec 10 } catch { $health = $null }
        if ($health.status -eq "ok") { Write-Host "    $carrierApi is up, serving $($health.shipments) shipments"; break }
        if ($i -ge 30) { throw "The carrier API did not come up at $carrierApi/health within 5 minutes." }
        Start-Sleep -Seconds 10
    }
}

if (-not $SkipData) {
    Step "Uploading to the landing container"
    $feeds = "routes", "weather", "shipments", "warehouses", "products", "customers", "carriers", "orders", "inventory"
    # With the carrier API on, the pipeline fetches shipments itself; uploading the file too would land them twice
    if ($carrierApi -ne "disabled") { $feeds = $feeds | Where-Object { $_ -ne "shipments" } }
    # The WMS file reaches landing through the SFTP drop and Data Factory (below), not from here
    if ($wmsSftp) { $feeds = $feeds | Where-Object { $_ -ne "inventory" } }
    foreach ($feed in $feeds) {
        $pattern = if ($feed -eq "inventory") { "*.csv" } else { "*.jsonl" }      # the WMS stock file is CSV
        for ($try = 1; ; $try++) {
            az storage blob upload-batch --account-name $storage --destination landing --destination-path $feed `
                --source (Join-Path $repo "data\$feed") --pattern $pattern --auth-mode login --overwrite -o none 2>$null
            if ($LASTEXITCODE -eq 0) { Write-Host "    uploaded $feed"; break }
            # A new role assignment can take a few minutes to become active
            if ($try -ge 10) { throw "Upload of $feed still failing after 10 tries." }
            Write-Host "    $feed not allowed yet (storage permission activating) - retry $try/10 in 30 s"
            Start-Sleep -Seconds 30
        }
    }

    if ($wmsSftp) {
        Step "WMS drops its stock file on the SFTP drop"
        # The password goes from Key Vault into this process's environment only: never on screen or in a file
        $env:WMS_SFTP_PASSWORD = az keyvault secret show --vault-name $out.key_vault_name.value --name $($wmsSftp.password_secret) --query value -o tsv
        try {
            Check "reading the wms SFTP password from Key Vault"
            python (Join-Path $repo "scripts\wms_sftp_upload.py") --host $wmsSftp.host --username $wmsSftp.username `
                --folder $wmsSftp.folder --source-dir (Join-Path $repo "data\inventory")
            Check "SFTP upload"
        } finally { Remove-Item Env:WMS_SFTP_PASSWORD -ErrorAction SilentlyContinue }

        Step "Data Factory copies it into landing (copy_wms_inventory, started now instead of at 01:30)"
        az extension show --name datafactory -o none 2>$null
        if ($LASTEXITCODE -ne 0) { az extension add --name datafactory --only-show-errors; Check "az extension add datafactory" }
        $runId = az datafactory pipeline create-run --factory-name $adf --resource-group $rg --name copy_wms_inventory --query runId -o tsv
        Check "starting the Data Factory pipeline"
        for ($i = 1; $i -le 60; $i++) {
            $status = az datafactory pipeline-run show --factory-name $adf --resource-group $rg --run-id $runId --query status -o tsv
            if ($status -in "Succeeded", "Failed", "Cancelled") { break }
            Start-Sleep -Seconds 15
        }
        if ($status -ne "Succeeded") { throw "Data Factory run $runId ended '$status'. See the factory's Monitor page in the portal." }
        Write-Host "    copied: landing/inventory/ has the CSV, the drop's inbox/ is archived"
    }
}

# ------------------------------------------------------------------ 7. jobs
Step "Deploying the bundle to $Target"
Push-Location $repo
try {
    databricks bundle deploy -t $Target "--var=warehouse_id=$warehouse" "--var=storage_account=$storage" `
        "--var=kafka_bootstrap=$kafka" "--var=alert_email=$($account.user.name)" "--var=quarantine_alert_id=$alertId" `
        "--var=carrier_api_url=$carrierApi"
    Check "bundle deploy"
} finally { Pop-Location }

Step "Ready"
Write-Host "    Workspace : $hostUrl"
Write-Host "    Run       : databricks bundle run -t $Target scm_daily_pipeline   (in this window)"
Write-Host "    In another window, first:  . .\scripts\session_env.ps1"
Write-Host "    Finish    : .\scripts\down.ps1   (billing continues until then)" -ForegroundColor Yellow
