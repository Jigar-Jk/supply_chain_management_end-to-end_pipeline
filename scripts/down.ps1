<#
.SYNOPSIS
  End a working session: destroy the whole Azure stack, so billing stops.

.DESCRIPTION
  1. Checks you are on the Azure for Students subscription.
  2. terraform destroy (asks you to type "yes").
  3. Clears the local bundle cache (.databricks/bundle), which remembers the deleted workspace's jobs.
  4. Waits (up to 30 min) until Azure has also removed the Databricks managed resource group, so the
     next scripts/up.ps1 will not clash with it.

.EXAMPLE
  .\scripts\down.ps1
  .\scripts\down.ps1 -NoWait     # don't wait for the managed resource group cleanup
#>
param([switch]$NoWait)

# Not "Stop": in Windows PowerShell 5.1 that turns any stderr text of az/terraform into a crash.
# Every external command is checked by its exit code instead (Check / $LASTEXITCODE), and throw always stops.
$ErrorActionPreference = "Continue"
$repo = Split-Path -Parent $PSScriptRoot
$infra = Join-Path $repo "infra"
$tfvars = Join-Path $infra "envs\student.tfvars"

function Step($text) { Write-Host ""; Write-Host "==> $text" -ForegroundColor Cyan }

Step "Checking the Azure login"
$account = az account show -o json 2>$null | ConvertFrom-Json
if (-not $account) { throw "Not logged in. Run: az login --tenant 75b3330d-6fde-4894-8d4c-b5541adbddad" }
if ($account.name -ne "Azure for Students") { throw "Active subscription is '$($account.name)', not 'Azure for Students'." }
$env:ARM_SUBSCRIPTION_ID = $account.id

Step "terraform destroy (type 'yes' to delete everything)"
terraform "-chdir=$infra" destroy "-var-file=$tfvars"
if ($LASTEXITCODE -ne 0) { throw "terraform destroy failed (exit code $LASTEXITCODE). Run it again; it continues where it stopped." }

$bundleCache = Join-Path $repo ".databricks\bundle"
if (Test-Path $bundleCache) { Remove-Item -Recurse -Force $bundleCache; Write-Host "    cleared the local bundle cache" }

$suffix = ([regex]::Match((Get-Content $tfvars -Raw), 'name_suffix\s*=\s*"([a-z0-9]+)"')).Groups[1].Value
$managedRg = "rg-dbw-scm-platform-$suffix-managed"
if (-not $NoWait) {
    Step "Waiting for Azure to remove $managedRg (usually 20-30 min; Ctrl+C is safe)"
    for ($i = 0; $i -lt 60; $i++) {
        if ((az group exists -n $managedRg) -eq "false") { Write-Host "    gone - the next up.ps1 can run"; break }
        Start-Sleep -Seconds 30
    }
    if ((az group exists -n $managedRg) -eq "true") { Write-Host "    still there after 30 min - check the portal before the next up.ps1" -ForegroundColor Yellow }
}

Step "Done - nothing of the project is billing"
az group list --query "[].name" -o tsv | ForEach-Object { Write-Host "    remaining resource group: $_" }
