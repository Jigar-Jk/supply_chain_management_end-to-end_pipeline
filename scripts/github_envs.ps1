<#
.SYNOPSIS
  GitHub Environments for CD, as code: dev, qa and prod, with a manual approval gate on prod.

.DESCRIPTION
  dev and qa  deploy without asking.
  prod        waits until a required reviewer approves the run (the charter's manual approval gate),
              and only the main branch may deploy to it - never a feature branch.

  Safe to run any number of times: every call sets the full configuration (GitHub API PUT), so the
  result is the same whether the environments are new, already set up, or changed by hand in the UI.
  One-time setup; the per-session values (workspace URL, ids, the CD secret) are set by up.ps1.

  Needs the GitHub CLI logged in as a repo admin: gh auth login --scopes workflow

.EXAMPLE
  .\scripts\github_envs.ps1
  .\scripts\github_envs.ps1 -Reviewer someone-else
#>
param(
    [string]$Repo = "",        # owner/name; default: the repo of this folder
    [string]$Reviewer = ""     # GitHub user who approves prod; default: the repo owner
)

$ErrorActionPreference = "Continue"   # see up.ps1: external commands are checked by exit code
function Step($text) { Write-Host ""; Write-Host "==> $text" -ForegroundColor Cyan }
function Check($what) { if ($LASTEXITCODE -ne 0) { throw "$what failed (exit code $LASTEXITCODE)." } }

# Sends a JSON body to the GitHub API. Through a file, not a pipe: Windows PowerShell 5.1 re-encodes
# piped text for native programs, which GitHub rejects ("Problems parsing JSON").
function GhApi($method, $path, $body) {
    if ($null -eq $body) { gh api --method $method $path --silent; Check "gh api $method $path"; return }
    $file = [IO.Path]::GetTempFileName()
    try {
        [IO.File]::WriteAllText($file, ($body | ConvertTo-Json -Depth 5 -Compress))   # UTF-8, no BOM
        gh api --method $method $path --input $file --silent
        Check "gh api $method $path"
    }
    finally { Remove-Item $file -ErrorAction SilentlyContinue }
}

if (-not $Repo) { $Repo = gh repo view --json nameWithOwner --jq .nameWithOwner; Check "gh repo view" }
if (-not $Reviewer) { $Reviewer = $Repo.Split("/")[0] }
$reviewerId = gh api "users/$Reviewer" --jq .id; Check "looking up GitHub user $Reviewer"

# ------------------------------------------------------------------ dev, qa: no rules
foreach ($name in "dev", "qa") {
    Step "$name (deploys without approval)"
    GhApi PUT "repos/$Repo/environments/$name" @{ deployment_branch_policy = $null; reviewers = @() }
}

# ------------------------------------------------------------------ prod: approval + main only
Step "prod (needs approval by $Reviewer; main branch only)"
GhApi PUT "repos/$Repo/environments/prod" @{
    # Not prevent_self_review: with one maintainer, the person who starts the run must be able to approve it
    prevent_self_review      = $false
    reviewers                = @(@{ type = "User"; id = [long]$reviewerId })
    deployment_branch_policy = @{ protected_branches = $false; custom_branch_policies = $true }
}
$policies = gh api "repos/$Repo/environments/prod/deployment-branch-policies" | ConvertFrom-Json; Check "reading prod branch rules"
foreach ($p in $policies.branch_policies | Where-Object { $_.name -ne "main" }) {
    GhApi DELETE "repos/$Repo/environments/prod/deployment-branch-policies/$($p.id)" $null   # only main may deploy
}
if (-not ($policies.branch_policies | Where-Object { $_.name -eq "main" })) {
    GhApi POST "repos/$Repo/environments/prod/deployment-branch-policies" @{ name = "main"; type = "branch" }
}

# ------------------------------------------------------------------ what GitHub now has
Step "Result"
# Formatted here, not with --jq: Windows PowerShell 5.1 breaks quotes inside native-command arguments.
foreach ($e in (gh api "repos/$Repo/environments" | ConvertFrom-Json).environments) {
    $rules = @($e.protection_rules | ForEach-Object { $_.type }) -join ", "
    Write-Host ("    {0,-5} {1}" -f $e.name, $(if ($rules) { $rules } else { "no rules" }))
}
$branches = @((gh api "repos/$Repo/environments/prod/deployment-branch-policies" | ConvertFrom-Json).branch_policies.name) -join ", "
Write-Host "    prod deploys from: $branches"
