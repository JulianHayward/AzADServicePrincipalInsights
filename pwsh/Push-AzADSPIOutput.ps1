<#
.SYNOPSIS
Commits the output of an AzADServicePrincipalInsights pipeline/workflow run and pushes it to the branch of the run.

.DESCRIPTION
The push step of the GitHub workflows and the Azure DevOps pipeline (one implementation for both):
- the git identity is set for this repository only (review F-64: --global changed it for later jobs of a self-hosted runner/agent)
- only the output folder is committed (-OutputPath; without it: every change of the checkout)
- nothing changed: no commit, success ('git commit' would fail)
- another run pushed in the meantime: fetch and rebase, then push
- the fetch fails: git's reason is shown; the rebase fails (two runs changed the same output files at the same time): the rebase is
  aborted, the step fails with a clear message - runs must not overlap (README 'Azure DevOps and GitHub Actions')
- Azure DevOps: on success the variable AzADSPIOutputPushed = true - the conditions of the ingestion and publishing steps
The build values (actor, workflow, pipeline name, branch) and the token are read from the environment, never from a script text.
Azure DevOps: the token (SYSTEM_ACCESSTOKEN, the 'env:' of the step) is passed to git as an HTTP header for the pull and the push
only - it is not written to the git config and not printed.
GitHub: git uses the credentials that actions/checkout persisted.

.PARAMETER Platform
GitHubActions or AzureDevOps. Default: detected from the environment (GITHUB_ACTIONS / TF_BUILD).

.PARAMETER RepositoryPath
The checkout of the repository. Default: the current directory.

.PARAMETER Branch
The branch to push to (refs/heads/<branch>). Default: GITHUB_REF (GitHub) or BUILD_SOURCEBRANCH (Azure DevOps).

.PARAMETER OutputPath
The output folder of the run (relative to -RepositoryPath, as in the arguments file): only its changes are committed.

.EXAMPLE
& ./pwsh/Push-AzADSPIOutput.ps1 -RepositoryPath $env:GITHUB_WORKSPACE
#>
[CmdletBinding()]
param(
    [ValidateSet('GitHubActions', 'AzureDevOps')]
    [string]
    $Platform,

    [string]
    $RepositoryPath = (Get-Location).Path,

    [string]
    $Branch,

    [string]
    $OutputPath
)

#a failing git command is handled by its exit code, not as a PowerShell error
$PSNativeCommandUseErrorActionPreference = $false

if (-not $Platform) {
    if ($env:GITHUB_ACTIONS -eq 'true') { $Platform = 'GitHubActions' }
    elseif ($env:TF_BUILD -eq 'True') { $Platform = 'AzureDevOps' }
}
$runKind = if ($Platform -eq 'AzureDevOps') { 'pipeline (schedule, manual runs; optionally an exclusive lock)' } else { 'workflow (concurrency group)' }

function stopWithError([string]$Message) {
    switch ($Platform) {
        'GitHubActions' { Write-Host "::error::$Message" }
        'AzureDevOps' { Write-Host "##vso[task.logissue type=error]$Message" }
        default { Write-Host "ERROR: $Message" -ForegroundColor Red }
    }
    exit 1
}

#Azure DevOps: the condition of the later steps (GitHub: the outcome of this step)
function setPushed {
    if ($Platform -eq 'AzureDevOps') { Write-Host '##vso[task.setvariable variable=AzADSPIOutputPushed]true' }
}

if (-not $Platform) {
    stopWithError 'Push-AzADSPIOutput.ps1 runs in a GitHub Actions workflow or an Azure DevOps pipeline (GITHUB_ACTIONS / TF_BUILD not set) - or use -Platform'
}
if (-not $Branch) {
    $Branch = if ($Platform -eq 'AzureDevOps') { $env:BUILD_SOURCEBRANCH } else { $env:GITHUB_REF }
}
if ($Branch -notlike 'refs/heads/*') {
    stopWithError "The run is not on a branch ('$($Branch)') - the output can only be pushed to a branch"
}

#Azure DevOps: the token as an HTTP header of the pull and the push (git -c: for this command only)
$authArgs = @()
if ($Platform -eq 'AzureDevOps') {
    if (-not $env:SYSTEM_ACCESSTOKEN) {
        stopWithError "SYSTEM_ACCESSTOKEN is empty - the push step needs 'env: SYSTEM_ACCESSTOKEN: `$(System.AccessToken)'"
    }
    $authArgs = @('-c', "http.extraheader=AUTHORIZATION: bearer $($env:SYSTEM_ACCESSTOKEN)")
}

Write-Host "Push the output to '$($Branch)'"

if ($Platform -eq 'AzureDevOps') {
    $userEmail = 'AzADServicePrincipalInsightsPipeline@azdo.com'
    $userName = "Pipeline: '$($env:BUILD_DEFINITIONNAME)' 'rev $($env:BUILD_BUILDNUMBER)' (Project: $([uri]::EscapeDataString("$($env:SYSTEM_TEAMPROJECT)")); Repository: $($env:BUILD_REPOSITORY_NAME); Branch: $($env:BUILD_SOURCEBRANCHNAME) Commit: $($env:BUILD_SOURCEVERSION))"
    $commitMessage = "wiki $(Get-Date -Format 'dd-MMM-yyyy HH:mm:ss') ($((Get-TimeZone).Id))"
}
else {
    $userEmail = 'AzADServicePrincipalInsightsGHActions@ghActions.com'
    $userName = "$($env:GITHUB_ACTOR)"
    $commitMessage = "$($env:GITHUB_WORKFLOW) $($env:GITHUB_JOB)"
}
if (-not $userName.Trim()) { $userName = 'AzADServicePrincipalInsights' }
if (-not $commitMessage.Trim()) { $commitMessage = 'AzADServicePrincipalInsights output' }

#review F-64: the identity of this repository only
git -C $RepositoryPath config user.email $userEmail
git -C $RepositoryPath config user.name $userName
#review SR-61: only the output folder - not a file another step left in the checkout
$addPaths = if ($OutputPath) { @('--', $OutputPath) } else { @() }
git -C $RepositoryPath add --all @addPaths
if ($LASTEXITCODE -ne 0) { stopWithError "git add $($addPaths -join ' ') failed" }

git -C $RepositoryPath diff --cached --quiet
if ($LASTEXITCODE -eq 0) {
    Write-Host 'No changes in the output - nothing to commit'
    setPushed
    exit 0
}

git -C $RepositoryPath commit --quiet -m $commitMessage
if ($LASTEXITCODE -ne 0) { stopWithError 'git commit failed' }

#another run may have pushed in the meantime; without this the push is rejected. Fetch and rebase apart: a failed fetch (network,
#token, deleted branch) is no overlap
git -C $RepositoryPath @authArgs fetch origin $Branch
if ($LASTEXITCODE -ne 0) {
    stopWithError "git fetch of '$($Branch)' failed (see the git output above) - the output was not pushed"
}
git -C $RepositoryPath rebase FETCH_HEAD
if ($LASTEXITCODE -ne 0) {
    #only when a rebase is in progress (an abort without one writes to stderr - an error on PowerShell 7.0/7.1)
    $gitDir = git -C $RepositoryPath rev-parse --absolute-git-dir
    if ((Test-Path -LiteralPath (Join-Path $gitDir 'rebase-merge')) -or (Test-Path -LiteralPath (Join-Path $gitDir 'rebase-apply'))) {
        git -C $RepositoryPath rebase --abort
    }
    stopWithError "git rebase on '$($Branch)' failed: another run changed the same output files at the same time. Runs of this $($runKind) must not overlap; run it again. https://github.com/JulianHayward/AzADServicePrincipalInsights/blob/main/README.md#azure-devops-and-github-actions"
}

git -C $RepositoryPath @authArgs push origin "HEAD:$($Branch)"
if ($LASTEXITCODE -ne 0) {
    $hint = if ($Platform -eq 'AzureDevOps') {
        "the build service needs 'Contribute' on the repository (setup/azure-devops.md, step 4), or a branch policy of '$($Branch)' blocks the push (TF402455 above): give the build service 'Bypass policies when pushing' on the branch, or run the pipeline on a branch without policy"
    }
    else {
        "a branch protection rule or ruleset of '$($Branch)' blocks the push (GH006 / GH013 above; Settings > Rules / Branches): allow the push for this workflow, or run the workflow on a branch without protection. The workflow sets 'permissions: contents: write' itself - the repository default for workflow permissions does not matter"
    }
    stopWithError "git push to '$($Branch)' failed - $($hint)"
}
Write-Host 'Output pushed'
setPushed
exit 0
