<#
.SYNOPSIS
Local end-to-end run of AzADServicePrincipalInsights for development: run it before and after a change, then compare the two
runs with tests/Compare-AzADSPIRuns.ps1.

.DESCRIPTION
Sign-in, one of:
- default: the current Azure context - run Connect-AzAccount first (as a user or as a service principal)
- -Credential and -TenantId: signs in as this service principal (user name: the application (client) ID, password: a client secret)
- -KeyringUser: reads the service principal from the Python keyring (`keyring get <service> <user>` with the services appid,
  apppw and apptenantid) - the values are never printed
The output goes to -OutputPath: by default zzzzz_Output in the repository root, which is gitignored - it contains data of your
tenant, never commit it. Use a separate output folder per run mode, so Compare-AzADSPIRuns.ps1 compares runs of the same mode.

.EXAMPLE
Connect-AzAccount -TenantId <tenant ID>
./tests/Invoke-LocalTestRun.ps1
./tests/Compare-AzADSPIRuns.ps1

.EXAMPLE
./tests/Invoke-LocalTestRun.ps1 -KeyringUser azadspi -PerfTracking *> run.log
#>
[CmdletBinding()]
param(
    #sign-in as a service principal (otherwise: the current Azure context)
    [pscredential]$Credential,
    [string]$TenantId,
    [string]$KeyringUser,
    #default: the Tenant Root Group (its ID is the tenant ID)
    [string[]]$ManagementGroupId,
    #default: the script's default (works with side-by-side installed versions)
    [string]$AzAPICallVersion,
    [switch]$PerfTracking,
    #other run modes of the main script
    [switch]$NoAzureResourceSideRelations,
    [switch]$OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes,
    [string]$OutputPath = (Join-Path -Path (Split-Path -Parent $PSScriptRoot) -ChildPath 'zzzzz_Output'),
    #repository to run the script from (e.g. a 'git worktree' of an older commit, to compare two versions)
    [string]$RepoRoot = (Split-Path -Parent $PSScriptRoot)
)
$ErrorActionPreference = 'Stop'

#region sign-in
if ($KeyringUser) {
    if ($Credential) { throw 'Use either -Credential or -KeyringUser' }
    if (-not (Get-Command -Name keyring -CommandType Application -ErrorAction SilentlyContinue)) { throw "-KeyringUser needs the Python keyring CLI ('keyring')" }
    $appId = "$(keyring get appid $KeyringUser)".Trim()
    $appPw = "$(keyring get apppw $KeyringUser)".Trim()
    $TenantId = "$(keyring get apptenantid $KeyringUser)".Trim()
    if (-not $appId -or -not $appPw -or -not $TenantId) { throw "The keyring has no appid / apppw / apptenantid for the user '$KeyringUser'" }
    $Credential = [pscredential]::new($appId, (ConvertTo-SecureString -String $appPw -AsPlainText -Force))
    Remove-Variable -Name appPw
}
if ($Credential) {
    if (-not $TenantId) { throw '-Credential needs -TenantId' }
    #-Scope Process: the default context of later sessions stays as it was (context autosave)
    $null = Connect-AzAccount -ServicePrincipal -TenantId $TenantId -Credential $Credential -Scope Process -WarningAction SilentlyContinue
}
$context = Get-AzContext
if (-not $context) { throw 'No Azure context - run Connect-AzAccount first, or pass -Credential and -TenantId, or -KeyringUser' }
Write-Host "Signed in (account type: $($context.Account.Type))"
#endregion sign-in

$params = @{
    ManagementGroupId = $(if ($ManagementGroupId) { $ManagementGroupId } else { $context.Tenant.Id })
    OutputPath        = $OutputPath
    DoTranscript      = $true
}
if (-not (Test-Path -LiteralPath $OutputPath)) { $null = New-Item -ItemType Directory -Path $OutputPath }
if ($AzAPICallVersion) { $params.azAPICallVersion = $AzAPICallVersion }
if ($PerfTracking) { $params.PerfTracking = $true }
if ($NoAzureResourceSideRelations) { $params.NoAzureResourceSideRelations = $true }
if ($OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes) { $params.OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes = $true }

$start = Get-Date
#the session keeps its location (the next command of the example may run in another folder)
Push-Location -LiteralPath $RepoRoot
try {
    & ./pwsh/AzADServicePrincipalInsights.ps1 @params
}
finally {
    Pop-Location
}
Write-Host "LOCALRUN_EXIT: success, duration $([math]::Round(((Get-Date) - $start).TotalMinutes, 1)) min"
