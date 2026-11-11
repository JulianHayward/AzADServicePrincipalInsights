<#
.SYNOPSIS
Returns the output paths of an AzADServicePrincipalInsights pipeline/workflow run (HTML file, JSON folder), from the
arguments file of the run.

.DESCRIPTION
The script names its output after the management groups it ran for (AzADServicePrincipalInsights.ps1, '$fileNameMGRef'):
- no ManagementGroupId: the Tenant Root Group = the tenant ID
- several management groups that contain the tenant ID: the tenant ID
- otherwise: the management group IDs joined by '_'
In pipelines/workflows the names carry no version and no timestamp: <OutputPath>/AzADServicePrincipalInsights_<ref>.html
and <OutputPath>/JSON_SP_<ref>.

.PARAMETER ArgumentsFile
Path of the arguments file of AzADServicePrincipalInsights.ps1 (the -OutputFile of New-AzADSPIArgumentFile.ps1).

.PARAMETER TenantId
The tenant ID of the run. Default: the tenant of the current Azure context (Get-AzContext).

.PARAMETER BasePath
Directory the OutputPath is relative to. Default: the current directory.

.EXAMPLE
$output = & ./pwsh/Get-AzADSPIOutputPath.ps1 -ArgumentsFile "$($env:RUNNER_TEMP)/AzADSPIArguments.json"
$output.JsonPath
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]
    $ArgumentsFile,

    [string]
    $TenantId,

    [string]
    $BasePath = (Get-Location).Path
)

$params = & (Join-Path -Path $PSScriptRoot -ChildPath 'Read-AzADSPIArgumentFile.ps1') -ArgumentsFile $ArgumentsFile
#as AzADServicePrincipalInsights.ps1: no OutputPath = the current directory
$outputPath = if ($params['OutputPath']) { $params['OutputPath'] } else { '.' }
$managementGroupIds = @($params['ManagementGroupId'] | Where-Object { $_ })

$needsTenantId = $managementGroupIds.Count -eq 0 -or $managementGroupIds.Count -gt 1
if ($needsTenantId -and -not $TenantId) {
    $TenantId = (Get-AzContext -ErrorAction Stop).Tenant.Id
    if (-not $TenantId) {
        throw 'The tenant ID of the run is not known (no Azure context) - pass -TenantId'
    }
}

#same rules as AzADServicePrincipalInsights.ps1
if ($managementGroupIds.Count -eq 0) {
    $reference = $TenantId
}
elseif ($managementGroupIds.Count -gt 1 -and $managementGroupIds -contains $TenantId) {
    $reference = $TenantId
}
else {
    $reference = $managementGroupIds -join '_'
}

$outputDirectory = if ([System.IO.Path]::IsPathRooted($outputPath)) { $outputPath } else { Join-Path -Path $BasePath -ChildPath $outputPath }
[PSCustomObject]@{
    OutputPath = $outputDirectory
    Reference  = $reference
    HtmlPath   = Join-Path -Path $outputDirectory -ChildPath "AzADServicePrincipalInsights_$($reference).html"
    JsonPath   = Join-Path -Path $outputDirectory -ChildPath "JSON_SP_$($reference)"
}
