<#
.SYNOPSIS
Reads the arguments file written by New-AzADSPIArgumentFile.ps1 and returns the parameters as hashtable (for splatting).

.DESCRIPTION
Fails the step (Azure DevOps '##vso[task.logissue]' / GitHub Actions '::error::' plus throw) when the file is missing,
empty, not valid JSON or not a JSON object, or when a -RequiredParameters key is missing - the calling step never runs the
script without its arguments.

.PARAMETER ArgumentsFile
Path of the arguments file (the -OutputFile of New-AzADSPIArgumentFile.ps1).

.PARAMETER RequiredParameters
Parameter names that must be in the file.

.EXAMPLE
$params = & ./pwsh/Read-AzADSPIArgumentFile.ps1 -ArgumentsFile "$($env:AGENT_TEMPDIRECTORY)/AzADSPIArguments.json"
. ./pwsh/AzADServicePrincipalInsights.ps1 @params
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]
    $ArgumentsFile,

    [string[]]
    $RequiredParameters
)

function stopWithError {
    param(
        [string]
        $Message
    )
    if ($env:TF_BUILD -eq 'True') { Write-Host "##vso[task.logissue type=error]$Message" }
    elseif ($env:GITHUB_ACTIONS -eq 'true') { Write-Host "::error::$Message" }
    else { Write-Host "ERROR: $Message" }
    #throw, not exit: exit only ends this script, the calling step would continue and splat $null
    throw $Message
}

if (-not (Test-Path -LiteralPath $ArgumentsFile -PathType Leaf)) {
    stopWithError "Arguments file '$($ArgumentsFile)' not found - the step 'Create arguments' must run before this step (same job)"
}
$json = Get-Content -LiteralPath $ArgumentsFile -Raw
if ([string]::IsNullOrWhiteSpace($json)) {
    stopWithError "Arguments file '$($ArgumentsFile)' is empty"
}
try {
    #-NoEnumerate: without it a JSON array with one object would be unwrapped to that object and pass the check below
    $params = $json | ConvertFrom-Json -AsHashtable -NoEnumerate -ErrorAction Stop
}
catch {
    stopWithError "Arguments file '$($ArgumentsFile)' is not valid JSON: $($_.Exception.Message)"
}
if ($params -isnot [System.Collections.IDictionary]) {
    stopWithError "Arguments file '$($ArgumentsFile)' must contain a JSON object (parameter name: value)"
}
foreach ($requiredParameter in $RequiredParameters) {
    if ($null -eq $params[$requiredParameter] -or [string]::IsNullOrWhiteSpace([string]$params[$requiredParameter])) {
        stopWithError "Arguments file '$($ArgumentsFile)' has no $($requiredParameter)"
    }
}
return $params
