<#
.SYNOPSIS
Publishes the AzADServicePrincipalInsights HTML report to an Azure Web App (Azure DevOps pipelines and GitHub Actions workflows).

.DESCRIPTION
The report contains the service principal insights of the tenant, so it is only published to a Web App that requires a login:
- App Service Authentication (Easy Auth) v1 (POST .../config/authsettings/list): enabled and unauthenticatedClientAction
  not 'AllowAnonymous'
- otherwise v2 (GET .../config/authsettingsV2/list - apps configured with v2 report 'enabled: false' in the v1 settings):
  platform.enabled and globalValidation.requireAuthentication
'enabled' alone is not enough: with 'AllowAnonymous' / requireAuthentication false the report would be public.
The HTML file is taken from the arguments file of the run (Get-AzADSPIOutputPath.ps1: OutputPath, management groups).
Any reason not to publish fails the step (Azure DevOps '##vso[task.logissue]' / GitHub Actions '::error::' plus throw).
Requires an Azure context (AzurePowerShell task / azure/login) and the AzAPICall module (installed by the run step).

.PARAMETER ArgumentsFile
Path of the arguments file of AzADServicePrincipalInsights.ps1 (the -OutputFile of New-AzADSPIArgumentFile.ps1).

.PARAMETER WebAppSubscriptionId
Subscription ID of the Web App.

.PARAMETER WebAppResourceGroup
Resource group of the Web App.

.PARAMETER WebAppName
Name of the Web App.

.PARAMETER BasePath
Directory the OutputPath is relative to. Default: the current directory.

.PARAMETER ZipDirectory
Directory for the zip file that is deployed. Default: the temp directory (not the output folder - it is pushed to the repository).

.EXAMPLE
./pwsh/Publish-AzADSPIWebApp.ps1 -ArgumentsFile "$($env:RUNNER_TEMP)/AzADSPIArguments.json" -WebAppSubscriptionId $env:WebAppSubscriptionId -WebAppResourceGroup $env:WebAppResourceGroup -WebAppName $env:WebAppName
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]
    $ArgumentsFile,

    [string]
    $WebAppSubscriptionId,

    [string]
    $WebAppResourceGroup,

    [string]
    $WebAppName,

    [string]
    $BasePath = (Get-Location).Path,

    [string]
    $ZipDirectory = [System.IO.Path]::GetTempPath()
)

function stopWithError {
    param(
        [string]
        $Message
    )
    if ($env:TF_BUILD -eq 'True') { Write-Host "##vso[task.logissue type=error]$Message" }
    elseif ($env:GITHUB_ACTIONS -eq 'true') { Write-Host "::error::$Message" }
    else { Write-Host "ERROR: $Message" }
    throw $Message
}

#region input
foreach ($setting in @(
        @{ Name = 'WebAppSubscriptionId'; Value = $WebAppSubscriptionId }
        @{ Name = 'WebAppResourceGroup'; Value = $WebAppResourceGroup }
        @{ Name = 'WebAppName'; Value = $WebAppName }
    )) {
    if ([string]::IsNullOrWhiteSpace($setting.Value) -or $setting.Value -match '^<|\s') {
        stopWithError "$($setting.Name) '$($setting.Value)' - provide the Web App details (not the template placeholder)"
    }
}

try {
    $output = & (Join-Path -Path $PSScriptRoot -ChildPath 'Get-AzADSPIOutputPath.ps1') -ArgumentsFile $ArgumentsFile -BasePath $BasePath
}
catch {
    stopWithError "The report file cannot be determined: $($_.Exception.Message)"
}
if (-not (Test-Path -LiteralPath $output.HtmlPath -PathType Leaf)) {
    stopWithError "'$($output.HtmlPath)' not found - check the ManagementGroupId (casing, Linux!=linuX) and the OutputPath"
}
$zipPath = Join-Path -Path $ZipDirectory -ChildPath "AzADServicePrincipalInsights_$($output.Reference).zip"
#endregion input

#region authentication of the Web App
$azAPICallConf = initAzAPICall -DebugAzAPICall $true
$uriConfig = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/subscriptions/$($WebAppSubscriptionId)/resourceGroups/$($WebAppResourceGroup)/providers/Microsoft.Web/sites/$($WebAppName)/config"

$authEnabled = $false
$requireAuthentication = $false
$authVersion = $null
try {
    $currentTask = "AzAPICall - Check if WebApp ($($WebAppName)) has Authentication (v1) enabled"
    $request = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri "$($uriConfig)/authsettings/list?api-version=2026-08-01" -method 'POST' -currentTask $currentTask -listenOn 'Content'
    if ($request.properties.enabled -eq $true) {
        $authEnabled = $true
        $authVersion = 'v1'
        #review F-58: only the action that requires a login counts (an unset or unknown action was taken as 'login required')
        $requireAuthentication = $request.properties.unauthenticatedClientAction -eq 'RedirectToLoginPage'
    }
    else {
        #apps configured with Authentication v2 report 'enabled: false' in the v1 settings
        $currentTask = "AzAPICall - Check if WebApp ($($WebAppName)) has Authentication (v2) enabled"
        $requestV2 = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri "$($uriConfig)/authsettingsV2/list?api-version=2026-08-01" -method 'GET' -currentTask $currentTask -listenOn 'Content'
        if ($requestV2.properties.platform.enabled -eq $true) {
            $authEnabled = $true
            $authVersion = 'v2'
            $requireAuthentication = $requestV2.properties.globalValidation.requireAuthentication -eq $true
            #review F-58: paths excluded from the authentication - the report is published at the root of the app (<name>.html)
            $excludedPaths = @($requestV2.properties.globalValidation.excludedPaths).where({ $_ })
            if ($excludedPaths.Count -gt 0) {
                $excludedPathsCoveringReport = @($excludedPaths.where({ $_ -in '/', '/*', '/**' -or $_ -like '*.htm*' -or $_ -like '/AzADServicePrincipalInsights*' }))
                if ($excludedPathsCoveringReport.Count -gt 0) {
                    stopWithError "HTML NOT published - the WebApp ($($WebAppName)) excludes paths from the authentication that cover the report ($($excludedPathsCoveringReport -join ', ')); the report would be publicly accessible"
                }
                Write-Host "WebApp ($($WebAppName)) excludes these paths from the authentication (not the report): $($excludedPaths -join ', ')" -ForegroundColor Yellow
            }
        }
    }
}
catch {
    stopWithError "The authentication settings of the WebApp ($($WebAppName)) could not be read ($($_.Exception.Message)) - HTML NOT published"
}

Write-Host "WebApp ($($WebAppName)) has Authentication enabled: $($authEnabled)$(if ($authVersion) { " ($($authVersion))" }); login required: $($requireAuthentication)"
if (-not $authEnabled) {
    stopWithError "HTML NOT published - the WebApp ($($WebAppName)) has no Authentication configured. Assuming and insisting that you do not want to publish your tenant insights to the public - configure Authentication on the WebApp"
}
if (-not $requireAuthentication) {
    $authSetting = if ($authVersion -eq 'v1') { "unauthenticatedClientAction '$($request.properties.unauthenticatedClientAction)', not 'RedirectToLoginPage'" } else { 'globalValidation.requireAuthentication false' }
    stopWithError "HTML NOT published - the WebApp ($($WebAppName)) has Authentication enabled but allows unauthenticated requests ($($authSetting)), the report would be publicly accessible. Configure the WebApp to require authentication"
}
#endregion authentication of the Web App

#region publish
try {
    Compress-Archive -LiteralPath $output.HtmlPath -DestinationPath $zipPath -Force -ErrorAction Stop
    $null = Select-AzSubscription -SubscriptionId $WebAppSubscriptionId -ErrorAction Stop
    if (-not (Publish-AzWebApp -ResourceGroupName $WebAppResourceGroup -Name $WebAppName -ArchivePath $zipPath -Force -ErrorAction Stop)) {
        throw 'Publish-AzWebApp returned no result'
    }
}
catch {
    stopWithError "HTML NOT published ($($_.Exception.Message)) - RBAC Role 'Website Contributor' on the WebApp ($($WebAppName)) is required"
}
Write-Host "Published '$($output.HtmlPath)'"
#endregion publish
