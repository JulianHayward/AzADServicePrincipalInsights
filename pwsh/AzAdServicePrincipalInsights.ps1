[CmdletBinding()]
Param
(
    [string]$Product = "AzAdServicePrincipalInsights",
    [string]$ProductVersion = "v1_20211021_2_POC",
    [string]$GithubRepository = "aka.ms/AzAdServicePrincipalInsights",
    [switch]$AzureDevOpsWikiAsCode, #Use this parameter only when running in a Azure DevOps Pipeline!
    [switch]$DebugAzAPICall,
    [switch]$NoCsvExport,
    [string]$CsvDelimiter = ";",
    [switch]$CsvExportUseQuotesAsNeeded,
    [string]$OutputPath,
    [array]$SubscriptionQuotaIdWhitelist = @("undefined"),
    [switch]$DoTranscript,
    [int]$HtmlTableRowsLimit = 40000, #HTML -> becomes unresponsive depending on client device performance. A recommendation will be shown to download the CSV instead of opening the TF table
    [int]$ThrottleLimit = 5, 
    [int]$ThrottleLimitGraph = 5, 
    [string]$SubscriptionId4AzContext = "undefined",
    [string]$FileTimeStampFormat = "yyyyMMdd_HHmmss",
    [switch]$NoJsonExport,
    [int]$AADGroupMembersLimit = 500,
    [switch]$NoAzureRoleAssignments,
    [int]$AADServicePrincipalExpiryWarningDays = 14,
    [switch]$StatsOptOut
)

$Error.clear()
$ErrorActionPreference = "Stop"

$checkContext = Get-AzContext -ErrorAction Stop
Write-Host "Environment: $($checkContext.Environment.Name)"
$ManagementGroupId = ($checkContext).Tenant.Id

#region filedir
if (-not [IO.Path]::IsPathRooted($outputPath)) {
    $outputPath = Join-Path -Path (Get-Location).Path -ChildPath $outputPath
}
$outputPath = Join-Path -Path $outputPath -ChildPath '.'
$outputPath = [IO.Path]::GetFullPath($outputPath)
if (-not (test-path $outputPath)) {
    Write-Host "path $outputPath does not exist - please create it!" -ForegroundColor Red
    Throw "Error - $($Product): check the last console output for details"
}
else {
    Write-Host "Output/Files will be created in path $outputPath"
}
$DirectorySeparatorChar = [IO.Path]::DirectorySeparatorChar
#endregion filedir

#region fileTimestamp
try {
    $fileTimestamp = (get-date -format $FileTimeStampFormat)
}
catch {
    Write-Host "fileTimestamp format: '$($FileTimeStampFormat)' invalid; continue with default format: 'yyyyMMdd_HHmmss'" -ForegroundColor Red
    $FileTimeStampFormat = "yyyyMMdd_HHmmss"
    $fileTimestamp = (get-date -format $FileTimeStampFormat)
}

if ($DoTranscript) {
    $fileNameTranscript = "$($Product)_$($ProductVersion)_$($fileTimestamp)_$($ManagementGroupId)_Log.txt"
    Start-Transcript -Path "$($outputPath)$($DirectorySeparatorChar)$($fileNameTranscript)" -NoClobber
}
#endregion fileTimestamp

#
$startProduct = get-date
$startTime = get-date -format "dd-MMM-yyyy HH:mm:ss"
$startTimeUTC = ((Get-Date).ToUniversalTime()).ToString("dd-MMM-yyyy HH:mm:ss")
Write-Host "Start $($Product) $($startTime) (#$($ProductVersion))"

#region htParameters (all switch params used in foreach-object -parallel)
$htParameters = @{ }
$htParameters.ProductVersion = $ProductVersion
$htParameters.AzCloudEnv = $checkContext.Environment.Name
$htParameters.GithubRepository = $GithubRepository

if ($AzureDevOpsWikiAsCode) {
    $htParameters.AzureDevOpsWikiAsCode = $true
}
else {
    $htParameters.AzureDevOpsWikiAsCode = $false
}

if ($DebugAzAPICall) {
    $htParameters.DebugAzAPICall = $true
}
else {
    $htParameters.DebugAzAPICall = $false
}

if (-not $NoJsonExport) {
    $htParameters.NoJsonExport = $false
}
else {
    $htParameters.NoJsonExport = $true
}

if (-not $NoAzureRoleAssignments) {
    $htParameters.NoAzureRoleAssignments = $false
}
else {
    $htParameters.NoAzureRoleAssignments = $true
}
#endregion htParameters

#region PowerShellEditionAnVersionCheck
Write-Host "Checking powershell edition and version"
$requiredPSVersion = "7.0.3"
$splitRequiredPSVersion = $requiredPSVersion.split('.')
$splitRequiredPSVersionMajor = $splitRequiredPSVersion[0]
$splitRequiredPSVersionMinor = $splitRequiredPSVersion[1]
$splitRequiredPSVersionPatch = $splitRequiredPSVersion[2]

$thisPSVersion = ($PSVersionTable.PSVersion)
$thisPSVersionMajor = ($thisPSVersion).Major
$thisPSVersionMinor = ($thisPSVersion).Minor
$thisPSVersionPatch = ($thisPSVersion).Patch

$psVersionCheckResult = "letsCheck"

if ($PSVersionTable.PSEdition -eq "Core" -and $thisPSVersionMajor -eq $splitRequiredPSVersionMajor) {
    if ($thisPSVersionMinor -gt $splitRequiredPSVersionMinor) {
        $psVersionCheckResult = "passed"
        $psVersionCheck = "(Major[$splitRequiredPSVersionMajor]; Minor[$thisPSVersionMinor] gt $($splitRequiredPSVersionMinor))"
    }
    else {
        if ($thisPSVersionPatch -ge $splitRequiredPSVersionPatch) {
            $psVersionCheckResult = "passed"
            $psVersionCheck = "(Major[$splitRequiredPSVersionMajor]; Minor[$splitRequiredPSVersionMinor]; Patch[$thisPSVersionPatch] gt $($splitRequiredPSVersionPatch))"
        }
        else {
            $psVersionCheckResult = "failed"
            $psVersionCheck = "(Major[$splitRequiredPSVersionMajor]; Minor[$splitRequiredPSVersionMinor]; Patch[$thisPSVersionPatch] lt $($splitRequiredPSVersionPatch))"
        }
    }
}
else {
    $psVersionCheckResult = "failed"
    $psVersionCheck = "(Major[$splitRequiredPSVersionMajor] ne $($splitRequiredPSVersionMajor))"
}

if ($psVersionCheckResult -eq "passed") {
    Write-Host " PS check $psVersionCheckResult : $($psVersionCheck); (minimum supported version '$requiredPSVersion')"
    Write-Host " PS Edition: $($PSVersionTable.PSEdition)"
    Write-Host " PS Version: $($PSVersionTable.PSVersion)"
}
else {
    Write-Host " PS check $psVersionCheckResult : $($psVersionCheck)"
    Write-Host " PS Edition: $($PSVersionTable.PSEdition)"
    Write-Host " PS Version: $($PSVersionTable.PSVersion)"
    Write-Host " This $($Product) version only supports Powershell 'Core' version '$($requiredPSVersion)' or higher"
    Write-Host " Get Powershell: https://github.com/PowerShell/PowerShell#get-powershell"
    Write-Host " Installing PowerShell on Windows: https://docs.microsoft.com/en-us/powershell/scripting/install/installing-powershell-core-on-windows"
    Write-Host " Installing PowerShell on Linux: https://docs.microsoft.com/en-us/powershell/scripting/install/installing-powershell-core-on-linux"
    if ($htParameters.AzureDevOpsWikiAsCode -eq $true) {
        Write-Error "Error"
    }
    else {
        Throw "Error - $($Product): check the last console output for details"
    }
}
#endregion PowerShellEditionAnVersionCheck

if ($htParameters.DebugAzAPICall -eq $false) {
    write-host "AzAPICall debug disabled" -ForegroundColor Cyan
}
else {
    write-host "AzAPICall debug enabled" -ForegroundColor Cyan
}

#region shutuppoluters
$ProgressPreference = 'SilentlyContinue'
Set-Item Env:\SuppressAzurePowerShellBreakingChangeWarnings "true"
#endregion shutuppoluters

#JWTDetails https://www.powershellgallery.com/packages/JWTDetails/1.0.2
#region jwtdetails
function getJWTDetails {
    [cmdletbinding()]
    param(
        [Parameter(Mandatory = $true, ValueFromPipeline = $true, Position = 0)]
        [string]$token
    )

    if (!$token -contains (".") -or !$token.StartsWith("eyJ")) { Write-Error "Invalid token" -ErrorAction Stop }

    #Token
    foreach ($i in 0..1) {
        $data = $token.Split('.')[$i].Replace('-', '+').Replace('_', '/')
        switch ($data.Length % 4) {
            0 { break }
            2 { $data += '==' }
            3 { $data += '=' }
        }
    }

    $decodedToken = [System.Text.Encoding]::UTF8.GetString([convert]::FromBase64String($data)) | ConvertFrom-Json 
    Write-Verbose "JWT Token:"
    Write-Verbose $decodedToken

    #Signature
    foreach ($i in 0..2) {
        $sig = $token.Split('.')[$i].Replace('-', '+').Replace('_', '/')
        switch ($sig.Length % 4) {
            0 { break }
            2 { $sig += '==' }
            3 { $sig += '=' }
        }
    }
    Write-Verbose "JWT Signature:"
    Write-Verbose $sig
    $decodedToken | Add-Member -Type NoteProperty -Name "sig" -Value $sig

    #Convert Expiry time to PowerShell DateTime
    $orig = (Get-Date -Year 1970 -Month 1 -Day 1 -hour 0 -Minute 0 -Second 0 -Millisecond 0)
    $timeZone = Get-TimeZone
    $utcTime = $orig.AddSeconds($decodedToken.exp)
    $offset = $timeZone.GetUtcOffset($(Get-Date)).TotalMinutes #Daylight saving needs to be calculated
    $localTime = $utcTime.AddMinutes($offset)     # Return local time,
    
    $decodedToken | Add-Member -Type NoteProperty -Name "expiryDateTime" -Value $localTime
    
    #Time to Expiry
    $timeToExpiry = ($localTime - (get-date))
    $decodedToken | Add-Member -Type NoteProperty -Name "timeToExpiry" -Value $timeToExpiry

    return $decodedToken
}
$funcGetJWTDetails = $function:getJWTDetails.ToString()
#endregion jwtdetails

#Bearer Token
#region createbearertoken
function createBearerToken($targetEndPoint) {
    Write-Host "+Processing new bearer token request ($targetEndPoint)"
    if ($targetEndPoint -eq "ManagementAPI") {
        $azureRmProfile = [Microsoft.Azure.Commands.Common.Authentication.Abstractions.AzureRmProfileProvider]::Instance.Profile;
        $profileClient = New-Object Microsoft.Azure.Commands.ResourceManager.Common.RMProfileClient($azureRmProfile);
        $catchResult = "letscheck"
        try {
            $newBearerAccessTokenRequest = ($profileClient.AcquireAccessToken($checkContext.Subscription.TenantId))
        }
        catch {
            $catchResult = $_
        }
    }
    if ($targetEndPoint -eq "MSGraphAPI") {
        $contextForMSGraphToken = [Microsoft.Azure.Commands.Common.Authentication.Abstractions.AzureRmProfileProvider]::Instance.Profile.DefaultContext
        $catchResult = "letscheck"
        try {
            $newBearerAccessTokenRequest = [Microsoft.Azure.Commands.Common.Authentication.AzureSession]::Instance.AuthenticationFactory.Authenticate($contextForMSGraphToken.Account, $contextForMSGraphToken.Environment, $contextForMSGraphToken.Tenant.Id.ToString(), $null, [Microsoft.Azure.Commands.Common.Authentication.ShowDialog]::Never, $null, "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)")
        }
        catch {
            $catchResult = $_
        }
    }
    if ($catchResult -ne "letscheck") {
        Write-Host "-ERROR processing new bearer token request ($targetEndPoint): $catchResult" -ForegroundColor Red
        Write-Host "Likely your Azure credentials have not been set up or have expired, please run 'Connect-AzAccount' to set up your Azure credentials."
        Write-Host "It could also well be that there are multiple context in cache, please run 'Clear-AzContext' and then run 'Connect-AzAccount'."
        Throw "Error - $($Product): check the last console output for details"
    }
    $dateTimeTokenCreated = (get-date -format "MM/dd/yyyy HH:mm:ss")
    if ($targetEndPoint -eq "ManagementAPI") {
        $script:htBearerAccessToken.AccessTokenManagement = $newBearerAccessTokenRequest.AccessToken
    }
    if ($targetEndPoint -eq "MSGraphAPI") {
        $script:htBearerAccessToken.AccessTokenMSGraph = $newBearerAccessTokenRequest.AccessToken
    }
    $bearerDetails = GetJWTDetails -token $newBearerAccessTokenRequest.AccessToken
    $bearerAccessTokenExpiryDateTime = $bearerDetails.expiryDateTime
    $bearerAccessTokenTimeToExpiry = $bearerDetails.timeToExpiry
    Write-Host "+Bearer token ($targetEndPoint): [tokenRequestProcessed: '$dateTimeTokenCreated']; [expiryDateTime: '$bearerAccessTokenExpiryDateTime']; [timeUntilExpiry: '$bearerAccessTokenTimeToExpiry']"
}
$funcCreateBearerToken = $function:createBearerToken.ToString()
$htBearerAccessToken = @{}
#endregion createbearertoken

#API

#region azapicall
function AzAPICall($uri, $method, $currentTask, $body, $listenOn, $getConsumption, $getGroup, $getGroupMembersCount, $getApp, $getSP, $getGuests, $caller, $consistencyLevel, $getCount, $getPolicyCompliance, $getMgAscSecureScore, $getRoleAssignmentSchedules, $getRoleAssignmentScheduledInstances, $getDiagnosticSettingsMg, $validate) {
    $tryCounter = 0
    $tryCounterUnexpectedError = 0
    $retryAuthorizationFailed = 5
    $retryAuthorizationFailedCounter = 0
    $apiCallResultsCollection = [System.Collections.ArrayList]@()
    $initialUri = $uri
    $restartDueToDuplicateNextlinkCounter = 0
    if ($htParameters.DebugAzAPICall -eq $true) {
        if ($caller -like "CustomDataCollection*") {
            $debugForeGroundColors = @('DarkBlue', 'DarkGreen', 'DarkCyan', 'Cyan', 'DarkMagenta', 'DarkYellow', 'Blue', 'Magenta', 'Yellow', 'Green')
            $debugForeGroundColorsCount = $debugForeGroundColors.Count
            $randomNumber = Get-Random -Minimum 0 -Maximum ($debugForeGroundColorsCount - 1)
            $debugForeGroundColor = $debugForeGroundColors[$randomNumber]
        }
        else {
            $debugForeGroundColor = "Cyan"
        }
    }

    do {
        if ($arrayAzureManagementEndPointUrls | Where-Object { $uri -match $_ }) {
            $targetEndpoint = "ManagementAPI"
            $bearerToUse = $htBearerAccessToken.AccessTokenManagement
        }
        else {
            $targetEndpoint = "MSGraphAPI"
            $bearerToUse = $htBearerAccessToken.AccessTokenMSGraph
        }

        #
        $unexpectedError = $false

        $Header = @{
            "Content-Type"  = "application/json"; 
            "Authorization" = "Bearer $bearerToUse" 
        }
        if ($consistencyLevel) {
            $Header = @{
                "Content-Type"     = "application/json"; 
                "Authorization"    = "Bearer $bearerToUse";
                "ConsistencyLevel" = "$consistencyLevel"
            }
        }

        $startAPICall = Get-Date
        try {
            if ($body) {
                $azAPIRequest = Invoke-WebRequest -Uri $uri -Method $method -body $body -Headers $Header -ContentType "application/json" -UseBasicParsing
            }
            else {
                $azAPIRequest = Invoke-WebRequest -Uri $uri -Method $method -Headers $Header -UseBasicParsing
            }
        }
        catch {
            try {
                $catchResultPlain = $_.ErrorDetails.Message
                if ($catchResultPlain) {
                    $catchResult = $catchResultPlain | ConvertFrom-Json -ErrorAction Stop
                }
            }
            catch {
                $catchResult = $catchResultPlain
                $tryCounterUnexpectedError++
                $unexpectedError = $true
            }
        }
        $endAPICall = get-date
        $durationAPICall = NEW-TIMESPAN -Start $startAPICall -End $endAPICall

        #API Call Tracking
        $tstmp = (Get-Date -format "yyyyMMddHHmmssms")
        $null = $script:arrayAPICallTracking.Add([PSCustomObject]@{ 
                CurrentTask                          = $currentTask
                TargetEndpoint                       = $targetEndpoint
                Uri                                  = $uri
                Method                               = $method
                TryCounter                           = $tryCounter
                TryCounterUnexpectedError            = $tryCounterUnexpectedError
                RetryAuthorizationFailedCounter      = $retryAuthorizationFailedCounter
                RestartDueToDuplicateNextlinkCounter = $restartDueToDuplicateNextlinkCounter
                TimeStamp                            = $tstmp
                Duration                             = $durationAPICall.TotalSeconds
            })
        
        if ($caller -eq "CustomDataCollection") {
            $null = $script:arrayAPICallTrackingCustomDataCollection.Add([PSCustomObject]@{ 
                    CurrentTask                          = $currentTask
                    TargetEndpoint                       = $targetEndpoint
                    Uri                                  = $uri
                    Method                               = $method
                    TryCounter                           = $tryCounter
                    TryCounterUnexpectedError            = $tryCounterUnexpectedError
                    RetryAuthorizationFailedCounter      = $retryAuthorizationFailedCounter
                    RestartDueToDuplicateNextlinkCounter = $restartDueToDuplicateNextlinkCounter
                    TimeStamp                            = $tstmp
                    Duration                             = $durationAPICall.TotalSeconds
                })
        }

        $tryCounter++
        if ($htParameters.DebugAzAPICall -eq $true -or $tryCounter -gt 3) { 
            if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "  DEBUGTASK: attempt#$($tryCounter) processing: $($currenttask) uri: '$($uri)'" -ForegroundColor $debugForeGroundColor }
            if ($htParameters.DebugAzAPICall -eq $false -and $tryCounter -gt 3) { Write-Host "  Forced DEBUG: attempt#$($tryCounter) processing: $($currenttask) uri: '$($uri)'" }
        }
        
        if ($unexpectedError -eq $false) {
            if ($htParameters.DebugAzAPICall -eq $true -or $tryCounter -gt 3) { 
                if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: unexpectedError: false" -ForegroundColor $debugForeGroundColor }
                if ($htParameters.DebugAzAPICall -eq $false -and $tryCounter -gt 3) { Write-Host "   Forced DEBUG: unexpectedError: false" }
            }
            if ($azAPIRequest.StatusCode -ne 200) {
                if ($htParameters.DebugAzAPICall -eq $true -or $tryCounter -gt 3) { 
                    if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: apiStatusCode: $($azAPIRequest.StatusCode)" -ForegroundColor $debugForeGroundColor }
                    if ($htParameters.DebugAzAPICall -eq $false -and $tryCounter -gt 3) { Write-Host "   Forced DEBUG: apiStatusCode: $($azAPIRequest.StatusCode)" }
                }
                if ($catchResult.error.code -like "*GatewayTimeout*" -or 
                    $catchResult.error.code -like "*BadGatewayConnection*" -or 
                    $catchResult.error.code -like "*InvalidGatewayHost*" -or 
                    $catchResult.error.code -like "*ServerTimeout*" -or 
                    $catchResult.error.code -like "*ServiceUnavailable*" -or 
                    $catchResult.code -like "*ServiceUnavailable*" -or 
                    $catchResult.error.code -like "*MultipleErrorsOccurred*" -or 
                    $catchResult.code -like "*InternalServerError*" -or 
                    $catchResult.error.code -like "*InternalServerError*" -or 
                    $catchResult.error.code -like "*RequestTimeout*" -or 
                    $catchResult.error.code -like "*AuthorizationFailed*" -or 
                    $catchResult.error.code -like "*ExpiredAuthenticationToken*" -or 
                    $catchResult.error.code -like "*Authentication_ExpiredToken*" -or 
                    ($getPolicyCompliance -and $catchResult.error.code -like "*ResponseTooLarge*") -or 
                    $catchResult.error.code -like "*InvalidAuthenticationToken*" -or 
                    (
                        ($getConsumption -and $catchResult.error.code -eq 404) -or 
                        ($getConsumption -and $catchResult.error.code -eq "AccountCostDisabled") -or 
                        ($getConsumption -and $catchResult.error.message -like "*does not have any valid subscriptions*") -or 
                        ($getConsumption -and $catchResult.error.code -eq "Unauthorized") -or 
                        ($getConsumption -and $catchResult.error.code -eq "BadRequest" -and $catchResult.error.message -like "*The offer*is not supported*" -and $catchResult.error.message -notlike "*The offer MS-AZR-0110P is not supported*") -or
                        ($getConsumption -and $catchResult.error.code -eq "BadRequest" -and $catchResult.error.message -like "Invalid query definition*") -or
                        ($getConsumption -and $catchResult.error.code -eq "NotFound" -and $catchResult.error.message -like "*have valid WebDirect/AIRS offer type*") -or
                        ($getConsumption -and $catchResult.error.code -eq "NotFound" -and $catchResult.error.message -like "Cost management data is not supported for subscription(s)*") -or
                        ($getConsumption -and $catchResult.error.code -eq "IndirectCostDisabled")
                    ) -or 
                    $catchResult.error.message -like "*The offer MS-AZR-0110P is not supported*" -or
                    ($getSP -and $catchResult.error.code -like "*Request_ResourceNotFound*") -or 
                    ($getSP -and $catchResult.error.code -like "*Authorization_RequestDenied*") -or
                    ($getApp -and $catchResult.error.code -like "*Request_ResourceNotFound*") -or 
                    ($getApp -and $catchResult.error.code -like "*Authorization_RequestDenied*") -or 
                    ($getGroup -and $catchResult.error.code -like "*Request_ResourceNotFound*") -or 
                    ($getGroupMembersCount -and $catchResult.error.code -like "*Request_ResourceNotFound*") -or
                    ($getGuests -and $catchResult.error.code -like "*Authorization_RequestDenied*") -or 
                    $catchResult.error.code -like "*UnknownError*" -or
                    $catchResult.error.code -like "*BlueprintNotFound*" -or
                    $catchResult.error.code -eq "500" -or
                    $catchResult.error.code -eq "ResourceRequestsThrottled" -or
                    $catchResult.error.code -eq "429" -or
                    ($getMgAscSecureScore -and $catchResult.error.code -eq "BadRequest") -or
                    ($getRoleAssignmentSchedules -and $catchResult.error.code -eq "ResourceNotOnboarded") -or
                    ($getRoleAssignmentSchedules -and $catchResult.error.code -eq "TenantNotOnboarded") -or
                    ($getRoleAssignmentSchedules -and $catchResult.error.code -eq "InvalidResourceType") -or
                    ($getRoleAssignmentSchedules -and $catchResult.error.code -eq "InvalidResource") -or
                    ($getRoleAssignmentScheduledInstances -and $catchResult.error.code -eq "InvalidResource") -or
                    ($getDiagnosticSettingsMg -and $catchResult.error.code -eq "InvalidResourceType") -or
                    ($catchResult.error.code -eq "InsufficientPermissions") -or
                    $catchResult.error.code -eq "ClientCertificateValidationFailure" -or
                    ($validate -and $catchResult.error.code -eq "Authorization_RequestDenied")
                ) {
                    if ($getPolicyCompliance -and $catchResult.error.code -like "*ResponseTooLarge*") {
                        Write-Host "Info: $currentTask - (StatusCode: '$($azAPIRequest.StatusCode)') Response too large, skipping this scope."
                        return "ResponseTooLarge"
                    }
                    if ($catchResult.error.message -like "*The offer MS-AZR-0110P is not supported*") {
                        Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - seems we´re hitting a malicious endpoint .. try again in $tryCounter second(s)"
                        Start-Sleep -Seconds $tryCounter
                    }
                    if ($catchResult.error.code -like "*GatewayTimeout*" -or $catchResult.error.code -like "*BadGatewayConnection*" -or $catchResult.error.code -like "*InvalidGatewayHost*" -or $catchResult.error.code -like "*ServerTimeout*" -or $catchResult.error.code -like "*ServiceUnavailable*" -or $catchResult.code -like "*ServiceUnavailable*" -or $catchResult.error.code -like "*MultipleErrorsOccurred*" -or $catchResult.code -like "*InternalServerError*" -or $catchResult.error.code -like "*InternalServerError*" -or $catchResult.error.code -like "*RequestTimeout*" -or $catchResult.error.code -like "*UnknownError*" -or $catchResult.error.code -eq "500") {
                        Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - try again in $tryCounter second(s)"
                        Start-Sleep -Seconds $tryCounter
                    }
                    if ($catchResult.error.code -like "*AuthorizationFailed*") {
                        if ($validate) {
                            #Write-Host "$currentTask failed ('$($catchResult.error.code)' | '$($catchResult.error.message)')" -ForegroundColor DarkRed
                            return "failed"
                        }
                        else {
                            if ($retryAuthorizationFailedCounter -gt $retryAuthorizationFailed) {
                                Write-Host "- - - - - - - - - - - - - - - - - - - - "
                                Write-Host "!Please report at $($htParameters.GithubRepository) and provide the following dump" -ForegroundColor Yellow
                                Write-Host "$currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') '$($catchResult.error.code)' | '$($catchResult.error.message)' - $retryAuthorizationFailed retries failed - EXIT"
                                Write-Host ""
                                Write-Host "Parameters:"
                                foreach ($htParameter in ($htParameters.Keys | Sort-Object)) {
                                    Write-Host "$($htParameter):$($htParameters.($htParameter))"
                                }
                                if ($htParameters.AzureDevOpsWikiAsCode -eq $true) {
                                    Write-Error "Error"
                                }
                                else {
                                    Throw "Error - AzGovViz: check the last console output for details"
                                }
                            }
                            else {
                                if ($retryAuthorizationFailedCounter -gt 2) {
                                    Start-Sleep -Seconds 5
                                }
                                if ($retryAuthorizationFailedCounter -gt 3) {
                                    Start-Sleep -Seconds 10
                                }
                                Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') '$($catchResult.error.code)' | '$($catchResult.error.message)' - not reasonable, retry #$retryAuthorizationFailedCounter of $retryAuthorizationFailed"
                                $retryAuthorizationFailedCounter ++
                            }
                        }

                    }
                    if ($catchResult.error.code -like "*ExpiredAuthenticationToken*" -or $catchResult.error.code -like "*Authentication_ExpiredToken*" -or $catchResult.error.code -like "*InvalidAuthenticationToken*") {
                        Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') '$($catchResult.error.code)' | '$($catchResult.error.message)' - requesting new bearer token ($targetEndpoint)"
                        createBearerToken -targetEndPoint $targetEndpoint
                    }
                    if (
                        ($getConsumption -and $catchResult.error.code -eq 404) -or 
                        ($getConsumption -and $catchResult.error.code -eq "AccountCostDisabled") -or 
                        ($getConsumption -and $catchResult.error.message -like "*does not have any valid subscriptions*") -or 
                        ($getConsumption -and $catchResult.error.code -eq "Unauthorized") -or 
                        ($getConsumption -and $catchResult.error.code -eq "BadRequest" -and $catchResult.error.message -like "*The offer*is not supported*" -and $catchResult.error.message -notlike "*The offer MS-AZR-0110P is not supported*") -or
                        ($getConsumption -and $catchResult.error.code -eq "BadRequest" -and $catchResult.error.message -like "Invalid query definition*")
                    ) {
                        if ($getConsumption -and $catchResult.error.code -eq 404) {
                            Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) seems Subscriptions was created only recently - skipping"
                            return $apiCallResultsCollection
                        }
                        if ($getConsumption -and $catchResult.error.code -eq "AccountCostDisabled") {
                            Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) seems Access to cost data has been disabled for this Account - skipping CostManagement"
                            return "AccountCostDisabled"
                        }
                        if ($getConsumption -and $catchResult.error.message -like "*does not have any valid subscriptions*") {
                            Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) seems there are no valid Subscriptions present - skipping CostManagement"
                            return "NoValidSubscriptions"
                        }
                        if ($getConsumption -and $catchResult.error.code -eq "Unauthorized") {
                            Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) Unauthorized - handling as exception"
                            return "Unauthorized"
                        }
                        if ($getConsumption -and $catchResult.error.code -eq "BadRequest" -and $catchResult.error.message -like "*The offer*is not supported*" -and $catchResult.error.message -notlike "*The offer MS-AZR-0110P is not supported*") {
                            Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) Unauthorized - handling as exception"
                            return "OfferNotSupported"
                        }
                        if ($getConsumption -and $catchResult.error.code -eq "BadRequest" -and $catchResult.error.message -like "Invalid query definition*") {
                            Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) Unauthorized - handling as exception"
                            return "InvalidQueryDefinition"
                        }
                        if ($getConsumption -and $catchResult.error.code -eq "NotFound" -and $catchResult.error.message -like "*have valid WebDirect/AIRS offer type*") {
                            Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) Unauthorized - handling as exception"
                            return "NonValidWebDirectAIRSOfferType"
                        }
                        if ($getConsumption -and $catchResult.error.code -eq "NotFound" -and $catchResult.error.message -like "Cost management data is not supported for subscription(s)*") {
                            return "NotFoundNotSupported"
                        }

                        if ($getConsumption -and $catchResult.error.code -eq "IndirectCostDisabled") {
                            return "IndirectCostDisabled"
                        }
                    }
                    if (($getGroup) -and $catchResult.error.code -like "*Request_ResourceNotFound*") {
                        Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) uncertain Group status - skipping for now :)"
                        return "Request_ResourceNotFound"
                    }
                    if (($getGroupMembersCount) -and $catchResult.error.code -like "*Request_ResourceNotFound*") {
                        Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) uncertain Group status - skipping for now :)"
                        return "Request_ResourceNotFound"
                    }
                    if (($getApp -or $getSP) -and $catchResult.error.code -like "*Request_ResourceNotFound*") {
                        Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) uncertain ServicePrincipal status - skipping for now :)"
                        return "Request_ResourceNotFound"
                    }
                    if ($currentTask -eq "Checking AAD UserType" -and $catchResult.error.code -like "*Authorization_RequestDenied*") {
                        Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) cannot get the executing user´s userType information (member/guest) - proceeding as 'unknown'"
                        return "unknown"
                    }
                    if ((($getApp -or $getSP) -and $catchResult.error.code -like "*Authorization_RequestDenied*") -or ($getGuests -and $catchResult.error.code -like "*Authorization_RequestDenied*")) {
                        if ($userType -eq "Guest" -or $userType -eq "unknown") {
                            Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult)"
                            if ($userType -eq "Guest") {
                                Write-Host " AzGovViz says: Your UserType is 'Guest' (member/guest/unknown) in the tenant therefore not enough permissions. You have the following options: [1. request membership to AAD Role 'Directory readers'.] Grant explicit Microsoft Graph API permission." -ForegroundColor Yellow
                            }
                            if ($userType -eq "unknown") {
                                Write-Host " AzGovViz says: Your UserType is 'unknown' (member/guest/unknown) in the tenant. Seems you do not have enough permissions geeting AAD related data. You have the following options: [1. request membership to AAD Role 'Directory readers'.]" -ForegroundColor Yellow
                            }
                            if ($htParameters.AzureDevOpsWikiAsCode -eq $true) {
                                Write-Error "Error"
                            }
                            else {
                                Throw "Authorization_RequestDenied"
                            }
                        }
                        else {
                            Write-Host "- - - - - - - - - - - - - - - - - - - - "
                            Write-Host "!Please report at $($htParameters.GithubRepository) and provide the following dump" -ForegroundColor Yellow
                            Write-Host "$currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) - EXIT"
                            Write-Host ""
                            Write-Host "Parameters:"
                            foreach ($htParameter in ($htParameters.Keys | Sort-Object)) {
                                Write-Host "$($htParameter):$($htParameters.($htParameter))"
                            }
                            if ($htParameters.AzureDevOpsWikiAsCode -eq $true) {
                                Write-Error "Error"
                            }
                            else {
                                Throw "Authorization_RequestDenied"
                            }
                        }
                    }
                    if ($catchResult.error.code -like "*BlueprintNotFound*") {
                        Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) seems Blueprint definition is gone - skipping for now :)"
                        return "BlueprintNotFound"
                    }
                    if ($catchResult.error.code -eq "ResourceRequestsThrottled" -or $catchResult.error.code -eq "429") {
                        $sleepSeconds = 11
                        if ($catchResult.error.code -eq "ResourceRequestsThrottled") {
                            Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') '$($catchResult.error.code)' | '$($catchResult.error.message)' - throttled! sleeping $sleepSeconds seconds"
                            start-sleep -Seconds $sleepSeconds
                        }
                        if ($catchResult.error.code -eq "429") {
                            if ($catchResult.error.message -like "*60 seconds*") {
                                $sleepSeconds = 60
                            }
                            Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') '$($catchResult.error.code)' | '$($catchResult.error.message)' - throttled! sleeping $sleepSeconds seconds"
                            start-sleep -Seconds $sleepSeconds
                        }

                    }    

                    if ($getMgAscSecureScore -and $catchResult.error.code -eq "BadRequest") {
                        $sleepSec = @(1, 1, 2, 3, 5, 7, 9, 10, 13, 15, 20, 25, 30, 45, 60, 60, 60, 60)[$tryCounter]
                        $maxTries = 15
                        if ($tryCounter -gt $maxTries) {
                            Write-Host " $currentTask - capitulation after $maxTries attempts"
                            return "capitulation"
                        }
                        Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - try again (trying $maxTries times) in $sleepSec second(s)"
                        Start-Sleep -Seconds $sleepSec
                    }

                    if (($getRoleAssignmentSchedules -and $catchResult.error.code -eq "ResourceNotOnboarded") -or ($getRoleAssignmentSchedules -and $catchResult.error.code -eq "TenantNotOnboarded") -or ($getRoleAssignmentSchedules -and $catchResult.error.code -eq "InvalidResourceType") -or ($getRoleAssignmentSchedules -and $catchResult.error.code -eq "InvalidResource") -or ($getRoleAssignmentScheduledInstances -and $catchResult.error.code -eq "InvalidResource")) {
                        if ($getRoleAssignmentSchedules -and $catchResult.error.code -eq "ResourceNotOnboarded") {
                            return "ResourceNotOnboarded"
                        }
                        if ($getRoleAssignmentSchedules -and $catchResult.error.code -eq "TenantNotOnboarded") {
                            return "TenantNotOnboarded"
                        }
                        if ($getRoleAssignmentSchedules -and $catchResult.error.code -eq "InvalidResourceType") {
                            return "InvalidResourceType"
                        }
                        if ($getRoleAssignmentSchedules -and $catchResult.error.code -eq "InvalidResource") {
                            return "InvalidResource"
                        }
                        if ($getRoleAssignmentScheduledInstances -and $catchResult.error.code -eq "InvalidResource") {
                            return "InvalidResource"
                        }
                    }

                    if ($getDiagnosticSettingsMg -and $catchResult.error.code -eq "InvalidResourceType") {
                        return "InvalidResourceType"
                    }

                    if ($catchResult.error.code -eq "InsufficientPermissions" -or $catchResult.error.code -eq "ClientCertificateValidationFailure") {
                        $maxTries = 5
                        $sleepSec = @(1, 3, 5, 7, 10, 12, 20, 30)[$tryCounter]
                        if ($tryCounter -gt $maxTries) {
                            Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') '$($catchResult.error.code)' | '$($catchResult.error.message)' - exit"
                            if ($htParameters.AzureDevOpsWikiAsCode -eq $true) {
                                Write-Error "Error"
                            }
                            else {
                                Throw "Error - AzGovViz: check the last console output for details"
                            }
                        }
                        Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') '$($catchResult.error.code)' | '$($catchResult.error.message)' sleeping $($sleepSec) seconds"
                        start-sleep -Seconds $sleepSec
                    }

                    if ($validate -and $catchResult.error.code -eq "Authorization_RequestDenied") {
                        #Write-Host "$currentTask failed ('$($catchResult.error.code)' | '$($catchResult.error.message)')" -ForegroundColor DarkRed
                        return "failed"
                    }

                }
                else {
                    if (-not $catchResult.code -and -not $catchResult.error.code -and -not $catchResult.message -and -not $catchResult.error.message -and -not $catchResult -and $tryCounter -lt 6) {
                        if ($azAPIRequest.StatusCode -eq 204 -and $getConsumption) {
                            return $apiCallResultsCollection
                        } 
                        else {
                            $sleepSec = @(3, 7, 12, 20, 30, 45, 60)[$tryCounter]
                            Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) try again in $sleepSec second(s)"
                            Start-Sleep -Seconds $sleepSec
                        }
                    }
                    elseif (-not $catchResult.code -and -not $catchResult.error.code -and -not $catchResult.message -and -not $catchResult.error.message -and $catchResult -and $tryCounter -lt 6) {
                        $sleepSec = @(3, 7, 12, 20, 30, 45, 60)[$tryCounter]
                        Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) try again in $sleepSec second(s)"
                        Start-Sleep -Seconds $sleepSec
                    }
                    else {
                        Write-Host "- - - - - - - - - - - - - - - - - - - - "
                        Write-Host "!Please report at $($htParameters.GithubRepository) and provide the following dump" -ForegroundColor Yellow
                        Write-Host "$currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) - EXIT"
                        Write-Host ""
                        Write-Host "Parameters:"
                        foreach ($htParameter in ($htParameters.Keys | Sort-Object)) {
                            Write-Host "$($htParameter):$($htParameters.($htParameter))"
                        }
                        if ($getConsumption) {
                            Write-Host "If Consumption data is not that important for you, please try parameter: -NoAzureConsumption (however, please still report the issue - thank you)"
                        }
                        if ($htParameters.AzureDevOpsWikiAsCode -eq $true) {
                            Write-Error "Error"
                        }
                        else {
                            Throw "Error - AzGovViz: check the last console output for details"
                        }
                    }
                }
            }
            else {
                if ($htParameters.DebugAzAPICall -eq $true -or $tryCounter -gt 3) { 
                    if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: apiStatusCode: $($azAPIRequest.StatusCode)" -ForegroundColor $debugForeGroundColor }
                    if ($htParameters.DebugAzAPICall -eq $false -and $tryCounter -gt 3) { Write-Host "   Forced DEBUG: apiStatusCode: $($azAPIRequest.StatusCode)" }
                }
                $azAPIRequestConvertedFromJson = ($azAPIRequest.Content | ConvertFrom-Json)
                if ($listenOn -eq "Content") {        
                    if ($htParameters.DebugAzAPICall -eq $true -or $tryCounter -gt 3) { 
                        if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: listenOn=content ($((($azAPIRequestConvertedFromJson) | Measure-Object).count))" -ForegroundColor $debugForeGroundColor }
                        if ($htParameters.DebugAzAPICall -eq $false -and $tryCounter -gt 3) { Write-Host "   Forced DEBUG: listenOn=content ($((($azAPIRequestConvertedFromJson) | Measure-Object).count))" }
                    }    
                    $null = $apiCallResultsCollection.Add($azAPIRequestConvertedFromJson)
                }
                elseif ($listenOn -eq "ContentProperties") {
                    if (($azAPIRequestConvertedFromJson.properties.rows | Measure-Object).Count -gt 0) {
                        foreach ($consumptionline in $azAPIRequestConvertedFromJson.properties.rows) {
                            $hlper = $htSubscriptionsMgPath.($consumptionline[1])
                            $null = $apiCallResultsCollection.Add([PSCustomObject]@{ 
                                    "$($azAPIRequestConvertedFromJson.properties.columns.name[0])" = $consumptionline[0]
                                    "$($azAPIRequestConvertedFromJson.properties.columns.name[1])" = $consumptionline[1]
                                    SubscriptionName                                               = $hlper.DisplayName
                                    SubscriptionMgPath                                             = $hlper.ParentNameChainDelimited
                                    "$($azAPIRequestConvertedFromJson.properties.columns.name[2])" = $consumptionline[2]
                                    "$($azAPIRequestConvertedFromJson.properties.columns.name[3])" = $consumptionline[3]
                                    "$($azAPIRequestConvertedFromJson.properties.columns.name[4])" = $consumptionline[4]
                                    "$($azAPIRequestConvertedFromJson.properties.columns.name[5])" = $consumptionline[5]
                                    "$($azAPIRequestConvertedFromJson.properties.columns.name[6])" = $consumptionline[6]
                                })
                        }
                    }
                }
                else {       
                    if (($azAPIRequestConvertedFromJson).value) {
                        if ($htParameters.DebugAzAPICall -eq $true -or $tryCounter -gt 3) { 
                            if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: listenOn=default(value) value exists ($((($azAPIRequestConvertedFromJson).value | Measure-Object).count))" -ForegroundColor $debugForeGroundColor }
                            if ($htParameters.DebugAzAPICall -eq $false -and $tryCounter -gt 3) { Write-Host "   Forced DEBUG: listenOn=default(value) value exists ($((($azAPIRequestConvertedFromJson).value | Measure-Object).count))" }
                        }  
                        foreach ($entry in $azAPIRequestConvertedFromJson.value) {
                            $null = $apiCallResultsCollection.Add($entry)
                        }
                        
                        if ($getGuests) {
                            $guestAccountsCount = ($apiCallResultsCollection).Count
                            if ($guestAccountsCount % 1000 -eq 0) {
                                write-host " $guestAccountsCount processed"
                            }
                        }
                    }
                    else {
                        if ($htParameters.DebugAzAPICall -eq $true -or $tryCounter -gt 3) { 
                            if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: listenOn=default(value) value not exists; return empty array" -ForegroundColor $debugForeGroundColor }
                            if ($htParameters.DebugAzAPICall -eq $false -and $tryCounter -gt 3) { Write-Host "   Forced DEBUG: listenOn=default(value) value not exists; return empty array" }
                        }  
                    }
                }

                $isMore = $false
                if (-not $validate) {
                    if ($azAPIRequestConvertedFromJson.nextLink) {
                        $isMore = $true
                        if ($uri -eq $azAPIRequestConvertedFromJson.nextLink) {
                            if ($restartDueToDuplicateNextlinkCounter -gt 3) {
                                Write-Host " $currentTask restartDueToDuplicateNextlinkCounter: #$($restartDueToDuplicateNextlinkCounter) - Please report this error/exit"
                                if ($htParameters.AzureDevOpsWikiAsCode -eq $true) {
                                    Write-Error "Error"
                                }
                                else {
                                    Throw "Error - AzGovViz: check the last console output for details"
                                }
                            }
                            else {
                                $restartDueToDuplicateNextlinkCounter++
                                Write-Host "nextLinkLog: uri is equal to nextLinkUri"
                                Write-Host "nextLinkLog: uri: $uri"
                                Write-Host "nextLinkLog: nextLinkUri: $($azAPIRequestConvertedFromJson.nextLink)"
                                Write-Host "nextLinkLog: re-starting (#$($restartDueToDuplicateNextlinkCounter)) '$currentTask'"
                                $apiCallResultsCollection = [System.Collections.ArrayList]@()
                                $uri = $initialUri
                                Start-Sleep -Seconds 10
                                createBearerToken -targetEndPoint $targetEndpoint
                                Start-Sleep -Seconds 10
                            }
                        }
                        else {
                            $uri = $azAPIRequestConvertedFromJson.nextLink
                        }
                        if ($htParameters.DebugAzAPICall -eq $true -or $tryCounter -gt 3) { 
                            if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: nextLink: $Uri" -ForegroundColor $debugForeGroundColor }
                            if ($htParameters.DebugAzAPICall -eq $false -and $tryCounter -gt 3) { Write-Host "   Forced DEBUG: nextLink: $Uri" }
                        }  
                    }
                    elseIf ($azAPIRequestConvertedFromJson."@oData.nextLink") {
                        $isMore = $true
                        if ($uri -eq $azAPIRequestConvertedFromJson."@odata.nextLink") {
                            if ($restartDueToDuplicateNextlinkCounter -gt 3) {
                                Write-Host " $currentTask restartDueToDuplicate@odataNextlinkCounter: #$($restartDueToDuplicateNextlinkCounter) - Please report this error/exit"
                                if ($htParameters.AzureDevOpsWikiAsCode -eq $true) {
                                    Write-Error "Error"
                                }
                                else {
                                    Throw "Error - AzGovViz: check the last console output for details"
                                }
                            }
                            else {
                                $restartDueToDuplicateNextlinkCounter++
                                Write-Host "nextLinkLog: uri is equal to @odata.nextLinkUri"
                                Write-Host "nextLinkLog: uri: $uri"
                                Write-Host "nextLinkLog: @odata.nextLinkUri: $($azAPIRequestConvertedFromJson."@odata.nextLink")"
                                Write-Host "nextLinkLog: re-starting (#$($restartDueToDuplicateNextlinkCounter)) '$currentTask'"
                                $apiCallResultsCollection = [System.Collections.ArrayList]@()
                                $uri = $initialUri
                                Start-Sleep -Seconds 10
                                createBearerToken -targetEndPoint $targetEndpoint
                                Start-Sleep -Seconds 10
                            }
                        }
                        else {
                            $uri = $azAPIRequestConvertedFromJson."@odata.nextLink"
                        }
                        if ($htParameters.DebugAzAPICall -eq $true -or $tryCounter -gt 3) { 
                            if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: @oData.nextLink: $Uri" -ForegroundColor $debugForeGroundColor }
                            if ($htParameters.DebugAzAPICall -eq $false -and $tryCounter -gt 3) { Write-Host "   Forced DEBUG: @oData.nextLink: $Uri" }
                        }  
                    }
                    elseif ($azAPIRequestConvertedFromJson.properties.nextLink) {              
                        $isMore = $true
                        if ($uri -eq $azAPIRequestConvertedFromJson.properties.nextLink) {
                            if ($restartDueToDuplicateNextlinkCounter -gt 3) {
                                Write-Host " $currentTask restartDueToDuplicateNextlinkCounter: #$($restartDueToDuplicateNextlinkCounter) - Please report this error/exit"
                                if ($htParameters.AzureDevOpsWikiAsCode -eq $true) {
                                    Write-Error "Error"
                                }
                                else {
                                    Throw "Error - AzGovViz: check the last console output for details"
                                }
                            }
                            else {
                                $restartDueToDuplicateNextlinkCounter++
                                Write-Host "nextLinkLog: uri is equal to nextLinkUri"
                                Write-Host "nextLinkLog: uri: $uri"
                                Write-Host "nextLinkLog: nextLinkUri: $($azAPIRequestConvertedFromJson.properties.nextLink)"
                                Write-Host "nextLinkLog: re-starting (#$($restartDueToDuplicateNextlinkCounter)) '$currentTask'"
                                $apiCallResultsCollection = [System.Collections.ArrayList]@()
                                $uri = $initialUri
                                Start-Sleep -Seconds 10
                                createBearerToken -targetEndPoint $targetEndpoint
                                Start-Sleep -Seconds 10
                            }
                        }
                        else {
                            $uri = $azAPIRequestConvertedFromJson.properties.nextLink
                        }
                        if ($htParameters.DebugAzAPICall -eq $true -or $tryCounter -gt 3) { 
                            if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: nextLink: $Uri" -ForegroundColor $debugForeGroundColor }
                            if ($htParameters.DebugAzAPICall -eq $false -and $tryCounter -gt 3) { Write-Host "   Forced DEBUG: nextLink: $Uri" }
                        } 
                    }
                    else {
                        if ($htParameters.DebugAzAPICall -eq $true -or $tryCounter -gt 3) { 
                            if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: NextLink: none" -ForegroundColor $debugForeGroundColor }
                            if ($htParameters.DebugAzAPICall -eq $false -and $tryCounter -gt 3) { Write-Host "   Forced DEBUG: NextLink: none" }
                        } 
                    }
                }
            }
        }
        else {
            if ($htParameters.DebugAzAPICall -eq $true -or $tryCounter -gt 3) { 
                if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: unexpectedError: notFalse" -ForegroundColor $debugForeGroundColor }
                if ($htParameters.DebugAzAPICall -eq $false -and $tryCounter -gt 3) { Write-Host "   Forced DEBUG: unexpectedError: notFalse" }
            } 
            if ($tryCounterUnexpectedError -lt 13) {
                $sleepSec = @(1, 2, 3, 5, 7, 10, 13, 17, 20, 30, 40, 50, , 55, 60)[$tryCounterUnexpectedError]
                Write-Host " $currentTask #$tryCounterUnexpectedError 'Unexpected Error' occurred (trying 10 times); sleep $sleepSec seconds"
                Write-Host $catchResult
                Start-Sleep -Seconds $sleepSec
            }
            else {
                Write-Host " $currentTask #$tryCounterUnexpectedError 'Unexpected Error' occurred (tried 5 times)/exit"
                if ($htParameters.AzureDevOpsWikiAsCode -eq $true) {
                    Write-Error "Error"
                }
                else {
                    Throw "Error - AzGovViz: check the last console output for details"
                }
            }
        }
    }
    until($azAPIRequest.StatusCode -eq 200 -and -not $isMore)
    return $apiCallResultsCollection
}
$funcAzAPICall = $function:AzAPICall.ToString()
#endregion azapicall

#check required Az modules cmdlets
#region testAzModules
$testCommands = @('Get-AzContext')
$azModules = @('Az.Accounts')

Write-Host "Testing required Az modules cmdlets"
foreach ($testCommand in $testCommands) {
    if (-not (Get-Command $testCommand -ErrorAction Ignore)) {
        if ($htParameters.AzureDevOpsWikiAsCode -eq $true) {
            Write-Error "AzModule test failed: cmdlet $testCommand not available - make sure the modules $($azModules -join ", ") are installed"
            Write-Error "Error"
        }
        else {
            Write-Host " AzModule test failed: cmdlet $testCommand not available - make sure the modules $($azModules -join ", ") are installed" -ForegroundColor Red
            Write-Host " Install the Azure Az PowerShell module: https://docs.microsoft.com/en-us/powershell/azure/install-az-ps"
            Throw "Error - $($Product): check the last console output for details"
        }
    }
    else {
        Write-Host " AzModule test passed: Az ps module supporting cmdlet $testCommand installed" -ForegroundColor Green
    }
}

Write-Host "Collecting Az modules versions"
foreach ($azModule in $azModules) {
    $azModuleVersion = (Get-InstalledModule -name "$azModule" -ErrorAction Ignore).Version
    if ($azModuleVersion) {
        Write-Host " Az Module $azModule Version: $azModuleVersion"
        $resolvedAzModuleVersion = $azModuleVersion
    }
    else {
        Write-Host " Az Module $azModule Version: could not be assessed"
        $resolvedAzModuleVersion = "could not be assessed"
    }
}
#endregion testAzModules

#check AzContext
#region checkAzContext
Write-Host "Checking Az Context"
if (-not $checkContext) {
    Write-Host " Context test failed: No context found. Please connect to Azure (run: Connect-AzAccount) and re-run $($Product)" -ForegroundColor Red
    if ($htParameters.AzureDevOpsWikiAsCode -eq $true) {
        Write-Error "Error"
    }
    else {
        Throw "Error - $($Product): check the last console output for details"
    }
}
else {
    $accountType = $checkContext.Account.Type
    $accountId = $checkContext.Account.Id
    Write-Host " Context AccountId: '$($accountId)'" -ForegroundColor Yellow
    Write-Host " Context AccountType: '$($accountType)'" -ForegroundColor Yellow

    if ($SubscriptionId4AzContext -ne "undefined") {
        Write-Host " Setting AzContext to SubscriptionId: '$SubscriptionId4AzContext'" -ForegroundColor Yellow
        try {
            Set-AzContext -SubscriptionId $SubscriptionId4AzContext
        }
        catch {
            if ($htParameters.AzureDevOpsWikiAsCode -eq $true) {
                Write-Error "Error"
            }
            else {
                Throw "Error - $($Product): check the last console output for details"
            }
        }
        $checkContext = Get-AzContext -ErrorAction Stop
    }
    
    #else{
    if (-not $checkContext.Subscription) {
        $checkContext
        Write-Host " Context test failed: Context is not set to any Subscription. Set your context to a subscription by running: Set-AzContext -subscription <subscriptionId> (run Get-AzSubscription to get the list of available Subscriptions). When done re-run $($Product)" -ForegroundColor Red
        
        if ($htParameters.AzureDevOpsWikiAsCode -eq $true) {
            Write-host " If this error occurs you may want to leverage parameter 'SubscriptionId4AzContext' (<script>.ps1 -SubscriptionId4AzContext '<SubscriptionId>')"
            Write-Error "Error"
        }
        else {
            Throw "Error - $($Product): check the last console output for details"
        }
    }
    else {
        Write-Host " Context test passed: Context OK" -ForegroundColor Green
    }
    #}

}
#endregion checkAzContext

#environment check
#region environmentcheck
$checkAzEnvironments = Get-AzEnvironment -ErrorAction Stop

#FutureUse
#Graph Endpoints https://docs.microsoft.com/en-us/graph/deployments#microsoft-graph-and-graph-explorer-service-root-endpoints
#AzureCloud https://graph.microsoft.com
#AzureUSGovernment L4 https://graph.microsoft.us
#AzureUSGovernment L5 (DOD) https://dod-graph.microsoft.us
#AzureChinaCloud https://microsoftgraph.chinacloudapi.cn
#AzureGermanCloud https://graph.microsoft.de

#AzureEnvironmentRelatedUrls
$htAzureEnvironmentRelatedUrls = @{ }
$arrayAzureManagementEndPointUrls = @()
foreach ($checkAzEnvironment in $checkAzEnvironments) {
    ($htAzureEnvironmentRelatedUrls).($checkAzEnvironment.Name) = @{ }
    ($htAzureEnvironmentRelatedUrls).($checkAzEnvironment.Name).ResourceManagerUrl = $checkAzEnvironment.ResourceManagerUrl
    $arrayAzureManagementEndPointUrls += $checkAzEnvironment.ResourceManagerUrl
    if ($checkAzEnvironment.Name -eq "AzureCloud") {
        ($htAzureEnvironmentRelatedUrls).($checkAzEnvironment.Name).MSGraphUrl = "https://graph.microsoft.com"
    }
    if ($checkAzEnvironment.Name -eq "AzureChinaCloud") {
        ($htAzureEnvironmentRelatedUrls).($checkAzEnvironment.Name).MSGraphUrl = "https://microsoftgraph.chinacloudapi.cn"
    }
    if ($checkAzEnvironment.Name -eq "AzureUSGovernment") {
        ($htAzureEnvironmentRelatedUrls).($checkAzEnvironment.Name).MSGraphUrl = "https://graph.microsoft.us"
    }
    if ($checkAzEnvironment.Name -eq "AzureGermanCloud") {
        ($htAzureEnvironmentRelatedUrls).($checkAzEnvironment.Name).MSGraphUrl = "https://graph.microsoft.de"
    }
}
#endregion environmentcheck

#create bearer token
if (-not $NoAzureRoleAssignments) {
    createBearerToken -targetEndPoint "ManagementAPI"
}
createBearerToken -targetEndPoint "MSGraphAPI"

#helper file/dir, delimiter, time
#region helper
#delimiter
if ($CsvDelimiter -eq ";") {
    $CsvDelimiterOpposite = ","
}
if ($CsvDelimiter -eq ",") {
    $CsvDelimiterOpposite = ";"
}
#endregion helper

#region Function

#region resolveObectsById
function resolveObectsById($objects, $targetHt) {

    $counterBatch = [PSCustomObject] @{ Value = 0 }
    $batchSize = 1000
    $ObjectIdsBatch = $objects | Group-Object -Property { [math]::Floor($counterBatch.Value++ / $batchSize) }
    $ObjectIdsBatchCount = ($ObjectIdsBatch | measure-object).Count
    $batchCnt = 0

    foreach ($batch in $ObjectIdsBatch) {
        $batchCnt++
        Write-Host " processing Batch #$batchCnt/$($ObjectIdsBatchCount) ($(($batch.Group).Count) ObjectIds)"

        $nonResolvedIdentitiesToCheck = '"{0}"' -f ($batch.Group -join '","')
        Write-Host "    IdentitiesToCheck: $nonResolvedIdentitiesToCheck"
        
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/directoryObjects/getByIds?`$select=userType,id,displayName"
        $method = "POST"
        $body = @"
        {
            "ids":[$($nonResolvedIdentitiesToCheck)]
        }
"@
        $currentTask = "Resolving Identities - Batch #$batchCnt/$($ObjectIdsBatchCount) ($(($batch.Group).Count) ObjectIds)"
        $resolvedIdentities = AzAPICall -uri $uri -method $method -body $body -currentTask $currentTask

        $t = 0
        foreach ($resolvedIdentity in $resolvedIdentities) {
            $t++
            #Write-Host $t
            $type = "unforseen type"
            if ($resolvedIdentity.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
                $type = "Serviceprincipal"
            }
            if ($resolvedIdentity.'@odata.type' -eq '#microsoft.graph.application') {
                $type = "Application"
            }
            if ($resolvedIdentity.'@odata.type' -eq '#microsoft.graph.group') {
                $type = "Group"
            }
            if ($resolvedIdentity.'@odata.type' -eq '#microsoft.graph.user') {
                $type = "User"
            }

            if ($targetHt -eq "htUsersResolved"){
                $script:htUsersResolved.($resolvedIdentity.id) = @{}
                $script:htUsersResolved.($resolvedIdentity.id).full = "$($type) ($($resolvedIdentity.userType)), DisplayName: $($resolvedIdentity.displayName), Id: $(($resolvedIdentity.id))"
                $script:htUsersResolved.($resolvedIdentity.id).typeOnly = "$($type) ($($resolvedIdentity.userType))"
            }

        }
        $resolvedIdentitiesCount = $resolvedIdentities.Count
        Write-Host "    $resolvedIdentitiesCount identities resolved"
    }
}
#endregion resolveObectsById

#region Function_dataCollection
function dataCollection($mgId) {
    Write-Host " CustomDataCollection ManagementGroups"
    $startMgLoop = get-date
    
    $allManagementGroupsFromEntitiesChildOfRequestedMg = $arrayEntitiesFromAPI.where( { $_.type -eq "Microsoft.Management/managementGroups" -and ($_.Name -eq $mgId -or $_.properties.parentNameChain -contains $mgId) })
    $allManagementGroupsFromEntitiesChildOfRequestedMgCount = ($allManagementGroupsFromEntitiesChildOfRequestedMg | Measure-Object).Count

    $allManagementGroupsFromEntitiesChildOfRequestedMg | ForEach-Object -Parallel {
        $mgdetail = $_
        #region UsingVARs
        #Parameters MG&Sub related
        $CsvDelimiter = $using:CsvDelimiter
        $CsvDelimiterOpposite = $using:CsvDelimiterOpposite
        #fromOtherFunctions
        $arrayAzureManagementEndPointUrls = $using:arrayAzureManagementEndPointUrls
        $checkContext = $using:checkContext
        $htAzureEnvironmentRelatedUrls = $using:htAzureEnvironmentRelatedUrls
        $htBearerAccessToken = $using:htBearerAccessToken
        #Array&HTs
        $htParameters = $using:htParameters
        $customDataCollectionDuration = $using:customDataCollectionDuration
        $htCacheDefinitions = $using:htCacheDefinitions
        $htCacheAssignments = $using:htCacheAssignments
        $htManagementGroupsMgPath = $using:htManagementGroupsMgPath
        $arrayEntitiesFromAPI = $using:arrayEntitiesFromAPI
        $allManagementGroupsFromEntitiesChildOfRequestedMg = $using:allManagementGroupsFromEntitiesChildOfRequestedMg
        $allManagementGroupsFromEntitiesChildOfRequestedMgCount = $using:allManagementGroupsFromEntitiesChildOfRequestedMgCount
        $arrayDataCollectionProgressMg = $using:arrayDataCollectionProgressMg
        $arrayAPICallTracking = $using:arrayAPICallTracking
        $arrayAPICallTrackingCustomDataCollection = $using:arrayAPICallTrackingCustomDataCollection
        $htRoleAssignmentsFromAPIInheritancePrevention = $using:htRoleAssignmentsFromAPIInheritancePrevention
        #Functions
        $function:AzAPICall = $using:funcAzAPICall
        $function:createBearerToken = $using:funcCreateBearerToken
        $function:GetJWTDetails = $using:funcGetJWTDetails
        #endregion usingVARS

        $MgParentId = ($allManagementGroupsFromEntitiesChildOfRequestedMg.where( { $_.Name -eq $mgdetail.Name })).properties.parent.Id -replace ".*/"
        if ([string]::IsNullOrEmpty($MgParentId)) {
            $MgParentId = "TenantRoot"
            $MgParentName = "TenantRoot"
        }
        else {
            $MgParentName = $htManagementGroupsMgPath.($MgParentId).DisplayName
        }
        $hierarchyLevel = (($allManagementGroupsFromEntitiesChildOfRequestedMg.where( { $_.Name -eq $mgdetail.Name })).properties.parentNameChain | Measure-Object).Count

        $rndom = Get-Random -Minimum 10 -Maximum 750
        start-sleep -Millisecond $rndom
        $startMgLoopThis = get-date
 
        #MGCustomRolesRoles
        $currentTask = "Custom Role definitions '$($mgdetail.properties.displayName)' ('$($mgdetail.Name)')"
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).ResourceManagerUrl)providers/Microsoft.Management/managementGroups/$($mgdetail.Name)/providers/Microsoft.Authorization/roleDefinitions?api-version=2015-07-01&`$filter=type%20eq%20'CustomRole'"
        $method = "GET"
        $mgCustomRoleDefinitions = AzAPICall -uri $uri -method $method -currentTask $currentTask -caller "CustomDataCollection"

        foreach ($mgCustomRoleDefinition in $mgCustomRoleDefinitions) {
            if (-not $($htCacheDefinitions).role[$mgCustomRoleDefinition.name]) {
                ($script:htCacheDefinitions).role.$($mgCustomRoleDefinition.name) = @{}
                ($script:htCacheDefinitions).role.$($mgCustomRoleDefinition.name).definition = $mgCustomRoleDefinition
                #$mgCustomRoleDefinition
            }  
        }

        #PIM RoleAssignmentSchedules
        $currentTask = "Role assignment schedules API '$($mgdetail.properties.displayName)' ('$($mgdetail.Name)')"
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).ResourceManagerUrl)providers/Microsoft.Management/managementGroups/$($mgdetail.Name)/providers/Microsoft.Authorization/roleAssignmentSchedules?api-version=2020-10-01-preview"
        $method = "GET"
        $roleAssignmentSchedulesFromAPI = AzAPICall -uri $uri -method $method -currentTask $currentTask -caller "CustomDataCollection" -getRoleAssignmentSchedules $true
        
        if ($roleAssignmentSchedulesFromAPI -eq "ResourceNotOnboarded" -or $roleAssignmentSchedulesFromAPI -eq "TenantNotOnboarded" -or $roleAssignmentSchedulesFromAPI -eq "InvalidResourceType") {
            #Write-Host "Scope '$($childMgSubDisplayName)' ('$childMgSubId') not onboarded in PIM"
        }
        else {
            $roleAssignmentSchedules = ($roleAssignmentSchedulesFromAPI.where({ -not [string]::IsNullOrEmpty($_.properties.roleAssignmentScheduleRequestId) }))
            $roleAssignmentSchedulesCount = $roleAssignmentSchedules.Count
            #Write-Host "'$($mgdetail.properties.displayName)' ('$($mgdetail.Name)') : $($roleAssignmentSchedulesCount)"
            if ($roleAssignmentSchedulesCount -gt 0) {
                $htRoleAssignmentsPIM = @{}
                foreach ($roleAssignmentSchedule in $roleAssignmentSchedules) {
                    $keyName = "$($roleAssignmentSchedule.properties.scope)-$($roleAssignmentSchedule.properties.expandedProperties.principal.id)-$($roleAssignmentSchedule.properties.expandedProperties.roleDefinition.id)"
                    $htRoleAssignmentsPIM.($keyName) = $roleAssignmentSchedule.properties
                }
            }
        }

        #RoleAssignment API (system metadata e.g. createdOn)
        $currentTask = "Role assignments API '$($mgdetail.properties.displayName)' ('$($mgdetail.Name)')"
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).ResourceManagerUrl)providers/Microsoft.Management/managementGroups/$($mgdetail.Name)/providers/Microsoft.Authorization/roleAssignments?api-version=2015-07-01"
        $method = "GET"
        $roleAssignmentsFromAPI = AzAPICall -uri $uri -method $method -currentTask $currentTask -caller "CustomDataCollection"

        if ($roleAssignmentsFromAPI.Count -gt 0) {
            foreach ($roleAssignmentFromAPI in $roleAssignmentsFromAPI) {
                if (-not ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id)) {
                    $splitAssignment = ($roleAssignmentFromAPI.id).Split('/')
                    ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id) = @{}
                    ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignment = $roleAssignmentFromAPI
                    if ($roleAssignmentFromAPI.id -like "/providers/Microsoft.Authorization/roleAssignments/*") {
                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentScope = "Ten"
                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentScopeId = ""
                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentScopeName = ""
                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentResourceType = "Tenant"
                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentResourceName = "Tenant"
                    }
                    else {
                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentScope = "MG"
                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentScopeId = "/providers/Microsoft.Management/managementGroups/$($splitAssignment[4])"
                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentScopeName = "$($htManagementGroupsMgPath.($splitAssignment[4]).DisplayName)"
                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentResourceType = "Management Group"
                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentResourceName = $splitAssignment[4]
                    }

                    $keyName = "$($roleAssignmentFromAPI.properties.scope)-$($roleAssignmentFromAPI.properties.principalId)-$($roleAssignmentFromAPI.properties.roleDefinitionId)"
                    if ($htRoleAssignmentsPIM.($keyName)) {
                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentPIMDetails = $htRoleAssignmentsPIM.($keyName)
                    }

                    ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).roleName = ($htCacheDefinitions).role.($roleAssignmentFromAPI.properties.roleDefinitionId -replace ".*/").definition.properties.roleName
                    ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).roleId = $roleAssignmentFromAPI.properties.roleDefinitionId -replace ".*/"
                    ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).type = ($htCacheDefinitions).role.($roleAssignmentFromAPI.properties.roleDefinitionId -replace ".*/").definition.properties.type
                }
                if (-not $htRoleAssignmentsFromAPIInheritancePrevention.($roleAssignmentFromAPI.id -replace ".*/")) {
                    $htRoleAssignmentsFromAPIInheritancePrevention.($roleAssignmentFromAPI.id -replace ".*/") = @{}
                }
            }
        }

        $endMgLoopThis = get-date
        $null = $script:customDataCollectionDuration.Add([PSCustomObject]@{ 
                Type        = "Mg"
                Id          = $mgdetail.Name
                DurationSec = (NEW-TIMESPAN -Start $startMgLoopThis -End $endMgLoopThis).TotalSeconds
            })

        $null = $script:arrayDataCollectionProgressMg.Add($mgdetail.Name)
        $progressCount = ($arrayDataCollectionProgressMg).Count
        Write-Host "  $($progressCount)/$($allManagementGroupsFromEntitiesChildOfRequestedMgCount) ManagementGroups processed"

    } -ThrottleLimit $ThrottleLimit
    #[System.GC]::Collect()

    $endMgLoop = get-date
    Write-Host " CustomDataCollection ManagementGroups processing duration: $((NEW-TIMESPAN -Start $startMgLoop -End $endMgLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startMgLoop -End $endMgLoop).TotalSeconds) seconds)"


    #SUBSCRIPTION

    Write-Host " CustomDataCollection Subscriptions"
    $subsExcludedStateCount = ($outOfScopeSubscriptions | where-object { $_.outOfScopeReason -like "State*" } | Measure-Object).Count
    $subsExcludedWhitelistCount = ($outOfScopeSubscriptions | where-object { $_.outOfScopeReason -like "QuotaId*" } | Measure-Object).Count
    if ($subsExcludedStateCount -gt 0) {
        Write-Host "  CustomDataCollection $($subsExcludedStateCount) Subscriptions excluded (State != enabled)"
    }
    if ($subsExcludedWhitelistCount -gt 0) {
        Write-Host "  CustomDataCollection $($subsExcludedWhitelistCount) Subscriptions excluded (not in quotaId whitelist: '$($SubscriptionQuotaIdWhitelist -join ", ")' OR is AAD_ quotaId)"
    }
    Write-Host " CustomDataCollection Subscriptions will process $subsToProcessInCustomDataCollectionCount of $childrenSubscriptionsCount"

    $startSubLoop = get-date
    if ($subsToProcessInCustomDataCollectionCount -gt 0) {

        $counterBatch = [PSCustomObject] @{ Value = 0 }
        $batchSize = 100
        if ($subsToProcessInCustomDataCollectionCount -gt 100) {
            $batchSize = 250
        }
        Write-Host " Subscriptions Batch size: $batchSize"

        $subscriptionsBatch = $subsToProcessInCustomDataCollection | Group-Object -Property { [math]::Floor($counterBatch.Value++ / $batchSize) }
        $batchCnt = 0
        foreach ($batch in $subscriptionsBatch) { 
            #[System.GC]::Collect()
            $startBatch = get-date
            $batchCnt++
            Write-Host " processing Batch #$batchCnt/$(($subscriptionsBatch | Measure-Object).Count) ($(($batch.Group | Measure-Object).Count) Subscriptions)"

            $batch.Group | ForEach-Object -Parallel {
                $startSubLoopThis = get-date
                $childMgSubDetail = $_
                #region UsingVARs
                #Parameters MG&Sub related
                $CsvDelimiter = $using:CsvDelimiter
                $CsvDelimiterOpposite = $using:CsvDelimiterOpposite
                #Parameters Sub related
                #fromOtherFunctions
                $arrayAzureManagementEndPointUrls = $using:arrayAzureManagementEndPointUrls
                $checkContext = $using:checkContext
                $htAzureEnvironmentRelatedUrls = $using:htAzureEnvironmentRelatedUrls
                $htBearerAccessToken = $using:htBearerAccessToken
                #Array&HTs
                $htParameters = $using:htParameters
                $customDataCollectionDuration = $using:customDataCollectionDuration
                $htSubscriptionsMgPath = $using:htSubscriptionsMgPath
                $htManagementGroupsMgPath = $using:htManagementGroupsMgPath
                $htCacheDefinitions = $using:htCacheDefinitions
                $htCacheAssignments = $using:htCacheAssignments
                $childrenSubscriptionsCount = $using:childrenSubscriptionsCount
                $subsToProcessInCustomDataCollectionCount = $using:subsToProcessInCustomDataCollectionCount
                $arrayDataCollectionProgressSub = $using:arrayDataCollectionProgressSub
                $htAllSubscriptionsFromAPI = $using:htAllSubscriptionsFromAPI
                $arrayEntitiesFromAPI = $using:arrayEntitiesFromAPI
                $arrayAPICallTracking = $using:arrayAPICallTracking
                $arrayAPICallTrackingCustomDataCollection = $using:arrayAPICallTrackingCustomDataCollection
                $htRoleAssignmentsFromAPIInheritancePrevention = $using:htRoleAssignmentsFromAPIInheritancePrevention
                #Functions
                $function:AzAPICall = $using:funcAzAPICall
                $function:createBearerToken = $using:funcCreateBearerToken
                $function:GetJWTDetails = $using:funcGetJWTDetails
                #endregion UsingVARs
                
                $childMgSubId = $childMgSubDetail.subscriptionId
                $childMgSubDisplayName = $childMgSubDetail.subscriptionName
                $hierarchyInfo = $htSubscriptionsMgPath.($childMgSubDetail.subscriptionId)
                $hierarchyLevel = $hierarchyInfo.level
                $childMgId = $hierarchyInfo.Parent
                $childMgDisplayName = $hierarchyInfo.ParentName
                $childMgMgPath = $hierarchyInfo.pathDelimited
                $childMgParentInfo = $htManagementGroupsMgPath.($childMgId)
                $childMgParentId = $childMgParentInfo.Parent
                $childMgParentName = $htManagementGroupsMgPath.($childMgParentInfo.Parent).DisplayName
            
                $rndom = Get-Random -Minimum 10 -Maximum 750
                start-sleep -Millisecond $rndom

                $currentSubscription = $htAllSubscriptionsFromAPI.($childMgSubId).subDetails
                $subscriptionQuotaId = $currentSubscription.subscriptionPolicies.quotaId
                $subscriptionState = $currentSubscription.state

                #SubscriptionRoles
                $currentTask = "Custom Role definitions '$($childMgSubDisplayName)' ('$childMgSubId')"
                $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).ResourceManagerUrl)subscriptions/$childMgSubId/providers/Microsoft.Authorization/roleDefinitions?api-version=2015-07-01&`$filter=type%20eq%20'CustomRole'"
                $method = "GET"
                $subCustomRoleDefinitions = AzAPICall -uri $uri -method $method -currentTask $currentTask -caller "CustomDataCollection"

                foreach ($subCustomRoleDefinition in $subCustomRoleDefinitions) {
                    if (-not $($htCacheDefinitions).role[$subCustomRoleDefinition.name]) {
                        ($script:htCacheDefinitions).role.$($subCustomRoleDefinition.name) = @{}
                        ($script:htCacheDefinitions).role.$($subCustomRoleDefinition.name).definition = $subCustomRoleDefinition
                    }  
                }

                #PIM RoleAssignmentSchedules
                $currentTask = "Role assignment schedules API '$($childMgSubDisplayName)' ('$childMgSubId')"
                $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).ResourceManagerUrl)subscriptions/$childMgSubId/providers/Microsoft.Authorization/roleAssignmentSchedules?api-version=2020-10-01-preview"
                $method = "GET"
                $roleAssignmentSchedulesFromAPI = AzAPICall -uri $uri -method $method -currentTask $currentTask -caller "CustomDataCollection" -getRoleAssignmentSchedules $true

                if ($roleAssignmentSchedulesFromAPI -eq "ResourceNotOnboarded" -or $roleAssignmentSchedulesFromAPI -eq "TenantNotOnboarded" -or $roleAssignmentSchedulesFromAPI -eq "InvalidResourceType") {
                    #Write-Host "Scope '$($childMgSubDisplayName)' ('$childMgSubId') not onboarded in PIM"
                }
                else {
                    $roleAssignmentSchedules = ($roleAssignmentSchedulesFromAPI.where({ -not [string]::IsNullOrEmpty($_.properties.roleAssignmentScheduleRequestId) }))
                    $roleAssignmentSchedulesCount = $roleAssignmentSchedules.Count
                    #Write-Host "'$($childMgSubDisplayName)' ('$childMgSubId') : $($roleAssignmentSchedulesCount)"
                    if ($roleAssignmentSchedulesCount -gt 0) {
                        
                        $htRoleAssignmentsPIM = @{}
                        foreach ($roleAssignmentSchedule in $roleAssignmentSchedules) {
                            $keyName = "$($roleAssignmentSchedule.properties.scope)-$($roleAssignmentSchedule.properties.expandedProperties.principal.id)-$($roleAssignmentSchedule.properties.expandedProperties.roleDefinition.id)"
                            $htRoleAssignmentsPIM.($keyName) = $roleAssignmentSchedule.properties
                        }
                    }
                }


                #SubscriptionRoleAssignments
                #RoleAssignment API (system metadata e.g. createdOn)
                $currentTask = "Role assignments API '$($childMgSubDisplayName)' ('$childMgSubId')"
                $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).ResourceManagerUrl)subscriptions/$childMgSubId/providers/Microsoft.Authorization/roleAssignments?api-version=2015-07-01"
                $method = "GET"
                $roleAssignmentsFromAPI = AzAPICall -uri $uri -method $method -currentTask $currentTask -caller "CustomDataCollection"

                if ($roleAssignmentsFromAPI.Count -gt 0) {
                    foreach ($roleAssignmentFromAPI in $roleAssignmentsFromAPI) {
                        if (-not $htRoleAssignmentsFromAPIInheritancePrevention.($roleAssignmentFromAPI.id -replace ".*/")) {
                            if (-not ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id)) {
                                $splitAssignment = ($roleAssignmentFromAPI.id).Split('/')
                                ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id) = @{}
                                ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignment = $roleAssignmentFromAPI
                                
                                if ($roleAssignmentFromAPI.properties.scope -like "/subscriptions/*/resourcegroups/*") {
                                    if ($roleAssignmentFromAPI.properties.scope -like "/subscriptions/*/resourcegroups/*" -and $roleAssignmentFromAPI.properties.scope -notlike "/subscriptions/*/resourcegroups/*/providers*") {
                                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentScope = "RG"
                                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentScopeId = "$($splitAssignment[2])/$($splitAssignment[4])"
                                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentScopeName = "$($htSubscriptionsMgPath.($splitAssignment[2]).DisplayName)/$($splitAssignment[4])"
                                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentResourceType = "ResourceGroup"
                                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentResourceName = $splitAssignment[4]
                                    }
                                    if ($roleAssignmentFromAPI.properties.scope -like "/subscriptions/*/resourcegroups/*/providers*") {
                                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentScope = "Res"
                                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentScopeId = "$($splitAssignment[2])/$($splitAssignment[4])/$($splitAssignment[8])"
                                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentScopeName = "$($htSubscriptionsMgPath.($splitAssignment[2]).DisplayName)/$($splitAssignment[4])/$($splitAssignment[8])"
                                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentResourceType = "$($splitAssignment[6])/$($splitAssignment[7])"
                                        ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentResourceName = $splitAssignment[8]
                                    }
                                }
                                else {
                                    $hlperSubName = $htSubscriptionsMgPath.($splitAssignment[2]).DisplayName
                                    ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentScope = "Sub"
                                    ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentScopeId = "/subscriptions/$($splitAssignment[2])"
                                    ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentScopeName = $hlperSubName
                                    ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentResourceType = "Subscription"
                                    ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentResourceName = $hlperSubName
                                }

                                $keyName = "$($roleAssignmentFromAPI.properties.scope)-$($roleAssignmentFromAPI.properties.principalId)-$($roleAssignmentFromAPI.properties.roleDefinitionId)"
                                if ($htRoleAssignmentsPIM.($keyName)) {
                                    ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).assignmentPIMDetails = $htRoleAssignmentsPIM.($keyName)
                                }

                                ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).roleName = ($htCacheDefinitions).role.($roleAssignmentFromAPI.properties.roleDefinitionId -replace ".*/").definition.properties.roleName
                                ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).roleId = $roleAssignmentFromAPI.properties.roleDefinitionId -replace ".*/"
                                ($htCacheAssignments).roleFromAPI.($roleAssignmentFromAPI.id).type = ($htCacheDefinitions).role.($roleAssignmentFromAPI.properties.roleDefinitionId -replace ".*/").definition.properties.type
                            }
                        }
                    }
                }
                    
                $endSubLoopThis = get-date
                $null = $script:customDataCollectionDuration.Add([PSCustomObject]@{ 
                        Type        = "SUB"
                        Id          = $childMgSubId
                        DurationSec = (NEW-TIMESPAN -Start $startSubLoopThis -End $endSubLoopThis).TotalSeconds
                    })

                $null = $script:arrayDataCollectionProgressSub.Add($childMgSubId)
                $progressCount = ($arrayDataCollectionProgressSub).Count
                Write-Host "  $($progressCount)/$($subsToProcessInCustomDataCollectionCount) Subscriptions processed"
        
            } -ThrottleLimit $ThrottleLimit

            $endBatch = get-date
            Write-Host " Batch #$batchCnt processing duration: $((NEW-TIMESPAN -Start $startBatch -End $endBatch).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startBatch -End $endBatch).TotalSeconds) seconds)"
        }
        #[System.GC]::Collect()

        $endSubLoop = get-date
        Write-Host " CustomDataCollection Subscriptions processing duration: $((NEW-TIMESPAN -Start $startSubLoop -End $endSubLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startSubLoop -End $endSubLoop).TotalSeconds) seconds)"
    }
}

#endregion Function_dataCollection

#HTML

#rsu
#region TenantSummary
function summary() {
    Write-Host " Building TenantSummary"

    $htmlTenantSummary = [System.Text.StringBuilder]::new()


    #region SUMMARYServicePrincipals
    [void]$htmlTenantSummary.AppendLine(@"
    <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textServicePrincipal" data-content="Service Principals" /></button>
    <div class="content TenantSummaryContent">
"@)

    if ($cu.Count -gt 0) {
        $startCustPolLoop = get-date
        Write-Host "  processing TenantSummary ServicePrincipals"

        $tfCount = $cu.Count
        $htmlTableId = "TenantSummary_ServicePrincipals"
        $tf = "tf$($htmlTableId)"

        $categoryColorsMax = @("rgb(1,0,103)","rgb(213,255,0)","rgb(255,0,86)","rgb(158,0,142)","rgb(14,76,161)","rgb(255,229,2)","rgb(0,95,57)","rgb(0,255,0)","rgb(149,0,58)","rgb(255,147,126)","rgb(164,36,0)","rgb(0,21,68)","rgb(145,208,203)","rgb(98,14,0)","rgb(107,104,130)","rgb(0,0,255)","rgb(0,125,181)","rgb(106,130,108)","rgb(0,0,0)","rgb(0,174,126)","rgb(194,140,159)","rgb(190,153,112)","rgb(0,143,156)","rgb(95,173,78)","rgb(255,0,0)","rgb(255,0,246)","rgb(255,2,157)","rgb(104,61,59)","rgb(255,116,163)","rgb(150,138,232)","rgb(152,255,82)","rgb(167,87,64)","rgb(1,255,254)","rgb(255,238,232)","rgb(254,137,0)","rgb(189,198,255)","rgb(1,208,255)","rgb(187,136,0)","rgb(117,68,177)","rgb(165,255,210)","rgb(255,166,254)","rgb(119,77,0)","rgb(122,71,130)","rgb(38,52,0)","rgb(0,71,84)","rgb(67,0,44)","rgb(181,0,255)","rgb(255,177,103)","rgb(255,219,102)","rgb(144,251,146)","rgb(126,45,210)","rgb(189,211,147)","rgb(229,111,254)","rgb(222,255,116)","rgb(0,255,120)","rgb(0,155,255)","rgb(0,100,1)","rgb(0,118,255)","rgb(133,169,0)","rgb(0,185,23)","rgb(120,130,49)","rgb(0,255,198)","rgb(255,110,65)","rgb(232,94,190)")

        $groupedByOrg = $cu.SP.where( { $_.SPAppOwnerOrganizationId} ) | group-Object -Property SPAppOwnerOrganizationId

        $arrOrgCounts = @()
        $arrOrgIds = @()
        foreach ($grp in $groupedByOrg | sort-object -property count -Descending){
            $arrOrgCounts += $grp.Count
            $arrOrgIds += $grp.Name
        }
        $OrgCounts = "'{0}'" -f ($arrOrgCounts -join "','")
        $OrgIds = "'{0}'" -f ($arrOrgIds -join "','")
        
        $categoryColorsOrg = ($categoryColorsMax[0..(($arrOrgIds).Count -1)])
        $categoryColorsSeperatedOrg = "'{0}'" -f ($categoryColorsOrg -join "','")

        $groupedBySPType = $cu.SPType | group-Object

        $arrSPTypeCounts = @()
        $arrSPTypes = @()
        foreach ($grp in $groupedBySPType | sort-object -property count -Descending){
            $arrSPTypeCounts += $grp.Count
            $arrSPTypes += $grp.Name
        }
        $SPTypeCounts = "'{0}'" -f ($arrSPTypeCounts -join "','")
        $SPTypes = "'{0}'" -f ($arrSPTypes -join "','")
        
        $categoryColorsSPType = ($categoryColorsMax[($arrOrgIds.Count)..(($arrSPTypes).Count + ($arrOrgIds.Count) -1)])
        $categoryColorsSeperatedSPType = "'{0}'" -f ($categoryColorsSPType -join "','")

        $groupedByMIResourceType = $cu.where( { $_.SPType -like "SP MI*" } ).ManagedIdentity.resourceType | group-Object

        $arrMIResTypeCounts = @()
        $arrMIResTypes = @()
        foreach ($grp in $groupedByMIResourceType | sort-object -property count -Descending){
            $arrMIResTypeCounts += $grp.Count
            $arrMIResTypes += $grp.Name -replace "Microsoft."
        }
        $MIResTypeCounts = "'{0}'" -f ($arrMIResTypeCounts -join "','")
        $MIResTypes = "'{0}'" -f ($arrMIResTypes -join "','")

        $categoryColorsMIResType = ($categoryColorsMax[($arrOrgIds.Count + $arrMIResTypes.Count)..(($arrSPTypes).Count + ($arrOrgIds.Count) + ($arrMIResTypes.Count) -1)])
        $categoryColorsSeperatedMIResType = "'{0}'" -f ($categoryColorsMIResType -join "','")

        $SPAppINT = $cu.where( { $_.SPType -eq "SP APP INT"} )
        

        #sp
        $SPAppINTSPOwnerStatusLabel = "'{0}'" -f ((@('SP without owner', 'SP with owner')) -join "','")
        $SPAppINTWithSPOwnerCount = ($SPAppInt.where( { $_.SPOwners.Count -gt 0 } )).Count
        $SPAppINTWithoutSPOwnerCount = $SPAppINT.Count - $SPAppINTWithSPOwnerCount
        $SPAppINTSPOwnerStatusData = "'{0}'" -f ((@($SPAppINTWithoutSPOwnerCount, $SPAppINTWithSPOwnerCount)) -join "','")
        $categoryColorsSPAppINTSpOwnerStatus = ($categoryColorsMax[0..1])
        $categoryColorsSeperatedSPAppINTSPOwnerStatus = "'{0}'" -f ($categoryColorsSPAppINTSpOwnerStatus -join "','")

        #app
        $SPAppINTAppOwnerStatusLabel = "'{0}'" -f ((@('App without owner', 'App with owner')) -join "','")
        $SPAppINTWithAppOwnerCount = ($SPAppInt.where( { $_.APPAppOwners.Count -gt 0 } )).Count
        $SPAppINTWithoutAppOwnerCount = $SPAppINT.Count - $SPAppINTWithAppOwnerCount
        $SPAppINTAppOwnerStatusData = "'{0}'" -f ((@($SPAppINTWithoutAppOwnerCount, $SPAppINTWithAppOwnerCount)) -join "','")
        $categoryColorsSPAppINTAppOwnerStatus = ($categoryColorsMax[2..3])
        $categoryColorsSeperatedSPAppINTAppOwnerStatus = "'{0}'" -f ($categoryColorsSPAppINTAppOwnerStatus -join "','")

    
        [void]$htmlTenantSummary.AppendLine(@"
        <div class="noFloat">
            <button type="button" class="decollapsible">Charts</button>
        
            <div class="showContent chart-container">
                <div class="chartDiv">
                    <span>Organizations count: <b>$($arrOrgCounts.Count)</b></span>
                    <canvas id="myChart" style="height:150px; width: 250px"></canvas>
                </div>
                <div class="chartDiv">
                    <span>Service Principal types count: <b>$($arrSPTypeCounts.Count)</b></span>
                    <canvas id="myChart2" style="height:150px; width: 250px"></canvas>
                </div>
                <div class="chartDiv">
                    <span>Managed Identity Resource types count: <b>$($arrMIResTypeCounts.Count)</b></span> 
                    <canvas id="myChart3" style="height:150px; width: 250px"></canvas>
                </div>
                <div class="chartDiv">
                    <span>SP APP INT - SP/App Ownership</span> 
                    <canvas id="myChart4" style="height:150px; width: 250px"></canvas>
                </div>
            </div>
        </div>

<script>
var ctx = document.getElementById('myChart');
var myChart = new Chart(ctx, {
    type: 'pie',
                data: {
                    datasets: [
                        {
                            data: [$($OrgCounts)],
                            backgroundColor: [$($categoryColorsSeperatedOrg)],
                            labels: [$($OrgIds)],
                            borderWidth:0.5,
                        }
                    ]
                },
                options: {
                    responsive: false,
                    legend: {
                        display: false,
                    },
                    tooltips: {
                        bodyFontSize: 10,
                        callbacks: {
                            label: function (tooltipItem, data) {
                                var dataset = data.datasets[tooltipItem.datasetIndex];
                                var index = tooltipItem.index;
                                window. datasetitem = tooltipItem.datasetIndex;
                                window.target = dataset.labels[index];
                                return dataset.labels[index] + ': ' + dataset.data[index];
                            }
                        }
                    },

                    onClick: (e) => {
                        if (window. datasetitem == 0){
                            window. targetcolumn = '4'
                        }
                        $($tf).clearFilters();
                        $($tf).setFilterValue((window. targetcolumn), (window.target));
                        $($tf).filter();

                    }
                }
});

var ctx = document.getElementById('myChart2');
var myChart2 = new Chart(ctx, {
    type: 'pie',
                data: {
                    datasets: [
                        {
                            data: [$($SPTypeCounts)],
                            backgroundColor: [$($categoryColorsSeperatedSPType)],
                            labels: [$($SPTypes)],
                            borderWidth:0.5,
                        }
                    ]
                },
                options: {
                    responsive: false,
                    legend: {
                        display: false,
                    },
                    tooltips: {
                        bodyFontSize: 10,
                        callbacks: {
                            label: function (tooltipItem, data) {
                                var dataset = data.datasets[tooltipItem.datasetIndex];
                                var index = tooltipItem.index;
                                window. datasetitem = tooltipItem.datasetIndex;
                                window.target = dataset.labels[index];
                                return dataset.labels[index] + ': ' + dataset.data[index];
                            }
                        }
                    },

                    onClick: (e) => {
                        if (window. datasetitem == 0){
                            window. targetcolumn = '5'
                        }
                        $($tf).clearFilters();
                        $($tf).setFilterValue((window. targetcolumn), (window.target));
                        $($tf).filter();

                    }
                }
});

var ctx = document.getElementById('myChart3');
var myChart3 = new Chart(ctx, {
    type: 'pie',
                data: {
                    datasets: [
                        {
                            data: [$($MIResTypeCounts)],
                            backgroundColor: [$($categoryColorsSeperatedMIResType)],
                            labels: [$($MIResTypes)],
                            borderWidth:0.5,
                        }
                    ]
                },
                options: {
                    responsive: false,
                    legend: {
                        display: false,
                    },
                    tooltips: {
                        bodyFontSize: 10,
                        callbacks: {
                            label: function (tooltipItem, data) {
                                var dataset = data.datasets[tooltipItem.datasetIndex];
                                var index = tooltipItem.index;
                                window. datasetitem = tooltipItem.datasetIndex;
                                window.target = dataset.labels[index];
                                return dataset.labels[index] + ': ' + dataset.data[index];
                            }
                        }
                    },

                    onClick: (e) => {
                        if (window. datasetitem == 0){
                            window. targetcolumn = '11'
                        }
                        $($tf).clearFilters();
                        $($tf).setFilterValue((window. targetcolumn), (window.target));
                        $($tf).filter();

                    }
                }
});

var ctx = document.getElementById('myChart4');
var myChart4 = new Chart(ctx, {
    type: 'pie',
                data: {
                    datasets: [
                        {
                            data: [$($SPAppINTSPOwnerStatusData)],
                            backgroundColor: [$($categoryColorsSeperatedSPAppINTSPOwnerStatus)],
                            labels: [$($SPAppINTSPOwnerStatusLabel)],
                            borderWidth:0.5,
                        },
                        {
                            data: [$($SPAppINTAppOwnerStatusData)],
                            backgroundColor: [$($categoryColorsSeperatedSPAppINTAppOwnerStatus)],
                            labels: [$($SPAppINTAppOwnerStatusLabel)],
                            borderWidth:0.5,
                        }
                    ]
                },
                options: {
                    responsive: false,
                    legend: {
                        display: false,
                    },
                    tooltips: {
                        bodyFontSize: 10,
                        callbacks: {
                            label: function (tooltipItem, data) {
                                var dataset = data.datasets[tooltipItem.datasetIndex];
                                var index = tooltipItem.index;
                                window. datasetitem = tooltipItem.datasetIndex;
                                window.target = dataset.labels[index];
                                if (window.target == 'SP without owner'){
                                    window.target = '[empty]'
                                }
                                if (window.target == 'SP with owner'){
                                    window.target = '[nonempty]'
                                }
                                if (window.target == 'App without owner'){
                                    window.target = '[empty]'
                                }
                                if (window.target == 'App with owner'){
                                    window.target = '[nonempty]'
                                }
                                window.extratarget = 'SP APP INT';
                                return dataset.labels[index] + ': ' + dataset.data[index];
                            }
                        }
                    },

                    onClick: (e) => {
                        window. extratargetcolumn = '5'
                        if (window. datasetitem == 0){
                            window. targetcolumn = '3'
                        }
                        if (window. datasetitem == 1){
                            window. targetcolumn = '9'
                        }
                        $($tf).clearFilters();
                        $($tf).setFilterValue((window. extratargetcolumn), (window.extratarget));
                        $($tf).setFilterValue((window. targetcolumn), (window.target));
                        $($tf).filter();

                    }
                }
});

</script>
"@)



    

        [void]$htmlTenantSummary.AppendLine(@"
<div>
<i class="padlx fa fa-table" aria-hidden="true"></i> Download CSV <a class="externallink" href="#" onclick="download_table_as_csv_semicolon('$htmlTableId');">semicolon</a> | <a class="externallink" href="#" onclick="download_table_as_csv_comma('$htmlTableId');">comma</a>
</div>
<table id="$htmlTableId" class="summaryTable">
<thead>
<tr>
<th>SP object Id</th>
<th>SP application Id</th>
<th>SP displayName</th>
<th>SP Owners</th>
<th>SP App Owner Organization Id</th>
<th>Type</th>
<th>App object Id</th>
<th>App application (client) Id</th>
<th>App displayName</th>
<th>App Owners</th>
<th>AppReg</th>
<th>MI Resource type</th>
<th>MI Resource scope</th>
</tr>
</thead>
<tbody>
"@)

        foreach ($sp in ($cu)) {

            $spType = $sp.SPType

            $appObjectId = ""
            $appId = ""
            $appDisplayName = ""
            if ($sp.APP) {
                $appObjectId = $sp.APP.APPObjectId
                $appId = $sp.APP.APPAppClientId
                $appDisplayName = $sp.APP.APPDisplayName
            }

            $miResourceType = ""
            if ($sp.ManagedIdentity) {
                $miResourceType = $sp.ManagedIdentity.resourceType
            }
            $miResourceScope = ""
            if ($sp.ManagedIdentity) {
                $miResourceScope = $sp.ManagedIdentity.resourceScope
            }

            if ($sp.APP) {
                $hasApp = $true
            }
            else {
                $hasApp = $false
            }

            $spOwners = $null
            if (($sp.SPOwners)) {
                if (($sp.SPOwners.count -gt 0)) {
                    $array = @()
                    foreach ($owner in $sp.SPOwners) {
                        $array += "$($owner.applicability) - $($owner.displayName) $($owner.principalType) $($owner.id)"
                    }
                    $spOwners = "$(($sp.SPOwners).Count) ($($array -join "$CsvDelimiterOpposite "))"
                }
                else {
                    $spOwners = $null
                }
            }   
            
            $appOwners = $null
            if (($sp.APPAppOwners)) {
                if (($sp.APPAppOwners.count -gt 0)) {
                    $array = @()
                    foreach ($owner in $sp.APPAppOwners) {
                        $array += "$($owner.applicability) - $($owner.displayName) $($owner.principalType) $($owner.id)"
                    }
                    $appOwners = "$(($sp.APPAppOwners).Count) ($($array -join "$CsvDelimiterOpposite "))"
                }
                else {
                    $appOwners = $null
                }
            } 
            
            [void]$htmlTenantSummary.AppendLine(@"
<tr>
<td>$($sp.SP.SPObjectId)</td>
<td>$($sp.SP.SPAppId)</td>
<td class="breakwordall">$($sp.SP.SPDisplayName)</td>
<td class="breakwordall">$($spOwners)</td>
<td>$($sp.SP.SPAppOwnerOrganizationId)</td>
<td>$($spType)</td>
<td>$($appObjectId)</td>
<td>$($appId)</td>
<td class="breakwordall">$($appDisplayName)</td>
<td class="breakwordall">$($appOwners)</td>
<td>$($hasApp)</td>
<td class="breakwordall">$($miResourceType)</th>
<td class="breakwordall">$($miResourceScope)</th>
</tr>
"@)
        }

        [void]$htmlTenantSummary.AppendLine(@"
            </tbody>
        </table>

    <script>
        var tfConfig4$htmlTableId = {
            base_path: 'https://www.azadvertizer.net/azgovvizv4/tablefilter/', rows_counter: true,       
"@)      
        if ($tfCount -gt 10) {
            $spectrum = "10, $tfCount"
            if ($tfCount -gt 50) {
                $spectrum = "10, 25, 50, $tfCount"
            }        
            if ($tfCount -gt 100) {
                $spectrum = "10, 30, 50, 100, $tfCount"
            }
            if ($tfCount -gt 500) {
                $spectrum = "10, 30, 50, 100, 250, $tfCount"
            }
            if ($tfCount -gt 1000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, $tfCount"
            }
            if ($tfCount -gt 2000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, $tfCount"
            }
            if ($tfCount -gt 3000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, 3000, $tfCount"
            }
            [void]$htmlTenantSummary.AppendLine(@"
paging: {results_per_page: ['Records: ', [$spectrum]]},/*state: {types: ['local_storage'], filters: true, page_number: true, page_length: true, sort: true},*/
"@)
        }
        [void]$htmlTenantSummary.AppendLine(@"
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true,
col_widths: ['7%', '7%', '11%', '10%', '7%', '7%', '7%', '7%', '11%', '10%', '5%', '4%', '7%'],            
            col_4: 'select',
            col_5: 'multiple',
            col_10: 'select',
            locale: 'en-US',
            col_types: [
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring'
            ],
extensions: [{ name: 'sort' }]
        };
        var $($tf) = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
        $($tf).init();
    </script>
"@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@"
    <p><i class="padlx fa fa-ban" aria-hidden="true"></i> <span class="valignMiddle">$($cu.Count) Service Principals</span></p>
"@)
    }

    [void]$htmlTenantSummary.AppendLine(@"
    </div>
"@)
    

    $endCustPolLoop = get-date
    Write-Host "   processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipals

    #region SUMMARYServicePrincipalOwners

    $startCustPolLoop = get-date
    Write-Host "  processing TenantSummary ServicePrincipal Owners"

    if ($cu.SPOwners.Count -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup" data-content="Service Principal Owners" /></button>
        <div class="content TenantSummaryContent">
"@)

        $tfCount = $cu.SPOwners.Count
        $htmlTableId = "TenantSummary_ServicePrincipalOwners"
        [void]$htmlTenantSummary.AppendLine(@"
<i class="padlxx fa fa-table" aria-hidden="true"></i> Download CSV <a class="externallink" href="#" onclick="download_table_as_csv_semicolon('$htmlTableId');">semicolon</a> | <a class="externallink" href="#" onclick="download_table_as_csv_comma('$htmlTableId');">comma</a>
<table id="$htmlTableId" class="summaryTable">
<thead>
<tr>
<th>SP object Id</th>
<th>SP application Id</th>
<th>SP displayName</th>
<th>SP App Owner Organization Id</th>
<th>Type</th>
<th>Owner DisplayName</th>
<th>Owner PrincipalType</th>
<th>Owner Id</th>
<th>Owner Applicability</th>
<th>Owner OwnedBy</th>
</tr>
</thead>
<tbody>
"@)

        foreach ($sp in ($cu.where( { $_.SPOwners.Count -gt 0 } ))) {

            $spType = $sp.SPType
            $ownerOwnedBy = ""
            foreach ($ownerinfo in $sp.SPOwners){
                $hlpArrayDirect = @()
                $hlpArrayInDirect = @()
                $ownerDisplayName = "$($ownerinfo.displayName)"
                $ownerPrincipalType = "$($ownerinfo.principalType)"
                $ownerId = "$($ownerinfo.id)"
                $ownerApplicability = $($ownerinfo.applicability)

                if ($ownerPrincipalType -like "SP*"){
                    $ownedBy = ($htSPOwnersFinal.($ownerinfo.id))
                    $ownedByCount = $ownedBy.Count
                    if ($ownedByCount -gt 0){
                        foreach ($owned in $ownedBy){
                            if ($owned.applicability -eq "direct"){
                                $hlpArrayDirect += "$($owned.displayName) $($owned.principalType)"
                            }
                            if ($owned.applicability -eq "indirect"){
                                $hlpArrayInDirect += "$($owned.displayName) $($owned.principalType)"
                            }
                        }
                        if ($hlpArrayDirect.Count -gt 0 -and $hlpArrayInDirect.Count -gt 0){
                            $ownerOwnedBy = "direct $($hlpArrayDirect.Count) [$($hlpArrayDirect -Join ", ")]<br> indirect $($hlpArrayInDirect.Count) [$($hlpArrayInDirect -Join ", ")]"
                        }
                        else{
                            if ($hlpArrayDirect.Count -gt 0){
                                $ownerOwnedBy = "direct $($hlpArrayDirect.Count) [$($hlpArrayDirect -Join ", ")]"
                            }
                        }
                    }
                    else{
                        $ownerOwnedBy = ""
                    }
                }
                else{
                    $ownerOwnedBy = ""
                }        
            
            [void]$htmlTenantSummary.AppendLine(@"
<tr>
<td>$($sp.SP.SPObjectId)</td>
<td>$($sp.SP.SPAppId)</td>
<td class="breakwordall">$($sp.SP.SPDisplayName)</td>
<td>$($sp.SP.SPAppOwnerOrganizationId)</td>
<td>$($spType)</td>
<td class="breakwordall">$($ownerDisplayName)</td>
<td>$($ownerPrincipalType)</td>
<td>$($ownerId)</td>
<td>$($ownerApplicability)</td>
<td class="breakwordall">$($ownerOwnedBy)</td>
</tr>
"@)
            }
        }

        [void]$htmlTenantSummary.AppendLine(@"
            </tbody>
        </table>

    <script>
        var tfConfig4$htmlTableId = {
            base_path: 'https://www.azadvertizer.net/azgovvizv4/tablefilter/', rows_counter: true,       
"@)      
        if ($tfCount -gt 10) {
            $spectrum = "10, $tfCount"
            if ($tfCount -gt 50) {
                $spectrum = "10, 25, 50, $tfCount"
            }        
            if ($tfCount -gt 100) {
                $spectrum = "10, 30, 50, 100, $tfCount"
            }
            if ($tfCount -gt 500) {
                $spectrum = "10, 30, 50, 100, 250, $tfCount"
            }
            if ($tfCount -gt 1000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, $tfCount"
            }
            if ($tfCount -gt 2000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, $tfCount"
            }
            if ($tfCount -gt 3000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, 3000, $tfCount"
            }
            [void]$htmlTenantSummary.AppendLine(@"
paging: {results_per_page: ['Records: ', [$spectrum]]},/*state: {types: ['local_storage'], filters: true, page_number: true, page_length: true, sort: true},*/
"@)
        }
        [void]$htmlTenantSummary.AppendLine(@"
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true,
col_widths: ['10%', '10%', '11%', '10%', '10%', '11%', '10%', '10%', '7%', '11%'],            
            col_3: 'select',
            col_4: 'multiple',
            col_6: 'multiple',
            col_8: 'select',
            locale: 'en-US',
            col_types: [
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring'
            ],
extensions: [{ name: 'sort' }]
        };
        var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
        tf.init();
    </script>
"@)

[void]$htmlTenantSummary.AppendLine(@"
</div>
"@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@"
    <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup fontGrey" data-content="Service Principal Owners" /></button>
"@)
    }
    
    $endCustPolLoop = get-date
    Write-Host "   processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalOwners

    #region SUMMARYApplicationOwners
    $startCustPolLoop = get-date
    Write-Host "  processing TenantSummary Application Owners"

    if ($cu.APPAppOwners.Count -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup" data-content="Application Owners" /></button>
        <div class="content TenantSummaryContent">
"@)

        $tfCount = $cu.APPAppOwners.Count
        $htmlTableId = "TenantSummary_ApplicationOwners"
        [void]$htmlTenantSummary.AppendLine(@"
<i class="padlxx fa fa-table" aria-hidden="true"></i> Download CSV <a class="externallink" href="#" onclick="download_table_as_csv_semicolon('$htmlTableId');">semicolon</a> | <a class="externallink" href="#" onclick="download_table_as_csv_comma('$htmlTableId');">comma</a>
<table id="$htmlTableId" class="summaryTable">
<thead>
<tr>
<th>App object Id</th>
<th>App application Id</th>
<th>App displayName</th>
<th>SP App Owner Organization Id</th>
<th>Type</th>
<th>Owner DisplayName</th>
<th>Owner PrincipalType</th>
<th>Owner Id</th>
<th>Owner Applicability</th>
<th>Owner OwnedBy</th>
</tr>
</thead>
<tbody>
"@)

        foreach ($sp in ($cu.where( { $_.APPAppOwners.Count -gt 0 } ))) {

            $spType = $sp.SPType

            $ownerOwnedBy = ""
            foreach ($ownerinfo in $sp.APPAppOwners){
                $hlpArrayDirect = @()
                $hlpArrayInDirect = @()
                $ownerDisplayName = "$($ownerinfo.displayName)"
                $ownerPrincipalType = "$($ownerinfo.principalType)"
                $ownerId = "$($ownerinfo.id)"
                $ownerApplicability = $($ownerinfo.applicability)

                if ($ownerPrincipalType -like "SP*"){
                    $ownedBy = ($htSPOwnersFinal.($ownerinfo.id))
                    $ownedByCount = $ownedBy.Count
                    if ($ownedByCount -gt 0){
                        foreach ($owned in $ownedBy){
                            if ($owned.applicability -eq "direct"){
                                $hlpArrayDirect += "$($owned.displayName) $($owned.principalType)"
                            }
                            if ($owned.applicability -eq "indirect"){
                                $hlpArrayInDirect += "$($owned.displayName) $($owned.principalType)"
                            }
                        }
                        if ($hlpArrayDirect.Count -gt 0 -and $hlpArrayInDirect.Count -gt 0){
                            $ownerOwnedBy = "direct $($hlpArrayDirect.Count) [$($hlpArrayDirect -Join ", ")]<br> indirect $($hlpArrayInDirect.Count) [$($hlpArrayInDirect -Join ", ")]"
                        }
                        else{
                            if ($hlpArrayDirect.Count -gt 0){
                                $ownerOwnedBy = "direct $($hlpArrayDirect.Count) [$($hlpArrayDirect -Join ", ")]"
                            }
                        }
                    }
                    else{
                        $ownerOwnedBy = ""
                    }
                }
                else{
                    $ownerOwnedBy = ""
                }

            [void]$htmlTenantSummary.AppendLine(@"
<tr>
<td>$($sp.APP.APPObjectId)</td>
<td>$($sp.APP.APPAppClientId)</td>
<td class="breakwordall">$($sp.APP.APPDisplayName)</td>
<td>$($sp.SP.SPAppOwnerOrganizationId)</td>
<td>$($spType)</td>
<td class="breakwordall">$($ownerDisplayName)</td>
<td>$($ownerPrincipalType)</td>
<td>$($ownerId)</td>
<td>$($ownerApplicability)</td>
<td class="breakwordall">$($ownerOwnedBy)</td>
</tr>
"@)
            }
        }

        [void]$htmlTenantSummary.AppendLine(@"
            </tbody>
        </table>

    <script>
        var tfConfig4$htmlTableId = {
            base_path: 'https://www.azadvertizer.net/azgovvizv4/tablefilter/', rows_counter: true,       
"@)      
        if ($tfCount -gt 10) {
            $spectrum = "10, $tfCount"
            if ($tfCount -gt 50) {
                $spectrum = "10, 25, 50, $tfCount"
            }        
            if ($tfCount -gt 100) {
                $spectrum = "10, 30, 50, 100, $tfCount"
            }
            if ($tfCount -gt 500) {
                $spectrum = "10, 30, 50, 100, 250, $tfCount"
            }
            if ($tfCount -gt 1000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, $tfCount"
            }
            if ($tfCount -gt 2000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, $tfCount"
            }
            if ($tfCount -gt 3000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, 3000, $tfCount"
            }
            [void]$htmlTenantSummary.AppendLine(@"
paging: {results_per_page: ['Records: ', [$spectrum]]},/*state: {types: ['local_storage'], filters: true, page_number: true, page_length: true, sort: true},*/
"@)
        }
        [void]$htmlTenantSummary.AppendLine(@"
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true,
col_widths: ['10%', '10%', '11%', '10%', '10%', '11%', '10%', '10%', '7%', '11%'],            
            col_3: 'select',
            col_4: 'multiple',
            col_6: 'multiple',
            col_8: 'select',
            locale: 'en-US',
            col_types: [
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring'
            ],
extensions: [{ name: 'sort' }]
        };
        var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
        tf.init();
    </script>
"@)

[void]$htmlTenantSummary.AppendLine(@"
</div>
"@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup fontGrey" data-content="Application Owners" /></button>
"@)
    }
    

    $endCustPolLoop = get-date
    Write-Host "   processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYApplicationOwners

    #region SUMMARYServicePrincipalOwnedObjects
    $startCustPolLoop = get-date
    Write-Host "  processing TenantSummary ServicePrincipal Owned Objects"

    if ($cu.SPOwnedObjects.Count -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup" data-content="Service Principal Owned Objects" /></button>
        <div class="content TenantSummaryContent">
"@)

        $tfCount = $cu.SPOwnedObjects.Count
        $htmlTableId = "TenantSummary_ServicePrincipalOwnedObjects"
        [void]$htmlTenantSummary.AppendLine(@"
<i class="padlxx fa fa-table" aria-hidden="true"></i> Download CSV <a class="externallink" href="#" onclick="download_table_as_csv_semicolon('$htmlTableId');">semicolon</a> | <a class="externallink" href="#" onclick="download_table_as_csv_comma('$htmlTableId');">comma</a>
<table id="$htmlTableId" class="summaryTable">
<thead>
<tr>
<th>SP object Id</th>
<th>SP application Id</th>
<th>SP displayName</th>
<th>SP App Owner Organization Id</th>
<th>Type</th>
<th>Owned Objects</th>

</tr>
</thead>
<tbody>
"@)

        foreach ($sp in ($cu.where( { $_.SPOwnedObjects.Count -gt 0 } ))) {

            $spType = $sp.SPType
            $arrayOwnedObjects = @()
            foreach ($ownedObject in $sp.SPOwnedObjects | Sort-Object -Property type, typeDetailed, displayName){
                $arrayOwnedObjects += "$($ownedObject.displayName) <b>$($ownedObject.type)</b> $($ownedObject.objectId)"
            }
                  
            [void]$htmlTenantSummary.AppendLine(@"
<tr>
<td>$($sp.SP.SPObjectId)</td>
<td>$($sp.SP.SPAppId)</td>
<td class="breakwordall">$($sp.SP.SPDisplayName)</td>
<td>$($sp.SP.SPAppOwnerOrganizationId)</td>
<td>$($spType)</td>
<td>$($arrayOwnedObjects -join ", ")</td>
</tr>
"@)
            
        }

        [void]$htmlTenantSummary.AppendLine(@"
            </tbody>
        </table>

    <script>
        var tfConfig4$htmlTableId = {
            base_path: 'https://www.azadvertizer.net/azgovvizv4/tablefilter/', rows_counter: true,       
"@)      
        if ($tfCount -gt 10) {
            $spectrum = "10, $tfCount"
            if ($tfCount -gt 50) {
                $spectrum = "10, 25, 50, $tfCount"
            }        
            if ($tfCount -gt 100) {
                $spectrum = "10, 30, 50, 100, $tfCount"
            }
            if ($tfCount -gt 500) {
                $spectrum = "10, 30, 50, 100, 250, $tfCount"
            }
            if ($tfCount -gt 1000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, $tfCount"
            }
            if ($tfCount -gt 2000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, $tfCount"
            }
            if ($tfCount -gt 3000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, 3000, $tfCount"
            }
            [void]$htmlTenantSummary.AppendLine(@"
paging: {results_per_page: ['Records: ', [$spectrum]]},/*state: {types: ['local_storage'], filters: true, page_number: true, page_length: true, sort: true},*/
"@)
        }
        [void]$htmlTenantSummary.AppendLine(@"
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true,
col_widths: ['10%', '10%', '11%', '10%', '7%', '52%'],            
            col_3: 'select',
            col_4: 'multiple',
            locale: 'en-US',
            col_types: [
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring'
            ],
extensions: [{ name: 'sort' }]
        };
        var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
        tf.init();
    </script>
"@)

[void]$htmlTenantSummary.AppendLine(@"
</div>
"@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@"
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup fontGrey" data-content="Service Principal Owned Objects" /></button>
"@)
    }    

    $endCustPolLoop = get-date
    Write-Host "   processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalOwnedObjects

    #region SUMMARYServicePrincipalsAADRoleAssignments
    $startCustPolLoop = get-date
    Write-Host "  processing TenantSummary ServicePrincipalsAADRoleAssignments"
    $servicePrincipalsAADRoleAssignments = $cu.where( { $_.SPAADRoleAssignments.Count -ne 0 } )
    $servicePrincipalsAADRoleAssignmentsCount = $servicePrincipalsAADRoleAssignments.Count
    if ($servicePrincipalsAADRoleAssignmentsCount -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textAADRoleAssignment" data-content="Service Principals AAD RoleAssignments" /></button>
        <div class="content TenantSummaryContent">
"@)

        $tfCount = $servicePrincipalsAADRoleAssignmentsCount
        $htmlTableId = "TenantSummary_ServicePrincipalsAADRoleAssignments"
        [void]$htmlTenantSummary.AppendLine(@"
<i class="padlxx fa fa-table" aria-hidden="true"></i> Download CSV <a class="externallink" href="#" onclick="download_table_as_csv_semicolon('$htmlTableId');">semicolon</a> | <a class="externallink" href="#" onclick="download_table_as_csv_comma('$htmlTableId');">comma</a>
<table id="$htmlTableId" class="summaryTable">
<thead>
<tr>
<th>SP object Id</th>
<th>SP application Id</th>
<th>SP displayName</th>
<th>SP type</th>
<th>SP App Owner Organization Id</th>
<th>SP AAD RoleAssignments</th>
</tr>
</thead>
<tbody>
"@)

        foreach ($sp in ($servicePrincipalsAADRoleAssignments)) {

            $spType = $sp.SPType

            $spAADRoleAssignments = $null
            if (($sp.SPAADRoleAssignments)) {
                if (($sp.SPAADRoleAssignments.count -gt 0)) {
                    $array = @()
                    foreach ($ra in $sp.SPAADRoleAssignments) {
                        if ($ra.scopeDetail){
                            $array += "$($ra.roleDefinitionName) (scope: $($ra.scopeDetail))"
                        }
                        else{
                            $array += "$($ra.roleDefinitionName)"
                        }
                    }
                    $spAADRoleAssignments = "$(($sp.SPAADRoleAssignments).Count) ($($array -join "$CsvDelimiterOpposite "))"

                }
                else {
                    $spAADRoleAssignments = $null
                }
            }
        
            [void]$htmlTenantSummary.AppendLine(@"
<tr>
<td>$($sp.SP.SPObjectId)</td>
<td>$($sp.SP.SPappId)</td>
<td class="breakwordall">$($sp.SP.SPdisplayName)</td>
<td>$spType</td>
<td>$($sp.SP.SPappOwnerOrganizationId)</td>
<td class="breakwordall">$($spAADRoleAssignments)</td>
</tr>
"@)
        }

        [void]$htmlTenantSummary.AppendLine(@"
            </tbody>
        </table>

    <script>
        var tfConfig4$htmlTableId = {
            base_path: 'https://www.azadvertizer.net/azgovvizv4/tablefilter/', rows_counter: true,       
"@)      
        if ($tfCount -gt 10) {
            $spectrum = "10, $tfCount"
            if ($tfCount -gt 50) {
                $spectrum = "10, 25, 50, $tfCount"
            }        
            if ($tfCount -gt 100) {
                $spectrum = "10, 30, 50, 100, $tfCount"
            }
            if ($tfCount -gt 500) {
                $spectrum = "10, 30, 50, 100, 250, $tfCount"
            }
            if ($tfCount -gt 1000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, $tfCount"
            }
            if ($tfCount -gt 2000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, $tfCount"
            }
            if ($tfCount -gt 3000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, 3000, $tfCount"
            }
            [void]$htmlTenantSummary.AppendLine(@"
paging: {results_per_page: ['Records: ', [$spectrum]]},/*state: {types: ['local_storage'], filters: true, page_number: true, page_length: true, sort: true},*/
"@)
        }
        [void]$htmlTenantSummary.AppendLine(@"
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true,
col_widths: ['10%', '10%', '10%', '10%', '10%', '50%'],            
            locale: 'en-US',
            col_3: 'multiple',
            col_4: 'select',
            col_types: [
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring'
            ],
extensions: [{ name: 'sort' }]
        };
        var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
        tf.init();
    </script>
"@)

[void]$htmlTenantSummary.AppendLine(@"
</div>
"@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@"
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textAADRoleAssignment fontGrey" data-content="Service Principals AAD RoleAssignments" /></button>
"@)
    }

    $endCustPolLoop = get-date
    Write-Host "   processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsAADRoleAssignments

    #region SUMMARYServicePrincipalsAADRoleAssignedOn
    $startCustPolLoop = get-date
    Write-Host "  processing TenantSummary ServicePrincipalsAADRoleAssignedOn"
    $servicePrincipalsAADRoleAssignedOn = $cu.where( { $_.SPAAADRoleAssignedOn.Count -ne 0 } )
    $servicePrincipalsAADRoleAssignedOnCount = $servicePrincipalsAADRoleAssignedOn.Count
    if ($servicePrincipalsAADRoleAssignedOnCount -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textAADRoleAssignment" data-content="Service Principals AAD RoleAssignedOn" /></button>
        <div class="content TenantSummaryContent">
"@)

        $tfCount = $servicePrincipalsAADRoleAssignedOnCount
        $htmlTableId = "TenantSummary_ServicePrincipalsAADRoleAssignedOn"
        [void]$htmlTenantSummary.AppendLine(@"
<i class="padlxx fa fa-table" aria-hidden="true"></i> Download CSV <a class="externallink" href="#" onclick="download_table_as_csv_semicolon('$htmlTableId');">semicolon</a> | <a class="externallink" href="#" onclick="download_table_as_csv_comma('$htmlTableId');">comma</a>
<table id="$htmlTableId" class="summaryTable">
<thead>
<tr>
<th>SP object Id</th>
<th>SP application Id</th>
<th>SP displayName</th>
<th>SP type</th>
<th>SP App Owner Organization Id</th>
<th>SP AAD RoleAssignedOn</th>
</tr>
</thead>
<tbody>
"@)

        foreach ($sp in ($servicePrincipalsAADRoleAssignedOn)) {

            $spType = $sp.SPType

            $SPAAADRoleAssignedOn = $null
            if (($sp.SPAAADRoleAssignedOn)) {
                if (($sp.SPAAADRoleAssignedOn.count -gt 0)) {
                    $array = @()
                    foreach ($rao in $sp.SPAAADRoleAssignedOn) {
                        $array += "$($rao.roleName) ($($rao.roleId)) on $($rao.principalDisplayName) - $($rao.principalType) ($($rao.principalId))"
                    }
                    $SPAAADRoleAssignedOn = "$(($sp.SPAAADRoleAssignedOn).Count) ($($array -join "$CsvDelimiterOpposite "))"

                }
                else {
                    $SPAAADRoleAssignedOn = $null
                }
            }
        
            [void]$htmlTenantSummary.AppendLine(@"
<tr>
<td>$($sp.SP.SPObjectId)</td>
<td>$($sp.SP.SPappId)</td>
<td class="breakwordall">$($sp.SP.SPdisplayName)</td>
<td>$spType</td>
<td>$($sp.SP.SPappOwnerOrganizationId)</td>
<td class="breakwordall">$($SPAAADRoleAssignedOn)</td>
</tr>
"@)
        }

        [void]$htmlTenantSummary.AppendLine(@"
            </tbody>
        </table>

    <script>
        var tfConfig4$htmlTableId = {
            base_path: 'https://www.azadvertizer.net/azgovvizv4/tablefilter/', rows_counter: true,       
"@)      
        if ($tfCount -gt 10) {
            $spectrum = "10, $tfCount"
            if ($tfCount -gt 50) {
                $spectrum = "10, 25, 50, $tfCount"
            }        
            if ($tfCount -gt 100) {
                $spectrum = "10, 30, 50, 100, $tfCount"
            }
            if ($tfCount -gt 500) {
                $spectrum = "10, 30, 50, 100, 250, $tfCount"
            }
            if ($tfCount -gt 1000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, $tfCount"
            }
            if ($tfCount -gt 2000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, $tfCount"
            }
            if ($tfCount -gt 3000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, 3000, $tfCount"
            }
            [void]$htmlTenantSummary.AppendLine(@"
paging: {results_per_page: ['Records: ', [$spectrum]]},/*state: {types: ['local_storage'], filters: true, page_number: true, page_length: true, sort: true},*/
"@)
        }
        [void]$htmlTenantSummary.AppendLine(@"
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true,
col_widths: ['10%', '10%', '10%', '10%', '10%', '50%'],            
            locale: 'en-US',
            col_3: 'multiple',
            col_4: 'select',
            col_types: [
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring'
            ],
extensions: [{ name: 'sort' }]
        };
        var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
        tf.init();
    </script>
"@)

[void]$htmlTenantSummary.AppendLine(@"
</div>
"@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@"
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textAADRoleAssignment fontGrey" data-content="Service Principals AAD RoleAssignedOn" /></button>
"@)
    }
    
    $endCustPolLoop = get-date
    Write-Host "   processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsAADRoleAssignedOn

    #region SUMMARYApplicationsAADRoleAssignedOn
    $startCustPolLoop = get-date
    Write-Host "  processing TenantSummary ApplicationsAADRoleAssignedOn"
    $applicationsAADRoleAssignedOn = $cu.where( { $_.APPAAADRoleAssignedOn.Count -ne 0 } )
    $applicationsAADRoleAssignedOnCount = $applicationsAADRoleAssignedOn.Count
    if ($applicationsAADRoleAssignedOnCount -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textAADRoleAssignment" data-content="Appications AAD RoleAssignedOn" /></button>
        <div class="content TenantSummaryContent">
"@)

        $tfCount = $applicationsAADRoleAssignedOnCount
        $htmlTableId = "TenantSummary_ApplicationsAADRoleAssignedOn"
        [void]$htmlTenantSummary.AppendLine(@"
<i class="padlxx fa fa-table" aria-hidden="true"></i> Download CSV <a class="externallink" href="#" onclick="download_table_as_csv_semicolon('$htmlTableId');">semicolon</a> | <a class="externallink" href="#" onclick="download_table_as_csv_comma('$htmlTableId');">comma</a>
<table id="$htmlTableId" class="summaryTable">
<thead>
<tr>
<th>App object Id</th>
<th>App application Id</th>
<th>App displayName</th>
<th>type</th>
<th>SP App Owner Organization Id</th>
<th>SP AAD RoleAssignedOn</th>
</tr>
</thead>
<tbody>
"@)

        foreach ($sp in ($applicationsAADRoleAssignedOn)) {

            $spType = $sp.SPType

            $APPAAADRoleAssignedOn = $null
            if (($sp.APPAAADRoleAssignedOn)) {
                if (($sp.APPAAADRoleAssignedOn.count -gt 0)) {
                    $array = @()
                    foreach ($rao in $sp.APPAAADRoleAssignedOn) {
                        $array += "$($rao.roleName) ($($rao.roleId)) on $($rao.principalDisplayName) - $($rao.principalType) ($($rao.principalId))"
                    }
                    $APPAAADRoleAssignedOn = "$(($sp.APPAAADRoleAssignedOn).Count) ($($array -join "$CsvDelimiterOpposite "))"

                }
                else {
                    $APPAAADRoleAssignedOn = $null
                }
            }
        
            [void]$htmlTenantSummary.AppendLine(@"
<tr>
<td>$($sp.APP.APPObjectId)</td>
<td>$($sp.APP.APPAppClientId)</td>
<td class="breakwordall">$($sp.APP.APPDisplayName)</td>
<td>$spType</td>
<td>$($sp.SP.SPappOwnerOrganizationId)</td>
<td class="breakwordall">$($APPAAADRoleAssignedOn)</td>
</tr>
"@)
        }

        [void]$htmlTenantSummary.AppendLine(@"
            </tbody>
        </table>

    <script>
        var tfConfig4$htmlTableId = {
            base_path: 'https://www.azadvertizer.net/azgovvizv4/tablefilter/', rows_counter: true,       
"@)      
        if ($tfCount -gt 10) {
            $spectrum = "10, $tfCount"
            if ($tfCount -gt 50) {
                $spectrum = "10, 25, 50, $tfCount"
            }        
            if ($tfCount -gt 100) {
                $spectrum = "10, 30, 50, 100, $tfCount"
            }
            if ($tfCount -gt 500) {
                $spectrum = "10, 30, 50, 100, 250, $tfCount"
            }
            if ($tfCount -gt 1000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, $tfCount"
            }
            if ($tfCount -gt 2000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, $tfCount"
            }
            if ($tfCount -gt 3000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, 3000, $tfCount"
            }
            [void]$htmlTenantSummary.AppendLine(@"
paging: {results_per_page: ['Records: ', [$spectrum]]},/*state: {types: ['local_storage'], filters: true, page_number: true, page_length: true, sort: true},*/
"@)
        }
        [void]$htmlTenantSummary.AppendLine(@"
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true,
col_widths: ['10%', '10%', '10%', '10%', '10%', '50%'],            
            locale: 'en-US',
            col_3: 'multiple',
            col_4: 'select',
            col_types: [
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring'
            ],
extensions: [{ name: 'sort' }]
        };
        var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
        tf.init();
    </script>
"@)

[void]$htmlTenantSummary.AppendLine(@"
</div>
"@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@"
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textAADRoleAssignment fontGrey" data-content="Appications AAD RoleAssignedOn" /></button>
"@)
    }

    $endCustPolLoop = get-date
    Write-Host "   processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsAADRoleAssignedOn

    #region SUMMARYServicePrincipalsAppRoleAssignments
    $startCustPolLoop = get-date
    Write-Host "  processing TenantSummary ServicePrincipalsAppRoleAssignments"
    $servicePrincipalsAppRoleAssignments = $cu.where( { $_.SPAppRoleAssignments.Count -ne 0 } )
    $servicePrincipalsAppRoleAssignmentsCount = $servicePrincipalsAppRoleAssignments.Count
    if ($servicePrincipalsAppRoleAssignmentsCount -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textAPIPermissions" data-content="Service Principals App RoleAssignments (API permissions Application)" /></button>
        <div class="content TenantSummaryContent">
"@)

        $tfCount = $servicePrincipalsAppRoleAssignmentsCount
        $htmlTableId = "TenantSummary_ServicePrincipalsAppRoleAssignments"
        [void]$htmlTenantSummary.AppendLine(@"
<i class="padlxx fa fa-table" aria-hidden="true"></i> Download CSV <a class="externallink" href="#" onclick="download_table_as_csv_semicolon('$htmlTableId');">semicolon</a> | <a class="externallink" href="#" onclick="download_table_as_csv_comma('$htmlTableId');">comma</a>
<table id="$htmlTableId" class="summaryTable">
<thead>
<tr>
<th>SP object Id</th>
<th>SP application Id</th>
<th>SP displayName</th>
<th>SP type</th>
<th>SP App Owner Organization Id</th>
<th>SP App RoleAssignments</th>
</tr>
</thead>
<tbody>
"@)

        foreach ($sp in ($servicePrincipalsAppRoleAssignments)) {

            $spType = $sp.SPType

            $SPAppRoleAssignments = $null
            if (($sp.SPAppRoleAssignments)) {
                if (($sp.SPAppRoleAssignments.count -gt 0)) {
                    $array = @()
                    foreach ($approleAss in $sp.SPAppRoleAssignments) {
                        $array += "$($approleAss.AppRoleAssignmentResourceDisplayName) ($($approleAss.AppRolePermission))"
                    }
                    $SPAppRoleAssignments = "$(($sp.SPAppRoleAssignments).Count) ($($array -join "$CsvDelimiterOpposite "))"
                }
                else {
                    $SPAppRoleAssignments = $null
                }
            }
                
            [void]$htmlTenantSummary.AppendLine(@"
<tr>
<td>$($sp.SP.SPObjectId)</td>
<td>$($sp.SP.SPappId)</td>
<td class="breakwordall">$($sp.SP.SPdisplayName)</td>
<td>$spType</td>
<td>$($sp.SP.SPappOwnerOrganizationId)</td>
<td class="breakwordall">$($SPAppRoleAssignments)</td>
</tr>
"@)
        }

        [void]$htmlTenantSummary.AppendLine(@"
            </tbody>
        </table>

    <script>
        var tfConfig4$htmlTableId = {
            base_path: 'https://www.azadvertizer.net/azgovvizv4/tablefilter/', rows_counter: true,       
"@)      
        if ($tfCount -gt 10) {
            $spectrum = "10, $tfCount"
            if ($tfCount -gt 50) {
                $spectrum = "10, 25, 50, $tfCount"
            }        
            if ($tfCount -gt 100) {
                $spectrum = "10, 30, 50, 100, $tfCount"
            }
            if ($tfCount -gt 500) {
                $spectrum = "10, 30, 50, 100, 250, $tfCount"
            }
            if ($tfCount -gt 1000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, $tfCount"
            }
            if ($tfCount -gt 2000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, $tfCount"
            }
            if ($tfCount -gt 3000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, 3000, $tfCount"
            }
            [void]$htmlTenantSummary.AppendLine(@"
paging: {results_per_page: ['Records: ', [$spectrum]]},/*state: {types: ['local_storage'], filters: true, page_number: true, page_length: true, sort: true},*/
"@)
        }
        [void]$htmlTenantSummary.AppendLine(@"
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true,
col_widths: ['10%', '10%', '10%', '10%', '10%', '50%'],            
            locale: 'en-US',
            col_3: 'multiple',
            col_4: 'select',
            col_types: [
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring'
            ],
extensions: [{ name: 'sort' }]
        };
        var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
        tf.init();
    </script>
"@)

[void]$htmlTenantSummary.AppendLine(@"
</div>
"@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@"
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textAPIPermissions fontGrey" data-content="Service Principals App RoleAssignments (API permissions Application)" /></button>
"@)
    }
    
    $endCustPolLoop = get-date
    Write-Host "   processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsAppRoleAssignments

    #region SUMMARYServicePrincipalsAppRoleAssignedTo
    $startCustPolLoop = get-date
    Write-Host "  processing TenantSummary ServicePrincipalsAppRoleAssignedTo"
    $servicePrincipalsAppRoleAssignedTo = $cu.where( { $_.SPAppRoleAssignedTo.Count -ne 0 -and ($_.SPAppRoleAssignedTo.principalType -eq "User" -or $_.SPAppRoleAssignedTo.principalType -eq "Group") } )

    #$servicePrincipalsAppRoleAssignedTo = $cu.where( { $_.SPAppRoleAssignedTo.Count -ne 0} )
    $servicePrincipalsAppRoleAssignedToCount = $servicePrincipalsAppRoleAssignedTo.Count
    if ($servicePrincipalsAppRoleAssignedToCount -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup" data-content="Service Principals App RoleAssignedTo (Users and Groups)" /></button>
        <div class="content TenantSummaryContent">
"@)

        $tfCount = $servicePrincipalsAppRoleAssignedToCount
        $htmlTableId = "TenantSummary_ServicePrincipalsAppRoleAssignedTo"
        [void]$htmlTenantSummary.AppendLine(@"
<i class="padlxx fa fa-table" aria-hidden="true"></i> Download CSV <a class="externallink" href="#" onclick="download_table_as_csv_semicolon('$htmlTableId');">semicolon</a> | <a class="externallink" href="#" onclick="download_table_as_csv_comma('$htmlTableId');">comma</a>
<table id="$htmlTableId" class="summaryTable">
<thead>
<tr>
<th>SP object Id</th>
<th>SP application Id</th>
<th>SP displayName</th>
<th>SP type</th>
<th>SP App Owner Organization Id</th>
<th>SP App RoleAssignedTo</th>
</tr>
</thead>
<tbody>
"@)

        foreach ($sp in ($servicePrincipalsAppRoleAssignedTo)) {

            $spType = $sp.SPType

            $SPAppRoleAssignedTo = $null
            if (($sp.SPAppRoleAssignedTo)) {
                if (($sp.SPAppRoleAssignedTo.count -gt 0)) {
                    $array = @()
                    foreach ($approleAssTo in $sp.SPAppRoleAssignedTo) {
                        $array += "$($approleAssTo.principalDisplayName) ($($approleAssTo.principalType))"
                    }
                    $SPAppRoleAssignedTo = "$(($sp.SPAppRoleAssignedTo).Count) ($($array -join "$CsvDelimiterOpposite "))"
                }
                else {
                    $SPAppRoleAssignedTo = $null
                }
            }
                
            [void]$htmlTenantSummary.AppendLine(@"
<tr>
<td>$($sp.SP.SPObjectId)</td>
<td>$($sp.SP.SPappId)</td>
<td class="breakwordall">$($sp.SP.SPdisplayName)</td>
<td>$spType</td>
<td>$($sp.SP.SPappOwnerOrganizationId)</td>
<td class="breakwordall">$($SPAppRoleAssignedTo)</td>
</tr>
"@)
        }

        [void]$htmlTenantSummary.AppendLine(@"
            </tbody>
        </table>

    <script>
        var tfConfig4$htmlTableId = {
            base_path: 'https://www.azadvertizer.net/azgovvizv4/tablefilter/', rows_counter: true,       
"@)      
        if ($tfCount -gt 10) {
            $spectrum = "10, $tfCount"
            if ($tfCount -gt 50) {
                $spectrum = "10, 25, 50, $tfCount"
            }        
            if ($tfCount -gt 100) {
                $spectrum = "10, 30, 50, 100, $tfCount"
            }
            if ($tfCount -gt 500) {
                $spectrum = "10, 30, 50, 100, 250, $tfCount"
            }
            if ($tfCount -gt 1000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, $tfCount"
            }
            if ($tfCount -gt 2000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, $tfCount"
            }
            if ($tfCount -gt 3000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, 3000, $tfCount"
            }
            [void]$htmlTenantSummary.AppendLine(@"
paging: {results_per_page: ['Records: ', [$spectrum]]},/*state: {types: ['local_storage'], filters: true, page_number: true, page_length: true, sort: true},*/
"@)
        }
        [void]$htmlTenantSummary.AppendLine(@"
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true,
col_widths: ['10%', '10%', '10%', '10%', '10%', '50%'],            
            locale: 'en-US',
            col_3: 'multiple',
            col_4: 'select',
            col_types: [
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring'
            ],
extensions: [{ name: 'sort' }]
        };
        var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
        tf.init();
    </script>
"@)

[void]$htmlTenantSummary.AppendLine(@"
</div>
"@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@"
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup fontGrey" data-content="Service Principals App RoleAssignedTo (Users and Groups)" /></button>
"@)
    }
    
    $endCustPolLoop = get-date
    Write-Host "   processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsAppRoleAssignedTo

    #region SUMMARYServicePrincipalsOauth2PermissionGrants
    [void]$htmlTenantSummary.AppendLine(@"
<button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textAPIPermissions" data-content="Service Principals Oauth Permission grants (API permissions Delegated)" /></button>
<div class="content TenantSummaryContent">
"@)

    $startCustPolLoop = get-date
    Write-Host "  processing TenantSummary ServicePrincipalsOauth2PermissionGrants"

    $servicePrincipalsOauth2PermissionGrants = $cu.where( { $_.SPOauth2PermissionGrants.Count -ne 0 } )
    $servicePrincipalsOauth2PermissionGrantsCount = $servicePrincipalsOauth2PermissionGrants.Count

    if ($servicePrincipalsOauth2PermissionGrantsCount -gt 0) {
        $tfCount = $servicePrincipalsOauth2PermissionGrantsCount
        $htmlTableId = "TenantSummary_ServicePrincipalsOauth2PermissionGrants"
        [void]$htmlTenantSummary.AppendLine(@"
<i class="padlxx fa fa-table" aria-hidden="true"></i> Download CSV <a class="externallink" href="#" onclick="download_table_as_csv_semicolon('$htmlTableId');">semicolon</a> | <a class="externallink" href="#" onclick="download_table_as_csv_comma('$htmlTableId');">comma</a>
<table id="$htmlTableId" class="summaryTable">
<thead>
<tr>
<th>SP object Id</th>
<th>SP application Id</th>
<th>SP displayName</th>
<th>SP type</th>
<th>SP App Owner Organization Id</th>
<th>SP Oauth Permission grants</th>
</tr>
</thead>
<tbody>
"@)

        foreach ($sp in ($servicePrincipalsOauth2PermissionGrants)) {

            $spType = $sp.SPType

            $SPOauth2PermissionGrants = $null
            if (($sp.SPOauth2PermissionGrants)) {
                if (($sp.SPOauth2PermissionGrants.count -gt 0)) {
                    $array = @()
                    foreach ($oauthGrant in $sp.SPOauth2PermissionGrants) {
                        $array += "$($oauthGrant.SPDisplayName) ($($oauthGrant.permission))"
                    }
                    $SPOauth2PermissionGrants = "$(($sp.SPOauth2PermissionGrants).Count) ($($array -join "$CsvDelimiterOpposite "))"
                }
                else {
                    $SPOauth2PermissionGrants = $null
                }
            }
                
            [void]$htmlTenantSummary.AppendLine(@"
<tr>
<td>$($sp.SP.SPObjectId)</td>
<td>$($sp.SP.SPappId)</td>
<td class="breakwordall">$($sp.SP.SPdisplayName)</td>
<td>$spType</td>
<td>$($sp.SP.SPappOwnerOrganizationId)</td>
<td class="breakwordall">$($SPOauth2PermissionGrants)</td>
</tr>
"@)
        }

        [void]$htmlTenantSummary.AppendLine(@"
            </tbody>
        </table>

    <script>
        var tfConfig4$htmlTableId = {
            base_path: 'https://www.azadvertizer.net/azgovvizv4/tablefilter/', rows_counter: true,       
"@)      
        if ($tfCount -gt 10) {
            $spectrum = "10, $tfCount"
            if ($tfCount -gt 50) {
                $spectrum = "10, 25, 50, $tfCount"
            }        
            if ($tfCount -gt 100) {
                $spectrum = "10, 30, 50, 100, $tfCount"
            }
            if ($tfCount -gt 500) {
                $spectrum = "10, 30, 50, 100, 250, $tfCount"
            }
            if ($tfCount -gt 1000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, $tfCount"
            }
            if ($tfCount -gt 2000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, $tfCount"
            }
            if ($tfCount -gt 3000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, 3000, $tfCount"
            }
            [void]$htmlTenantSummary.AppendLine(@"
paging: {results_per_page: ['Records: ', [$spectrum]]},/*state: {types: ['local_storage'], filters: true, page_number: true, page_length: true, sort: true},*/
"@)
        }
        [void]$htmlTenantSummary.AppendLine(@"
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true,
col_widths: ['10%', '10%', '10%', '10%', '10%', '50%'],            
            locale: 'en-US',
            col_3: 'multiple',
            col_4: 'select',
            col_types: [
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring'
            ],
extensions: [{ name: 'sort' }]
        };
        var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
        tf.init();
    </script>
"@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@"
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textAPIPermissions fontGrey" data-content="Service Principals Oauth Permission grants (API permissions Delegated)" /></button>
"@)
    }
    
    [void]$htmlTenantSummary.AppendLine(@"
    </div>
"@)

    $endCustPolLoop = get-date
    Write-Host "   processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsOauth2PermissionGrants

    if (-not $NoAzureRoleAssignments) {
        #region SUMMARYServicePrincipalsAzureRoleAssignments
        $startCustPolLoop = get-date
        Write-Host "  processing TenantSummary ServicePrincipalsAzureRoleAssignments"

        $servicePrincipalsAzureRoleAssignments = $cu.where( { $_.SPAzureRoleAssignments.Count -ne 0 } )
        $servicePrincipalsAzureRoleAssignmentsCount = $servicePrincipalsAzureRoleAssignments.Count

        if ($servicePrincipalsAzureRoleAssignmentsCount -gt 0) {
            [void]$htmlTenantSummary.AppendLine(@"
            <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textAzureRoleAssignment" data-content="Service Principals Azure RoleAssignments" /></button>
            <div class="content TenantSummaryContent">
"@)

            $tfCount = $servicePrincipalsAzureRoleAssignmentsCount
            $htmlTableId = "TenantSummary_ServicePrincipalsAzureRoleAssignments"
            [void]$htmlTenantSummary.AppendLine(@"
<i class="padlxx fa fa-table" aria-hidden="true"></i> Download CSV <a class="externallink" href="#" onclick="download_table_as_csv_semicolon('$htmlTableId');">semicolon</a> | <a class="externallink" href="#" onclick="download_table_as_csv_comma('$htmlTableId');">comma</a>
<table id="$htmlTableId" class="summaryTable">
<thead>
<tr>
<th>SP object Id</th>
<th>SP application Id</th>
<th>SP displayName</th>
<th>SP type</th>
<th>SP App Owner Organization Id</th>
<th>SP Azure RoleAssignments</th>
</tr>
</thead>
<tbody>
"@)

            foreach ($sp in ($servicePrincipalsAzureRoleAssignments)) {

                $spType = $sp.SPType

                $SPAzureRoleAssignments = $null
                if (($sp.SPAzureRoleAssignments)) {
                    if (($sp.SPAzureRoleAssignments.count -gt 0)) {
                        $array = @()
                        foreach ($azureroleAss in $sp.SPAzureRoleAssignments) {
                            $array += "$($azureroleAss.roleName) ($($azureroleAss.roleAssignmentAssignmentResourceType) $($azureroleAss.roleAssignmentAssignmentScopeName))"
                        }
                        $SPAzureRoleAssignments = "$(($sp.SPAzureRoleAssignments).Count) ($($array -join "$CsvDelimiterOpposite "))"
                    }
                    else {
                        $SPAzureRoleAssignments = $null
                    }
                }
                
                [void]$htmlTenantSummary.AppendLine(@"
<tr>
<td>$($sp.SP.SPObjectId)</td>
<td>$($sp.SP.SPappId)</td>
<td class="breakwordall">$($sp.SP.SPdisplayName)</td>
<td>$spType</td>
<td>$($sp.SP.SPappOwnerOrganizationId)</td>
<td class="breakwordall">$($SPAzureRoleAssignments)</td>
</tr>
"@)
            }

            [void]$htmlTenantSummary.AppendLine(@"
            </tbody>
        </table>

    <script>
        var tfConfig4$htmlTableId = {
            base_path: 'https://www.azadvertizer.net/azgovvizv4/tablefilter/', rows_counter: true,       
"@)      
            if ($tfCount -gt 10) {
                $spectrum = "10, $tfCount"
                if ($tfCount -gt 50) {
                    $spectrum = "10, 25, 50, $tfCount"
                }        
                if ($tfCount -gt 100) {
                    $spectrum = "10, 30, 50, 100, $tfCount"
                }
                if ($tfCount -gt 500) {
                    $spectrum = "10, 30, 50, 100, 250, $tfCount"
                }
                if ($tfCount -gt 1000) {
                    $spectrum = "10, 30, 50, 100, 250, 500, 750, $tfCount"
                }
                if ($tfCount -gt 2000) {
                    $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, $tfCount"
                }
                if ($tfCount -gt 3000) {
                    $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, 3000, $tfCount"
                }
                [void]$htmlTenantSummary.AppendLine(@"
paging: {results_per_page: ['Records: ', [$spectrum]]},/*state: {types: ['local_storage'], filters: true, page_number: true, page_length: true, sort: true},*/
"@)
            }
            [void]$htmlTenantSummary.AppendLine(@"
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true,
col_widths: ['10%', '10%', '10%', '10%', '10%', '50%'],            
            locale: 'en-US',
            col_3: 'multiple',
            col_4: 'select',
            col_types: [
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring'
            ],
extensions: [{ name: 'sort' }]
        };
        var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
        tf.init();
    </script>
"@)

[void]$htmlTenantSummary.AppendLine(@"
</div>
"@)
        }
        else {
            [void]$htmlTenantSummary.AppendLine(@"
                <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textAzureRoleAssignment fontGrey" data-content="Service Principals Azure RoleAssignments" /></button>
"@)
        }
    
        $endCustPolLoop = get-date
        Write-Host "   processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
        #endregion SUMMARYServicePrincipalsAzureRoleAssignments
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@"
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textAzureRoleAssignment fontGrey" data-content="Service Principals Azure RoleAssignments" /></button>
"@)
    }

    #region SUMMARYServicePrincipalsGroupMemberships  
    $startCustPolLoop = get-date
    Write-Host "  processing TenantSummary ServicePrincipalsGroupMemberships"
    
    $servicePrincipalsGroupMemberships = $cu.where( { $_.SPGroupMemberships.Count -ne 0 } )
    $servicePrincipalsGroupMembershipsCount = $servicePrincipalsGroupMemberships.Count
    
    if ($servicePrincipalsGroupMembershipsCount -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup" data-content="Service Principals Group memberships" /></button>
        <div class="content TenantSummaryContent">
"@)

        $tfCount = $servicePrincipalsGroupMembershipsCount
        $htmlTableId = "TenantSummary_ServicePrincipalsGroupMemberships"
        [void]$htmlTenantSummary.AppendLine(@"
    <i class="padlxx fa fa-table" aria-hidden="true"></i> Download CSV <a class="externallink" href="#" onclick="download_table_as_csv_semicolon('$htmlTableId');">semicolon</a> | <a class="externallink" href="#" onclick="download_table_as_csv_comma('$htmlTableId');">comma</a>
    <table id="$htmlTableId" class="summaryTable">
    <thead>
    <tr>
    <th>SP object Id</th>
    <th>SP application Id</th>
    <th>SP displayName</th>
    <th>SP type</th>
    <th>SP App Owner Organization Id</th>
    <th>SP Group memberships</th>
    </tr>
    </thead>
    <tbody>
"@)
    
        foreach ($sp in ($servicePrincipalsGroupMemberships)) {
    
            $spType = $sp.SPType
    
            $SPGroupMemberships = $null
            if (($sp.SPGroupMemberships)) {
                if (($sp.SPGroupMemberships.count -gt 0)) {
                    $array = @()
                    foreach ($groupMembership in $sp.SPGroupMemberships) {
                        $array += "$($groupMembership.DisplayName) ($($groupMembership.ObjectId))"
                    }
                    $SPGroupMemberships = "$(($sp.SPGroupMemberships).Count) ($($array -join "$CsvDelimiterOpposite "))"
                }
                else {
                    $SPGroupMemberships = $null
                }
            }
                    
            [void]$htmlTenantSummary.AppendLine(@"
    <tr>
    <td>$($sp.SP.SPObjectId)</td>
    <td>$($sp.SP.SPappId)</td>
    <td class="breakwordall">$($sp.SP.SPdisplayName)</td>
    <td>$($spType)</td>
    <td>$($sp.SP.SPappOwnerOrganizationId)</td>
    <td class="breakwordall">$($SPGroupMemberships)</td>
    </tr>
"@)
        }
    
        [void]$htmlTenantSummary.AppendLine(@"
                </tbody>
            </table>
    
        <script>
            var tfConfig4$htmlTableId = {
                base_path: 'https://www.azadvertizer.net/azgovvizv4/tablefilter/', rows_counter: true,       
"@)      
        if ($tfCount -gt 10) {
            $spectrum = "10, $tfCount"
            if ($tfCount -gt 50) {
                $spectrum = "10, 25, 50, $tfCount"
            }        
            if ($tfCount -gt 100) {
                $spectrum = "10, 30, 50, 100, $tfCount"
            }
            if ($tfCount -gt 500) {
                $spectrum = "10, 30, 50, 100, 250, $tfCount"
            }
            if ($tfCount -gt 1000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, $tfCount"
            }
            if ($tfCount -gt 2000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, $tfCount"
            }
            if ($tfCount -gt 3000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, 3000, $tfCount"
            }
            [void]$htmlTenantSummary.AppendLine(@"
    paging: {results_per_page: ['Records: ', [$spectrum]]},/*state: {types: ['local_storage'], filters: true, page_number: true, page_length: true, sort: true},*/
"@)
        }
        [void]$htmlTenantSummary.AppendLine(@"
    btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true,
    col_widths: ['10%', '10%', '10%', '10%', '10%', '50%'],            
                locale: 'en-US',
                col_3: 'multiple',
                col_4: 'select',
                col_types: [
                    'caseinsensitivestring',
                    'caseinsensitivestring',
                    'caseinsensitivestring',
                    'caseinsensitivestring',
                    'caseinsensitivestring',
                    'caseinsensitivestring'
                ],
    extensions: [{ name: 'sort' }]
            };
            var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
            tf.init();
        </script>
"@)

[void]$htmlTenantSummary.AppendLine(@"
</div>
"@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@"
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup fontGrey" data-content="Service Principals Group memberships" /></button>
"@)
    }
    
    $endCustPolLoop = get-date
    Write-Host "   processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsGroupMemberships

    #region SUMMARYApplicationSecrets
    $applicationSecrets = $cu.where( { $_.APPPasswordCredentials.Count -gt 0 } )
    $applicationSecretsCount = $applicationSecrets.Count

    if ($applicationSecretsCount -gt 0) {

        $tfCount = $applicationSecretsCount
        $htmlTableId = "TenantSummary_ApplicationSecrets"
        $tf = "tf$($htmlTableId)"

        $applicationSecretsExpireSoon = $applicationSecrets.APPPasswordCredentials.expiryInfo.where( { $_ -like "expires soon*" } )
        $applicationSecretsExpireSoonCount = $applicationSecretsExpireSoon.Count

        if ($applicationSecretsExpireSoonCount -gt 0) {
            [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textSecretCert" data-content="Application Secrets ($applicationSecretsExpireSoonCount expire soon)" /></button>
        <div class="content TenantSummaryContent">
"@)
        }
        else {
            [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textSecretCert" data-content="Application Secrets" /></button>
        <div class="content TenantSummaryContent">
"@)
        }

        $groupedExpiryNoteWorthy = $applicationSecrets.APPPasswordCredentials.expiryInfo.where( { $_ -like "expires soon*" -or $_ -eq "expired" } ) | group-Object
        if (($groupedExpiryNoteWorthy | Measure-Object).Count -gt 0){
            $arrExpiryNoteWorthyCounts = @()
            $arrExpiryNoteWorthyStates = @()
            foreach ($grp in $groupedExpiryNoteWorthy | sort-object -property count -Descending){
                $arrExpiryNoteWorthyCounts += $grp.Count
                $arrExpiryNoteWorthyStates += $grp.Name
            }
            $ExpiryNoteWorthyCounts = "'{0}'" -f ($arrExpiryNoteWorthyCounts -join "','")
            $ExpiryNoteWorthyStates = "'{0}'" -f ($arrExpiryNoteWorthyStates -join "','")

            $categoryColoreExpiryNoteWorthy = ($categoryColorsMax[0..1])
            $categoryColorsSeperatedExpiryNoteWorthy = "'{0}'" -f ($categoryColoreExpiryNoteWorthy -join "','")

            [void]$htmlTenantSummary.AppendLine(@"
        <div class="noFloat">
            <button type="button" class="decollapsible">Charts</button>
        
            <div class="showContent chart-container">
                <div class="chartDiv">
                    <span>Noteworthy expiry states count: <b>$($arrExpiryNoteWorthyCounts.Count)</b></span>
                    <canvas id="chartSecretExpiryNoteWorthy" style="height:150px; width: 250px"></canvas>
                </div>
            </div>
        </div>

<script>
var ctx = document.getElementById('chartSecretExpiryNoteWorthy');
var chartSecretExpiryNoteWorthy = new Chart(ctx, {
    type: 'pie',
                data: {
                    datasets: [
                        {
                            data: [$($ExpiryNoteWorthyCounts)],
                            backgroundColor: [$($categoryColorsSeperatedExpiryNoteWorthy)],
                            labels: [$($ExpiryNoteWorthyStates)],
                            borderWidth:0.5,
                        }
                    ]
                },
                options: {
                    responsive: false,
                    legend: {
                        display: false,
                    },
                    tooltips: {
                        bodyFontSize: 10,
                        callbacks: {
                            label: function (tooltipItem, data) {
                                var dataset = data.datasets[tooltipItem.datasetIndex];
                                var index = tooltipItem.index;
                                window. datasetitem = tooltipItem.datasetIndex;
                                window.target = dataset.labels[index];
                                return dataset.labels[index] + ': ' + dataset.data[index];
                            }
                        }
                    },

                    onClick: (e) => {
                        if (window. datasetitem == 0){
                            window. targetcolumn = '7'
                        }
                        $($tf).clearFilters();
                        $($tf).setFilterValue((window. targetcolumn), (window.target));
                        $($tf).filter();

                    }
                }
});

</script>
"@)
    
        }

        $startCustPolLoop = get-date
        Write-Host "  processing TenantSummary ApplicationSecrets"

        #if ($applicationSecretsCount -gt 0) {
        $tfCount = $applicationSecretsCount
        $htmlTableId = "TenantSummary_ApplicationSecrets"
        [void]$htmlTenantSummary.AppendLine(@"
<i class="padlxx fa fa-table" aria-hidden="true"></i> Download CSV <a class="externallink" href="#" onclick="download_table_as_csv_semicolon('$htmlTableId');">semicolon</a> | <a class="externallink" href="#" onclick="download_table_as_csv_comma('$htmlTableId');">comma</a>
<table id="$htmlTableId" class="summaryTable">
<thead>
<tr>
<th>SP object Id</th>
<th>SP application Id</th>
<th>SP displayName</th>
<th>SP App Owner Organization Id</th>
<th>Application ObjectId</th>
<th>Application (client) Id</th>
<th>Application DisplayName</th>
<th>Application Secrets</th>
</tr>
</thead>
<tbody>
"@)

        foreach ($sp in ($applicationSecrets)) {
            if ($sp.APP) {

                $spType = $sp.SP.servicePrincipalType
                $appObjectId = $sp.APP.APPObjectId
                $appId = $sp.APP.APPAppClientId
                $appDisplayName = $sp.APP.APPDisplayName
                $APPPasswordCredentials = $null
                if (($sp.APPPasswordCredentials)) {
                    if (($sp.APPPasswordCredentials.count -gt 0)) {
                        $array = @()
                        foreach ($secret in $sp.APPPasswordCredentials) {
                            $array += "$($secret.keyId)/$($secret.displayName) ($($secret.expiryInfo); $($secret.endDateTimeFormated))"
                        }
                        $APPPasswordCredentials = "$(($sp.APPPasswordCredentials).Count) ($($array -join "$CsvDelimiterOpposite "))"
                    }
                    else {
                        $APPPasswordCredentials = $null
                    }
                }

                [void]$htmlTenantSummary.AppendLine(@"
<tr>
<td>$($sp.SP.SPObjectId)</td>
<td>$($sp.SP.SPappId)</td>
<td class="breakwordall">$($sp.SP.SPdisplayName)</td>
<td>$($sp.SP.SPappOwnerOrganizationId)</td>
<td>$($appObjectId)</td>
<td>$($appId)</td>
<td class="breakwordall">$($appDisplayName)</td>
<td class="breakwordall">$($APPPasswordCredentials)</td>
</tr>
"@)
            }
        }

        [void]$htmlTenantSummary.AppendLine(@"
        </tbody>
    </table>

<script>
    var tfConfig4$htmlTableId = {
        base_path: 'https://www.azadvertizer.net/azgovvizv4/tablefilter/', rows_counter: true,       
"@)      
        if ($tfCount -gt 10) {
            $spectrum = "10, $tfCount"
            if ($tfCount -gt 50) {
                $spectrum = "10, 25, 50, $tfCount"
            }        
            if ($tfCount -gt 100) {
                $spectrum = "10, 30, 50, 100, $tfCount"
            }
            if ($tfCount -gt 500) {
                $spectrum = "10, 30, 50, 100, 250, $tfCount"
            }
            if ($tfCount -gt 1000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, $tfCount"
            }
            if ($tfCount -gt 2000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, $tfCount"
            }
            if ($tfCount -gt 3000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, 3000, $tfCount"
            }
            [void]$htmlTenantSummary.AppendLine(@"
paging: {results_per_page: ['Records: ', [$spectrum]]},/*state: {types: ['local_storage'], filters: true, page_number: true, page_length: true, sort: true},*/
"@)
        }
        [void]$htmlTenantSummary.AppendLine(@"
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true,
col_widths: ['10%', '10%', '15%', '10%', '10%', '10%', '15%', '10%'],            
        locale: 'en-US',
        col_types: [
            'caseinsensitivestring',
            'caseinsensitivestring',
            'caseinsensitivestring',
            'caseinsensitivestring',
            'caseinsensitivestring',
            'caseinsensitivestring',
            'caseinsensitivestring',
            'caseinsensitivestring'
        ],
extensions: [{ name: 'sort' }]
    };
    var $tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
    $($tf).init();
</script>
"@)

        [void]$htmlTenantSummary.AppendLine(@"
</div>
"@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@"
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textSecretCert fontGrey" data-content="Application Secrets" /></button>
"@)
    }

    $endCustPolLoop = get-date
    Write-Host "   processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYApplicationSecrets

    #region SUMMARYApplicationCertificates
    $applicationCertificates = $cu.where( { $_.APPKeyCredentials.Count -gt 0 } )
    $applicationCertificatesCount = $applicationCertificates.Count

    if ($applicationCertificatesCount -gt 0) {

        $tfCount = $applicationCertificatesCount
        $htmlTableId = "TenantSummary_ApplicationCertificates"
        $tf = "tf$($htmlTableId)"

        $applicationCertificatesExpireSoon = $applicationCertificates.APPKeyCredentials.expiryInfo.where( { $_ -like "expires soon*" } )
        $applicationCertificatesExpireSoonCount = $applicationCertificatesExpireSoon.Count

        if ($applicationCertificatesExpireSoonCount -gt 0) {
            [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textSecretCert" data-content="Application Certificates ($applicationCertificatesExpireSoonCount expire soon)" /></button>
        <div class="content TenantSummaryContent">
"@)
        }
        else {
            [void]$htmlTenantSummary.AppendLine(@"
                <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textSecretCert" data-content="Application Certificates" /></button>
                <div class="content TenantSummaryContent">
"@) 
        }

        $groupedExpiryNoteWorthy = $applicationCertificates.APPKeyCredentials.expiryInfo.where( { $_ -like "expires soon*" -or $_ -eq "expired" } ) | group-Object
        if (($groupedExpiryNoteWorthy | Measure-Object).Count -gt 0) {
            $arrExpiryNoteWorthyCounts = @()
            $arrExpiryNoteWorthyStates = @()
            foreach ($grp in $groupedExpiryNoteWorthy | sort-object -property count -Descending) {
                $arrExpiryNoteWorthyCounts += $grp.Count
                $arrExpiryNoteWorthyStates += $grp.Name
            }
            $ExpiryNoteWorthyCounts = "'{0}'" -f ($arrExpiryNoteWorthyCounts -join "','")
            $ExpiryNoteWorthyStates = "'{0}'" -f ($arrExpiryNoteWorthyStates -join "','")

            $categoryColoreExpiryNoteWorthy = ($categoryColorsMax[0..1])
            $categoryColorsSeperatedExpiryNoteWorthy = "'{0}'" -f ($categoryColoreExpiryNoteWorthy -join "','")

            [void]$htmlTenantSummary.AppendLine(@"
    <div class="noFloat">
        <button type="button" class="decollapsible">Charts</button>
    
        <div class="showContent chart-container">
            <div class="chartDiv">
                <span>Noteworthy expiry states count: <b>$($arrExpiryNoteWorthyCounts.Count)</b></span>
                <canvas id="chartCertExpiryNoteWorthy" style="height:150px; width: 250px"></canvas>
            </div>
        </div>
    </div>

<script>
var ctx = document.getElementById('chartCertExpiryNoteWorthy');
var chartCertExpiryNoteWorthy = new Chart(ctx, {
type: 'pie',
            data: {
                datasets: [
                    {
                        data: [$($ExpiryNoteWorthyCounts)],
                        backgroundColor: [$($categoryColorsSeperatedExpiryNoteWorthy)],
                        labels: [$($ExpiryNoteWorthyStates)],
                        borderWidth:0.5,
                    }
                ]
            },
            options: {
                responsive: false,
                legend: {
                    display: false,
                },
                tooltips: {
                    bodyFontSize: 10,
                    callbacks: {
                        label: function (tooltipItem, data) {
                            var dataset = data.datasets[tooltipItem.datasetIndex];
                            var index = tooltipItem.index;
                            window. datasetitem = tooltipItem.datasetIndex;
                            window.target = dataset.labels[index];
                            return dataset.labels[index] + ': ' + dataset.data[index];
                        }
                    }
                },

                onClick: (e) => {
                    if (window. datasetitem == 0){
                        window. targetcolumn = '7'
                    }
                    $($tf).clearFilters();
                    $($tf).setFilterValue((window. targetcolumn), (window.target));
                    $($tf).filter();

                }
            }
});

</script>
"@)

        }

        $startCustPolLoop = get-date
        Write-Host "  processing TenantSummary ApplicationCertificates"


        [void]$htmlTenantSummary.AppendLine(@"
<i class="padlxx fa fa-table" aria-hidden="true"></i> Download CSV <a class="externallink" href="#" onclick="download_table_as_csv_semicolon('$htmlTableId');">semicolon</a> | <a class="externallink" href="#" onclick="download_table_as_csv_comma('$htmlTableId');">comma</a>
<table id="$htmlTableId" class="summaryTable">
<thead>
<tr>
<th>SP object Id</th>
<th>SP application Id</th>
<th>SP displayName</th>
<th>SP App Owner Organization Id</th>
<th>Application ObjectId</th>
<th>Application (client) Id</th>
<th>Application DisplayName</th>
<th>Application Certificates</th>
</tr>
</thead>
<tbody>
"@)

        foreach ($sp in ($applicationCertificates)) {
            if ($sp.APP) {

                $spType = $sp.SP.servicePrincipalType
                $appObjectId = $sp.APP.APPObjectId
                $appId = $sp.APP.APPAppClientId
                $appDisplayName = $sp.APP.APPDisplayName
                #$appId
                $APPKeyCredentials = $null
                if (($sp.APPKeyCredentials)) {
                    if (($sp.APPKeyCredentials.count -gt 0)) {
                        $array = @()
                        foreach ($key in $sp.APPKeyCredentials) {
                            $array += "$($key.keyId)($($key.customKeyIdentifier))/$($key.displayName) ($($key.expiryInfo); $($key.endDateTimeFormated))"
                        }
                        $APPKeyCredentials = "$(($sp.APPKeyCredentials).Count) ($($array -join "$CsvDelimiterOpposite "))"
                    }
                    else {
                        $APPKeyCredentials = $null
                    }
                }

                [void]$htmlTenantSummary.AppendLine(@"
<tr>
<td>$($sp.SP.SPObjectId)</td>
<td>$($sp.SP.SPappId)</td>
<td class="breakwordall">$($sp.SP.SPdisplayName)</td>
<td>$($sp.SP.SPappOwnerOrganizationId)</td>
<td>$($appObjectId)</td>
<td>$($appId)</td>
<td class="breakwordall">$($appDisplayName)</td>
<td class="breakwordall">$($APPKeyCredentials)</td>
</tr>
"@)
            }
        }

        [void]$htmlTenantSummary.AppendLine(@"
        </tbody>
    </table>

<script>
    var tfConfig4$htmlTableId = {
        base_path: 'https://www.azadvertizer.net/azgovvizv4/tablefilter/', rows_counter: true,       
"@)      
        if ($tfCount -gt 10) {
            $spectrum = "10, $tfCount"
            if ($tfCount -gt 50) {
                $spectrum = "10, 25, 50, $tfCount"
            }        
            if ($tfCount -gt 100) {
                $spectrum = "10, 30, 50, 100, $tfCount"
            }
            if ($tfCount -gt 500) {
                $spectrum = "10, 30, 50, 100, 250, $tfCount"
            }
            if ($tfCount -gt 1000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, $tfCount"
            }
            if ($tfCount -gt 2000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, $tfCount"
            }
            if ($tfCount -gt 3000) {
                $spectrum = "10, 30, 50, 100, 250, 500, 750, 1000, 1500, 3000, $tfCount"
            }
            [void]$htmlTenantSummary.AppendLine(@"
paging: {results_per_page: ['Records: ', [$spectrum]]},/*state: {types: ['local_storage'], filters: true, page_number: true, page_length: true, sort: true},*/
"@)
        }
        [void]$htmlTenantSummary.AppendLine(@"
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true,
col_widths: ['10%', '10%', '10%', '10%', '10%', '10%', '10%', '20%'],            
        locale: 'en-US',
        col_types: [
            'caseinsensitivestring',
            'caseinsensitivestring',
            'caseinsensitivestring',
            'caseinsensitivestring',
            'caseinsensitivestring',
            'caseinsensitivestring',
            'caseinsensitivestring',
            'caseinsensitivestring'
        ],
extensions: [{ name: 'sort' }]
    };
    var $tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
    $($tf).init();
</script>
"@)

        [void]$htmlTenantSummary.AppendLine(@"
</div>
"@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@"
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textSecretCert fontGrey" data-content="Application Certificates" /></button>
"@)
    }

    $endCustPolLoop = get-date
    Write-Host "   processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYApplicationCertificates

    $script:html += $htmlTenantSummary

}
#endregion TenantSummary

#endregion Function

#region dataCollection

#region helper ht / collect results /save some time
if (-not $NoAzureRoleAssignments) {
    $htCacheDefinitions = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    ($htCacheDefinitions).role = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htCacheAssignments = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    ($htCacheAssignments).roleFromAPI = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htRoleAssignmentsFromAPIInheritancePrevention = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $outOfScopeSubscriptions = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))
    $htAllSubscriptionsFromAPI = @{}
    $customDataCollectionDuration = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))  
    $arrayDataCollectionProgressMg = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))
    $arrayDataCollectionProgressSub = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList)) 
}
$arrayAPICallTracking = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))
$arrayAPICallTrackingCustomDataCollection = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))
#endregion helper ht / collect results /save some time

#region validation / check 'Microsoft Graph API' Access
if ($htParameters.AzureDevOpsWikiAsCode -eq $true -or $accountType -eq "ServicePrincipal") {
    Write-Host "Checking ServicePrincipal permissions"
    
    $permissionCheckResults = @()
    $permissionsCheckFailed = $false
    $currentTask = "Test AAD Users Read permission"
    $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/users?`$count=true&`$top=1"
    $method = "GET"
    $res = AzAPICall -uri $uri -method $method -currentTask $currentTask -consistencyLevel "eventual" -validate $true

    if ($res -eq "failed") {
        $permissionCheckResults += "AAD Users Read permission check FAILED"
        $permissionsCheckFailed = $true
    }
    else {
        $permissionCheckResults += "AAD Users Read permission check PASSED"
    }

    $currentTask = "Test AAD Groups Read permission"
    $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/groups?`$count=true&`$top=1"
    $method = "GET"
    $res = AzAPICall -uri $uri -method $method -currentTask $currentTask -consistencyLevel "eventual" -validate $true

    if ($res -eq "failed") {
        $permissionCheckResults += "AAD Groups Read permission check FAILED"
        $permissionsCheckFailed = $true
    }
    else {
        $permissionCheckResults += "AAD Groups Read permission check PASSED"
    }

    $currentTask = "Test AAD ServicePrincipals Read permission"
    $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/servicePrincipals?`$count=true&`$top=1"
    $method = "GET"
    $res = AzAPICall -uri $uri -method $method -currentTask $currentTask -consistencyLevel "eventual" -validate $true

    if ($res -eq "failed") {
        $permissionCheckResults += "AAD ServicePrincipals Read permission check FAILED"
        $permissionsCheckFailed = $true
    }
    else {
        $permissionCheckResults += "AAD ServicePrincipals Read permission check PASSED"
    }    

    $currentTask = "Test AAD RoleManagement Read permission"
    $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/roleManagement/directory/roleDefinitions"
    $method = "GET"
    $res = AzAPICall -uri $uri -method $method -currentTask $currentTask -consistencyLevel "eventual" -validate $true

    if ($res -eq "failed") {
        $permissionCheckResults += "AAD RoleManagement Read permission check FAILED"
        $permissionsCheckFailed = $true
    }
    else {
        $permissionCheckResults += "AAD RoleManagement Read permission check PASSED"
    }   
}
#endregion validation / check 'Microsoft Graph API' Access

if (-not $NoAzureRoleAssignments) {
    Write-Host "Running $($Product) for ManagementGroupId: '$ManagementGroupId'" -ForegroundColor Yellow

    $currentTask = "Checking permissions for ManagementGroup '$ManagementGroupId'"
    Write-Host $currentTask
    $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).ResourceManagerUrl)providers/Microsoft.Management/managementGroups/$($ManagementGroupId)?api-version=2020-05-01"
    $method = "GET"
    $selectedManagementGroupId = AzAPICall -uri $uri -method $method -currentTask $currentTask -listenOn "Content" -validate $true

    if ($selectedManagementGroupId -eq "failed") {
        $permissionCheckResults += "MG Reader permission check FAILED"
        $permissionsCheckFailed = $true
    }
    else {
        $permissionCheckResults += "MG Reader permission check PASSED"
    }

    Write-Host "Permission check results"
    foreach ($permissionCheckResult in $permissionCheckResults) {
        if ($permissionCheckResult -like "*PASSED*") {
            Write-Host $permissionCheckResult -ForegroundColor Green
        }
        else {
            Write-Host $permissionCheckResult -ForegroundColor DarkRed
        }
    }

    if ($permissionsCheckFailed -eq $true) {
        Write-Host "Please consult the documentation: https://$($GithubRepository)#required-permissions-in-azure"
        if ($htParameters.AzureDevOpsWikiAsCode -eq $true) {
            Write-Error "Error"
        }
        else {
            Throw "Error - AzGovViz: check the last console output for details"
        }
    }

    #region AADUserType
    $userType = "n/a"
    if ($accountType -eq "User") {
        $currentTask = "Checking AAD UserType"
        Write-Host $currentTask
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/me?`$select=userType"
        $method = "GET"
        $checkUserType = AzAPICall -uri $uri -method $method -listenOn "Content" -currentTask $currentTask

        if ($checkUserType -eq "unknown") {
            $userType = $checkUserType
        }
        else {
            $userType = $checkUserType.userType
        }
        Write-Host "AAD UserType: $($userType)" -ForegroundColor Yellow
    }
    #endregion AADUserType

    #region GettingEntities
    $startEntities = get-date
    $currentTask = "Getting Entities"
    Write-Host "$currentTask"
    #https://management.azure.com/providers/Microsoft.Management/getEntities?api-version=2020-02-01
    $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).ResourceManagerUrl)providers/Microsoft.Management/getEntities?api-version=2020-02-01"
    $method = "POST"
    $arrayEntitiesFromAPI = AzAPICall -uri $uri -method $method -currentTask $currentTask

    $htSubscriptionsMgPath = @{ }
    $htManagementGroupsMgPath = @{ }
    $htEntities = @{ }
    $htEntitiesPlain = @{ }

    foreach ($entity in $arrayEntitiesFromAPI) {
        $htEntitiesPlain.($entity.Name) = @{ }
        $htEntitiesPlain.($entity.Name) = $entity
    }

    foreach ($entity in $arrayEntitiesFromAPI) {
        if ($entity.Type -eq "/subscriptions") {
            $htSubscriptionsMgPath.($entity.name) = @{ }
            $htSubscriptionsMgPath.($entity.name).ParentNameChain = $entity.properties.parentNameChain
            $htSubscriptionsMgPath.($entity.name).ParentNameChainDelimited = $entity.properties.parentNameChain -join "/"
            $htSubscriptionsMgPath.($entity.name).Parent = $entity.properties.parent.Id -replace ".*/"
            $htSubscriptionsMgPath.($entity.name).ParentName = $htEntitiesPlain.($entity.properties.parent.Id -replace ".*/").properties.displayName
            $htSubscriptionsMgPath.($entity.name).DisplayName = $entity.properties.displayName
            $array = $entity.properties.parentNameChain
            $array += $entity.name
            $htSubscriptionsMgPath.($entity.name).path = $array
            $htSubscriptionsMgPath.($entity.name).pathDelimited = $array -join "/"
            $htSubscriptionsMgPath.($entity.name).level = (($entity.properties.parentNameChain).Count - 1)
        }
        if ($entity.Type -eq "Microsoft.Management/managementGroups") {
            if ([string]::IsNullOrEmpty($entity.properties.parent.Id)) {
                $parent = "_TenantRoot_"
            }
            else {
                $parent = $entity.properties.parent.Id -replace ".*/"
            }
            $htManagementGroupsMgPath.($entity.name) = @{ }
            $htManagementGroupsMgPath.($entity.name).ParentNameChain = $entity.properties.parentNameChain
            $htManagementGroupsMgPath.($entity.name).ParentNameChainDelimited = $entity.properties.parentNameChain -join "/"
            $htManagementGroupsMgPath.($entity.name).ParentNameChainCount = ($entity.properties.parentNameChain | Measure-Object).Count
            $htManagementGroupsMgPath.($entity.name).Parent = $parent
            $htManagementGroupsMgPath.($entity.name).ChildMgsAll = ($arrayEntitiesFromAPI.where( { $_.Type -eq "Microsoft.Management/managementGroups" -and $_.properties.ParentNameChain -contains $entity.name } )).Name
            $htManagementGroupsMgPath.($entity.name).ChildMgsDirect = ($arrayEntitiesFromAPI.where( { $_.Type -eq "Microsoft.Management/managementGroups" -and $_.properties.Parent.Id -replace ".*/" -eq $entity.name } )).Name
            $htManagementGroupsMgPath.($entity.name).DisplayName = $entity.properties.displayName
            $array = $entity.properties.parentNameChain
            $array += $entity.name
            $htManagementGroupsMgPath.($entity.name).path = $array
            $htManagementGroupsMgPath.($entity.name).pathDelimited = $array -join "/"
        }
    
        $htEntities.($entity.name) = @{ }
        $htEntities.($entity.name).ParentNameChain = $entity.properties.parentNameChain
        $htEntities.($entity.name).Parent = $parent
        if ($parent -eq "_TenantRoot_") {
            $parentDisplayName = "_TenantRoot_"
        }
        else {
            $parentDisplayName = $htEntitiesPlain.($htEntities.($entity.name).Parent).properties.displayName
        }
        $htEntities.($entity.name).ParentDisplayName = $parentDisplayName
        $htEntities.($entity.name).DisplayName = $entity.properties.displayName
        $htEntities.($entity.name).Id = $entity.Name
    }

    $endEntities = get-date
    Write-Host "Getting Entities duration: $((NEW-TIMESPAN -Start $startEntities -End $endEntities).TotalSeconds) seconds"
    #endregion GettingEntities

    #region subscriptions
    $startGetSubscriptions = get-date
    $currentTask = "Getting all Subscriptions"
    Write-Host "$currentTask"
    #https://management.azure.com/subscriptions?api-version=2020-01-01
    $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).ResourceManagerUrl)subscriptions?api-version=2019-10-01"
    $method = "GET"
    $requestAllSubscriptionsAPI = AzAPICall -uri $uri -method $method -currentTask $currentTask

    foreach ($subscription in $requestAllSubscriptionsAPI) {   
        $htAllSubscriptionsFromAPI.($subscription.subscriptionId) = @{ }
        $htAllSubscriptionsFromAPI.($subscription.subscriptionId).subDetails = $subscription
    }
    $endGetSubscriptions = get-date
    Write-Host "Getting all Subscriptions duration: $((NEW-TIMESPAN -Start $startGetSubscriptions -End $endGetSubscriptions).TotalSeconds) seconds"  
    #endregion subscriptions

    #region newAADCheck   
    function CheckContextSubscriptionQuotaId($AADQuotaId) {  
        $sleepSec = @(0, 0, 2, 2, 4, 4, 10, 10)
        do {
            Start-Sleep -Seconds $sleepSec[$tryCounter]
            $script:tryCounter++
            $checkContext = Get-AzContext -ErrorAction Stop
            if ($htAllSubscriptionsFromAPI.($checkContext.Subscription.Id).subDetails.subscriptionPolicies.quotaId -like "$($AADQuotaId)*") {
                Write-Host "Current AzContext Subscription not OK: $($checkContext.Subscription.Name); $($checkContext.Subscription.Id); QuotaId: $($htAllSubscriptionsFromAPI.($checkContext.Subscription.Id).subDetails.subscriptionPolicies.quotaId)"
                $alternativeSubscriptionIdForContext = (($requestAllSubscriptionsAPI.where( { $_.subscriptionPolicies.quotaId -notlike "$($AADQuotaId)*" -and $_.state -eq "Enabled" }))[0]).subscriptionId
                Write-Host "Setting AzContext with alternative Subscription: $($htAllSubscriptionsFromAPI.($alternativeSubscriptionIdForContext).subDetails.displayName); $($alternativeSubscriptionIdForContext); $($htAllSubscriptionsFromAPI.($alternativeSubscriptionIdForContext).subDetails.subscriptionPolicies.quotaId)"
                Set-AzContext -SubscriptionId "$($alternativeSubscriptionIdForContext)" -Tenant "$($checkContext.Tenant.Id)" -ErrorAction Stop
            }
            else {
                Write-Host "Current AzContext OK: $($checkContext.Subscription.Name); $($checkContext.Subscription.Id); QuotaId: $($htAllSubscriptionsFromAPI.($checkContext.Subscription.Id).subDetails.subscriptionPolicies.quotaId)"
                $contextSubscriptionQuotaId = "OK"
            }
        }
        until($contextSubscriptionQuotaId -eq "OK" -or $tryCounter -gt 6)
    }
    $tryCounter = 0
    $contextSubscriptionQuotaId = $null
    $AADQuotaId = "AAD"
    CheckContextSubscriptionQuotaId -AADQuotaId $AADQuotaId
    $checkContext = Get-AzContext -ErrorAction Stop

    if ($tryCounter -gt 6) {
        Write-Host "Problem switching the context to a Subscription that has a non AAD_ QuotaId"
        if ($htParameters.AzureDevOpsWikiAsCode -eq $true) {
            Write-Error "Error"
        }
        else {
            Throw "Error - $($Product): check the last console output for details"
        }
    }
    #endregion newAADCheck 

    #region subscriptionFilter
    #API in rare cases returns duplicats, therefor sorting unique (id)
    $childrenSubscriptions = $arrayEntitiesFromAPI.where( { $_.properties.parentNameChain -contains $ManagementGroupID -and $_.type -eq "/subscriptions" } ) | Sort-Object -Property id -Unique
    $childrenSubscriptionsCount = ($childrenSubscriptions | Measure-Object).Count
    $script:subsToProcessInCustomDataCollection = [System.Collections.ArrayList]@()

    foreach ($childrenSubscription in $childrenSubscriptions) {

        $sub = $htAllSubscriptionsFromAPI.($childrenSubscription.name)
        if ($sub.subDetails.subscriptionPolicies.quotaId.startswith("AAD_", "CurrentCultureIgnoreCase") -or $sub.subDetails.state -ne "Enabled") {
            if (($sub.subDetails.subscriptionPolicies.quotaId).startswith("AAD_", "CurrentCultureIgnoreCase")) {
                $null = $script:outOfScopeSubscriptions.Add([PSCustomObject]@{ 
                        subscriptionId      = $childrenSubscription.name
                        subscriptionName    = $childrenSubscription.properties.displayName
                        outOfScopeReason    = "QuotaId: AAD_ (State: $($sub.subDetails.state))"
                        ManagementGroupId   = $htSubscriptionsMgPath.($childrenSubscription.name).Parent
                        ManagementGroupName = $htSubscriptionsMgPath.($childrenSubscription.name).ParentName
                        Level               = $htSubscriptionsMgPath.($childrenSubscription.name).level
                    })
            }
            if ($sub.subDetails.state -ne "Enabled") {
                $null = $script:outOfScopeSubscriptions.Add([PSCustomObject]@{ 
                        subscriptionId      = $childrenSubscription.name
                        subscriptionName    = $childrenSubscription.properties.displayName
                        outOfScopeReason    = "State: $($sub.subDetails.state)"
                        ManagementGroupId   = $htSubscriptionsMgPath.($childrenSubscription.name).Parent
                        ManagementGroupName = $htSubscriptionsMgPath.($childrenSubscription.name).ParentName
                        Level               = $htSubscriptionsMgPath.($childrenSubscription.name).level
                    })
            }
        }
        else {
            if ($SubscriptionQuotaIdWhitelist[0] -ne "undefined") {
                $whitelistMatched = "unknown"
                foreach ($subscriptionQuotaIdWhitelistQuotaId in $SubscriptionQuotaIdWhitelist) {
                    if (($sub.subDetails.subscriptionPolicies.quotaId).startswith($subscriptionQuotaIdWhitelistQuotaId, "CurrentCultureIgnoreCase")) {
                        $whitelistMatched = "inWhitelist"
                    }
                }
    
                if ($whitelistMatched -eq "inWhitelist") {
                    #write-host "$($childrenSubscription.properties.displayName) in whitelist"
                    $null = $script:subsToProcessInCustomDataCollection.Add([PSCustomObject]@{ 
                            subscriptionId      = $childrenSubscription.name
                            subscriptionName    = $childrenSubscription.properties.displayName
                            subscriptionQuotaId = $sub.subDetails.subscriptionPolicies.quotaId
                        })
                }
                else {
                    #Write-Host " preCustomDataCollection: $($childrenSubscription.properties.displayName) ($($childrenSubscription.name)) Subscription Quota Id: $($sub.subDetails.subscriptionPolicies.quotaId) is out of scope for $($Product) (not in Whitelist)"
                    $null = $script:outOfScopeSubscriptions.Add([PSCustomObject]@{ 
                            subscriptionId      = $childrenSubscription.name
                            subscriptionName    = $childrenSubscription.properties.displayName
                            outOfScopeReason    = "QuotaId: '$($sub.subDetails.subscriptionPolicies.quotaId)' not in Whitelist"
                            ManagementGroupId   = $htSubscriptionsMgPath.($childrenSubscription.name).Parent
                            ManagementGroupName = $htSubscriptionsMgPath.($childrenSubscription.name).ParentName
                            Level               = $htSubscriptionsMgPath.($childrenSubscription.name).level
                        })
                }
            }
            else {
                $null = $script:subsToProcessInCustomDataCollection.Add([PSCustomObject]@{ 
                        subscriptionId      = $childrenSubscription.name
                        subscriptionName    = $childrenSubscription.properties.displayName
                        subscriptionQuotaId = $sub.subDetails.subscriptionPolicies.quotaId
                    })
            }
        }
    }
    $subsToProcessInCustomDataCollectionCount = ($subsToProcessInCustomDataCollection | Measure-Object).Count
    #endregion subscriptionFilter

    #region dataprocessingDefinitionCaching
    $startDefinitionsCaching = get-date
      
    $currentTask = "Caching built-in Role definitions"
    Write-Host " $currentTask"
    $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).ResourceManagerUrl)subscriptions/$($checkContext.Subscription.Id)/providers/Microsoft.Authorization/roleDefinitions?api-version=2018-07-01&`$filter=type eq 'BuiltInRole'"
    $method = "GET"
    $requestRoleDefinitionAPI = AzAPICall -uri $uri -method $method -currentTask $currentTask

    foreach ($roleDefinition in $requestRoleDefinitionAPI) {
        ($htCacheDefinitions).role.($roleDefinition.name) = @{ }
        ($htCacheDefinitions).role.($roleDefinition.name).definition = ($roleDefinition)
        ($htCacheDefinitions).role.($roleDefinition.name).linkToAzAdvertizer = "<a class=`"externallink`" href=`"https://www.azadvertizer.net/azrolesadvertizer/$($roleDefinition.name).html`" target=`"_blank`">$($roleDefinition.properties.roleName)</a>"
    }

    $endDefinitionsCaching = get-date
    Write-Host "Caching built-in definitions duration: $((NEW-TIMESPAN -Start $startDefinitionsCaching -End $endDefinitionsCaching).TotalSeconds) seconds"
    #endregion dataprocessingDefinitionCaching


    $arrayEntitiesFromAPISubscriptionsCount = ($arrayEntitiesFromAPI | Where-Object { $_.type -eq "/subscriptions" -and $_.properties.parentNameChain -contains $ManagementGroupId } | Sort-Object -Property id -Unique | Measure-Object).count
    $arrayEntitiesFromAPIManagementGroupsCount = ($arrayEntitiesFromAPI | Where-Object { $_.type -eq "Microsoft.Management/managementGroups" -and $_.properties.parentNameChain -contains $ManagementGroupId }  | Sort-Object -Property id -Unique | Measure-Object).count + 1

    Write-Host "Collecting custom data"
    $startDataCollection = get-date

    dataCollection -mgId $ManagementGroupId

    #region dataColletionAz summary
    $endDataCollection = get-date
    Write-Host "Collecting custom data duration: $((NEW-TIMESPAN -Start $startDataCollection -End $endDataCollection).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startDataCollection -End $endDataCollection).TotalSeconds) seconds)"

    $durationDataMG = ($customDataCollectionDuration | Where-Object { $_.Type -eq "MG" })
    $durationDataSUB = ($customDataCollectionDuration | Where-Object { $_.Type -eq "SUB" })
    $durationMGAverageMaxMin = ($durationDataMG.DurationSec | Measure-Object -Average -Maximum -Minimum)
    $durationSUBAverageMaxMin = ($durationDataSUB.DurationSec | Measure-Object -Average -Maximum -Minimum)
    Write-Host "Collecting custom data for $($arrayEntitiesFromAPIManagementGroupsCount) ManagementGroups Avg/Max/Min duration in seconds: Average: $([math]::Round($durationMGAverageMaxMin.Average,4)); Maximum: $([math]::Round($durationMGAverageMaxMin.Maximum,4)); Minimum: $([math]::Round($durationMGAverageMaxMin.Minimum,4))"
    Write-Host "Collecting custom data for $($arrayEntitiesFromAPISubscriptionsCount) Subscriptions Avg/Max/Min duration in seconds: Average: $([math]::Round($durationSUBAverageMaxMin.Average,4)); Maximum: $([math]::Round($durationSUBAverageMaxMin.Maximum,4)); Minimum: $([math]::Round($durationSUBAverageMaxMin.Minimum,4))"


    $APICallTrackingCount = ($arrayAPICallTrackingCustomDataCollection | Measure-Object).Count
    $APICallTrackingRetriesCount = ($arrayAPICallTrackingCustomDataCollection | Where-Object { $_.TryCounter -gt 0 } | Measure-Object).Count
    $APICallTrackingRestartDueToDuplicateNextlinkCounterCount = ($arrayAPICallTrackingCustomDataCollection | Where-Object { $_.RestartDueToDuplicateNextlinkCounter -gt 0 } | Measure-Object).Count
    Write-Host "Collecting custom data APICalls (Management) total count: $APICallTrackingCount ($APICallTrackingRetriesCount retries; $APICallTrackingRestartDueToDuplicateNextlinkCounterCount nextLinkReset)"
    #endregion dataColletionAz summary  

}
else {

    Write-Host "Permission check results"
    foreach ($permissionCheckResult in $permissionCheckResults) {
        if ($permissionCheckResult -like "*PASSED*") {
            Write-Host $permissionCheckResult -ForegroundColor Green
        }
        else {
            Write-Host $permissionCheckResult -ForegroundColor DarkRed
        }
    }

    if ($permissionsCheckFailed -eq $true) {
        Write-Host "Please consult the documentation: https://$($GithubRepository)#required-permissions-in-azure"
        if ($htParameters.AzureDevOpsWikiAsCode -eq $true) {
            Write-Error "Error"
        }
        else {
            Throw "Error - AzGovViz: check the last console output for details"
        }
    }
    Write-Host "Running $($Product) without resolving Role assignments in Azure" -ForegroundColor Yellow
}

#region AADSP

#PW in this region the data gets collected (search: ForEach-Object -Parallel)
#region dataColletionAADSP
$start = get-date
Write-Host "Getting Service Principal count"
$currentTask = "getSPCount"
$uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/servicePrincipals/`$count"
$method = "GET"
$spCount = AzAPICall -uri $uri -method $method -currentTask $currentTask -listenOn "Content" -consistencyLevel "eventual"

Write-Host "API `$Count returned $spCount Service Principals count"

$currentTask = "Get all Service Principals"
$start = get-date
$uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/servicePrincipals"
$method = "GET"
$getServicePrincipalsFromAPI = AzAPICall -uri $uri -method $method -currentTask $currentTask -consistencyLevel "eventual"

Write-Host "API returned count: $($getServicePrincipalsFromAPI.Count)"
$getServicePrincipals = $getServicePrincipalsFromAPI | Sort-Object -Property id -Unique
Write-Host "Sorting unique by Id count: $($getServicePrincipalsFromAPI.Count)"
$end = get-date
$duration = NEW-TIMESPAN -Start $start -End $end
Write-Host "Getting $($getServicePrincipals.Count) Service Principals duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"

if ($getServicePrincipals.Count -eq 0) {
    throw "No SPs found"
}
else {
    $htServicePrincipalsEnriched = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htServicePrincipalsAppRoles = @{}
    $htServicePrincipalsPublishedPermissionScopes = @{}
    $htAppRoles = @{}
    $htPublishedPermissionScopes = @{}
    $htAadGroupsToResolve = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htAppRoleAssignments = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htSPOauth2PermissionGrantedTo = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htUsersAndGroupsToCheck4AppRoleAssignments = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htUsersAndGroupsToCheck4AppRoleAssignments.User = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htUsersAndGroupsToCheck4AppRoleAssignments.Group = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htApplications = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htSPOwners = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htAppOwners = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htOwnedBy = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htProcessedTracker = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htMeanwhileDeleted = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}

    Write-Host "Creating mapping AppRoles & PublishedPermissionScopes"
    foreach ($sp in $getServicePrincipals) {
        #appRoles
        if (($sp.appRoles).Count -gt 0) {
            $htServicePrincipalsAppRoles.($sp.id) = @{}
            $htServicePrincipalsAppRoles.($sp.id).spDetails = $sp
            $htServicePrincipalsAppRoles.($sp.id).appRoles = @{}
            foreach ($spAppRole in $sp.appRoles) {
                $htServicePrincipalsAppRoles.($sp.id).appRoles.($spAppRole.id) = $spAppRole
                if (-not $htAppRoles.($spAppRole.id)) {
                    $htAppRoles.($spAppRole.id) = $spAppRole
                }
            }
        }
        #publishedPermissionScopes
        if (($sp.oauth2PermissionScopes).Count -gt 0) {
            $htServicePrincipalsPublishedPermissionScopes.($sp.id) = @{}
            $htServicePrincipalsPublishedPermissionScopes.($sp.id).spDetails = $sp
            $htServicePrincipalsPublishedPermissionScopes.($sp.id).publishedPermissionScopes = @{}
            foreach ($spPublishedPermissionScope in $sp.oauth2PermissionScopes) {
                $htServicePrincipalsPublishedPermissionScopes.($sp.id).publishedPermissionScopes.($spPublishedPermissionScope.id) = $spPublishedPermissionScope
                if (-not $htPublishedPermissionScopes.($sp.id)) {
                    $htPublishedPermissionScopes.($sp.id) = @{}
                }
                if (-not $htPublishedPermissionScopes.($sp.id).($spPublishedPermissionScope.value)) {
                    $htPublishedPermissionScopes.($sp.id).($spPublishedPermissionScope.value) = $spPublishedPermissionScope
                }
            }
        }
    }

    Write-Host "Getting all AAD Role definitions"
    $currentTask = "get AAD RoleDefinitions"
    $htAadRoleDefinitions = @{}
    $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/roleManagement/directory/roleDefinitions"
    $method = "GET"
    $aadRoleDefinitions = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSP $true 

    foreach ($aadRoleDefinition in $aadRoleDefinitions) {
        $htAadRoleDefinitions.($aadRoleDefinition.id) = $aadRoleDefinition
    }   
    
    Write-Host "Validating Identity Governance state"
    $currentTask = "Validate roleAssignmentScheduleInstance"
    $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/roleManagement/directory/roleAssignmentScheduleInstances?`$count=true&`$top=1"
    $method = "GET"
    $getRoleAssignmentScheduleInstance = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSp $true -validate $true -getRoleAssignmentScheduledInstances $true
    if ($getRoleAssignmentScheduleInstance -eq "InvalidResource"){
        Write-Host "Identity Governance state: n/a"
        $identityGovernance = "false"
    }
    else{
        Write-Host "Identity Governance state: available"
        $identityGovernance = "true"
    }

    $currentTask = "Validate roleAssignmentSchedules"
    $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/roleManagement/directory/roleAssignmentSchedules?`$count=true&`$top=1"
    $method = "GET"
    $getRoleAssignmentSchedules = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSp $true -validate $true -getRoleAssignmentSchedules $true
    if ($getRoleAssignmentSchedules -eq "InvalidResource"){
        Write-Host "Identity Governance state: n/a"
        $identityGovernance = "false"
    }
    else{
        Write-Host "Identity Governance state: available"
        $identityGovernance = "true"
    }
    
    Write-Host "Collecting data for all Service Principals"
    $startForeachSP = get-date

    switch ($getServicePrincipals.Count) {
        { $_ -gt 0 } { $indicator = 1 }
        { $_ -gt 10 } { $indicator = 5 }
        { $_ -gt 50 } { $indicator = 10 }
        { $_ -gt 100 } { $indicator = 20 }
        { $_ -gt 250 } { $indicator = 25 }
        { $_ -gt 500 } { $indicator = 50 }
        { $_ -gt 1000 } { $indicator = 100 }
        { $_ -gt 10000 } { $indicator = 250 }
    }

    Write-Host " processing $($getServicePrincipals.Count) ServicePrincipals"
    
    $getServicePrincipals | ForEach-Object -Parallel {
        $sp = $_
        #array&ht
        $arrayAzureManagementEndPointUrls = $using:arrayAzureManagementEndPointUrls
        $checkContext = $using:checkContext
        $htAzureEnvironmentRelatedUrls = $using:htAzureEnvironmentRelatedUrls
        $htBearerAccessToken = $using:htBearerAccessToken
        $arrayAPICallTracking = $using:arrayAPICallTracking
        $htServicePrincipalsEnriched = $using:htServicePrincipalsEnriched
        $htServicePrincipalsAppRoles = $using:htServicePrincipalsAppRoles
        $htAppRoles = $using:htAppRoles
        $htServicePrincipalsPublishedPermissionScopes = $using:htServicePrincipalsPublishedPermissionScopes
        $htPublishedPermissionScopes = $using:htPublishedPermissionScopes
        $htAadRoleDefinitions = $using:htAadRoleDefinitions
        $htParameters = $using:htParameters
        $htAadGroupsToResolve = $using:htAadGroupsToResolve
        $htAppRoleAssignments = $using:htAppRoleAssignments
        $htSPOauth2PermissionGrantedTo = $using:htSPOauth2PermissionGrantedTo
        $htUsersAndGroupsToCheck4AppRoleAssignments = $using:htUsersAndGroupsToCheck4AppRoleAssignments
        $htApplications = $using:htApplications
        $indicator = $using:indicator
        $htSPOwners = $using:htSPOwners
        $htAppOwners = $using:htAppOwners
        $htOwnedBy = $using:htOwnedBy
        $htProcessedTracker = $using:htProcessedTracker
        $htMeanwhileDeleted = $using:htMeanwhileDeleted
        #func
        $function:AzAPICall = $using:funcAzAPICall
        $function:createBearerToken = $using:funcCreateBearerToken
        $function:GetJWTDetails = $using:funcGetJWTDetails
        #var
        $identityGovernance = $using:identityGovernance

        #write-host "processing $($sp.id) - $($sp.displayName) (type: $($sp.servicePrincipalType) org: $($sp.appOwnerOrganizationId))"

        $meanwhileDeleted = $false

        $script:htServicePrincipalsEnriched.($sp.id) = @{}
        $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal = [ordered] @{}
        $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.ServicePrincipalDetails = $sp
    
        if ($sp.appOwnerOrganizationId -eq $checkContext.Tenant.Id) {
            $spTypeINTEXT = "INT"
        }
        else {
            $spTypeINTEXT = "EXT"
        }

        #region spownedObjects
        $currentTask = "getSP OwnedObjects $($sp.id)"
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/servicePrincipals/$($sp.id)/ownedObjects"
        $method = "GET"
        $getSPOwnedObjects = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSP $true

        if ($getSPOwnedObjects -eq "Request_ResourceNotFound"){
            if (-not $htMeanwhileDeleted.($sp.id)){
                write-host "  $($sp.displayName) ($($sp.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                $script:htMeanwhileDeleted.($sp.id) = @{}
                $meanwhileDeleted = $true
            }
        }
        else{
            if ($getSPOwnedObjects.Count -gt 0) {
                $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.ServicePrincipalOwnedObjects = $getSPOwnedObjects | Select-Object '@odata.type', displayName, id
            }
        }
        #endregion spownedObjects

        #region spAADRoleAssignments
        #if ($identityGovernance -eq "false"){
            if (-not $meanwhileDeleted){
                $currentTask = "getSP AADRoleAssignments $($sp.id)"
                #v1 does not return principalOrganizationId, resourceScope
                $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/roleManagement/directory/roleAssignments?`$filter=principalId eq '$($sp.id)'"
                $method = "GET"
                $getSPAADRoleAssignments = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSP $true 
        
                if ($getSPAADRoleAssignments -eq "Request_ResourceNotFound"){
                    if (-not $htMeanwhileDeleted.($sp.id)){
                        write-host "  $($sp.displayName) ($($sp.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                        $script:htMeanwhileDeleted.($sp.id) = @{}
                        $meanwhileDeleted = $true
                    }
                }
                else{
                    if ($getSPAADRoleAssignments.Count -gt 0) {
                        $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.ServicePrincipalAADRoleAssignments = $getSPAADRoleAssignments
                    }
                }
            }
        #}
        #endregion spAADRoleAssignments

        #test later
        if (1 -ne 1){
        if ($identityGovernance -eq "true"){
            #region AADRoleAssignmentSchedules
            if (-not $meanwhileDeleted){
                $currentTask = "getSP AADRoleAssignmentSchedules $($sp.id)"
                $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/roleManagement/directory/roleAssignmentSchedules?`$filter=principalId eq '$($sp.id)'"
                $method = "GET"
                $getSPAADRoleAssignmentSchedules = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSP $true 
        
                if ($getSPAADRoleAssignmentSchedules -eq "Request_ResourceNotFound"){
                    if (-not $htMeanwhileDeleted.($sp.id)){
                        write-host "  $($sp.displayName) ($($sp.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                        $script:htMeanwhileDeleted.($sp.id) = @{}
                        $meanwhileDeleted = $true
                    }
                }
                else{
                    if ($getSPAADRoleAssignmentSchedules.Count -gt 0) {
                        $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.ServicePrincipalAADRoleAssignmentSchedules = $getSPAADRoleAssignmentSchedules
                    }
                }
            }
            #endregion AADRoleAssignmentSchedules

            #region AADRoleAssignmentScheduleInstances
            if (-not $meanwhileDeleted){
                $currentTask = "getSP AADRoleAssignmentScheduleInstances $($sp.id)"
                $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/roleManagement/directory/roleAssignmentScheduleInstances?`$filter=principalId eq '$($sp.id)'"
                $method = "GET"
                $getSPAADRoleAssignmentScheduleInstances = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSP $true 
        
                if ($getSPAADRoleAssignmentScheduleInstances -eq "Request_ResourceNotFound"){
                    if (-not $htMeanwhileDeleted.($sp.id)){
                        write-host "  $($sp.displayName) ($($sp.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                        $script:htMeanwhileDeleted.($sp.id) = @{}
                        $meanwhileDeleted = $true
                    }
                }
                else{
                    if ($getSPAADRoleAssignmentScheduleInstances.Count -gt 0) {
                        $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.ServicePrincipalAADRoleAssignmentScheduleInstances = $getSPAADRoleAssignmentScheduleInstances
                    }
                }
            }
            #endregion AADRoleAssignmentScheduleInstances
        }
        }

        <#
        #test compare ra vs rasi
        if ($getSPAADRoleAssignments.Count -ne $getSPAADRoleAssignmentScheduleInstances.Count){
            Write-Host "processing $($sp.id) - $($sp.displayName) (type: $($sp.servicePrincipalType) ra: $($getSPAADRoleAssignments.Count) rasi: $($getSPAADRoleAssignmentScheduleInstances.Count)"
        }

        #test compare rasi vs ras
        if ($getSPAADRoleAssignmentScheduleInstances.Count -ne $getSPAADRoleAssignmentSchedules.Count){
            Write-Host "processing $($sp.id) - $($sp.displayName) (type: $($sp.servicePrincipalType) rasi: $($getSPAADRoleAssignmentScheduleInstances.Count) ras: $($getSPAADRoleAssignmentSchedules.Count)"
        }
        #>

        #region spAppRoleAssignments
        if (-not $meanwhileDeleted){
            $currentTask = "getSP AppRoleAssignments $($sp.id)"
            $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/servicePrincipals/$($sp.id)/appRoleAssignments"
            $method = "GET"
            $getSPAppRoleAssignments = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSP $true 
    
            if ($getSPAppRoleAssignments -eq "Request_ResourceNotFound"){
                if (-not $htMeanwhileDeleted.($sp.id)){
                    write-host "  $($sp.displayName) ($($sp.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                    $script:htMeanwhileDeleted.($sp.id) = @{}
                    $meanwhileDeleted = $true
                }
            }
            else{
                if ($getSPAppRoleAssignments.Count -gt 0) {
                    $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.ServicePrincipalAppRoleAssignments = $getSPAppRoleAssignments
                    foreach ($SPAppRoleAssignment in $getSPAppRoleAssignments) {
                        if (-not $htAppRoleAssignments.($SPAppRoleAssignment.id)) {
                            $script:htAppRoleAssignments.($SPAppRoleAssignment.id) = $SPAppRoleAssignment
                        }
                    }
                }
            }
        }
        #endregion spAppRoleAssignments

        #region spAppRoleAssignedTo
        if (-not $meanwhileDeleted){
            $currentTask = "getSP appRoleAssignedTo $($sp.id)"
            $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/servicePrincipals/$($sp.id)/appRoleAssignedTo"
            $method = "GET"
            $getSPAppRoleAssignedTo = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSP $true 
    
            if ($getSPAppRoleAssignedTo -eq "Request_ResourceNotFound"){
                if (-not $htMeanwhileDeleted.($sp.id)){
                    write-host "  $($sp.displayName) ($($sp.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                    $script:htMeanwhileDeleted.($sp.id) = @{}
                    $meanwhileDeleted = $true
                }
            }
            else{
                if ($getSPAppRoleAssignedTo.Count -gt 0) {
                    $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.ServicePrincipalAppRoleAssignedTo = $getSPAppRoleAssignedTo
                    foreach ($SPAppRoleAssignedTo in $getSPAppRoleAssignedTo) {
                        if ($SPAppRoleAssignedTo.principalType -eq "User" -or $SPAppRoleAssignedTo.principalType -eq "Group") {
                            if ($SPAppRoleAssignedTo.principalType -eq "User") {
                                if (-not $htUsersAndGroupsToCheck4AppRoleAssignments."User".($SPAppRoleAssignedTo.principalId)) {
                                    $script:htUsersAndGroupsToCheck4AppRoleAssignments."User".($SPAppRoleAssignedTo.principalId) = @{}
                                }
                            }
                            if ($SPAppRoleAssignedTo.principalType -eq "Group") {
                                if (-not $htUsersAndGroupsToCheck4AppRoleAssignments."Group".($SPAppRoleAssignedTo.principalId)) {
                                    $script:htUsersAndGroupsToCheck4AppRoleAssignments."Group".($SPAppRoleAssignedTo.principalId) = @{}
                                }
                            }
                        }
                    }
                }
            }
        }
        #endregion spAppRoleAssignedTo

        #region spGetMemberGroups
        if (-not $meanwhileDeleted){
            $currentTask = "getSP GroupMemberships $($sp.id)"
            $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/servicePrincipals/$($sp.id)/getMemberGroups"
            $method = "POST"
            $body = @"
            {
                "securityEnabledOnly": false
            }
"@
            $getSPGroupMemberships = AzAPICall -uri $uri -method $method -body $body -currentTask $currentTask -getSP $true
    
            if ($getSPGroupMemberships -eq "Request_ResourceNotFound"){
                if (-not $htMeanwhileDeleted.($sp.id)){
                    write-host "  $($sp.displayName) ($($sp.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                    $script:htMeanwhileDeleted.($sp.id) = @{}
                    $meanwhileDeleted = $true
                }
            }
            else{
                if ($getSPGroupMemberships.Count -gt 0) {
                    $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.ServicePrincipalGroupMemberships = $getSPGroupMemberships
                    foreach ($aadGroupId in $getSPGroupMemberships) {
                        if (-not $script:htAadGroupsToResolve.($aadGroupId)) {
                            $script:htAadGroupsToResolve.($aadGroupId) = @{}
                        }
                    }
                }
            }
        }
        #endregion spGetMemberGroups

        #region spDelegatedPermissions
        if (-not $meanwhileDeleted){
            $currentTask = "getSP oauth2PermissionGrants $($sp.id)"
            #v1 does not return startTime, expiryTime
            $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/servicePrincipals/$($sp.id)/oauth2PermissionGrants"
            $method = "GET"
            $getSPOauth2PermissionGrants = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSP $true 
    
            if ($getSPOauth2PermissionGrants -eq "Request_ResourceNotFound"){
                if (-not $htMeanwhileDeleted.($sp.id)){
                    write-host "  $($sp.displayName) ($($sp.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                    $script:htMeanwhileDeleted.($sp.id) = @{}
                    $meanwhileDeleted = $true
                }
            }
            else{
                if ($getSPOauth2PermissionGrants.Count -gt 0) {
                    $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.ServicePrincipalOauth2PermissionGrants = $getSPOauth2PermissionGrants
                    foreach ($permissionGrant in $getSPOauth2PermissionGrants) {
                        $splitPermissionGrant = ($permissionGrant.scope).split(" ")
                        foreach ($permissionscope in $splitPermissionGrant) {
                            if (-not [string]::IsNullOrEmpty($permissionscope) -and -not [string]::IsNullOrWhiteSpace($permissionscope)) {
                                $permissionGrantArray = [System.Collections.ArrayList]@()
                                $null = $permissionGrantArray.Add([PSCustomObject]@{
                                        '@odata.id' = $permissionGrant
                                        clientId    = $permissionGrant.clientId
                                        consentType = $permissionGrant.consentType
                                        expiryTime  = $permissionGrant.expiryTime
                                        id          = $permissionGrant.id
                                        principalId = $permissionGrant.principalId
                                        resourceId  = $permissionGrant.resourceId
                                        scope       = $permissionscope
                                        startTime   = $permissionGrant.startTime
                                    })
        
                                if (-not $htSPOauth2PermissionGrantedTo.($permissionGrant.resourceId)) {
                                    $script:htSPOauth2PermissionGrantedTo.($permissionGrant.resourceId) = [array]$permissionGrantArray
                                }
                                else {
                                    $script:htSPOauth2PermissionGrantedTo.($permissionGrant.resourceId) += $permissionGrantArray
                                }
                            }
                        }
                    }
                }
            }
        }
        #endregion spDelegatedPermissions
        
        <#Optional
        #delegatedPermissionClassifications
        if ($sp.servicePrincipalType -eq "Application") {
            
            $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/servicePrincipals/$($sp.id)/delegatedPermissionClassifications"
            $currentTask = $uri
            $method = "GET"
            $getSPDelegatedPermissionClassifications = AzAPICall -uri $uri -method $method -currentTask $currentTask -listenOn "Content"
            Write-Host "$($sp.id) --> $($getSPDelegatedPermissionClassifications.Count)"
            if ($getSPDelegatedPermissionClassifications.Count -gt 0){
                foreach ($delegatedPermissionClassification in $getSPDelegatedPermissionClassifications){
                    $delegatedPermissionClassification
                    #Write-Host "$($sp.displayName) owns: $($ownedObject.'@odata.type') - $($ownedObject.displayName) ($($ownedObject.id))"
                }
            }
        }
        #>
        
        #region spOwner 
        if (-not $meanwhileDeleted){
            $currentTask = "getSPOwner $($sp.id)"
            $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/servicePrincipals/$($sp.id)/owners"
            $method = "GET"
            $getSPOwner = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSP $true
            
            if ($getSPOwner -eq "Request_ResourceNotFound"){
                if (-not $htMeanwhileDeleted.($sp.id)){
                    write-host "  $($sp.displayName) ($($sp.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                    $script:htMeanwhileDeleted.($sp.id) = @{}
                    $meanwhileDeleted = $true
                }
            }
            else{
                if ($getSPOwner.Count -gt 0) {
                    foreach ($spOwner in $getSPOwner) {
        
                        if (-not $htOwnedBy.($sp.id)) {
                            $script:htOwnedBy.($sp.id) = @{}
                            $script:htOwnedBy.($sp.id).ownedBy = [array]$($spOwner | select-Object id, displayName, '@odata.type')
                        }
                        else {
                            $array = [array]($htOwnedBy.($sp.id).ownedBy)
                            $array += $spOwner | select-Object id, displayName, '@odata.type'
                            $script:htOwnedBy.($sp.id).ownedBy = $array
                        }
                    }
                    if (-not $htSPOwners.($sp.id)) {
                        $script:htSPOwners.($sp.id) = $getSPOwner | select-Object id, displayName, '@odata.type'
                    }
                }
                else {
                    $script:htOwnedBy.($sp.id) = @{}
                    $script:htOwnedBy.($sp.id).ownedBy = "noOwner"
                }
            }
        }
        #endregion spOwner 
        
        #region spApp
        if (-not $meanwhileDeleted){
            if ($sp.servicePrincipalType -eq "Application") {

                $spType = "APP"
                
                $currentTask = "getApp $($sp.appId)"
                $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/applications?`$filter=appId eq '$($sp.appId)'"
                $method = "GET"
                $getApplication = AzAPICall -uri $uri -method $method -currentTask $currentTask -getApp $true

                if ($getApplication -eq "Request_ResourceNotFound"){
                    if (-not $htMeanwhileDeleted.($sp.id)){
                        write-host "  $($sp.displayName) ($($sp.id)) AppId $($sp.appId) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                        $script:htMeanwhileDeleted.($sp.id) = @{}
                        $meanwhileDeleted = $true
                    }
                }
                else{
                    if ($getApplication.Count -gt 0) {
                        $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.Application = @{}
                        $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.Application.ApplicationDetails = $getApplication
                        $script:htApplications.($getApplication.id) = $getApplication
        
                        #region getAppOwner
                        $currentTask = "getAppOwner $($getApplication.id)"
                        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/applications/$($getApplication.id)/owners"              
                        $method = "GET"
                        $getAppOwner = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSP $true
        
                        if ($getAppOwner.Count -gt 0) {
                            if (-not $htAppOwners.($getApplication.id)) {
                                $script:htAppOwners.($getApplication.id) = $getAppOwner | select-Object id, displayName, '@odata.type'
                            }
                        }
                        #endregion getAppOwner
        
                        #region spAppKeyCredentials
                        if (($getApplication.keyCredentials).Count -gt 0) {
                            $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.Application.ApplicationKeyCredentials = @{}
                            foreach ($keyCredential in $getApplication.keyCredentials) {
                                $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.Application.ApplicationKeyCredentials.($keyCredential.keyId) = $keyCredential
                            }
                        }
                        #endregion spAppKeyCredentials
        
                        #region spAppPasswordCredentials
                        if (($getApplication.passwordCredentials).Count -gt 0) {
                            $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.Application.ApplicationPasswordCredentials = @{}
                            foreach ($passwordCredential in $getApplication.passwordCredentials) {
                                $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.Application.ApplicationPasswordCredentials.($passwordCredential.keyId) = $passwordCredential
                            }
                        }
                        #endregion spAppPasswordCredentials
                    }
                }
            }
        }
        #endregion spApp

        #region spManagedIdentity
        if (-not $meanwhileDeleted){
            if ($sp.servicePrincipalType -eq "ManagedIdentity") {
                $spType = "MI"

                $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.ManagedIdentity = @{}
                $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.ManagedIdentityDetails = $sp
            
                if (($sp.alternativeNames).Count -gt 0) {
                    $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.ManagedIdentity.ManagedIdentityAlternativeNames = $sp.alternativeNames
                }

                $miType = "unknown"
                foreach ($altName in $sp.alternativeNames) {
                    if ($altName -like "isExplicit=*") {
                        $splitAltName = $altName.split("=")
                        if ($splitAltName[1] -eq "true") {
                            $miType = "User assigned"
                        }
                        if ($splitAltName[1] -eq "false") {
                            $miType = "System assigned"
                        }
                    }
                    else {
                        $s1 = $altName -replace ".*/providers/"; $rm = $s1 -replace ".*/"; $resourceType = $s1 -replace "/$($rm)"              
                        $miResourceType = $resourceType
                        $altNameSplit = $altName.split('/')
                        if ($altName -like "/subscriptions/*"){
                            if ($resourceType -eq "Microsoft.Authorization/policyAssignments"){
                                if ($altName -like "/subscriptions/*/resourceGroups/*"){
                                    $miResourceScope = "Sub $($altNameSplit[2]) RG $($altNameSplit[4])"
                                }
                                else{
                                    $miResourceScope = "Sub $($altNameSplit[2])"
                                }
                            }
                            else{
                                $miResourceScope = "Sub $($altNameSplit[2])"
                            }
                        }
                        else{
                            $miResourceScope = "MG $($altNameSplit[4])"
                        }
                    }              
                }
            }
        }
        #endregion spManagedIdentity


        if (-not $meanwhileDeleted){
            if ($spType -eq "APP") {
                $script:htServicePrincipalsEnriched.($sp.id).spTypeConcatinated = "SP $($spType) $($spTypeINTEXT)"
                $script:htServicePrincipalsEnriched.($sp.id).type = $spType
                $script:htServicePrincipalsEnriched.($sp.id).subtype = $spTypeINTEXT
            }
            elseif ($spType -eq "MI") {
                $script:htServicePrincipalsEnriched.($sp.id).spTypeConcatinated = "SP $($spType) $($miType)"
                $script:htServicePrincipalsEnriched.($sp.id).type = $spType
                $script:htServicePrincipalsEnriched.($sp.id).subtype = $miType
                $script:htServicePrincipalsEnriched.($sp.id).altname = $altName
                $script:htServicePrincipalsEnriched.($sp.id).resourceType = $miResourceType
                $script:htServicePrincipalsEnriched.($sp.id).resourceScope = $miResourceScope
            }
            else {
                $script:htServicePrincipalsEnriched.($sp.id).spTypeConcatinated = "SP $($spTypeINTEXT)"
                $script:htServicePrincipalsEnriched.($sp.id).type = "SP"
                $script:htServicePrincipalsEnriched.($sp.id).subtype = $spTypeINTEXT
            }
        }
        else{
            $script:htServicePrincipalsEnriched.($sp.id).MeanWhileDeleted = $true
        }

        $processedServicePrincipalsCount = ($script:htServicePrincipalsEnriched.Keys).Count
        if ($processedServicePrincipalsCount) {
            if ($processedServicePrincipalsCount % $indicator -eq 0) {
                if (-not $script:htProcessedTracker.($processedServicePrincipalsCount)) {
                    $script:htProcessedTracker.($processedServicePrincipalsCount) = @{}
                    Write-Host " $processedServicePrincipalsCount Service Principals processed"
                }
            }
        }

    } -ThrottleLimit $ThrottleLimitGraph

    $endForeachSP = get-date
    $duration = NEW-TIMESPAN -Start $startForeachSP -End $endForeachSP
    Write-Host " Collecting data for all Service Principals ($($getServicePrincipals.Count)) duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"
    Write-Host " Service Principals that have been meanwhile deleted: $($htMeanwhileDeleted.Keys.Count)"
}
$end = get-date
$duration = NEW-TIMESPAN -Start $start -End $end
Write-Host "SP Collection duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"
#endregion dataColletionAADSP

$htUsersToResolveGuestMember = @{}

#region AppRoleAssignments4UsersAndGroups

$htUsersAndGroupsRoleAssignments = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
if ($htUsersAndGroupsToCheck4AppRoleAssignments.User.Keys.Count -gt 0) {

    #UsersToResolveGuestMember
    foreach ($user in $htUsersAndGroupsToCheck4AppRoleAssignments.User.Keys) {
        if (-not $htUsersToResolveGuestMember.($user)) {
            #Write-Host "UsersToResolveGuestMember user added ($($user))"
            $htUsersToResolveGuestMember.($user) = @{}
        }
    }

    $htUsersAndGroupsRoleAssignments.User = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htUsersAndGroupsToCheck4AppRoleAssignments.User.Keys | ForEach-Object -Parallel {
        $userObjectId = $_

        #array&ht
        $arrayAzureManagementEndPointUrls = $using:arrayAzureManagementEndPointUrls
        $checkContext = $using:checkContext
        $htAzureEnvironmentRelatedUrls = $using:htAzureEnvironmentRelatedUrls
        $htBearerAccessToken = $using:htBearerAccessToken
        $arrayAPICallTracking = $using:arrayAPICallTracking
        $htUsersAndGroupsRoleAssignments = $using:htUsersAndGroupsRoleAssignments
        #func
        $function:AzAPICall = $using:funcAzAPICall
        $function:createBearerToken = $using:funcCreateBearerToken
        $function:GetJWTDetails = $using:funcGetJWTDetails

        $currentTask = "getUser AppRoleAssignments $($userObjectId)"
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/users/$($userObjectId)/appRoleAssignments"
        $method = "GET"
        $getUserAppRoleAssignments = AzAPICall -uri $uri -method $method -currentTask $currentTask

        if ($getUserAppRoleAssignments.Count -gt 0) {
            foreach ($userAppRoleAssignment in $getUserAppRoleAssignments) {
                if (-not $htUsersAndGroupsRoleAssignments.User.($userObjectId).($userAppRoleAssignment.id)) {
                    if (-not $htUsersAndGroupsRoleAssignments.User.($userObjectId)) {
                        $script:htUsersAndGroupsRoleAssignments.User.($userObjectId) = @{}
                        $script:htUsersAndGroupsRoleAssignments.User.($userObjectId).($userAppRoleAssignment.id) = $userAppRoleAssignment
                    }
                    else {
                        $script:htUsersAndGroupsRoleAssignments.User.($userObjectId).($userAppRoleAssignment.id) = $userAppRoleAssignment
                    }
                }
            }
        }
    } -ThrottleLimit $ThrottleLimitGraph
}

if ($htUsersAndGroupsToCheck4AppRoleAssignments.Group.Keys.Count -gt 0) {
    $htUsersAndGroupsRoleAssignments.Group = @{}
    $htUsersAndGroupsToCheck4AppRoleAssignments.Group.Keys | ForEach-Object -Parallel {
        $groupObjectId = $_

        #array&ht
        $arrayAzureManagementEndPointUrls = $using:arrayAzureManagementEndPointUrls
        $checkContext = $using:checkContext
        $htAzureEnvironmentRelatedUrls = $using:htAzureEnvironmentRelatedUrls
        $htBearerAccessToken = $using:htBearerAccessToken
        $arrayAPICallTracking = $using:arrayAPICallTracking
        $htUsersAndGroupsRoleAssignments = $using:htUsersAndGroupsRoleAssignments
        #func
        $function:AzAPICall = $using:funcAzAPICall
        $function:createBearerToken = $using:funcCreateBearerToken
        $function:GetJWTDetails = $using:funcGetJWTDetails

        $currentTask = "getGroup AppRoleAssignments $($groupObjectId)"
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/Groups/$($groupObjectId)/appRoleAssignments"
        $method = "GET"
        $getGroupAppRoleAssignments = AzAPICall -uri $uri -method $method -currentTask $currentTask

        if ($getGroupAppRoleAssignments.Count -gt 0) {
            foreach ($groupAppRoleAssignment in $getGroupAppRoleAssignments) {
                if (-not $htUsersAndGroupsRoleAssignments.Group.($groupObjectId).($groupAppRoleAssignment.id)) {
                    if (-not $htUsersAndGroupsRoleAssignments.Group.($groupObjectId)) {
                        $script:htUsersAndGroupsRoleAssignments.Group.($groupObjectId) = @{}
                        $script:htUsersAndGroupsRoleAssignments.Group.($groupObjectId).($groupAppRoleAssignment.id) = $groupAppRoleAssignment
                    }
                    else {
                        $script:htUsersAndGroupsRoleAssignments.Group.($groupObjectId).($groupAppRoleAssignment.id) = $groupAppRoleAssignment
                    }
                }
            }
        }
    } -ThrottleLimit $ThrottleLimitGraph
}
#endregion AppRoleAssignments4UsersAndGroups

#region AADGroupsResolve

$htAadGroups = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}

#region groupsFromSPs
$start = get-date
Write-Host "Resolving AAD Groups where any SP is memberOf"
if (($htAadGroupsToResolve.Keys).Count -gt 0) {
    Write-Host " Resolving $(($htAadGroupsToResolve.Keys).Count) AAD Groups where any SP is memberOf"
    $start = get-date
        
    ($htAadGroupsToResolve.Keys) | ForEach-Object -Parallel {
        $aadGroupId = $_
    
        #array&ht
        $arrayAzureManagementEndPointUrls = $using:arrayAzureManagementEndPointUrls
        $checkContext = $using:checkContext
        $htAzureEnvironmentRelatedUrls = $using:htAzureEnvironmentRelatedUrls
        $htBearerAccessToken = $using:htBearerAccessToken
        $arrayAPICallTracking = $using:arrayAPICallTracking
        $htAadGroups = $using:htAadGroups
        #func
        $function:AzAPICall = $using:funcAzAPICall
        $function:createBearerToken = $using:funcCreateBearerToken
        $function:GetJWTDetails = $using:funcGetJWTDetails
            
        #Write-Host "resolving AAD Group: $aadGroupId"
        $currentTask = "get AAD Group $($aadGroupId)"
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/groups/$($aadGroupId)"
        $method = "GET"
        $getAadGroup = AzAPICall -uri $uri -method $method -currentTask $currentTask -listenOn "Content"

        $script:htAadGroups.($aadGroupId) = @{}
        $script:htAadGroups.($aadGroupId).groupDetails = $getAadGroup

        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/groups/$($aadGroupId)/transitivemembers/microsoft.graph.group?`$count=true"
        $method = "GET"
        $getNestedGroups = AzAPICall -uri $uri -method $method -currentTask $currentTask -consistencyLevel "eventual"

        if ($getNestedGroups) {
            write-host " $aadGroupId -> has nested Groups $($getNestedGroups.Count)"
            $script:htAadGroups.($aadGroupId).nestedGroups = $getNestedGroups
        }
    } -ThrottleLimit $ThrottleLimitGraph

    $end = get-date
    $duration = NEW-TIMESPAN -Start $start -End $end
    Write-Host "AADGroupsResolve duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"
}
else {
    Write-Host " Resolving $(($htAadGroupsToResolve.Keys).Count) AAD Groups where any SP is memberOf"
}
$end = get-date
$duration = NEW-TIMESPAN -Start $start -End $end
Write-Host "Resolving AAD Groups where any SP is memberOf duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"
#endregion groupsFromSPs
    
#region GroupsFromAzureRoleAssignments
$start = get-date
#batching
$counterBatch = [PSCustomObject] @{ Value = 0 }
$batchSize = 1000
$arrayObjectIdsToProcess = [System.Collections.ArrayList]@()
$objectIdsUnique = $htCacheAssignments.roleFromAPI.values.assignment.properties.principalId | Sort-Object -Unique
Write-Host " Unique objectIds that have Azure Role assignments: $($objectIdsUnique.Count)"
foreach ($objectId in $objectIdsUnique) {
    if ($htAadGroups.Keys -notcontains $objectId) {
        $null = $arrayObjectIdsToProcess.Add($objectId) 
    }
}
$objectIdsCount = $arrayObjectIdsToProcess.Count
Write-Host " Unique objectIds that have Azure Role assignments and are not resolved, yet: $($objectIdsCount)"

$objectIdsBatch = $arrayObjectIdsToProcess | Group-Object -Property { [math]::Floor($counterBatch.Value++ / $batchSize) }
$objectIdsBatchCount = ($objectIdsBatch | Measure-Object).Count
$batchCnt = 0

Write-Host "Processing $objectIdsCount objectIds"
foreach ($batch in $objectIdsBatch) {

    $batchCnt++
    Write-Host " processing Batch #$batchCnt/$($objectIdsBatchCount) ($(($batch.Group).Count) objectIds)"
    $objectIdsToCheckIfGroup = '"{0}"' -f ($batch.Group -join '","')

    $currentTask = " Resolving identity type Group - Batch #$batchCnt/$($objectIdsBatchCount) ($(($batch.Group).Count)"
    $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/directoryObjects/getByIds"
    $method = "POST"
    $body = @"
        {
            "ids":[$($objectIdsToCheckIfGroup)],
            "types":["group"]
        }
"@
    $resolveObjectIdsTypeGroup = AzAPICall -uri $uri -method $method -body $body -currentTask $currentTask
    
    foreach ($group in $resolveObjectIdsTypeGroup) {
        $script:htAadGroups.($group.id) = @{}
        $script:htAadGroups.($group.id).groupDetails = $group

        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/groups/$($group.id)/transitivemembers/microsoft.graph.group?`$count=true"
        $method = "GET"
        $getNestedGroups = AzAPICall -uri $uri -method $method -currentTask $currentTask -consistencyLevel "eventual"

        if ($getNestedGroups) {
            write-host " -> has nested Groups $($getNestedGroups.Count)"
            $script:htAadGroups.($group.id).nestedGroups = $getNestedGroups
            foreach ($nestedGroup in $getNestedGroups) {
                if (-not $htAadGroups.($nestedGroup.id)) {
                    $htAadGroups.($nestedGroup.id) = @{}
                    $htAadGroups.($nestedGroup.id).groupDetails = $nestedGroup
                }
            }
        }
    }
    Write-Host "Groups resolved: $($htAadGroups.Keys.Count)"
}

$end = get-date
$duration = NEW-TIMESPAN -Start $start -End $end
Write-Host "Getting all AAD Groups duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"
#endregion GroupsFromAzureRoleAssignments

#endregion AADGroupsResolve

#region owners
Write-Host "Processing SP/App Owners"

#UsersToResolveGuestMember
$htUsersResolved = @{}
foreach ($spOwner in $htSPOwners.Values) {
    foreach ($owner in $spOwner) {
        if ($owner.'@odata.type' -eq "#microsoft.graph.user") {
            if (-not $htUsersToResolveGuestMember.($owner.id)) {
                #Write-Host "UsersToResolveGuestMember SPowner added ($($owner.id))"
                $htUsersToResolveGuestMember.($owner.id) = @{}
            }
        }
    }
}
foreach ($appOwner in $htAppOwners.Values) {
    foreach ($owner in $appOwner) {
        if ($owner.'@odata.type' -eq "#microsoft.graph.user") {
            if (-not $htUsersToResolveGuestMember.($owner.id)) {
                #Write-Host "UsersToResolveGuestMember appOwner added ($($owner.id))"
                $htUsersToResolveGuestMember.($owner.id) = @{}
            }
        }
    }
}
resolveObectsById -objects $htUsersToResolveGuestMember.Keys -targetHt "htUsersResolved"

$htOwnedByEnriched = @{}
foreach ($sp in $htOwnedBy.Keys) {
    $htOwnedByEnriched.($sp) = @{}    
    foreach ($ownedBy in $htOwnedBy.($sp).ownedBy) {
        $arrayx = @()
        if ($ownedBy -ne "noOwner") {
            foreach ($owner in $ownedBy) {
                $htTmp = [ordered] @{}
                $htTmp.id = $owner.id
                $htTmp.displayName = $owner.displayName
                $htTmp.'@odata.type' = $owner.'@odata.type'
                if ($owner.'@odata.type' -eq "#microsoft.graph.servicePrincipal"){
                    $hlpType = $htServicePrincipalsEnriched.($owner.id).spTypeConcatinated
                    $htTmp.spType = $hlpType
                    $htTmp.principalType = $hlpType
                }
                if ($owner.'@odata.type' -eq "#microsoft.graph.user"){
                    $htTmp.principalType = $htUsersResolved.($owner.id).typeOnly
                }
                $htTmp.applicability = "direct"
                $arrayx += $htTmp
                if (-not $htOwnedByEnriched.($sp)) {
                    $htOwnedByEnriched.($sp) = @{}
                    $htOwnedByEnriched.($sp).ownedBy = [array]$arrayx
                }
                else {
                    $array = [array]($htOwnedByEnriched.($sp).ownedBy)
                    $array += $arrayx
                    $htOwnedByEnriched.($sp).ownedBy = $array
                }
            }
        }
        else {
            $arrayx += $ownedBy
            if (-not $htOwnedByEnriched.($sp)) {
                $htOwnedByEnriched.($sp) = @{}
                $htOwnedByEnriched.($sp).ownedBy = [array]$arrayx
            }
            else {
                $array = [array]($htOwnedByEnriched.($sp).ownedBy)
                $array += $arrayx
                $htOwnedByEnriched.($sp).ownedBy = $array
            }
        }
    }
}

function getOwner($owner) {
    return $htOwnedByEnriched.($owner).ownedBy
}
$htSPOwnersTmp = @{}
$htSPOwnersFinal = @{}

foreach ($sp in $htServicePrincipalsEnriched.Keys) {
    
    $stopIt = $false
    $htSPOwnersTmp.($sp) = @{}
    $htSPOwnersTmp.($sp).direct = @()
    $htSPOwnersTmp.($sp).indirect = @()
    foreach ($owner in $htSPOwners.($sp).where({ $_.'@odata.type' -eq '#microsoft.graph.user' })) {
        $htSPOwnersTmp.($sp).direct += $owner
    }
    foreach ($owner in $htSPOwners.($sp).where({ $_.'@odata.type' -eq '#microsoft.graph.servicePrincipal' })) {
        $htSPOwnersTmp.($sp).direct += $owner
    }
    $owners = $htSPOwnersTmp.($sp).direct
    $directsDone = $false
    do {
        if ($owners.Count -gt 0) {
            foreach ($owner in $owners | sort-object -property '@odata.type' -Descending) {
                #write-host $owner.displayName
                if ($owner.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
                    $directsDone = $true
                    $owners = getowner -owner $owner.id
                    foreach ($owner in ($owners)) {
                        if ($htSPOwnersTmp.($sp).indirect.id -contains $owner.id) {
                            #Write-Host "stepping out $($owner.displayName)"
                            $stopIt = $true
                            continue       
                        }
                        else {
                            $htSPOwnersTmp.($sp).indirect += ($owner)
                        }
                    }
                }
                else {
                    if ($directsDone -eq $true) {
                        if ($owner -eq "noOwner") {
                            #Write-Host "stepping out noOwner"
                            $stopIt = $true
                            continue    
                        }
                        if ($htSPOwnersTmp.($sp).indirect.id -contains $owner.id) {
                            #Write-Host "stepping out $($owner.displayName)"
                            $stopIt = $true
                            continue       
                        }
                        $htSPOwnersTmp.($sp).indirect += ($owner)
                    }
                    else {
                        if ($htSPOwnersTmp.($sp).direct.id -contains $owner.id) {
                            #Write-Host "stepping out $($owner.displayName)"
                            $stopIt = $true
                            continue       
                        }
                    }
                }
            }
        }
        else {
            $stopIt = $true
        }
    }
    until($stopIt -eq $true)

    $arrayOwners = [System.Collections.ArrayList]@()
    foreach ($owner in $htSPOwnersTmp.($sp).direct) {
        $htOptInfo = [ordered] @{}
        $htOptInfo.id = $($owner.id)
        $htOptInfo.displayName = $($owner.displayName)
        $htOptInfo.type = $($owner.'@odata.type')
        if ($owner.'@odata.type' -eq "#microsoft.graph.servicePrincipal") {
            $htOptInfo.spType = $htServicePrincipalsEnriched.($owner.id).spTypeConcatinated
            $htOptInfo.principalType = $htServicePrincipalsEnriched.($owner.id).spTypeConcatinated
        }
        if ($owner.'@odata.type' -eq "#microsoft.graph.user") {
            $htOptInfo.principalType = $htUsersResolved.($owner.id).typeOnly
        }
        $htOptInfo.applicability = "direct"
        $owners = $null
        if ($owner.'@odata.type' -eq "#microsoft.graph.servicePrincipal") {
            $owners = getowner -owner $owner.id
        }
        $htOptInfo.ownedBy = $owners
        $null = $arrayOwners.Add($htOptInfo)
    }

    foreach ($owner in $htSPOwnersTmp.($sp).indirect) {
        if ($owner -eq "noOwner" -or $owner.'@odata.type' -eq '#microsoft.graph.user') {
            if ($owner.'@odata.type' -eq '#microsoft.graph.user') {
                if (($arrayOwners.where({ $_.applicability -eq "indirect" })).id -notcontains $owner.id) {
                    $htOptInfo = [ordered] @{}
                    $htOptInfo.id = $($owner.id)
                    $htOptInfo.displayName = $($owner.displayName)
                    $htOptInfo.type = $($owner.'@odata.type')
                    $htOptInfo.principalType = $htUsersResolved.($owner.id).typeOnly
                    $htOptInfo.applicability = "indirect"
                    $null = $arrayOwners.Add($htOptInfo)
                }
            }
        }
        else {
            $htOptInfo = [ordered] @{}
            $htOptInfo.id = $($owner.id)
            $htOptInfo.displayName = $($owner.displayName)
            $htOptInfo.type = $($owner.'@odata.type')
            $htOptInfo.applicability = "indirect"
            if ($owner.'@odata.type' -eq "#microsoft.graph.servicePrincipal") {
                $htOptInfo.principalType = $htServicePrincipalsEnriched.($owner.id).spTypeConcatinated
            }
            if ($owner.'@odata.type' -eq "#microsoft.graph.user") {
                $htOptInfo.principalType = $htOptInfo.principalType = $htUsersResolved.($owner.id).typeOnly
            }

            $owners = getowner -owner $owner.id
            $htOptInfo.ownedBy = $owners
            $null = $arrayOwners.Add($htOptInfo)
        }
    }

    if ($arrayOwners.Count -gt 0) {
        $htSPOwnersFinal.($sp) = @{}
        $htSPOwnersFinal.($sp) = $arrayOwners
    }  

}

#App
$htAppOwnersFinal = @{}
foreach ($app in $htAppOwners.Keys) {
    $htAppOwnersFinal.($app) = @{}
    $array = @()
    foreach ($owner in $htAppOwners.($app)) {
        if ($owner.'@odata.type' -eq '#microsoft.graph.user') {
            $htOpt = [ordered] @{}
            $htOpt.id = $owner.id
            $htOpt.displayName = $owner.displayName
            $htOpt.type = $owner.'@odata.type'
            $htOpt.principalType = $htUsersResolved.($owner.id).typeOnly
            $array += $htOpt
        }
        else {
            $htOpt = [ordered] @{}
            $htOpt.id = $owner.id
            $htOpt.displayName = $owner.displayName
            $htOpt.type = $owner.'@odata.type'
            $htOpt.spType = $htServicePrincipalsEnriched.($owner.id).spTypeConcatinated
            $htOpt.principalType = $htServicePrincipalsEnriched.($owner.id).spTypeConcatinated
            $htOpt.ownedBy = $htSPOwnersFinal.($owner.id)
            $array += $htOpt
        }
    }
    $htAppOwnersFinal.($app) = $array
}

#endregion owners

if (-not $NoAzureRoleAssignments) {
    #region AzureRoleAssignmentMapping
    $start = get-date

    #resolving createdby/updatedby
    #$htUsersResolved = @{}
    $htCreatedByUpdatedByObjectIdsToBeResolved = @{}
    foreach ($createdByItem in $htCacheAssignments.roleFromAPI.values.assignment.properties.createdBy | Sort-Object -Unique) {
        
        if ([guid]::TryParse(($createdByItem), $([ref][guid]::Empty))){
            if (-not $htUsersResolved.($createdByItem)) {            
                if ($getServicePrincipals.id -contains $createdByItem) {
                    if ($htServicePrincipalsEnriched.($createdByItem)) {
                        $hlper = $htServicePrincipalsEnriched.($createdByItem)
                        $htUsersResolved.($createdByItem) = @{}
                        $htUsersResolved.($createdByItem).full = "$($hlper.spTypeConcatinated), DisplayName: $($hlper.ServicePrincipal.ServicePrincipalDetails.displayName), Id: $($createdByItem)"
                        $htUsersResolved.($createdByItem).typeOnly = $hlper.spTypeConcatinated
                    }
                }
                else {
                    if ($htUsersResolved.($createdByItem)){
                        $htUsersResolved.($createdByItem) = @{}
                        $htUsersResolved.($createdByItem).full = $htUsersResolved.($createdByItem).full
                        $htUsersResolved.($createdByItem).typeOnly = $htUsersResolved.($createdByItem).typeOnly
                    }
                    else{
                        if (-not $htCreatedByUpdatedByObjectIdsToBeResolved.($createdByItem)) {
                            $htCreatedByUpdatedByObjectIdsToBeResolved.($createdByItem) = @{}
                        }
                    }
                }
            }
        }
    }

    $createdByUpdatedByObjectIdsToBeResolvedCount = ($htCreatedByUpdatedByObjectIdsToBeResolved.Keys).Count
    if ($createdByUpdatedByObjectIdsToBeResolvedCount -gt 0) {
        Write-Host "$createdByUpdatedByObjectIdsToBeResolvedCount unresolved createdBy identities"
        $arrayUnresolvedIdentities = @()
        $arrayUnresolvedIdentities = foreach ($unresolvedIdentity in $htCreatedByUpdatedByObjectIdsToBeResolved.keys) {
            if (-not [string]::IsNullOrEmpty($unresolvedIdentity)) {
                $unresolvedIdentity
            }
        }
        $arrayUnresolvedIdentitiesCount = $arrayUnresolvedIdentities.Count
        Write-Host "    $arrayUnresolvedIdentitiesCount unresolved identities that have a value"
        resolveObectsById -objects $arrayUnresolvedIdentities -targetHt "htUsersResolved"        
    }

    if ($htCacheAssignments.Keys.Count -gt 0) {
        $htAssignmentsByPrincipalId = @{}
        $htAssignmentsByPrincipalId."servicePrincipals" = @{}
        $htAssignmentsByPrincipalId."groups" = @{}
        foreach ($assignment in $htCacheAssignments.roleFromAPI.values) {
            #todo sp created ra in azure
            if (-not [string]::IsNullOrEmpty($assignment.assignment.properties.createdBy)){
                if ($htUsersResolved.($assignment.assignment.properties.createdBy)) {
                    $assignment.assignment.properties.createdBy = $htUsersResolved.($assignment.assignment.properties.createdBy).full
                }
            }
            if ($getServicePrincipals.id -contains $assignment.assignment.properties.principalId) {
                if (-not $htAssignmentsByPrincipalId."servicePrincipals".($assignment.assignment.properties.principalId)) {
                    $htAssignmentsByPrincipalId."servicePrincipals".($assignment.assignment.properties.principalId) = [array]$assignment
                }
                else {
                    $htAssignmentsByPrincipalId."servicePrincipals".($assignment.assignment.properties.principalId) += $assignment
                }
            }
            if ($htAadGroups.Keys -contains $assignment.assignment.properties.principalId) {
                if (-not $htAssignmentsByPrincipalId."groups".($assignment.assignment.properties.principalId)) {
                    $htAssignmentsByPrincipalId."groups".($assignment.assignment.properties.principalId) = [array]$assignment
                }
                else {
                    $htAssignmentsByPrincipalId."groups".($assignment.assignment.properties.principalId) += $assignment
                }
            }
        }
    }
    else {
        Write-Host " No RoleAssignments?!"
        break
    }
    $end = get-date
    $duration = NEW-TIMESPAN -Start $start -End $end
    Write-Host "AzureRoleAssignmentMapping duration: $(($duration).TotalMinutes) minutes ($(($duration).TotalSeconds) seconds)"
    #endregion AzureRoleAssignmentMapping
}

#region enrichedAADSPData
Write-Host "Enrichment starting prep"
$cu = [System.Collections.ArrayList]@()
$appPasswordCredentialsExpiredCount = 0
$appPasswordCredentialsGracePeriodExpiryCount = 0
$appPasswordCredentialsExpiryOKCount = 0
$appPasswordCredentialsExpiryOKMoreThan2YearsCount = 0
$appKeyCredentialsExpiredCount = 0
$appKeyCredentialsGracePeriodExpiryCount = 0
$appKeyCredentialsExpiryOKCount = 0
$appKeyCredentialsExpiryOKMoreThan2YearsCount = 0

$htSPandAPPHelper4AADRoleAssignmentsWithScope = @{}
$htAADRoleAssignmentOnSPOrAPP = @{}
$htAADRoleAssignmentOnSPOrAPP.SP = @{}
$htAADRoleAssignmentOnSPOrAPP.APP = @{}
foreach ($aadRoleAssignment in $htServicePrincipalsEnriched.values.ServicePrincipal.ServicePrincipalAADRoleAssignments) {
    if ($aadRoleAssignment.resourceScope -ne "/") {
        
        if ($htApplications.($aadRoleAssignment.resourceScope -replace "/")) {
            if (-not $htSPandAPPHelper4AADRoleAssignmentsWithScope.($aadRoleAssignment.resourceScope -replace "/")) {
                $hlp = $htApplications.($aadRoleAssignment.resourceScope -replace "/")
                $htSPandAPPHelper4AADRoleAssignmentsWithScope.($aadRoleAssignment.resourceScope -replace "/") = "Application: $($hlp.displayname) ($($hlp.id))"
                if (-not $htAADRoleAssignmentOnSPOrAPP.APP.($hlp.id)) {
                    $htAADRoleAssignmentOnSPOrAPP.APP.($hlp.id) = [array]$aadRoleAssignment
                }
                else {
                    $htAADRoleAssignmentOnSPOrAPP.APP.($hlp.id) += $aadRoleAssignment
                }
            }
            
        }
        else {
            if ($htServicePrincipalsEnriched.($aadRoleAssignment.resourceScope -replace "/")) {
                if (-not $htSPandAPPHelper4AADRoleAssignmentsWithScope.($aadRoleAssignment.resourceScope -replace "/")) {
                    $hlp = $htServicePrincipalsEnriched.($aadRoleAssignment.resourceScope -replace "/").ServicePrincipal.ServicePrincipalDetails
                    $htSPandAPPHelper4AADRoleAssignmentsWithScope.($aadRoleAssignment.resourceScope -replace "/") = "ServicePrincipal: $($hlp.displayname) ($($hlp.id))"
                    if (-not $htAADRoleAssignmentOnSPOrAPP.SP.($hlp.id)) {
                        $htAADRoleAssignmentOnSPOrAPP.SP.($hlp.id) = [array]$aadRoleAssignment
                    }
                    else {
                        $htAADRoleAssignmentOnSPOrAPP.SP.($hlp.id) += $aadRoleAssignment
                    }
                }
            }
        }
    }
}
Write-Host "Enrichment completed prep"

Write-Host "Enrichment starting"
$indicator = 100
$processedServicePrincipalsCount = 0
$startEnrichmentSP = get-date
#foreach ($sp in $htServicePrincipalsEnriched.values) {
foreach ($sp in ($htServicePrincipalsEnriched.values).where( { -not $_.MeanWhileDeleted } )) {

    #Write-host "processing SP:" $sp.ServicePrincipal.ServicePrincipalDetails.displayName "objId: $($sp.ServicePrincipal.ServicePrincipalDetails.id)" "appId: $($sp.ServicePrincipal.ServicePrincipalDetails.appId)"

    if ($processedServicePrincipalsCount -gt 0 -and $processedServicePrincipalsCount % $indicator -eq 0) {
        Write-Host "$processedServicePrincipalsCount ServicePrincipals processed"
    }

    #region ServicePrincipalOwnedObjects
    $arrayServicePrincipalOwnedObjectsOpt = [System.Collections.ArrayList]@()
    if (($sp.ServicePrincipal.ServicePrincipalOwnedObjects).Count -gt 0) {
        foreach ($ownedObject in $sp.ServicePrincipal.ServicePrincipalOwnedObjects) {
            
            $type = "unforseen type"
            if ($ownedObject.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
                $type = "Serviceprincipal"
            }
            if ($ownedObject.'@odata.type' -eq '#microsoft.graph.application') {
                $type = "Application"
            }
            if ($ownedObject.'@odata.type' -eq '#microsoft.graph.group') {
                $type = "Group"
            }
            $htOptInfo = [ordered] @{}
            $htOptInfo.type = $type
            if ($type -eq "Serviceprincipal"){
                $htOptInfo.typeDetailed = $htServicePrincipalsEnriched.($ownedObject.id).spTypeConcatinated
            }
            $htOptInfo.displayName = $ownedObject.displayName
            $htOptInfo.objectId = $ownedObject.id
            $null = $arrayServicePrincipalOwnedObjectsOpt.Add($htOptInfo)
            #Write-Host "SP OwnedObjects             : $($type) $($ownedObject.displayName) ($($ownedObject.id))"
        }
    }
    #endregion ServicePrincipalOwnedObjects
    
    #region ServicePrincipalOwners
    $arrayServicePrincipalOwnerOpt = [System.Collections.ArrayList]@()
    if ($htSPOwnersFinal.($sp.ServicePrincipal.ServicePrincipalDetails.id)) {
        foreach ($servicePrincipalOwner in $htSPOwnersFinal.($sp.ServicePrincipal.ServicePrincipalDetails.id)) {
            $htOptInfo = [ordered] @{}
            $htOptInfo.id = $servicePrincipalOwner.id
            $htOptInfo.displayName = $servicePrincipalOwner.displayName
            $htOptInfo.principalType = $servicePrincipalOwner.principalType
            $htOptInfo.applicability = $servicePrincipalOwner.applicability
            $arrayOwnedBy = @()
            
            foreach ($owner in $servicePrincipalOwner.ownedBy) {
                if ($owner -ne "noOwner") {
                    if ($htSPOwnersFinal.($owner.id)) {
                        $arrayOwnedBy += $htSPOwnersFinal.($owner.id)
                    }
                    else {
                        $arrayOwnedBy += $owner
                    }
                }
                else {
                    $arrayOwnedBy += $owner
                }
                
            }
            if ($servicePrincipalOwner.type -ne "#microsoft.graph.user") {
                $htOptInfo.ownedBy = $arrayOwnedBy
            }
            
            $null = $arrayServicePrincipalOwnerOpt.Add($htOptInfo)
        }
    }
    #endregion ServicePrincipalOwners

    #region ServicePrincipalAADRoleAssignments
    $arrayServicePrincipalAADRoleAssignmentsOpt = [System.Collections.ArrayList]@()
    if ($sp.ServicePrincipal.ServicePrincipalAADRoleAssignments) {
        foreach ($servicePrincipalAADRoleAssignment in $sp.ServicePrincipal.ServicePrincipalAADRoleAssignments) {
            $hlper = $htAadRoleDefinitions.($servicePrincipalAADRoleAssignment.roleDefinitionId)
            if ($hlper.isBuiltIn) {
                $roleType = "BuiltIn"
            }
            else {
                $roleType = "Custom"
            }

            $htOptInfo = [ordered] @{}
            $htOptInfo.id = $servicePrincipalAADRoleAssignment.id
            $htOptInfo.roleDefinitionId = $servicePrincipalAADRoleAssignment.roleDefinitionId
            $htOptInfo.roleDefinitionName = $hlper.displayName
            $htOptInfo.roleDefinitionDescription = $hlper.description
            $htOptInfo.roleType = $roleType
            $htOptInfo.directoryScopeId = $servicePrincipalAADRoleAssignment.directoryScopeId
            $htOptInfo.resourceScope = $servicePrincipalAADRoleAssignment.resourceScope
            if ($servicePrincipalAADRoleAssignment.resourceScope -ne "/") {
                if ($htSPandAPPHelper4AADRoleAssignmentsWithScope.($servicePrincipalAADRoleAssignment.resourceScope -replace "/")) {
                    $htOptInfo.scopeDetail = $htSPandAPPHelper4AADRoleAssignmentsWithScope.($servicePrincipalAADRoleAssignment.resourceScope -replace "/")
                }
            }
            $null = $arrayServicePrincipalAADRoleAssignmentsOpt.Add($htOptInfo)
        }
    }
    #endregion ServicePrincipalAADRoleAssignments

    <#region ServicePrincipalAADRoleAssignmentScheduleInstances
    $arrayServicePrincipalAADRoleAssignmentScheduleInstancesOpt = [System.Collections.ArrayList]@()
    if ($sp.ServicePrincipal.ServicePrincipalAADRoleAssignmentScheduleInstances) {
        foreach ($servicePrincipalAADRoleAssignmentScheduleInstance in $sp.ServicePrincipal.ServicePrincipalAADRoleAssignmentScheduleInstances) {
            $hlper = $htAadRoleDefinitions.($servicePrincipalAADRoleAssignmentScheduleInstance.roleDefinitionId)
            if ($hlper.isBuiltIn) {
                $roleType = "BuiltIn"
            }
            else {
                $roleType = "Custom"
            }

            $htOptInfo = [ordered] @{}
            $htOptInfo.id = $servicePrincipalAADRoleAssignmentScheduleInstance.id
            $htOptInfo.roleDefinitionId = $servicePrincipalAADRoleAssignmentScheduleInstance.roleDefinitionId
            $htOptInfo.roleDefinitionName = $hlper.displayName
            $htOptInfo.roleDefinitionDescription = $hlper.description
            $htOptInfo.roleType = $roleType
            $htOptInfo.directoryScopeId = $servicePrincipalAADRoleAssignmentScheduleInstance.directoryScopeId
            $htOptInfo.resourceScope = $servicePrincipalAADRoleAssignmentScheduleInstance.resourceScope
            if ($servicePrincipalAADRoleAssignmentScheduleInstance.resourceScope -ne "/") {
                if ($htSPandAPPHelper4AADRoleAssignmentsWithScope.($servicePrincipalAADRoleAssignmentScheduleInstance.resourceScope -replace "/")) {
                    $htOptInfo.scopeDetail = $htSPandAPPHelper4AADRoleAssignmentsWithScope.($servicePrincipalAADRoleAssignmentScheduleInstance.resourceScope -replace "/")
                }
            }
            $null = $arrayServicePrincipalAADRoleAssignmentScheduleInstancesOpt.Add($htOptInfo)
        }
    }
    #endregion ServicePrincipalAADRoleAssignmentScheduleInstances
    #>

    #region ServicePrincipalAADRoleAssignedOn
    $arrayServicePrincipalAADRoleAssignedOnOpt = [System.Collections.ArrayList]@()
    if ($htAADRoleAssignmentOnSPOrAPP.SP.($sp.ServicePrincipal.ServicePrincipalDetails.id)) {
        foreach ($aadRoleAssignedOn in $htAADRoleAssignmentOnSPOrAPP.SP.($sp.ServicePrincipal.ServicePrincipalDetails.id)) {
            $htOptInfo = [ordered] @{}
            $htOptInfo.id = $aadRoleAssignedOn.id
            $htOptInfo.roleName = $htAadRoleDefinitions.($aadRoleAssignedOn.roleDefinitionId).displayName
            $htOptInfo.roleId = $aadRoleAssignedOn.roleDefinitionId
            $htOptInfo.roleDescription = $htAadRoleDefinitions.($aadRoleAssignedOn.roleDefinitionId).description
            $htOptInfo.principalId = $aadRoleAssignedOn.principalId
            $htOptInfo.principalDisplayName = $htServicePrincipalsEnriched.($aadRoleAssignedOn.principalId).ServicePrincipal.ServicePrincipalDetails.DisplayName
            $htOptInfo.principalType = $htServicePrincipalsEnriched.($aadRoleAssignedOn.principalId).spTypeConcatinated
            $null = $arrayServicePrincipalAADRoleAssignedOnOpt.Add($htOptInfo)
        }
    }
    #endregion ServicePrincipalAADRoleAssignedOn

    #region ServicePrincipalOauth2PermissionGrants
    $arrayServicePrincipalOauth2PermissionGrantsOpt = [System.Collections.ArrayList]@()
    if ($sp.ServicePrincipal.ServicePrincipalOauth2PermissionGrants) {
        foreach ($servicePrincipalOauth2PermissionGrant in $sp.ServicePrincipal.ServicePrincipalOauth2PermissionGrants | Sort-Object -Property resourceId) {
            $multipleScopes = $servicePrincipalOauth2PermissionGrant.scope.split(" ")
            foreach ($scope in $multipleScopes | Sort-Object) {
                if (-not [string]::IsNullOrEmpty($scope) -and -not [string]::IsNullOrWhiteSpace($scope)) {
                    $hlperServicePrincipalsPublishedPermissionScope = $htServicePrincipalsPublishedPermissionScopes.($servicePrincipalOauth2PermissionGrant.resourceId)
                    $hlperPublishedPermissionScope = $htPublishedPermissionScopes.($servicePrincipalOauth2PermissionGrant.resourceId).($scope)

                    $htOptInfo = [ordered] @{}
                    $htOptInfo.SPId = $hlperServicePrincipalsPublishedPermissionScope.spdetails.id
                    $htOptInfo.SPAppId = $hlperServicePrincipalsPublishedPermissionScope.spdetails.appId
                    $htOptInfo.SPDisplayName = $hlperServicePrincipalsPublishedPermissionScope.spdetails.displayName
                    $htOptInfo.scope = $scope
                    $htOptInfo.permission = $hlperPublishedPermissionScope.value
                    $htOptInfo.id = $hlperPublishedPermissionScope.id
                    $htOptInfo.type = $hlperPublishedPermissionScope.type
                    $htOptInfo.adminConsentDisplayName = $hlperPublishedPermissionScope.adminConsentDisplayName
                    $htOptInfo.adminConsentDescription = $hlperPublishedPermissionScope.adminConsentDescription
                    $htOptInfo.userConsentDisplayName = $hlperPublishedPermissionScope.userConsentDisplayName
                    $htOptInfo.userConsentDescription = $hlperPublishedPermissionScope.userConsentDescription
                    $null = $arrayServicePrincipalOauth2PermissionGrantsOpt.Add($htOptInfo)
                }
            }
        }
    }
    #endregion ServicePrincipalOauth2PermissionGrants

    #region SPOauth2PermissionGrantedTo
    $arraySPOauth2PermissionGrantedTo = [System.Collections.ArrayList]@()
    if ($htSPOauth2PermissionGrantedTo.($sp.ServicePrincipal.ServicePrincipalDetails.id)) {
        foreach ($SPOauth2PermissionGrantedTo in $htSPOauth2PermissionGrantedTo.($sp.ServicePrincipal.ServicePrincipalDetails.id) | Sort-Object -Property clientId, id) {
            foreach ($SPOauth2PermissionGrantedToScope in $SPOauth2PermissionGrantedTo.scope | Sort-Object) {
                $spHlper = $htServicePrincipalsEnriched.($SPOauth2PermissionGrantedTo.clientId).ServicePrincipal
                $htOptInfo = [ordered] @{}
                $htOptInfo.servicePrincipalDisplayName = $spHlper.ServicePrincipalDetails.displayName
                $htOptInfo.servicePrincipalObjectId = $spHlper.ServicePrincipalDetails.id
                $htOptInfo.servicePrincipalAppId = $spHlper.ServicePrincipalDetails.appId
                $htOptInfo.applicationDisplayName = $spHlper.Application.ApplicationDetails.displayName
                $htOptInfo.applicationObjectId = $spHlper.Application.ApplicationDetails.id
                $htOptInfo.applicationAppId = $spHlper.Application.ApplicationDetails.appId
                $htOptInfo.clientId = $SPOauth2PermissionGrantedTo.clientId
                $htOptInfo.id = $SPOauth2PermissionGrantedTo.id
                $htOptInfo.permissionId = $htPublishedPermissionScopes.($SPOauth2PermissionGrantedTo.resourceId).($SPOauth2PermissionGrantedTo.scope).id
                $htOptInfo.scope = $SPOauth2PermissionGrantedToScope
                $htOptInfo.consentType = $SPOauth2PermissionGrantedTo.consentType
                $htOptInfo.startTime = $SPOauth2PermissionGrantedTo.startTime
                $htOptInfo.expiryTime = $SPOauth2PermissionGrantedTo.expiryTime
                $null = $arraySPOauth2PermissionGrantedTo.Add($htOptInfo)
            }
        }
        #$arraySPOauth2PermissionGrantedTo.servicePrincipalObjectId
    }
    #endregion SPOauth2PermissionGrantedTo
        
    #region ServicePrincipalAppRoleAssignments
    $arrayServicePrincipalAppRoleAssignmentsOpt = [System.Collections.ArrayList]@()
    if ($sp.ServicePrincipal.ServicePrincipalAppRoleAssignments) {
        foreach ($servicePrincipalAppRoleAssignment in $sp.ServicePrincipal.ServicePrincipalAppRoleAssignments) {
            $hlper = $htAppRoles.($servicePrincipalAppRoleAssignment.appRoleId)

            $htOptInfo = [ordered] @{}
            $htOptInfo.AppRoleAssignmentId = $servicePrincipalAppRoleAssignment.id
            $htOptInfo.AppRoleAssignmentResourceId = $servicePrincipalAppRoleAssignment.resourceId
            $htOptInfo.AppRoleAssignmentResourceDisplayName = $servicePrincipalAppRoleAssignment.resourceDisplayName
            $htOptInfo.AppRoleAssignmentCreatedDateTime = $servicePrincipalAppRoleAssignment.createdDateTime
            $htOptInfo.AppRoleId = $hlper.id
            $htOptInfo.AppRoleAllowedMemberTypes = $hlper.allowedMemberTypes
            $htOptInfo.AppRoleOrigin = $hlper.origin
            $htOptInfo.AppRolePermission = $hlper.value
            $htOptInfo.AppRoleDisplayName = $hlper.displayName
            $htOptInfo.AppRoleDescription = $hlper.description
            $null = $arrayServicePrincipalAppRoleAssignmentsOpt.Add($htOptInfo)
        }
    }
    #endregion ServicePrincipalAppRoleAssignments

    #region ServicePrincipalAppRoleAssignedTo
    $arrayServicePrincipalAppRoleAssignedToOpt = [System.Collections.ArrayList]@()
    if ($sp.ServicePrincipal.ServicePrincipalAppRoleAssignedTo) {

        foreach ($servicePrincipalAppRoleAssignedTo in $sp.ServicePrincipal.ServicePrincipalAppRoleAssignedTo) {
            $htOptInfo = [ordered] @{}
            $htOptInfo.principalDisplayName = $servicePrincipalAppRoleAssignedTo.principalDisplayName
            $htOptInfo.principalId = $servicePrincipalAppRoleAssignedTo.principalId
            $htOptInfo.principalType = $servicePrincipalAppRoleAssignedTo.principalType
            $htOptInfo.id = $servicePrincipalAppRoleAssignedTo.id
            $htOptInfo.resourceDisplayName = $servicePrincipalAppRoleAssignedTo.resourceDisplayName
            $htOptInfo.resourceId = $servicePrincipalAppRoleAssignedTo.resourceId
            if ($htAppRoleAssignments."$($servicePrincipalAppRoleAssignedTo.id)") {
                $hlper = $htAppRoles.($htAppRoleAssignments."$($servicePrincipalAppRoleAssignedTo.id)".appRoleId)
                $htOptInfo.roleId = $hlper.id
                $htOptInfo.roleOrigin = $hlper.origin
                $htOptInfo.roleAllowedMemberTypes = $hlper.allowedMemberTypes
                $htOptInfo.roleDisplayName = $hlper.displayName
                $htOptInfo.roleDescription = $hlper.description
                $htOptInfo.roleValue = $hlper.value
            }
            else {
                if ($servicePrincipalAppRoleAssignedTo.principalType -eq "User") {
                    if ($htUsersAndGroupsRoleAssignments.User.($servicePrincipalAppRoleAssignedTo.principalId).($servicePrincipalAppRoleAssignedTo.id)) {
                        $appRoleId = $htUsersAndGroupsRoleAssignments.User.($servicePrincipalAppRoleAssignedTo.principalId).($servicePrincipalAppRoleAssignedTo.id).appRoleId
                        if ($htAppRoles.($appRoleId)) {
                            $htOptInfo.roleId = $appRoleId
                            $htOptInfo.roleOrigin = $htAppRoles.($appRoleId).origin
                            $htOptInfo.roleAllowedMemberTypes = $htAppRoles.($appRoleId).allowedMemberTypes
                            $htOptInfo.roleDisplayName = $htAppRoles.($appRoleId).displayName
                            $htOptInfo.roleDescription = $htAppRoles.($appRoleId).description
                            $htOptInfo.roleValue = $htAppRoles.($appRoleId).value
                        }
                        else {
                            $htOptInfo.roleId = $appRoleId
                        }
                    }
                }
                if ($servicePrincipalAppRoleAssignedTo.principalType -eq "Group") {
                    if ($htUsersAndGroupsRoleAssignments.Group.($servicePrincipalAppRoleAssignedTo.principalId).($servicePrincipalAppRoleAssignedTo.id)) {
                        $appRoleId = $htUsersAndGroupsRoleAssignments.Group.($servicePrincipalAppRoleAssignedTo.principalId).($servicePrincipalAppRoleAssignedTo.id).appRoleId
                        if ($htAppRoles.($appRoleId)) {
                            $htOptInfo.roleId = $appRoleId
                            $htOptInfo.roleOrigin = $htAppRoles.($appRoleId).origin
                            $htOptInfo.roleAllowedMemberTypes = $htAppRoles.($appRoleId).allowedMemberTypes
                            $htOptInfo.roleDisplayName = $htAppRoles.($appRoleId).displayName
                            $htOptInfo.roleDescription = $htAppRoles.($appRoleId).description
                            $htOptInfo.roleValue = "$($htAppRoles.($appRoleId).value)"
                        }
                        else {
                            $htOptInfo.roleId = $appRoleId
                        }
                    }
                }
            }
            $null = $arrayServicePrincipalAppRoleAssignedToOpt.Add($htOptInfo)
        }
    }
    #endregion ServicePrincipalAppRoleAssignedTo

    if (-not $NoAzureRoleAssignments) {
        #region AzureRoleAssignmentsPrep
        $htSPAzureRoleAssignments = @{}
        $arrayServicePrincipalGroupMembershipsOpt = @()
        if ($sp.ServicePrincipal.ServicePrincipalGroupMemberships) {
            foreach ($servicePrincipalGroupMembership in $sp.ServicePrincipal.ServicePrincipalGroupMemberships | Sort-Object) {
                $htOptInfo = [ordered] @{}
                if ($htAaDGroups.($servicePrincipalGroupMembership)) {
                    $htOptInfo.DisplayName = $htAaDGroups.($servicePrincipalGroupMembership).groupDetails.displayName
                    $htOptInfo.ObjectId = $servicePrincipalGroupMembership
                }
                else {
                    $htOptInfo.DisplayName = "<n/a>"
                    $htOptInfo.ObjectId = $servicePrincipalGroupMembership
                }
                $arrayServicePrincipalGroupMembershipsOpt += $htOptInfo

                if ($htAadGroups.($servicePrincipalGroupMembership).nestedGroups) {
                    foreach ($nestegGroupId in $htAadGroups.($servicePrincipalGroupMembership).nestedGroups.id) {
                        if ($htGroupRoleAssignmentThroughNesting.($nestegGroupId).RoleAssignmentsInherited) {
                            foreach ($roleAssignmentThroughNesting in $htGroupRoleAssignmentThroughNesting.($nestegGroupId).RoleAssignmentsInherited) {
                                if (-not $htSPAzureRoleAssignments.($roleAssignmentThroughNesting.id)) {
                                    $htSPAzureRoleAssignments.($roleAssignmentThroughNesting.id) = @{}
                                    $htSPAzureRoleAssignments.($roleAssignmentThroughNesting.id).results = @()
                                }
                                $htTemp = @{}
                                $htTemp.roleAssignment = $roleAssignmentThroughNesting.id
                                $htTemp.appliesThrough = "$($htAaDGroups.($nestegGroupId).groupDetails.displayName) ($nestegGroupId) -> member of $($htAaDGroups.($roleAssignmentThroughNesting.properties.principalId).groupDetails.displayName) ($($roleAssignmentThroughNesting.properties.principalId))"
                                $htTemp.applicability = "indirect (nested Group)"
                                $htSPAzureRoleAssignments.($roleAssignmentThroughNesting.id).results += $htTemp
                            }
                        }
                    }
                }
            }
            #raSPThroughGroup
            foreach ($servicePrincipalGroupMembership in $sp.ServicePrincipal.ServicePrincipalGroupMemberships) {
                if ($htAssignmentsByPrincipalId."groups".($servicePrincipalGroupMembership)) {
                    foreach ($roleAssignmentSPThroughGroup in $htAssignmentsByPrincipalId."groups".($servicePrincipalGroupMembership)) {
                        if (-not $htSPAzureRoleAssignments.($roleAssignmentSPThroughGroup.assignment.id)) {
                            $htSPAzureRoleAssignments.($roleAssignmentSPThroughGroup.assignment.id) = @{}
                            $htSPAzureRoleAssignments.($roleAssignmentSPThroughGroup.assignment.id).results = @()
                        }
                        $htTemp = @{}
                        $htTemp.roleAssignment = $roleAssignmentSPThroughGroup.assignment.id
                        $htTemp.roleAssignmentFull = $roleAssignmentSPThroughGroup
                        $htTemp.appliesThrough = "$($htAaDGroups.($servicePrincipalGroupMembership).groupDetails.displayName) ($servicePrincipalGroupMembership)"
                        $htTemp.applicability = "indirect (Group)"
                        $htSPAzureRoleAssignments.($roleAssignmentSPThroughGroup.assignment.id).results += $htTemp
                    }
                }
            }
        }
        #endregion AzureRoleAssignmentsPrep

        #region AzureRoleAssignmentsOpt
        if ($htAssignmentsByPrincipalId."servicePrincipals".($sp.ServicePrincipal.ServicePrincipalDetails.id)) {
            foreach ($roleAssignmentSP in $htAssignmentsByPrincipalId."servicePrincipals".($sp.ServicePrincipal.ServicePrincipalDetails.id)) {
                if (-not $htSPAzureRoleAssignments.($roleAssignmentSP.assignment.id)) {
                    $htSPAzureRoleAssignments.($roleAssignmentSP.assignment.id) = @{}
                    $htSPAzureRoleAssignments.($roleAssignmentSP.assignment.id).results = @()
                }
                $htTemp = @{}
                $htTemp.roleAssignment = $roleAssignmentSP.assignment.id
                $htTemp.roleAssignmentFull = $roleAssignmentSP
                $htTemp.appliesThrough = ""
                $htTemp.applicability = "direct"
                $htSPAzureRoleAssignments.($roleAssignmentSP.assignment.id).results += $htTemp
            }
        }

        $arrayServicePrincipalAzureRoleAssignmentsOpt = [System.Collections.ArrayList]@()
        if ($htSPAzureRoleAssignments.Keys.Count -gt 0) {
            foreach ($roleAssignment in $htSPAzureRoleAssignments.Keys | sort-object) {
                foreach ($result in $htSPAzureRoleAssignments.($roleAssignment).results) {
                    $htOptInfo = [ordered] @{}
                    if ($result.roleAssignmentFull.assignmentPIMDetails) {
                        $pimBased = $true                
                    }
                    else {
                        $pimBased = $false
                    }
                    $htOptInfo.priviledgedIdentityManagementBased = $pimBased
                    $htOptInfo.roleAssignmentId = $roleAssignment
                    $htOptInfo.roleName = $result.roleAssignmentFull.roleName
                    $htOptInfo.roleId = $result.roleAssignmentFull.roleId
                    $htOptInfo.roleType = $result.roleAssignmentFull.type
                    $htOptInfo.roleAssignmentApplicability = $result.applicability
                    $htOptInfo.roleAssignmentAppliesThrough = $result.appliesThrough
                    $htOptInfo.roleAssignmentAssignmentScope = $result.roleAssignmentFull.assignmentScope
                    $htOptInfo.roleAssignmentAssignmentScopeId = $result.roleAssignmentFull.assignmentScopeId
                    $htOptInfo.roleAssignmentAssignmentScopeName = $result.roleAssignmentFull.assignmentScopeName
                    $htOptInfo.roleAssignmentAssignmentResourceName = $result.roleAssignmentFull.assignmentResourceName
                    $htOptInfo.roleAssignmentAssignmentResourceType = $result.roleAssignmentFull.assignmentResourceType
                    $htOptInfo.roleAssignment = $result.roleAssignmentFull.assignment.properties
                    if ($pimBased) {
                        $htOptInfo.priviledgedIdentityManagement = [ordered] @{}
                        $htOptInfo.priviledgedIdentityManagement.assignmentType = $result.roleAssignmentFull.assignmentPIMDetails.assignmentType
                        $htOptInfo.priviledgedIdentityManagement.startDateTime = $result.roleAssignmentFull.assignmentPIMDetails.startDateTime
                        $htOptInfo.priviledgedIdentityManagement.endDateTime = $result.roleAssignmentFull.assignmentPIMDetails.endDateTime
                        $htOptInfo.priviledgedIdentityManagement.createdOn = $result.roleAssignmentFull.assignmentPIMDetails.createdOn
                        $htOptInfo.priviledgedIdentityManagement.updatedOn = $result.roleAssignmentFull.assignmentPIMDetails.updatedOn                  
                    }
                    $null = $arrayServicePrincipalAzureRoleAssignmentsOpt.Add($htOptInfo)
                }
            }
        }
        #endregion AzureRoleAssignmentsOpt
    }
    else {
        $arrayServicePrincipalAzureRoleAssignmentsOpt = $null

        $arrayServicePrincipalGroupMembershipsOpt = @()
        if ($sp.ServicePrincipal.ServicePrincipalGroupMemberships) {
            foreach ($servicePrincipalGroupMembership in $sp.ServicePrincipal.ServicePrincipalGroupMemberships | Sort-Object) {
                $htOptInfo = [ordered] @{}
                if ($htAaDGroups.($servicePrincipalGroupMembership)) {
                    #Write-Host "SP GroupMembership      :" $htAaDGroups.($servicePrincipalGroupMembership).groupDetails.displayName "($($servicePrincipalGroupMembership))"
                    $htOptInfo.DisplayName = $htAaDGroups.($servicePrincipalGroupMembership).groupDetails.displayName
                    $htOptInfo.ObjectId = $servicePrincipalGroupMembership
                }
                else {
                    #Write-Host "SP GroupMembership      :" "notResolved" "($($servicePrincipalGroupMembership))"
                    $htOptInfo.DisplayName = "<n/a>"
                    $htOptInfo.ObjectId = $servicePrincipalGroupMembership
                }
                $arrayServicePrincipalGroupMembershipsOpt += $htOptInfo
            }
        }
    }

    #region Application
    if ($sp.ServicePrincipal.Application) {
        #Write-host "SP type:                : Application - objId: $($sp.ServicePrincipal.Application.ApplicationDetails.id) appId: $($sp.ServicePrincipal.Application.ApplicationDetails.appId)"

        #region ApplicationAADRoleAssignedOn
        $arrayApplicationAADRoleAssignedOnOpt = [System.Collections.ArrayList]@()
        if ($htAADRoleAssignmentOnSPOrAPP.APP.($sp.ServicePrincipal.Application.ApplicationDetails.id)) {
            foreach ($aadRoleAssignedOn in $htAADRoleAssignmentOnSPOrAPP.APP.($sp.ServicePrincipal.Application.ApplicationDetails.id)) {
                $htOptInfo = [ordered] @{}
                $htOptInfo.id = $aadRoleAssignedOn.id
                $htOptInfo.roleName = $htAadRoleDefinitions.($aadRoleAssignedOn.roleDefinitionId).displayName
                $htOptInfo.roleId = $aadRoleAssignedOn.roleDefinitionId
                $htOptInfo.roleDescription = $htAadRoleDefinitions.($aadRoleAssignedOn.roleDefinitionId).description
                $htOptInfo.principalId = $aadRoleAssignedOn.principalId
                $htOptInfo.principalDisplayName = $htServicePrincipalsEnriched.($aadRoleAssignedOn.principalId).ServicePrincipal.ServicePrincipalDetails.DisplayName
                $htOptInfo.principalType = $htServicePrincipalsEnriched.($aadRoleAssignedOn.principalId).spTypeConcatinated
                $null = $arrayApplicationAADRoleAssignedOnOpt.Add($htOptInfo)
            }
        }
        #endregion ApplicationAADRoleAssignedOn

        #region ApplicationOwner
        $arrayApplicationOwnerOpt = [System.Collections.ArrayList]@()
        if ($htAppOwnersFinal.($sp.ServicePrincipal.Application.ApplicationDetails.id)) {
            $arrayApplicationOwnerOpt = $htAppOwnersFinal.($sp.ServicePrincipal.Application.ApplicationDetails.id)
        }

        $arrayApplicationOwnerOpt = [System.Collections.ArrayList]@()
        foreach ($appOwner in $htAppOwners.($sp.ServicePrincipal.Application.ApplicationDetails.id)) {
            $arrayApplicationOwner = [System.Collections.ArrayList]@()
            if ($htSPOwnersFinal.($appOwner.id)) {

                foreach ($servicePrincipalOwner in $htSPOwnersFinal.($appOwner.id)) {
                    $htOptInfo = [ordered] @{}
                    $htOptInfo.id = $servicePrincipalOwner.id
                    $htOptInfo.displayName = $servicePrincipalOwner.displayName
                    $htOptInfo.principalType = $servicePrincipalOwner.principalType
                    $htOptInfo.applicability = $servicePrincipalOwner.applicability
                    $arrayOwnedBy = @()
                    
                    foreach ($owner in $servicePrincipalOwner.ownedBy) {
                        if ($owner -ne "noOwner") {
                            if ($htSPOwnersFinal.($owner.id)) {
                                $arrayOwnedBy += $htSPOwnersFinal.($owner.id)
                            }
                            else {
                                $arrayOwnedBy += $owner
                            }
                        }
                        else {
                            $arrayOwnedBy += $owner
                        }
                        
                    }
                    if ($servicePrincipalOwner.type -ne "#microsoft.graph.user") {
                        $htOptInfo.ownedBy = $arrayOwnedBy
                    }
                    $null = $arrayApplicationOwner.Add($htOptInfo)
                }

            }
            $htOptInfo = [ordered] @{}
            $htOptInfo.id = $appOwner.id
            $htOptInfo.displayName = $appOwner.displayName
            if ($appOwner.'@odata.type' -eq "#microsoft.graph.servicePrincipal") {
                $htOptInfo.principalType = $htServicePrincipalsEnriched.($appOwner.id).spTypeConcatinated
            }
            if ($appOwner.'@odata.type' -eq "#microsoft.graph.user") {
                $htOptInfo.principalType = $htUsersResolved.($appOwner.id).typeOnly
            }
            $htOptInfo.applicability = "direct"
            if ($appOwner.'@odata.type' -ne "#microsoft.graph.user") {
                $htOptInfo.ownedBy = $arrayApplicationOwner
            }
            $null = $arrayApplicationOwnerOpt.Add($htOptInfo)
        }
        #endregion ApplicationOwner

        #region ApplicationSecrets
        $currentDateUTC = (Get-Date).ToUniversalTime()
        $arrayApplicationPasswordCredentialsOpt = [System.Collections.ArrayList]@()
        if ($sp.ServicePrincipal.Application.ApplicationPasswordCredentials) {
            $appPasswordCredentialsCount = ($sp.ServicePrincipal.Application.ApplicationPasswordCredentials).count
            if ($appPasswordCredentialsCount -gt 0) {
                foreach ($appPasswordCredential in $sp.ServicePrincipal.Application.ApplicationPasswordCredentials.keys | Sort-Object) {
                    $hlperApplicationPasswordCredential = $sp.ServicePrincipal.Application.ApplicationPasswordCredentials.($appPasswordCredential)
                    if ($hlperApplicationPasswordCredential.displayName) {
                        $displayName = $hlperApplicationPasswordCredential.displayName
                    }
                    else {
                        $displayName = "notGiven"
                    }
                    
                    $passwordCredentialExpiryTotalDays = (NEW-TIMESPAN -Start $currentDateUTC -End $hlperApplicationPasswordCredential.endDateTime).TotalDays
                    $expiryApplicationPasswordCredential = [math]::Round($passwordCredentialExpiryTotalDays, 0)
                    if ($passwordCredentialExpiryTotalDays -lt 0) {
                        $expiryApplicationPasswordCredential = "expired"
                        $appPasswordCredentialsExpiredCount++
                    }
                    elseif ($passwordCredentialExpiryTotalDays -lt $AADServicePrincipalExpiryWarningDays) {
                        $appPasswordCredentialsGracePeriodExpiryCount++
                        $expiryApplicationPasswordCredential = "expires soon (less than grace period $AADServicePrincipalExpiryWarningDays)"
                    }
                    else {
                        if ($passwordCredentialExpiryTotalDays -gt 730) {
                            $appPasswordCredentialsExpiryOKMoreThan2YearsCount++
                            $expiryApplicationPasswordCredential = "expires > 2 years"
                        }
                        else {
                            $appPasswordCredentialsExpiryOKCount++
                            $expiryApplicationPasswordCredential = "expires > $AADServicePrincipalExpiryWarningDays days < 2 years"
                        }
                    }

                    $htOptInfo = [ordered] @{}
                    $htOptInfo.keyId = $hlperApplicationPasswordCredential.keyId
                    $htOptInfo.displayName = $displayName
                    $htOptInfo.expiryInfo = $expiryApplicationPasswordCredential
                    $htOptInfo.endDateTime = $hlperApplicationPasswordCredential.endDateTime
                    $htOptInfo.endDateTimeFormated = ($hlperApplicationPasswordCredential.endDateTime).ToString("dd-MMM-yyyy HH:mm:ss")
                    $null = $arrayApplicationPasswordCredentialsOpt.Add($htOptInfo)
                }
            }
        }
        #endregion ApplicationSecrets
        
        #region ApplicationCertificates
        $arrayApplicationKeyCredentialsOpt = [System.Collections.ArrayList]@()
        if ($sp.ServicePrincipal.Application.ApplicationKeyCredentials) {
            $appKeyCredentialsCount = ($sp.ServicePrincipal.Application.ApplicationKeyCredentials).count
            if ($appKeyCredentialsCount -gt 0) {

                foreach ($appKeyCredential in $sp.ServicePrincipal.Application.ApplicationKeyCredentials.keys | Sort-Object) {
                    $hlperApplicationKeyCredential = $sp.ServicePrincipal.Application.ApplicationKeyCredentials.($appKeyCredential)
                    
                    $keyCredentialExpiryTotalDays = (NEW-TIMESPAN -Start $currentDateUTC -End $hlperApplicationKeyCredential.endDateTime).TotalDays
                    $expiryApplicationKeyCredential = [math]::Round($keyCredentialExpiryTotalDays, 0)
                    
                    if ($keyCredentialExpiryTotalDays -lt 0) {
                        $expiryApplicationKeyCredential = "expired"
                        $appKeyCredentialsExpiredCount++
                    }
                    elseif ($keyCredentialExpiryTotalDays -lt $AADServicePrincipalExpiryWarningDays) {
                        $expiryApplicationKeyCredential = "expires soon (less than grace period $AADServicePrincipalExpiryWarningDays)"
                        $appKeyCredentialsGracePeriodExpiryCount++
                    }
                    else {
                        if ($keyCredentialExpiryTotalDays -gt 730) {
                            $expiryApplicationKeyCredential = "expires > 2 years"
                            $appKeyCredentialsExpiryOKMoreThan2YearsCount++
                        }
                        else {
                            $expiryApplicationKeyCredential = "expires > $AADServicePrincipalExpiryWarningDays days < 2 years"
                            $appKeyCredentialsExpiryOKCount++
                        }
                    }

                    $htOptInfo = [ordered] @{}
                    $htOptInfo.keyId = $hlperApplicationKeyCredential.keyId
                    $htOptInfo.displayName = $hlperApplicationKeyCredential.displayName
                    $htOptInfo.customKeyIdentifier = $hlperApplicationKeyCredential.customKeyIdentifier
                    $htOptInfo.expiryInfo = $expiryApplicationKeyCredential
                    $htOptInfo.endDateTime = $hlperApplicationKeyCredential.endDateTime
                    $htOptInfo.endDateTimeFormated = $hlperApplicationKeyCredential.endDateTime.ToString("dd-MMM-yyyy HH:mm:ss")
                    $null = $arrayApplicationKeyCredentialsOpt.Add($htOptInfo)
                }
            }
        }
        #endregion ApplicationCertificates
    }
    #endregion Application

    #region ManagedIdentity
    $arrayManagedIdentityOpt = [System.Collections.ArrayList]@()
    if ($sp.ServicePrincipal.ManagedIdentity) {
        $htOptInfo = [ordered]@{}
        #$hlper = $htServicePrincipalsEnriched.($sp.ServicePrincipal.ServicePrincipalDetails.id)
        $htOptInfo.type = $sp.subtype
        $htOptInfo.alternativeName = $sp.altname
        $htOptInfo.resourceType = $sp.resourceType
        $htOptInfo.resourceScope = $sp.resourceScope
        $null = $arrayManagedIdentityOpt.Add($htOptInfo)
    }
    #endregion ManagedIdentity

    #region finalArray
    
    $spArray = [System.Collections.ArrayList]@()
    $null = $spArray.Add([PSCustomObject]@{ 
            SPObjectId                  = $sp.ServicePrincipal.ServicePrincipalDetails.id
            SPAppId                     = $sp.ServicePrincipal.ServicePrincipalDetails.appId
            SPDisplayName               = $sp.ServicePrincipal.ServicePrincipalDetails.displayName
            SPDescription               = $sp.ServicePrincipal.ServicePrincipalDetails.description
            SPNotes                     = $sp.ServicePrincipal.ServicePrincipalDetails.notes
            SPAppOwnerOrganizationId    = $sp.ServicePrincipal.ServicePrincipalDetails.appOwnerOrganizationId
            SPServicePrincipalType      = $sp.ServicePrincipal.ServicePrincipalDetails.servicePrincipalType
            SPAccountEnabled            = $sp.ServicePrincipal.ServicePrincipalDetails.accountEnabled
            SPCreatedDateTime           = $sp.ServicePrincipal.ServicePrincipalDetails.createdDateTime
            #SPPublisherName             = $sp.ServicePrincipal.ServicePrincipalDetails.publisherName
            SPVerifiedPublisher         = $sp.ServicePrincipal.ServicePrincipalDetails.verifiedPublisher
            SPHomepage                  = $sp.ServicePrincipal.ServicePrincipalDetails.homepage
            SPErrorUrl                  = $sp.ServicePrincipal.ServicePrincipalDetails.errorUrl
            SPLoginUrl                  = $sp.ServicePrincipal.ServicePrincipalDetails.loginUrl
            SPLogoutUrl                 = $sp.ServicePrincipal.ServicePrincipalDetails.logoutUrl
            SPPreferredSingleSignOnMode = $sp.ServicePrincipal.ServicePrincipalDetails.preferredSingleSignOnMode
            SPAppRoles                  = $sp.ServicePrincipal.ServicePrincipalDetails.appRoles
            SPOauth2PermissionScopes    = $sp.ServicePrincipal.ServicePrincipalDetails.oauth2PermissionScopes
        })

    if ($sp.ServicePrincipal.Application) {
        #Write-Host "$($sp.ServicePrincipal.ServicePrincipalDetails.displayName) is App"

        $appArray = [System.Collections.ArrayList]@()
        $null = $appArray.Add([PSCustomObject]@{ 
            APPObjectId                 = $sp.ServicePrincipal.Application.ApplicationDetails.id
            APPAppClientId              = $sp.ServicePrincipal.Application.ApplicationDetails.appId
            APPDisplayName              = $sp.ServicePrincipal.Application.ApplicationDetails.displayName
            APPDescription              = $sp.ServicePrincipal.Application.ApplicationDetails.description
            APPNotes                    = $sp.ServicePrincipal.Application.ApplicationDetails.notes 
            APPTags                     = $sp.ServicePrincipal.Application.ApplicationDetails.tags 
            APPCreatedDateTime          = $sp.ServicePrincipal.Application.ApplicationDetails.createdDateTime
            APPSignInAudience           = $sp.ServicePrincipal.Application.ApplicationDetails.signInAudience 
            APPPublisherDomain          = $sp.ServicePrincipal.Application.ApplicationDetails.publisherDomain
            APPVerifiedPublisher        = $sp.ServicePrincipal.Application.ApplicationDetails.verifiedPublisher
            APPGroupMembershipClaims    = $sp.ServicePrincipal.Application.ApplicationDetails.groupMembershipClaims 
            APPDefaultRedirectUri       = $sp.ServicePrincipal.Application.ApplicationDetails.defaultRedirectUri 
            APPRequiredResourceAccess   = $sp.ServicePrincipal.Application.ApplicationDetails.requiredResourceAccess
        })

        $null = $cu.Add([PSCustomObject]@{ 
                #SPObjId                     = $sp.ServicePrincipal.ServicePrincipalDetails.id
                #SPDisplayName               = $sp.ServicePrincipal.ServicePrincipalDetails.displayName
                #SPType                      = $sp.ServicePrincipal.ServicePrincipalDetails.servicePrincipalType
                #SPAppRoles                  = $sp.ServicePrincipal.ServicePrincipalDetails.appRoles
                #SPpublishedPermissionScopes = $sp.ServicePrincipal.ServicePrincipalDetails.publishedPermissionScopes
                SPType                      = $sp.spTypeConcatinated
                #SP                          = $sp.ServicePrincipal.ServicePrincipalDetails | Select-Object -ExcludeProperty '@odata.id'
                SP                          = $spArray
                SPOwners                    = $arrayServicePrincipalOwnerOpt
                SPOwnedObjects              = $arrayServicePrincipalOwnedObjectsOpt
                SPAADRoleAssignments        = $arrayServicePrincipalAADRoleAssignmentsOpt
                SPAAADRoleAssignedOn        = $arrayServicePrincipalAADRoleAssignedOnOpt
                SPOauth2PermissionGrants    = $arrayServicePrincipalOauth2PermissionGrantsOpt
                SPOauth2PermissionGrantedTo = $arraySPOauth2PermissionGrantedTo
                SPAppRoleAssignments        = $arrayServicePrincipalAppRoleAssignmentsOpt
                SPAppRoleAssignedTo         = $arrayServicePrincipalAppRoleAssignedToOpt
                SPGroupMemberships          = $arrayServicePrincipalGroupMembershipsOpt
                SPAzureRoleAssignments      = $arrayServicePrincipalAzureRoleAssignmentsOpt
                #APP                         = $sp.ServicePrincipal.Application.ApplicationDetails | Select-Object -ExcludeProperty '@odata.id'
                APP                         = $appArray
                APPAAADRoleAssignedOn       = $arrayApplicationAADRoleAssignedOnOpt
                #approles always inherited from sp
                #APPAppRoles                 = $sp.ServicePrincipal.Application.ApplicationDetails.appRoles
                APPAppOwners                = $arrayApplicationOwnerOpt
                APPPasswordCredentials      = $arrayApplicationPasswordCredentialsOpt
                APPKeyCredentials           = $arrayApplicationKeyCredentialsOpt
            })
    }
    elseif ($sp.ServicePrincipal.ManagedIdentity) {
        #Write-Host "$($sp.ServicePrincipal.ServicePrincipalDetails.displayName) is MI"
        $null = $cu.Add([PSCustomObject]@{ 
                #SPObjId                     = $sp.ServicePrincipal.ServicePrincipalDetails.id
                #SPDisplayName               = $sp.ServicePrincipal.ServicePrincipalDetails.displayName
                #SPType                      = $sp.ServicePrincipal.ServicePrincipalDetails.servicePrincipalType
                #SPAppRoles                  = $sp.ServicePrincipal.ServicePrincipalDetails.appRoles
                #SPpublishedPermissionScopes = $sp.ServicePrincipal.ServicePrincipalDetails.publishedPermissionScopes
                SPType                      = $sp.spTypeConcatinated
                #SP                          = $sp.ServicePrincipal.ServicePrincipalDetails | Select-Object -ExcludeProperty '@odata.id'
                SP                          = $spArray
                SPOwners                    = $arrayServicePrincipalOwnerOpt
                SPOwnedObjects              = $arrayServicePrincipalOwnedObjectsOpt
                SPAADRoleAssignments        = $arrayServicePrincipalAADRoleAssignmentsOpt
                SPAAADRoleAssignedOn        = $arrayServicePrincipalAADRoleAssignedOnOpt
                SPOauth2PermissionGrants    = $arrayServicePrincipalOauth2PermissionGrantsOpt
                SPOauth2PermissionGrantedTo = $arraySPOauth2PermissionGrantedTo
                SPAppRoleAssignments        = $arrayServicePrincipalAppRoleAssignmentsOpt
                SPAppRoleAssignedTo         = $arrayServicePrincipalAppRoleAssignedToOpt
                SPGroupMemberships          = $arrayServicePrincipalGroupMembershipsOpt
                SPAzureRoleAssignments      = $arrayServicePrincipalAzureRoleAssignmentsOpt
                ManagedIdentity             = $arrayManagedIdentityOpt
            })
    }
    else {
        #Write-Host "$($sp.ServicePrincipal.ServicePrincipalDetails.displayName) is neither App, nore MI"
        $null = $cu.Add([PSCustomObject]@{ 
                #SPObjId                     = $sp.ServicePrincipal.ServicePrincipalDetails.id
                #SPDisplayName               = $sp.ServicePrincipal.ServicePrincipalDetails.displayName
                #SPType                      = $sp.ServicePrincipal.ServicePrincipalDetails.servicePrincipalType
                #SPAppRoles                  = $sp.ServicePrincipal.ServicePrincipalDetails.appRoles
                #SPpublishedPermissionScopes = $sp.ServicePrincipal.ServicePrincipalDetails.publishedPermissionScopes
                SPType                      = $sp.spTypeConcatinated
                #SP                          = $sp.ServicePrincipal.ServicePrincipalDetails | Select-Object -ExcludeProperty '@odata.id'
                SP                          = $spArray
                SPOwners                    = $arrayServicePrincipalOwnerOpt
                SPOwnedObjects              = $arrayServicePrincipalOwnedObjectsOpt
                SPAADRoleAssignments        = $arrayServicePrincipalAADRoleAssignmentsOpt
                SPAAADRoleAssignedOn        = $arrayServicePrincipalAADRoleAssignedOnOpt
                SPOauth2PermissionGrants    = $arrayServicePrincipalOauth2PermissionGrantsOpt
                SPOauth2PermissionGrantedTo = $arraySPOauth2PermissionGrantedTo
                SPAppRoleAssignments        = $arrayServicePrincipalAppRoleAssignmentsOpt
                SPAppRoleAssignedTo         = $arrayServicePrincipalAppRoleAssignedToOpt
                SPGroupMemberships          = $arrayServicePrincipalGroupMembershipsOpt
                SPAzureRoleAssignments      = $arrayServicePrincipalAzureRoleAssignmentsOpt
            })
    }
    #endregion finalArray
    $processedServicePrincipalsCount++
}
Write-Host "Enrichment completed: $processedServicePrincipalsCount ServicePrincipals processed"
$endEnrichmentSP = get-date
$duration = NEW-TIMESPAN -Start $startEnrichmentSP -End $endEnrichmentSP
Write-Host "Service Principals enrichment duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"

#
if ($AzureDevOpsWikiAsCode) {
    $JSONPath = "JSON_SP_$($ManagementGroupId)"
    if (Test-Path -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($JSONPath)") {
        Write-Host " Cleaning old state (Pipeline only)"
        Remove-Item -Recurse -Force "$($outputPath)$($DirectorySeparatorChar)$($JSONPath)"
    }
}
else {
    #test
    $fileTimestamp = (get-date -format $FileTimeStampFormat)
    $JSONPath = "JSON_SP_$($ManagementGroupId)_$($fileTimestamp)"
    Write-Host " Creating new state ($($JSONPath)) (local only))"
}

$null = new-item -Name $JSONPath -ItemType directory -path $outputPath
foreach ($entry in $cu) {
    if (-not $entry.APP -and -not $entry.ManagedIdentity) {
    }
    $entry | ConvertTo-JSON -Depth 99 | Set-Content -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($JSONPath)$($DirectorySeparatorChar)$($entry.SP.SPObjectId).json" -Encoding utf8 -Force
}
#endregion enrichedAADSPData

#endregion AADSP

#endregion dataCollection

#region createoutputs

#region BuildHTML
#
#testhelper
$fileTimestamp = (get-date -format $FileTimeStampFormat)

$startBuildHTML = get-date

#filename
if ($htParameters.AzureDevOpsWikiAsCode -eq $true) { 
    $fileName = "$($Product)_$($ManagementGroupId)"
}
else {
    $fileName = "$($Product)_$($ProductVersion)_$($fileTimestamp)_$($ManagementGroupId)"
}

Write-Host "Building HTML"

$html = $null
$html += @"
<!doctype html>
<html lang="en">
<html style="height: 100%">
<head>
    <meta charset="utf-8" />
    <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
    <meta http-equiv="Pragma" content="no-cache" />
    <meta http-equiv="Expires" content="0" />
    <title>$($Product)</title>
    <link rel="stylesheet" type="text/css" href="https://www.azadvertizer.net/azadserviceprincipalinsights/css/azadserviceprincipalinsightsmain_001_005.css">
    <script src="https://www.azadvertizer.net/azgovvizv4/js/jquery-1.12.1.js"></script>
    <script src="https://www.azadvertizer.net/azgovvizv4/js/jquery-ui-1.12.1.js"></script>
    <script type="text/javascript" src="https://www.azadvertizer.net/azgovvizv4/js/highlight_v004_002.js"></script>
    <script src="https://www.azadvertizer.net/azgovvizv4/js/fontawesome-0c0b5cbde8.js"></script>
    <script src="https://www.azadvertizer.net/azgovvizv4/tablefilter/tablefilter.js"></script>
    <link rel="stylesheet" href="https://www.azadvertizer.net/azgovvizv4/css/highlight-10.5.0.min.css">
    <script src="https://www.azadvertizer.net/azgovvizv4/js/highlight-10.5.0.min.js"></script>
    <script>hljs.initHighlightingOnLoad();</script>
    <script src="https://www.chartjs.org/dist/2.8.0/Chart.min.js"></script>

    <script>
        `$(window).load(function() {
            // Animate loader off screen
            `$(".se-pre-con").fadeOut("slow");;
        });
    </script>

    <script>
    // Quick and simple export target #table_id into a csv
    function download_table_as_csv_semicolon(table_id) {
        // Select rows from table_id
        var rows = document.querySelectorAll('table#' + table_id + ' tr');
        // Construct csv
        var csv = [];
        if (window.helpertfConfig4TenantSummary_roleAssignmentsAll !== 1){
            for (var i = 0; i < rows.length; i++) {
                var row = [], cols = rows[i].querySelectorAll('td, th');
                for (var j = 0; j < cols.length; j++) {
                    // Clean innertext to remove multiple spaces and jumpline (break csv)
                    var data = cols[j].innerText.replace(/(\r\n|\n|\r)/gm, '').replace(/(\s\s)/gm, ' ')
                    // Escape double-quote with double-double-quote (see https://stackoverflow.com/questions/17808511/properly-escape-a-double-quote-in-csv)
                    data = data.replace(/"/g, '""');
                    // Push escaped string
                    row.push('"' + data + '"');
                }
                csv.push(row.join(';'));
            }
        }
        else{
            for (var i = 1; i < rows.length; i++) {
                var row = [], cols = rows[i].querySelectorAll('td, th');
                for (var j = 0; j < cols.length; j++) {
                    // Clean innertext to remove multiple spaces and jumpline (break csv)
                    var data = cols[j].innerText.replace(/(\r\n|\n|\r)/gm, '').replace(/(\s\s)/gm, ' ')
                    // Escape double-quote with double-double-quote (see https://stackoverflow.com/questions/17808511/properly-escape-a-double-quote-in-csv)
                    data = data.replace(/"/g, '""');
                    // Push escaped string
                    row.push('"' + data + '"');
                }
                csv.push(row.join(';'));
            }
        }
        var csv_string = csv.join('\n');
        // Download it
        var filename = 'export_' + table_id + '_' + new Date().toLocaleDateString('en-CA') + '.csv';
        var link = document.createElement('a');
        link.style.display = 'none';
        link.setAttribute('target', '_blank');
        link.setAttribute('href', 'data:text/csv;charset=utf-8,' + encodeURIComponent(csv_string));
        link.setAttribute('download', filename);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }
    </script>

    <script>
    // Quick and simple export target #table_id into a csv
    function download_table_as_csv_comma(table_id) {
        // Select rows from table_id
        var rows = document.querySelectorAll('table#' + table_id + ' tr');
        // Construct csv
        var csv = [];
        if (window.helpertfConfig4TenantSummary_roleAssignmentsAll !== 1){
            for (var i = 0; i < rows.length; i++) {
                var row = [], cols = rows[i].querySelectorAll('td, th');
                for (var j = 0; j < cols.length; j++) {
                    // Clean innertext to remove multiple spaces and jumpline (break csv)
                    var data = cols[j].innerText.replace(/(\r\n|\n|\r)/gm, '').replace(/(\s\s)/gm, ' ')
                    // Escape double-quote with double-double-quote (see https://stackoverflow.com/questions/17808511/properly-escape-a-double-quote-in-csv)
                    data = data.replace(/"/g, '""');
                    // Push escaped string
                    row.push('"' + data + '"');
                }
                csv.push(row.join(','));
            }
        }
        else{
            for (var i = 1; i < rows.length; i++) {
                var row = [], cols = rows[i].querySelectorAll('td, th');
                for (var j = 0; j < cols.length; j++) {
                    // Clean innertext to remove multiple spaces and jumpline (break csv)
                    var data = cols[j].innerText.replace(/(\r\n|\n|\r)/gm, '').replace(/(\s\s)/gm, ' ')
                    // Escape double-quote with double-double-quote (see https://stackoverflow.com/questions/17808511/properly-escape-a-double-quote-in-csv)
                    data = data.replace(/"/g, '""');
                    // Push escaped string
                    row.push('"' + data + '"');
                }
                csv.push(row.join(','));
            }
        }
        var csv_string = csv.join('\n');
        // Download it
        var filename = 'export_' + table_id + '_' + new Date().toLocaleDateString('en-CA') + '.csv';
        var link = document.createElement('a');
        link.style.display = 'none';
        link.setAttribute('target', '_blank');
        link.setAttribute('href', 'data:text/csv;charset=utf-8,' + encodeURIComponent(csv_string));
        link.setAttribute('download', filename);
        document.body.appendChild(link);
        link.click();
        document.body.removeChild(link);
    }
    </script>
</head>
<body>
    <div class="se-pre-con"></div>
"@

$html += @"
    <div class="summprnt" id="summprnt">
    <div class="summary" id="summary"><p class="pbordered">Insights</p>
"@

$startSummary = get-date

summary
#[System.GC]::Collect()

$endSummary = get-date
Write-Host " Building TenantSummary duration: $((NEW-TIMESPAN -Start $startSummary -End $endSummary).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startSummary -End $endSummary).TotalSeconds) seconds)"


$html += @"
    </div><!--summary-->
    </div><!--summprnt-->
"@


$html += @"
    <div class="footer">
    <div class="VersionDiv VersionLatest"></div>
    <div class="VersionDiv VersionThis"></div>
    <div class="VersionAlert"></div>
"@


$html += @"
        <abbr style="text-decoration:none" title="$($paramsUsed)"><i class="fa fa-question-circle" aria-hidden="true"></i></abbr>
        <hr>
"@

$html += @"
    </div>
    <script src="https://www.azadvertizer.net/azgovvizv4/js/toggle_v004_004.js"></script>
    <script src="https://www.azadvertizer.net/azadserviceprincipalinsights/js/collapsetable_v004_002.js"></script>
    <script src="https://www.azadvertizer.net/azgovvizv4/js/fitty_v004_001.min.js"></script>
    <script src="https://www.azadvertizer.net/azadserviceprincipalinsights/js/version_v001_001.js"></script>
    <script src="https://www.azadvertizer.net/azgovvizv4/js/autocorrectOff_v004_001.js"></script>
    <script>
        fitty('#fitme', {
            minSize: 7,
            maxSize: 10
        });
    </script>
</body>
</html>
"@  

$html | Set-Content -Path "$($outputPath)$($DirectorySeparatorChar)$($fileName).html" -Encoding utf8 -Force

$endBuildHTML = get-date
Write-Host "Building HTML total duration: $((NEW-TIMESPAN -Start $startBuildHTML -End $endBuildHTML).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startBuildHTML -End $endBuildHTML).TotalSeconds) seconds)"
#endregion BuildHTML

#endregion createoutputs

#APITracking
$APICallTrackingCount = ($arrayAPICallTracking | Measure-Object).Count
$APICallTrackingManagementCount = ($arrayAPICallTracking | Where-Object { $_.TargetEndpoint -eq "ManagementAPI" } | Measure-Object).Count
$APICallTrackingGraphCount = ($arrayAPICallTracking | Where-Object { $_.TargetEndpoint -eq "MSGraphAPI" } | Measure-Object).Count
$APICallTrackingRetriesCount = ($arrayAPICallTracking | Where-Object { $_.TryCounter -gt 0 } | Measure-Object).Count
$APICallTrackingRestartDueToDuplicateNextlinkCounterCount = ($arrayAPICallTracking | Where-Object { $_.RestartDueToDuplicateNextlinkCounter -gt 0 } | Measure-Object).Count
Write-Host "$($Product) APICalls total count: $APICallTrackingCount ($APICallTrackingManagementCount ManagementAPI; $APICallTrackingGraphCount MSGraphAPI; $APICallTrackingRetriesCount retries; $APICallTrackingRestartDueToDuplicateNextlinkCounterCount nextLinkReset)"

$endProduct = get-date
$durationProduct = NEW-TIMESPAN -Start $startProduct -End $endProduct
Write-Host "$($Product) duration: $(($durationProduct).TotalMinutes) minutes ($(($durationProduct).TotalSeconds) seconds)"

#end
$endTime = get-date -format "dd-MMM-yyyy HH:mm:ss"
Write-Host "End $($Product) $endTime"

Write-Host "Checking for errors"
if ($Error.Count -gt 0) {
    Write-Host "Dumping $($Error.Count) Errors (handled by $($Product)):" -ForegroundColor Yellow
    $Error | Out-host
}
else {
    Write-Host "Error count is 0"
}

#region Stats
if (-not $StatsOptOut) {

    if ($htParameters.AzureDevOpsWikiAsCode) {
        if ($env:BUILD_REPOSITORY_ID){
            $hashTenantIdOrRepositoryId = [string]($env:BUILD_REPOSITORY_ID)
        }
        else{
            $hashTenantIdOrRepositoryId = [string]($checkContext.Tenant.Id)
        }
    }
    else{
        $hashTenantIdOrRepositoryId = [string]($checkContext.Tenant.Id)
    }

    $hashAccId = [string]($checkContext.Account.Id)

    $hasher384 = [System.Security.Cryptography.HashAlgorithm]::Create('sha384')
    $hasher512 = [System.Security.Cryptography.HashAlgorithm]::Create('sha512')

    $hashTenantIdOrRepositoryIdSplit = $hashTenantIdOrRepositoryId.split("-")
    $hashAccIdSplit = $hashAccId.split("-")

    if (($hashTenantIdOrRepositoryIdSplit[0])[0] -match "[a-z]") {
        $hashTenantIdOrRepositoryIdUse = "$(($hashTenantIdOrRepositoryIdSplit[0]).substring(2))$($hashAccIdSplit[2])"
        $hashTenantIdOrRepositoryIdUse = $hasher512.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($hashTenantIdOrRepositoryIdUse))
        $hashTenantIdOrRepositoryIdUse = "$(([System.BitConverter]::ToString($hashTenantIdOrRepositoryIdUse)) -replace '-')"
    }
    else {
        $hashTenantIdOrRepositoryIdUse = "$(($hashTenantIdOrRepositoryIdSplit[4]).substring(6))$($hashAccIdSplit[1])"
        $hashTenantIdOrRepositoryIdUse = $hasher384.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($hashTenantIdOrRepositoryIdUse))
        $hashTenantIdOrRepositoryIdUse = "$(([System.BitConverter]::ToString($hashTenantIdOrRepositoryIdUse)) -replace '-')"
    }

    if (($hashAccIdSplit[0])[0] -match "[a-z]") {
        $hashAccIdUse = "$($hashAccIdSplit[0].substring(2))$($hashTenantIdOrRepositoryIdSplit[2])"
        $hashAccIdUse = $hasher512.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($hashAccIdUse))
        $hashAccIdUse = "$(([System.BitConverter]::ToString($hashAccIdUse)) -replace '-')"
        $hashUse = "$($hashAccIdUse)$($hashTenantIdOrRepositoryIdUse)"
    }
    else {
        $hashAccIdUse = "$($hashAccIdSplit[4].substring(6))$($hashTenantIdOrRepositoryIdSplit[1])"
        $hashAccIdUse = $hasher384.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($hashAccIdUse))
        $hashAccIdUse = "$(([System.BitConverter]::ToString($hashAccIdUse)) -replace '-')"
        $hashUse = "$($hashTenantIdOrRepositoryIdUse)$($hashAccIdUse)"
    }

    $identifierBase = $hasher512.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($hashUse))
    $identifier = "$(([System.BitConverter]::ToString($identifierBase)) -replace '-')"

    $platform = "Console"
    if ($htParameters.AzureDevOpsWikiAsCode) {
        if ($env:SYSTEM_TEAMPROJECTID) {
            $platform = "AzureDevOps"
        }
        else {
            $platform = "unclear"
        }
    }

    $accountInfo = "$($accountType)$($userType)"
    if ($accountType -eq "ServicePrincipal") {
        $accountInfo = $accountType
    }

    $statsCountSubscriptions = "less than 100"
    if (($htSubscriptionsMgPath.Keys).Count -ge 100) {
        $statsCountSubscriptions = "more than 100"
    }

    $statsCountSPs = "less than 1000"
    if ($cu.Count -ge 1000) {
        $statsCountSPs = "more than 1000"
    }

    $tryCounter = 0
    do {
        if ($tryCounter -gt 0) {
            start-sleep -seconds ($tryCounter * 3)
        }
        $tryCounter++
        $statsSuccess = $true
        try {
            $statusBody = @"
{
    "name": "Microsoft.ApplicationInsights.Event",
    "time": "$((Get-Date).ToUniversalTime())",
    "iKey": "ffcd6b2e-1a5e-429f-9495-e3492decfe06",
    "data": {
        "baseType": "EventData",
        "baseData": {
            "name": "$($Product)",
            "ver": 2,
            "properties": {
                "accType": "$($accountInfo)",
                "azCloud": "$($checkContext.Environment.Name)",
                "identifier": "$($identifier)",
                "platform": "$($platform)",
                "productVersion": "$($ProductVersion)",
                "psAzAccountsVersion": "$($resolvedAzModuleVersion)",
                "psVersion": "$($PSVersionTable.PSVersion)",
                "statsCountErrors": "$($Error.Count)",
                "statsCountSPs": "$($statsCountSPs)",
                "statsCountSubscriptions": "$($statsCountSubscriptions)",
                "statsTry": "$($tryCounter)"
            }
        }
    }
}
"@
            $stats = Invoke-WebRequest -Uri 'https://dc.services.visualstudio.com/v2/track' -Method 'POST' -body $statusBody
        }
        catch {
            $statsSuccess = $false
        }
    }
    until($statsSuccess -eq $true -or $tryCounter -gt 5)
}
else {
    #noStats
    $identifier = (New-Guid).Guid

    $tryCounter = 0
    do {
        if ($tryCounter -gt 0) {
            start-sleep -seconds ($tryCounter * 3)
        }
        $tryCounter++
        $statsSuccess = $true
        try {
            $statusBody = @"
{
    "name": "Microsoft.ApplicationInsights.Event",
    "time": "$((Get-Date).ToUniversalTime())",
    "iKey": "ffcd6b2e-1a5e-429f-9495-e3492decfe06",
    "data": {
        "baseType": "EventData",
        "baseData": {
            "name": "$($Product)",
            "ver": 2,
            "properties": {
                "identifier": "$($identifier)",
                "statsTry": "$($tryCounter)"
            }
        }
    }
}
"@
            $stats = Invoke-WebRequest -Uri 'https://dc.services.visualstudio.com/v2/track' -Method 'POST' -body $statusBody
        }
        catch {
            $statsSuccess = $false
        }
    }
    until($statsSuccess -eq $true -or $tryCounter -gt 5)
}
#endregion Stats

if ($DoTranscript) {
    Stop-Transcript
}
