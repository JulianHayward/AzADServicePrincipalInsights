[CmdletBinding()]
Param
(
    [string]$Product = "AzAdServicePrincipalInsights",
    [string]$ProductVersion = "v1_20210106_1_POC",
    [string]$GithubRepository = "someTinyURL/AzAdServicePrincipalInsights",
    [switch]$AzureDevOpsWikiAsCode, #Use this parameter only when running in a Azure DevOps Pipeline!
    [switch]$DebugAzAPICall,
    [switch]$NoCsvExport,
    [string]$CsvDelimiter = ";",
    [switch]$CsvExportUseQuotesAsNeeded,
    [string]$OutputPath,
    [array]$SubscriptionQuotaIdWhitelist = @("undefined"),
    [switch]$DoTranscript,
    [int]$HtmlTableRowsLimit = 40000, #HTML -> becomes unresponsive depending on client device performance. A recommendation will be shown to download the CSV instead of opening the TF table
    [int]$ThrottleLimit = 10, 
    [int]$ThrottleLimitGraph = 10, 
    [string]$SubscriptionId4AzContext = "undefined",
    [string]$FileTimeStampFormat = "yyyyMMdd_HHmmss",
    [switch]$NoJsonExport,
    [int]$AADGroupMembersLimit = 500,
    [switch]$NoAzureRoleAssignments,
    [int]$AADServicePrincipalExpiryWarningDays = 14
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

#start
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
function AzAPICall($uri, $method, $currentTask, $body, $listenOn, $getConsumption, $getGroup, $getGroupMembersCount, $getApp, $getSp, $getGuests, $caller, $consistencyLevel, $getCount, $getPolicyCompliance, $getMgAscSecureScore, $getRoleAssignmentSchedules, $getDiagnosticSettingsMg, $validateAccess) {
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
                #write-host "has BODY"
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
        if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "  DEBUGTASK: attempt#$($tryCounter) processing: $($currenttask) uri: '$($uri)'" -ForegroundColor $debugForeGroundColor }
        
        if ($unexpectedError -eq $false) {
            if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: unexpectedError: false" -ForegroundColor $debugForeGroundColor }
            if ($azAPIRequest.StatusCode -ne 200) {
                if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: apiStatusCode: $($azAPIRequest.StatusCode)" -ForegroundColor $debugForeGroundColor }
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
                    ($getSp -and $catchResult.error.code -like "*Request_ResourceNotFound*") -or 
                    ($getSp -and $catchResult.error.code -like "*Authorization_RequestDenied*") -or
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
                    ($getDiagnosticSettingsMg -and $catchResult.error.code -eq "InvalidResourceType") -or
                    ($catchResult.error.code -eq "InsufficientPermissions") -or
                    $catchResult.error.code -eq "ClientCertificateValidationFailure" -or
                    ($validateAccess -and $catchResult.error.code -eq "Authorization_RequestDenied")
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
                        if ($validateAccess) {
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
                    if (($getApp -or $getSp) -and $catchResult.error.code -like "*Request_ResourceNotFound*") {
                        Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) uncertain ServicePrincipal status - skipping for now :)"
                        return "Request_ResourceNotFound"
                    }
                    if ($currentTask -eq "Checking AAD UserType" -and $catchResult.error.code -like "*Authorization_RequestDenied*") {
                        Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - (plain : $catchResult) cannot get the executing user´s userType information (member/guest) - proceeding as 'unknown'"
                        return "unknown"
                    }
                    if ((($getApp -or $getSp) -and $catchResult.error.code -like "*Authorization_RequestDenied*") -or ($getGuests -and $catchResult.error.code -like "*Authorization_RequestDenied*")) {
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
                        $sleepSec = @(1, 1, 2, 3, 5, 7, 9, 10, 13, 15, 20, 25, 30, 45, 60, 60, 60)[$tryCounter]
                        $maxTries = 15
                        if ($tryCounter -gt $maxTries) {
                            Write-Host " $currentTask - capitulation after $maxTries attempts"
                            return "capitulation"
                        }
                        Write-Host " $currentTask - try #$tryCounter; returned: (StatusCode: '$($azAPIRequest.StatusCode)') <.code: '$($catchResult.code)'> <.error.code: '$($catchResult.error.code)'> | <.message: '$($catchResult.message)'> <.error.message: '$($catchResult.error.message)'> - try again (trying $maxTries times) in $sleepSec second(s)"
                        Start-Sleep -Seconds $sleepSec
                    }

                    if (($getRoleAssignmentSchedules -and $catchResult.error.code -eq "ResourceNotOnboarded") -or ($getRoleAssignmentSchedules -and $catchResult.error.code -eq "TenantNotOnboarded") -or ($getRoleAssignmentSchedules -and $catchResult.error.code -eq "InvalidResourceType")) {
                        if ($getRoleAssignmentSchedules -and $catchResult.error.code -eq "ResourceNotOnboarded") {
                            return "ResourceNotOnboarded"
                        }
                        if ($getRoleAssignmentSchedules -and $catchResult.error.code -eq "TenantNotOnboarded") {
                            return "TenantNotOnboarded"
                        }
                        if ($getRoleAssignmentSchedules -and $catchResult.error.code -eq "InvalidResourceType") {
                            return "InvalidResourceType"
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

                    if ($validateAccess -and $catchResult.error.code -eq "Authorization_RequestDenied") {
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
                if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: apiStatusCode: $($azAPIRequest.StatusCode)" -ForegroundColor $debugForeGroundColor }
                $azAPIRequestConvertedFromJson = ($azAPIRequest.Content | ConvertFrom-Json)
                if ($listenOn -eq "Content") {       
                    if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: listenOn=content ($((($azAPIRequestConvertedFromJson) | Measure-Object).count))" -ForegroundColor $debugForeGroundColor }      
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
                        if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: listenOn=default(value) value exists ($((($azAPIRequestConvertedFromJson).value | Measure-Object).count))" -ForegroundColor $debugForeGroundColor }
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
                        if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: listenOn=default(value) value not exists; return empty array" -ForegroundColor $debugForeGroundColor }
                    }
                }

                $isMore = $false
                if (-not $validateAccess) {
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
                                Start-Sleep -Seconds 1
                                createBearerToken -targetEndPoint $targetEndpoint
                                Start-Sleep -Seconds 1
                            }
                        }
                        else {
                            $uri = $azAPIRequestConvertedFromJson.nextLink
                        }
                        if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: nextLink: $Uri" -ForegroundColor $debugForeGroundColor }
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
                                Start-Sleep -Seconds 1
                                createBearerToken -targetEndPoint $targetEndpoint
                                Start-Sleep -Seconds 1
                            }
                        }
                        else {
                            $uri = $azAPIRequestConvertedFromJson."@odata.nextLink"
                        }
                        if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: @oData.nextLink: $Uri" -ForegroundColor $debugForeGroundColor }
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
                                Start-Sleep -Seconds 1
                                createBearerToken -targetEndPoint $targetEndpoint
                                Start-Sleep -Seconds 1
                            }
                        }
                        else {
                            $uri = $azAPIRequestConvertedFromJson.properties.nextLink
                        }
                        if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: nextLink: $Uri" -ForegroundColor $debugForeGroundColor }
                    }
                    else {
                        if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: NextLink: none" -ForegroundColor $debugForeGroundColor }
                    }
                }
            }
        }
        else {
            if ($htParameters.DebugAzAPICall -eq $true) { Write-Host "   DEBUG: unexpectedError: notFalse" -ForegroundColor $debugForeGroundColor }
            if ($tryCounterUnexpectedError -lt 13) {
                $sleepSec = @(1, 2, 3, 5, 7, 10, 13, 17, 20, 30, 40, 50, 60)[$tryCounterUnexpectedError]
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
$azModules = @('Az.Accounts', 'Az.Resources')

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
    }
    else {
        Write-Host " Az Module $azModule Version: could not be assessed"
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
    $batchSize = 100
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
            $targetHt.($resolvedIdentity.id) = @{}
            $targetHt.($resolvedIdentity.id).full = "$($type) ($($resolvedIdentity.userType)), DisplayName: $($resolvedIdentity.displayName), Id: $(($resolvedIdentity.id))"
            $targetHt.($resolvedIdentity.id).typeOnly = "$($type) ($($resolvedIdentity.userType))"
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
        #$path = "/providers/Microsoft.Management/managementGroups/$($mgdetail.Name)/providers/Microsoft.Authorization/roleDefinitions?api-version=2015-07-01&`$filter=type%20eq%20'CustomRole'"
        $method = "GET"
            
        $mgCustomRoleDefinitions = ((AzAPICall -uri $uri -method $method -currentTask $currentTask -caller "CustomDataCollection"))
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
        #$path = "/providers/Microsoft.Management/managementGroups/$($mgdetail.Name)/providers/Microsoft.Authorization/roleAssignmentSchedules?api-version=2020-10-01-preview"
        $method = "GET"
        $roleAssignmentSchedulesFromAPI = ((AzAPICall -uri $uri -method $method -currentTask $currentTask -caller "CustomDataCollection" -getRoleAssignmentSchedules $true))
        
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
        #$path = "/providers/Microsoft.Management/managementGroups/$($mgdetail.Name)/providers/Microsoft.Authorization/roleAssignments?api-version=2015-07-01"
        $method = "GET"
        $roleAssignmentsFromAPI = ((AzAPICall -uri $uri -method $method -currentTask $currentTask -caller "CustomDataCollection"))

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
                #$path = "/subscriptions/$childMgSubId/providers/Microsoft.Authorization/roleDefinitions?api-version=2015-07-01&`$filter=type%20eq%20'CustomRole'"
                $method = "GET"
                        
                $subCustomRoleDefinitions = ((AzAPICall -uri $uri -method $method -currentTask $currentTask -caller "CustomDataCollection"))
                foreach ($subCustomRoleDefinition in $subCustomRoleDefinitions) {
                    if (-not $($htCacheDefinitions).role[$subCustomRoleDefinition.name]) {
                        ($script:htCacheDefinitions).role.$($subCustomRoleDefinition.name) = @{}
                        ($script:htCacheDefinitions).role.$($subCustomRoleDefinition.name).definition = $subCustomRoleDefinition
                    }  
                }

                #PIM RoleAssignmentSchedules
                $currentTask = "Role assignment schedules API '$($childMgSubDisplayName)' ('$childMgSubId')"
                $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).ResourceManagerUrl)subscriptions/$childMgSubId/providers/Microsoft.Authorization/roleAssignmentSchedules?api-version=2020-10-01-preview"
                #$path = "/providers/Microsoft.Management/managementGroups/$($mgdetail.Name)/providers/Microsoft.Authorization/roleAssignmentSchedules?api-version=2020-10-01-preview"
                $method = "GET"
                $roleAssignmentSchedulesFromAPI = ((AzAPICall -uri $uri -method $method -currentTask $currentTask -caller "CustomDataCollection" -getRoleAssignmentSchedules $true))
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
                #$path = "/subscriptions/$childMgSubId/providers/Microsoft.Authorization/roleAssignmentsUsageMetrics?api-version=2019-08-01-preview"
                $method = "GET"
                $roleAssignmentsFromAPI = ((AzAPICall -uri $uri -method $method -currentTask $currentTask -caller "CustomDataCollection"))

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

    $startCustPolLoop = get-date
    Write-Host "  processing TenantSummary ServicePrincipals"

    if ($cu.Count -gt 0) {
        $tfCount = $cu.Count
        $htmlTableId = "TenantSummary_ServicePrincipals"
        [void]$htmlTenantSummary.AppendLine(@"
<i class="padlxx fa fa-table" aria-hidden="true"></i> Download CSV <a class="externallink" href="#" onclick="download_table_as_csv_semicolon('$htmlTableId');">semicolon</a> | <a class="externallink" href="#" onclick="download_table_as_csv_comma('$htmlTableId');">comma</a>
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
            col_3: 'select',
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
        var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
        tf.init();
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
    Write-Host "   Custom Policy processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipals

    #region SUMMARYServicePrincipalsAADRoleAssignments
    [void]$htmlTenantSummary.AppendLine(@"
<button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textAADRoleAssignment" data-content="Service Principals AAD RoleAssignments" /></button>
<div class="content TenantSummaryContent">
"@)

    $startCustPolLoop = get-date
    Write-Host "  processing TenantSummary ServicePrincipalsAADRoleAssignments"
    $servicePrincipalsAADRoleAssignments = $cu.where( { $_.SPAADRoleAssignments.Count -ne 0 } )
    $servicePrincipalsAADRoleAssignmentsCount = $servicePrincipalsAADRoleAssignments.Count
    if ($servicePrincipalsAADRoleAssignmentsCount -gt 0) {
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
                        $array += "$($ra.roleDefinitionName)"
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
col_widths: ['10%', '10%', '30%', '10%', '20%', '10%', '10%'],            
            locale: 'en-US',
            col_3: 'multiple',
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
    <p><i class="padlx fa fa-ban" aria-hidden="true"></i> <span class="valignMiddle">$($servicePrincipalsAADRoleAssignmentsCount) Service Principals</span></p>
"@)
    }
    
    [void]$htmlTenantSummary.AppendLine(@"
    </div>
"@)

    $endCustPolLoop = get-date
    Write-Host "   Custom Policy processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsAADRoleAssignments

    #region SUMMARYServicePrincipalsAppRoleAssignments
    [void]$htmlTenantSummary.AppendLine(@"
<button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textServicePrincipal" data-content="Service Principals App RoleAssignments (API permissions Application)" /></button>
<div class="content TenantSummaryContent">
"@)

    $startCustPolLoop = get-date
    Write-Host "  processing TenantSummary ServicePrincipalsAppRoleAssignments"
    $servicePrincipalsAppRoleAssignments = $cu.where( { $_.SPAppRoleAssignments.Count -ne 0 } )
    $servicePrincipalsAppRoleAssignmentsCount = $servicePrincipalsAppRoleAssignments.Count
    if ($servicePrincipalsAppRoleAssignmentsCount -gt 0) {
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
col_widths: ['10%', '10%', '30%', '10%', '20%', '10%', '10%'],            
            locale: 'en-US',
            col_3: 'multiple',
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
    <p><i class="padlx fa fa-ban" aria-hidden="true"></i> <span class="valignMiddle">$($servicePrincipalsAppRoleAssignmentsCount) Service Principals</span></p>
"@)
    }
    
    [void]$htmlTenantSummary.AppendLine(@"
    </div>
"@)

    $endCustPolLoop = get-date
    Write-Host "   Custom Policy processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsAppRoleAssignments

    #region SUMMARYServicePrincipalsOauth2PermissionGrants
    [void]$htmlTenantSummary.AppendLine(@"
<button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textServicePrincipal" data-content="Service Principals Oauth Permission grants (API permissions Delegated)" /></button>
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
<th>SP App Owner Organization Id</th>
<th>SP Oauth Permission grants</th>
</tr>
</thead>
<tbody>
"@)

        foreach ($sp in ($servicePrincipalsOauth2PermissionGrants)) {

            $spType = $sp.SP.servicePrincipalType

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
col_widths: ['10%', '10%', '30%', '10%', '20%', '10%', '10%'],            
            locale: 'en-US',
            col_types: [
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
    <p><i class="padlx fa fa-ban" aria-hidden="true"></i> <span class="valignMiddle">$($servicePrincipalsOauth2PermissionGrantsCount) Service Principals</span></p>
"@)
    }
    
    [void]$htmlTenantSummary.AppendLine(@"
    </div>
"@)

    $endCustPolLoop = get-date
    Write-Host "   Custom Policy processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsAppRoleAssignments

    if (-not $NoAzureRoleAssignments) {
        #region SUMMARYServicePrincipalsAzureRoleAssignments
        [void]$htmlTenantSummary.AppendLine(@"
<button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textAzureRoleAssignment" data-content="Service Principals Azure RoleAssignments" /></button>
<div class="content TenantSummaryContent">
"@)

        $startCustPolLoop = get-date
        Write-Host "  processing TenantSummary ServicePrincipalsAzureRoleAssignments"

        $servicePrincipalsAzureRoleAssignments = $cu.where( { $_.SPAzureRoleAssignments.Count -ne 0 } )
        $servicePrincipalsAzureRoleAssignmentsCount = $servicePrincipalsAzureRoleAssignments.Count

        if ($servicePrincipalsAzureRoleAssignmentsCount -gt 0) {
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
col_widths: ['10%', '10%', '30%', '10%', '20%', '10%', '10%'],            
            locale: 'en-US',
            col_3: 'multiple',
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
    <p><i class="padlx fa fa-ban" aria-hidden="true"></i> <span class="valignMiddle">$($servicePrincipalsAzureRoleAssignmentsCount) Service Principals</span></p>
"@)
        }
    
        [void]$htmlTenantSummary.AppendLine(@"
    </div>
"@)

        $endCustPolLoop = get-date
        Write-Host "   Custom Policy processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
        #endregion SUMMARYServicePrincipalsAzureRoleAssignments
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textServicePrincipal" data-content="Service Principals Azure RoleAssignments / excluded by parameter '-NoAzureRoleAssignments'" /></button>
        <div class="content TenantSummaryContent"></div>
"@)
    }

    #region SUMMARYServicePrincipalsGroupMemberships
    [void]$htmlTenantSummary.AppendLine(@"
    <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup" data-content="Service Principals Group memberships" /></button>
    <div class="content TenantSummaryContent">
"@)
    
    $startCustPolLoop = get-date
    Write-Host "  processing TenantSummary ServicePrincipalsGroupMemberships"
    
    $servicePrincipalsGroupMemberships = $cu.where( { $_.SPGroupMemberships.Count -ne 0 } )
    $servicePrincipalsGroupMembershipsCount = $servicePrincipalsGroupMemberships.Count
    
    if ($servicePrincipalsGroupMembershipsCount -gt 0) {
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
    <th>SP App Owner Organization Id</th>
    <th>SP Group memberships</th>
    </tr>
    </thead>
    <tbody>
"@)
    
        foreach ($sp in ($servicePrincipalsGroupMemberships)) {
    
            $spType = $sp.SP.servicePrincipalType
    
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
    col_widths: ['10%', '10%', '30%', '10%', '20%', '10%', '10%'],            
                locale: 'en-US',
                col_types: [
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
        <p><i class="padlx fa fa-ban" aria-hidden="true"></i> <span class="valignMiddle">$($servicePrincipalsGroupMembershipsCount) Service Principals</span></p>
"@)
    }
        
    [void]$htmlTenantSummary.AppendLine(@"
        </div>
"@)
    
    $endCustPolLoop = get-date
    Write-Host "   Custom Policy processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsAzureRoleAssignments

    #region SUMMARYApplicationSecrets
    $applicationSecrets = $cu.where( { $_.APPPasswordCredentials.Count -ne 0 } )
    $applicationSecretsCount = $applicationSecrets.Count
    $applicationSecretsExpireSoon = $applicationSecrets.where( {$_.APPKeyCredentials.expiryInfo -like "expires soon*"} )
    $applicationSecretsExpireSoonCount = $applicationSecretsExpireSoon.Count

    if ($applicationSecretsExpireSoonCount -gt 0){
        [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textServicePrincipal" data-content="Application Secrets ($applicationSecretsExpireSoonCount expire soon)" /></button>
        <div class="content TenantSummaryContent">
"@)
    }
    else{
        [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textServicePrincipal" data-content="Application Secrets" /></button>
        <div class="content TenantSummaryContent">
"@)
    }


    $startCustPolLoop = get-date
    Write-Host "  processing TenantSummary ApplicationSecrets"

    if ($applicationSecretsCount -gt 0) {
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
                #$appId
                $APPPasswordCredentials = $null
                if (($sp.APPPasswordCredentials)) {
                    if (($sp.APPPasswordCredentials.count -gt 0)) {
                        $array = @()
                        foreach ($secret in $sp.APPPasswordCredentials) {
                            $array += "$($secret.keyId)/$($secret.displayName) ($($secret.expiryInfo))"
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
    var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
    tf.init();
</script>
"@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@"
<p><i class="padlx fa fa-ban" aria-hidden="true"></i> <span class="valignMiddle">$($applicationSecretsCount) Service Principals</span></p>
"@)
    }

    [void]$htmlTenantSummary.AppendLine(@"
</div>
"@)

    $endCustPolLoop = get-date
    Write-Host "   Custom Policy processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYApplicationSecrets

    #region SUMMARYApplicationCertificates
    $applicationCertificates = $cu.where( { $_.APPKeyCredentials.Count -ne 0 } )
    $applicationCertificatesCount = $applicationCertificates.Count
    $applicationCertificatesExpireSoon = $applicationCertificates.where( {$_.APPKeyCredentials.expiryInfo -like "expires soon*"} )
    $applicationCertificatesExpireSoonCount = $applicationCertificatesExpireSoon.Count

    if ($applicationCertificatesExpireSoonCount -gt 0){
        [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textServicePrincipal" data-content="Application Certificates ($applicationCertificatesExpireSoonCount expire soon)" /></button>
        <div class="content TenantSummaryContent">
"@)
    }
    else{
        [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textServicePrincipal" data-content="Application Certificates" /></button>
        <div class="content TenantSummaryContent">
"@) 
    }


    $startCustPolLoop = get-date
    Write-Host "  processing TenantSummary ApplicationCertificates"

    if ($applicationCertificatesCount -gt 0) {
        $tfCount = $applicationCertificatesCount
        $htmlTableId = "TenantSummary_ApplicationCertificates"
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
                $APPPasswordCredentials = $null
                if (($sp.APPKeyCredentials)) {
                    if (($sp.APPKeyCredentials.count -gt 0)) {
                        $array = @()
                        foreach ($key in $sp.APPKeyCredentials) {
                            $array += "$($key.keyId)($($key.customKeyIdentifier))/$($key.displayName) ($($key.expiryInfo))"
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
    var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
    tf.init();
</script>
"@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@"
<p><i class="padlx fa fa-ban" aria-hidden="true"></i> <span class="valignMiddle">$($applicationCertificatesCount) Service Principals</span></p>
"@)
    }

    [void]$htmlTenantSummary.AppendLine(@"
</div>
"@)

    $endCustPolLoop = get-date
    Write-Host "   Custom Policy processing duration: $((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((NEW-TIMESPAN -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYApplicationCertificates

    $script:html += $htmlTenantSummary
    #$htmlTenantSummary = $null
    #$script:html | Add-Content -Path "$($outputPath)$($DirectorySeparatorChar)$($fileName).html" -Encoding utf8 -Force
    #$script:html = $null

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
    $res = AzAPICall -uri $uri -method $method -currentTask $currentTask -consistencyLevel "eventual" -validateAccess $true
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
    $res = AzAPICall -uri $uri -method $method -currentTask $currentTask -consistencyLevel "eventual" -validateAccess $true
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
    $res = AzAPICall -uri $uri -method $method -currentTask $currentTask -consistencyLevel "eventual" -validateAccess $true
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
    $res = AzAPICall -uri $uri -method $method -currentTask $currentTask -consistencyLevel "eventual" -validateAccess $true
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
    $selectedManagementGroupId = AzAPICall -uri $uri -method $method -currentTask $currentTask -listenOn "Content" -validateAccess $true
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

    $arrayEntitiesFromAPI = ((AzAPICall -uri $uri -method $method -currentTask $currentTask))

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

    #region GetTenantDetails
    if ($htParameters.AzureDevOpsWikiAsCode -eq $false) {
        $currentTask = "Get Tenant details"
        Write-Host $currentTask
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).ResourceManagerUrl)tenants?api-version=2020-01-01"
        #$path = "/tenants?api-version=2020-01-01"
        $method = "GET"

        $tenantDetailsResult = ((AzAPICall -uri $uri -method $method -currentTask $currentTask))
        if (($tenantDetailsResult | measure-object).count -gt 0) {
            $tenantDetails = $tenantDetailsResult | Where-Object { $_.tenantId -eq ($checkContext).Tenant.Id }
            $tenantDisplayName = $tenantDetails.displayName
            $tenantDefaultDomain = $tenantDetails.defaultDomain
            Write-Host " Tenant DisplayName: $tenantDisplayName"
        }
        else {
            Write-Host " something unexpected"
        }
    }
    #endregion GetTenantDetails

    #region subscriptions
    $startGetSubscriptions = get-date
    $currentTask = "Getting all Subscriptions"
    Write-Host "$currentTask"
    #https://management.azure.com/subscriptions?api-version=2020-01-01
    $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).ResourceManagerUrl)subscriptions?api-version=2019-10-01"
    $method = "GET"

    $requestAllSubscriptionsAPI = ((AzAPICall -uri $uri -method $method -currentTask $currentTask))
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

    $requestRoleDefinitionAPI = ((AzAPICall -uri $uri -method $method -currentTask $currentTask))
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

#region dataColletionAADSP
$start = get-date
Write-Host "Getting Service Principal count"
$currentTask = "getSPCount"
$uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/servicePrincipals/`$count"
$method = "GET"
$spCount = AzAPICall -uri $uri -method $method -currentTask $currentTask -listenOn "Content" -consistencyLevel "eventual" -getSp $true

Write-Host "Getting $spCount Service Principals"
$currentTask = "getAllSPs"
$start = get-date
$uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/servicePrincipals"
$method = "GET"
$getServicePrincipals = AzAPICall -uri $uri -method $method -currentTask $currentTask -consistencyLevel "eventual" -getSp $true -getSPShowProgress $spCount
$end = get-date
$duration = NEW-TIMESPAN -Start $start -End $end
Write-Host "Getting $($getServicePrincipals.Count) Service Principals duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"

if ($getServicePrincipals.Count -eq 0) {
    Write-Host " No SPs found"
    break
}
else {
    $htServicePrincipalsEnriched = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htServicePrincipalsAppRoles = @{}
    $htServicePrincipalsPublishedPermissionScopes = @{}
    $htAppRoles = @{}
    $htPublishedPermissionScopes = @{}
    $htAadGroupsToResolve = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    #$htUsersToResolveGuestMember = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htAppRoleAssignments = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htSPOauth2PermissionGrantedTo = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htUsersAndGroupsToCheck4AppRoleAssignments = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htUsersAndGroupsToCheck4AppRoleAssignments.User = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htUsersAndGroupsToCheck4AppRoleAssignments.Group = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htApplications = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htSPOwners = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htAppOwners = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    #$htOwners = @{}
    $htOwnedBy = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htProcessedTracker = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}

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
        if (($sp.publishedPermissionScopes).Count -gt 0) {
            $htServicePrincipalsPublishedPermissionScopes.($sp.id) = @{}
            $htServicePrincipalsPublishedPermissionScopes.($sp.id).spDetails = $sp
            $htServicePrincipalsPublishedPermissionScopes.($sp.id).publishedPermissionScopes = @{}
            foreach ($spPublishedPermissionScope in $sp.publishedPermissionScopes) {
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
    #$uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/roleManagement/directory/roleDefinitions?expand=inheritsPermissionsFrom"
    $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/roleManagement/directory/roleDefinitions"
    $method = "GET"
    $aadRoleDefinitions = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSp $true 
    foreach ($aadRoleDefinition in $aadRoleDefinitions) {
        $htAadRoleDefinitions.($aadRoleDefinition.id) = $aadRoleDefinition
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

    #Write-Host " processing $($getServicePrincipals.Count) ServicePrincipals (indicating progress in steps of $indicator)"
    Write-Host " processing $($getServicePrincipals.Count) ServicePrincipals"
    
    ($getServicePrincipals | Sort-Object -Property id -unique) | ForEach-Object -Parallel {
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
        #$htUsersToResolveGuestMember = $using:htUsersToResolveGuestMember
        $htAppRoleAssignments = $using:htAppRoleAssignments
        $htSPOauth2PermissionGrantedTo = $using:htSPOauth2PermissionGrantedTo
        $htUsersAndGroupsToCheck4AppRoleAssignments = $using:htUsersAndGroupsToCheck4AppRoleAssignments
        $htApplications = $using:htApplications
        $indicator = $using:indicator
        $htSPOwners = $using:htSPOwners
        $htAppOwners = $using:htAppOwners
        #$htOwners = $using:htOwners
        $htOwnedBy = $using:htOwnedBy
        $htProcessedTracker = $using:htProcessedTracker
        #func
        $function:AzAPICall = $using:funcAzAPICall
        $function:createBearerToken = $using:funcCreateBearerToken
        $function:GetJWTDetails = $using:funcGetJWTDetails

        #write-host "processing $($sp.id) - $($sp.displayName) (type: $($sp.servicePrincipalType) org: $($sp.appOwnerOrganizationId))"

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
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/servicePrincipals/$($sp.id)/ownedObjects"
        $method = "GET"
        $getSPOwnedObjects = AzAPICall -uri $uri -method $method -currentTask $currentTask
        if ($getSPOwnedObjects.Count -gt 0) {
            $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.ServicePrincipalOwnedObjects = $getSPOwnedObjects | Select-Object '@odata.type', displayName, id
        }
        #endregion spownedObjects

        #region spAADRoleAssignments
        $currentTask = "getSP AADRoleAssignments $($sp.id)"
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/roleManagement/directory/roleAssignments?`$filter=principalId eq '$($sp.id)'"
        $method = "GET"
        $getSPAADRoleAssignments = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSp $true 
        if ($getSPAADRoleAssignments.Count -gt 0) {
            $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.ServicePrincipalAADRoleAssignments = $getSPAADRoleAssignments
        }
        #endregion spAADRoleAssignments

        #region spAppRoleAssignments
        $currentTask = "getSP AppRoleAssignments $($sp.id)"
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/servicePrincipals/$($sp.id)/appRoleAssignments"
        $method = "GET"
        $getSPAppRoleAssignments = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSp $true 
        if ($getSPAppRoleAssignments.Count -gt 0) {
            $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.ServicePrincipalAppRoleAssignments = $getSPAppRoleAssignments
            foreach ($SPAppRoleAssignment in $getSPAppRoleAssignments) {
                if (-not $htAppRoleAssignments.($SPAppRoleAssignment.id)) {
                    $script:htAppRoleAssignments.($SPAppRoleAssignment.id) = $SPAppRoleAssignment
                }
            }
        }
        #endregion spAppRoleAssignments

        #region spAppRoleAssignedTo
        $currentTask = "getSP appRoleAssignedTo $($sp.id)"
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/servicePrincipals/$($sp.id)/appRoleAssignedTo"
        $method = "GET"
        $getSPAppRoleAssignedTo = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSp $true 
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
        #endregion spAppRoleAssignedTo

        #region spGetMemberGroups
        $currentTask = "getSP GroupMemberships $($sp.id)"
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/servicePrincipals/$($sp.id)/getMemberGroups"
        $method = "POST"
        $body = @"
        {
            "securityEnabledOnly": false
        }
"@
        $getSPGroupMemberships = AzAPICall -uri $uri -method $method -body $body -currentTask $currentTask
        if ($getSPGroupMemberships.Count -gt 0) {
            $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.ServicePrincipalGroupMemberships = $getSPGroupMemberships
            foreach ($aadGroupId in $getSPGroupMemberships) {
                if (-not $script:htAadGroupsToResolve.($aadGroupId)) {
                    $script:htAadGroupsToResolve.($aadGroupId) = @{}
                }
            }
        }
        #endregion spGetMemberGroups

        #region spDelegatedPermissions
        $currentTask = "getSP oauth2PermissionGrants $($sp.id)"
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/servicePrincipals/$($sp.id)/oauth2PermissionGrants"
        $method = "GET"
        $getSPOauth2PermissionGrants = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSp $true 

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
        $currentTask = "getSPOwner $($sp.id)"
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/servicePrincipals/$($sp.id)/owners"
        $method = "GET"
        $getSPOwner = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSP $true
        if ($getSPOwner.Count -gt 0) {
            foreach ($spOwner in $getSPOwner) {

                <#
                if ($spOwner.'@odata.type' -eq "#microsoft.graph.user"){
                    if (-not $htUsersToResolveGuestMember.($spOwner.id)){
                        Write-Host "SPowner added ($($appOwner.id))"
                        $script:htUsersToResolveGuestMember.($spOwner.id) = @{}
                    }
                }
                #>

                <#if (-not $htOwners.($spOwner.id)) {
                    $script:htOwners.($spOwner.id) = @{}
                    $script:htOwners.($spOwner.id).owners = [array]$($sp.id)
                }
                else {
                    $array = [array]($htOwners.($spOwner.id).owners)
                    $array += $sp.id
                    $script:htOwners.($spOwner.id).owners = $array
                }
                #>

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
        #endregion spOwner 
        
        #region spApp
        if ($sp.servicePrincipalType -eq "Application") {

            $spType = "APP"
            
            $currentTask = "getApp $($sp.appId)"
            $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/v1.0/applications?`$filter=appId eq '$($sp.appId)'"
            $method = "GET"
            $getApplication = AzAPICall -uri $uri -method $method -currentTask $currentTask -getApp $true

            if ($getApplication.Count -gt 0) {
                $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.Application = @{}
                $script:htServicePrincipalsEnriched.($sp.id).ServicePrincipal.Application.ApplicationDetails = $getApplication
                $script:htApplications.($getApplication.id) = $getApplication

                #region getAppOwner
                $currentTask = "getAppOwner $($getApplication.id)"
                $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/applications/$($getApplication.id)/owners"
                $method = "GET"
                $getAppOwner = AzAPICall -uri $uri -method $method -currentTask $currentTask -getSP $true        
                if ($getAppOwner.Count -gt 0) {
                    if (-not $htAppOwners.($getApplication.id)) {
                        $script:htAppOwners.($getApplication.id) = $getAppOwner | select-Object id, displayName, '@odata.type'
                    }
                    <#
                    foreach ($appOwner in $getAppOwner){
                        if ($appOwner.'@odata.type' -eq "#microsoft.graph.user"){
                            if (-not $htUsersToResolveGuestMember.($appOwner.id)){
                                Write-Host "Appowner added ($($appOwner.id))"
                                $script:htUsersToResolveGuestMember.($appOwner.id) = @{}
                            }
                        }
                    }
                    #>
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
        #endregion spApp

        #region spManagedIdentity
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
                            #$altNameSplit = $altName.split('/')
                            $miResourceScope = "Sub $($altNameSplit[2])"
                        }
                    }
                    else{
                        #$altNameSplit = $altName.split('/')
                        $miResourceScope = "MG $($altNameSplit[4])"
                    }
                }              
            }
        }
        #endregion spManagedIdentity


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

        $processedServicePrincipalsCount = ($script:htServicePrincipalsEnriched.Keys).Count
        if ($processedServicePrincipalsCount) {
            if ($processedServicePrincipalsCount % $indicator -eq 0) {
                #$rand = Get-Random -Minimum 100 -Maximum 2000
                #start-sleep -Milliseconds $rand
                if (-not $script:htProcessedTracker.($processedServicePrincipalsCount)) {
                    $script:htProcessedTracker.($processedServicePrincipalsCount) = @{}
                    Write-Host " $processedServicePrincipalsCount Service Principals processed"
                }
            }
        }

    } -ThrottleLimit $ThrottleLimitGraph

    $endForeachSP = get-date
    $duration = NEW-TIMESPAN -Start $startForeachSP -End $endForeachSP
    Write-Host " Collecting data for all Service Principals ($($getServicePrincipals.Count)) duration: $($duration.TotalMinutes) minutes ($($durationAADSP.TotalSeconds) seconds)"
}
$end = get-date
$duration = NEW-TIMESPAN -Start $start -End $end
Write-Host "SP Collection duration: $($duration.TotalMinutes) minutes ($($durationAADSP.TotalSeconds) seconds)"
#endregion dataColletionAADSP

$htUsersToResolveGuestMember = @{}

#region AppRoleAssignments4UsersAndGroups

$htUsersAndGroupsRoleAssignments = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
if ($htUsersAndGroupsToCheck4AppRoleAssignments.User.Keys.Count -gt 0) {

    #UsersToResolveGuestMember
    foreach ($user in $htUsersAndGroupsToCheck4AppRoleAssignments.User.Keys) {
        if (-not $htUsersToResolveGuestMember.($user)) {
            Write-Host "UsersToResolveGuestMember user added ($($user))"
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
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/users/$($userObjectId)/appRoleAssignments"
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
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/Groups/$($groupObjectId)/appRoleAssignments"
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
            
        Write-Host "resolving AAD Group: $aadGroupId"
        $currentTask = "get AAD Group $($aadGroupId)"
        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/groups/$($aadGroupId)"
        $method = "GET"
        $getAadGroup = AzAPICall -uri $uri -method $method -currentTask $currentTask -listenOn "Content"
        $script:htAadGroups.($aadGroupId) = @{}
        $script:htAadGroups.($aadGroupId).groupDetails = $getAadGroup

        $uri = "$(($htAzureEnvironmentRelatedUrls).($checkContext.Environment.Name).MSGraphUrl)/beta/groups/$($aadGroupId)/transitivemembers/microsoft.graph.group?`$count=true"
        $method = "GET"
        $getNestedGroups = AzAPICall -uri $uri -method $method -currentTask $currentTask -consistencyLevel "eventual"
        if ($getNestedGroups) {
            write-host " -> has nested Groups $($getNestedGroups.Count)"
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
Write-Host "Resolving AAD Groups where any SP is memberOf duration: $($duration.TotalMinutes) minutes ($($durationAADSP.TotalSeconds) seconds)"
#endregion groupsFromSPs
    
#region GroupsFromAzureRoleAssignments
$start = get-date
#batching
$counterBatch = [PSCustomObject] @{ Value = 0 }
$batchSize = 100
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
                Write-Host "UsersToResolveGuestMember SPowner added ($($owner.id))"
                $htUsersToResolveGuestMember.($owner.id) = @{}
            }
        }
    }
}
foreach ($appOwner in $htAppOwners.Values) {
    foreach ($owner in $appOwner) {
        if ($owner.'@odata.type' -eq "#microsoft.graph.user") {
            if (-not $htUsersToResolveGuestMember.($owner.id)) {
                Write-Host "UsersToResolveGuestMember appOwner added ($($owner.id))"
                $htUsersToResolveGuestMember.($owner.id) = @{}
            }
        }
    }
}
resolveObectsById -objects $htUsersToResolveGuestMember.Keys -targetHt $htUsersResolved

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
                $htTmp.spType = $htServicePrincipalsEnriched.($owner.id).spTypeConcatinated
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
        $htOptInfo.ownerId = $($owner.id)
        $htOptInfo.owner = $($owner.displayName)
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
        #write-host "DIRECT: $($owner.displayName) owns $sp" 
    }

    foreach ($owner in $htSPOwnersTmp.($sp).indirect) {
        if ($owner -eq "noOwner" -or $owner.'@odata.type' -eq '#microsoft.graph.user') {
            if ($owner.'@odata.type' -eq '#microsoft.graph.user') {
                if (($arrayOwners.where({ $_.applicability -eq "indirect" })).ownerId -notcontains $owner.id) {
                    $htOptInfo = [ordered] @{}
                    $htOptInfo.ownerId = $($owner.id)
                    $htOptInfo.owner = $($owner.displayName)
                    $htOptInfo.type = $($owner.'@odata.type')
                    $htOptInfo.principalType = $htUsersResolved.($owner.id).typeOnly
                    $htOptInfo.applicability = "indirect"
                    $null = $arrayOwners.Add($htOptInfo)
                }
            }
        }
        else {
            $htOptInfo = [ordered] @{}
            $htOptInfo.ownerId = $($owner.id)
            $htOptInfo.owner = $($owner.displayName)
            $htOptInfo.type = $($owner.'@odata.type')
            $htOptInfo.applicability = "indirect"
            if ($owner.'@odata.type' -eq "#microsoft.graph.servicePrincipal") {
                #$htOptInfo.spType = $htServicePrincipalsEnriched.($owner.id).spTypeConcatinated
                $htOptInfo.principalType = $htServicePrincipalsEnriched.($owner.id).spTypeConcatinated
            }
            if ($owner.'@odata.type' -eq "#microsoft.graph.user") {
                $htOptInfo.principalType = $htOptInfo.principalType = $htUsersResolved.($owner.id).typeOnly
            }

            $owners = getowner -owner $owner.id

            $htOptInfo.ownedBy = $($owners)

            <#
            foreach ($userOwner in $owners.where({ $_.'@odata.type' -eq '#microsoft.graph.user' })) {
                #Write-Host "U-INDIRECT $($owner.displayName) owns $sp and is owned by $($userOwner.displayName)"
            }
            foreach ($spOwner in $owners.where({ $_.'@odata.type' -eq '#microsoft.graph.servicePrincipal' })) {
                #Write-Host "S-INDIRECT $($owner.displayName) owns $sp and is owned by $($spOwner.displayName)"
    
            }
            foreach ($noOwner in $owners.where({ $_ -eq 'noOwner' })) {
                #Write-Host "N-INDIRECT $($owner.displayName) owns $sp and is owned by $($noOwner)"
            }
            #>
            $null = $arrayOwners.Add($htOptInfo)
        }
    }

    if ($arrayOwners.Count -gt 0) {
        $htSPOwnersFinal.($sp) = @{}
        $htSPOwnersFinal.($sp) = $arrayOwners
    }  

    <#
    $allUsers = $htSPOwnersFinal.keys | where { $htSPOwnersFinal.($_).type -eq "#microsoft.graph.user" }
    foreach ($user in $allUsers){
        $htSPOwnersFinal.($user)
        pause
    }
    #>

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
    $htAppOwnersFinal.($app).owner = $array
}

#endregion owners

if (-not $NoAzureRoleAssignments) {
    #region AzureRoleAssignmentMapping
    $start = get-date

    #resolving createdby/updatedby
    $htARMRaResolvedCreatedByUpdatedBy = @{}
    $htCreatedByUpdatedByObjectIdsToBeResolved = @{}
    foreach ($createdByItem in $htCacheAssignments.roleFromAPI.values.assignment.properties.createdBy | Sort-Object -Unique) {
        
        if ([guid]::TryParse(($createdByItem), $([ref][guid]::Empty))){
            $createdByItem
            if (-not $htARMRaResolvedCreatedByUpdatedBy.($createdByItem)) {            
                if ($getServicePrincipals.id -contains $createdByItem) {
                    if ($htServicePrincipalsEnriched.($createdByItem)) {
                        $hlper = $htServicePrincipalsEnriched.($createdByItem)
                        $htARMRaResolvedCreatedByUpdatedBy.($createdByItem) = @{}
                        $htARMRaResolvedCreatedByUpdatedBy.($createdByItem).full = "$($hlper.spTypeConcatinated), DisplayName: $($hlper.ServicePrincipal.ServicePrincipalDetails.displayName), Id: $($createdByItem)"
                        $htARMRaResolvedCreatedByUpdatedBy.($createdByItem).typeOnly = $hlper.spTypeConcatinated
                    }
                }
                else {
                    if ($htUsersResolved.($createdByItem)){
                        #Write-Host $createdByItem "already known form other HT"
                        #$htUsersResolved.($createdByItem)
                        $htARMRaResolvedCreatedByUpdatedBy.($createdByItem) = @{}
                        $htARMRaResolvedCreatedByUpdatedBy.($createdByItem).full = $htUsersResolved.($createdByItem).full
                        $htARMRaResolvedCreatedByUpdatedBy.($createdByItem).typeOnly = $htUsersResolved.($createdByItem).typeOnly
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
        resolveObectsById -objects $arrayUnresolvedIdentities -targetHt $htARMRaResolvedCreatedByUpdatedBy        
    }

    if ($htCacheAssignments.Keys.Count -gt 0) {
        $htAssignmentsByPrincipalId = @{}
        $htAssignmentsByPrincipalId."servicePrincipals" = @{}
        $htAssignmentsByPrincipalId."groups" = @{}
        foreach ($assignment in $htCacheAssignments.roleFromAPI.values) {
            #todo sp created ra in azure
            if (-not [string]::IsNullOrEmpty($assignment.assignment.properties.createdBy)){
                if ($htARMRaResolvedCreatedByUpdatedBy.($assignment.assignment.properties.createdBy)) {
                    $assignment.assignment.properties.createdBy = $htARMRaResolvedCreatedByUpdatedBy.($assignment.assignment.properties.createdBy).full
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
else {

}


#region enrichedAADSPData
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

foreach ($sp in $htServicePrincipalsEnriched.values) {
    Write-host "processing SP:" $sp.ServicePrincipal.ServicePrincipalDetails.displayName "objId: $($sp.ServicePrincipal.ServicePrincipalDetails.id)" "appId: $($sp.ServicePrincipal.ServicePrincipalDetails.appId)"

    <# redundant
    if (($sp.ServicePrincipal.ServicePrincipalDetails.appRoles).Count -gt 0) {
        foreach ($spAppRole in $sp.ServicePrincipal.ServicePrincipalDetails.appRoles) {
            Write-Host "SP AppRoles             : $($spAppRole.displayName) ($($spAppRole.id)) - $($spAppRole.allowedMemberTypes -join ", ") - $($spAppRole.value)"

        }
    }
    #>

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


    <# redundant
    if (($sp.ServicePrincipal.ServicePrincipalDetails.publishedPermissionScopes).Count -gt 0) {
        foreach ($publishedPermissionScope in $sp.ServicePrincipal.ServicePrincipalDetails.publishedPermissionScopes) {
            Write-Host "SP publishedPermissionScope : $($publishedPermissionScope.adminConsentDisplayName)"
        }
    }
    #>
    
    #region ServicePrincipalOwners
    $arrayServicePrincipalOwnerOpt = [System.Collections.ArrayList]@()
    if ($htSPOwnersFinal.($sp.ServicePrincipal.ServicePrincipalDetails.id)) {
        foreach ($servicePrincipalOwner in $htSPOwnersFinal.($sp.ServicePrincipal.ServicePrincipalDetails.id)) {
            $htOptInfo = [ordered] @{}
            $htOptInfo.id = $servicePrincipalOwner.ownerId
            $htOptInfo.displayName = $servicePrincipalOwner.owner
            #$htOptInfo.type = $servicePrincipalOwner.type
            $htOptInfo.principalType = $servicePrincipalOwner.principalType
            #if ($servicePrincipalOwner.spType) {
            #    $htOptInfo.spType = $servicePrincipalOwner.spType 
            #}
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
            #Write-Host "SP AAD Role assigned    :" $hlper.displayName "-" $hlper.description
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
                    #Write-Host "SP delegated            :" $hlperServicePrincipalsPublishedPermissionScope.spdetails.displayName "|" $scope "-" $hlperPublishedPermissionScope.adminConsentDescription
                
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
        foreach ($SPOauth2PermissionGrantedTo in $htSPOauth2PermissionGrantedTo.($sp.ServicePrincipal.ServicePrincipalDetails.id) | Sort-Object -Property clientId) {
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
            #Write-Host "SP AppRole Ass          :" $servicePrincipalAppRoleAssignment.resourceDisplayName "|" $hlper.value "-" $hlper.displayName

            $htOptInfo = [ordered] @{}
            $htOptInfo.AppRoleAssignmentId = $servicePrincipalAppRoleAssignment.id
            $htOptInfo.AppRoleAssignmentResourceId = $servicePrincipalAppRoleAssignment.resourceId
            $htOptInfo.AppRoleAssignmentResourceDisplayName = $servicePrincipalAppRoleAssignment.resourceDisplayName
            $htOptInfo.AppRoleAssignmentCreationTimestamp = $servicePrincipalAppRoleAssignment.creationTimestamp
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
            #Write-Host "SP AppRoleAssignedTo    :" $servicePrincipalAppRoleAssignedTo.principalDisplayName "($($servicePrincipalAppRoleAssignedTo.principalId)) |" $servicePrincipalAppRoleAssignedTo.principalType
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
            foreach ($servicePrincipalGroupMembership in $sp.ServicePrincipal.ServicePrincipalGroupMemberships) {
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

                if ($htAadGroups.($servicePrincipalGroupMembership).nestedGroups) {
                    foreach ($nestegGroupId in $htAadGroups.($servicePrincipalGroupMembership).nestedGroups.id) {
                        if ($htGroupRoleAssignmentThroughNesting.($nestegGroupId).RoleAssignmentsInherited) {
                            foreach ($roleAssignmentThroughNesting in $htGroupRoleAssignmentThroughNesting.($nestegGroupId).RoleAssignmentsInherited) {
                                #Write-Host "Azure Role Assignment (through nested group membership ($nestegGroupId -> member of $($roleAssignmentThroughNesting.properties.principalId))) : $($roleAssignmentThroughNesting.id)"
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
                        #Write-Host "Azure RoleAssignment (from Group $servicePrincipalGroupMembership)    : $($roleAssignmentSP.id)"
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
                #Write-Host "Azure RoleAssignment (direct)    : $($roleAssignmentSP.id)"
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
                    #write-host "$($result.applicability) ($($result.appliesThrough)) - !$($result.roleAssignmentFull.assignmentScope) -> $($result.roleAssignmentFull.assignmentScopeId)! $roleAssignment"
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
            foreach ($servicePrincipalGroupMembership in $sp.ServicePrincipal.ServicePrincipalGroupMemberships) {
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
            $arrayApplicationOwnerOpt = $htAppOwnersFinal.($sp.ServicePrincipal.Application.ApplicationDetails.id).owner
        }

        $arrayApplicationOwnerOpt = [System.Collections.ArrayList]@()
        foreach ($appOwner in $htAppOwners.($sp.ServicePrincipal.Application.ApplicationDetails.id)) {
            $arrayApplicationOwner = [System.Collections.ArrayList]@()
            if ($htSPOwnersFinal.($appOwner.id)) {

                foreach ($servicePrincipalOwner in $htSPOwnersFinal.($appOwner.id)) {
                    $htOptInfo = [ordered] @{}
                    $htOptInfo.id = $servicePrincipalOwner.ownerId
                    $htOptInfo.displayName = $servicePrincipalOwner.owner
                    #$htOptInfo.type = $servicePrincipalOwner.type
                    $htOptInfo.principalType = $servicePrincipalOwner.principalType
                    #if ($servicePrincipalOwner.spType) {
                    #    $htOptInfo.spType = $servicePrincipalOwner.spType 
                    #}
                    $htOptInfo.applicability = $servicePrincipalOwner.applicability
                    $arrayOwnedBy = @()
                    
                    foreach ($owner in $servicePrincipalOwner.ownedBy) {
                        $owner
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
            #$htOptInfo.type = $appOwner.'@odata.type'
            #$htOptInfo.principalType = $appOwner.principalType
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

                    #Write-Host "SP Application Secret : $($hlperApplicationPasswordCredential.keyId) ($($displayName)) expiry: $($expiryApplicationPasswordCredential) "
                    $htOptInfo = [ordered] @{}
                    $htOptInfo.keyId = $hlperApplicationPasswordCredential.keyId
                    $htOptInfo.displayName = $displayName
                    $htOptInfo.expiryInfo = $expiryApplicationPasswordCredential
                    $htOptInfo.endDateTime = $hlperApplicationPasswordCredential.endDateTime
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
                    #Write-Host "SP Application Certificate : $($hlperApplicationKeyCredential.keyId) ($($hlperApplicationKeyCredential.displayName); $($hlperApplicationKeyCredential.customKeyIdentifier); $($hlperApplicationKeyCredential.type)) expiry:$($expiryApplicationKeyCredential) start:$($hlperApplicationKeyCredential.startDateTime) end:$($hlperApplicationKeyCredential.endDateTime)"
                    $htOptInfo = [ordered] @{}
                    $htOptInfo.keyId = $hlperApplicationKeyCredential.keyId
                    $htOptInfo.displayName = $hlperApplicationKeyCredential.displayName
                    $htOptInfo.customKeyIdentifier = $hlperApplicationKeyCredential.customKeyIdentifier
                    $htOptInfo.expiryInfo = $expiryApplicationKeyCredential
                    $htOptInfo.endDateTime = $hlperApplicationKeyCredential.endDateTime
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
        $hlper = $htServicePrincipalsEnriched.($sp.ServicePrincipal.ServicePrincipalDetails.id)
        $htOptInfo.type = $hlper.subtype
        $htOptInfo.alternativeName = $hlper.altname
        $htOptInfo.resourceType = $hlper.resourceType
        $htOptInfo.resourceScope = $hlper.resourceScope
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
        SPPublisherName             = $sp.ServicePrincipal.ServicePrincipalDetails.publisherName
        SPVerifiedPublisher         = $sp.ServicePrincipal.ServicePrincipalDetails.verifiedPublisher
        SPHomepage                  = $sp.ServicePrincipal.ServicePrincipalDetails.homepage
        SPErrorUrl                  = $sp.ServicePrincipal.ServicePrincipalDetails.errorUrl
        SPLoginUrl                  = $sp.ServicePrincipal.ServicePrincipalDetails.loginUrl
        SPLogoutUrl                 = $sp.ServicePrincipal.ServicePrincipalDetails.logoutUrl
        SPPreferredSingleSignOnMode = $sp.ServicePrincipal.ServicePrincipalDetails.preferredSingleSignOnMode
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
}

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
#$fileTimestamp = (get-date -format $FileTimeStampFormat)

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
    <link rel="stylesheet" type="text/css" href="https://www.azadvertizer.net/azadserviceprincipalinsights/css/azadserviceprincipalinsightsmain_001_003.css">
    <script src="https://www.azadvertizer.net/azgovvizv4/js/jquery-1.12.1.js"></script>
    <script src="https://www.azadvertizer.net/azgovvizv4/js/jquery-ui-1.12.1.js"></script>
    <script type="text/javascript" src="https://www.azadvertizer.net/azgovvizv4/js/highlight_v004_002.js"></script>
    <script src="https://www.azadvertizer.net/azgovvizv4/js/fontawesome-0c0b5cbde8.js"></script>
    <script src="https://www.azadvertizer.net/azgovvizv4/tablefilter/tablefilter.js"></script>
    <link rel="stylesheet" href="https://www.azadvertizer.net/azgovvizv4/css/highlight-10.5.0.min.css">
    <script src="https://www.azadvertizer.net/azgovvizv4/js/highlight-10.5.0.min.js"></script>
    <script>hljs.initHighlightingOnLoad();</script>

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
    <script src="https://www.azadvertizer.net/azgovvizv4/js/collapsetable_v004_001.js"></script>
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

if ($DoTranscript) {
    Stop-Transcript
}
