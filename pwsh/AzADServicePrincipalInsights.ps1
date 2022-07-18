[CmdletBinding()]
Param
(
    [string]$Product = 'AzADServicePrincipalInsights',
    [string]$ScriptPath = 'pwsh',
    [string]$ProductVersion = 'v1_20220717_1',
    [string]$azAPICallVersion = '1.1.18',
    [string]$GitHubRepository = 'aka.ms/AzADServicePrincipalInsights',
    [switch]$AzureDevOpsWikiAsCode, #deprecated - Based on environment variables the script will detect the code run platform
    [switch]$DebugAzAPICall,
    $ManagementGroupId,
    [switch]$NoCsvExport,
    [string]$CsvDelimiter = ';',
    [switch]$CsvExportUseQuotesAsNeeded,
    [string]$OutputPath,
    [array]$SubscriptionQuotaIdWhitelist = @('undefined'),
    [switch]$DoTranscript,
    [int]$HtmlTableRowsLimit = 20000, #HTML -> becomes unresponsive depending on client device performance. A recommendation will be shown to download the CSV instead of opening the TF table
    [int]$ThrottleLimitARM = 10,
    [int]$ThrottleLimitGraph = 20,
    [int]$ThrottleLimitLocal = 100,
    [string]$SubscriptionId4AzContext = 'undefined',
    [string]$FileTimeStampFormat = 'yyyyMMdd_HHmmss',
    [switch]$NoJsonExport,
    [int]$AADGroupMembersLimit = 500,
    [switch]$NoAzureRoleAssignments,
    [switch]$StatsOptOut,
    [int]$ApplicationSecretExpiryWarning = 14,
    [int]$ApplicationSecretExpiryMax = 730,
    [int]$ApplicationCertificateExpiryWarning = 14,
    [int]$ApplicationCertificateExpiryMax = 730,
    [string]$DirectorySeparatorChar = [IO.Path]::DirectorySeparatorChar,
    [switch]$OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes,
    [array]$CriticalAADRoles = @('62e90394-69f5-4237-9190-012177145e10', 'e8611ab8-c189-46e8-94e1-60213ab1f814', '7be44c8a-adaf-4e2a-84d6-ab2649e08a13') #Global Administrator, Privileged Role Administrator, Privileged Authentication Administrator
)

$Error.clear()
$ErrorActionPreference = 'Stop'
#removeNoise
$ProgressPreference = 'SilentlyContinue'
Set-Item Env:\SuppressAzurePowerShellBreakingChangeWarnings 'true'

$startProduct = Get-Date
$startTime = Get-Date -Format 'dd-MMM-yyyy HH:mm:ss'
Write-Host "Start $($Product) $($startTime) (#$($ProductVersion))"

#region testPowerShellVersion
function testPowerShellVersion {

    Write-Host ' Checking PowerShell edition and version'
    $requiredPSVersion = '7.0.3'
    $splitRequiredPSVersion = $requiredPSVersion.split('.')
    $splitRequiredPSVersionMajor = $splitRequiredPSVersion[0]
    $splitRequiredPSVersionMinor = $splitRequiredPSVersion[1]
    $splitRequiredPSVersionPatch = $splitRequiredPSVersion[2]

    $thisPSVersion = ($PSVersionTable.PSVersion)
    $thisPSVersionMajor = ($thisPSVersion).Major
    $thisPSVersionMinor = ($thisPSVersion).Minor
    $thisPSVersionPatch = ($thisPSVersion).Patch

    $psVersionCheckResult = 'letsCheck'

    if ($PSVersionTable.PSEdition -eq 'Core' -and $thisPSVersionMajor -eq $splitRequiredPSVersionMajor) {
        if ($thisPSVersionMinor -gt $splitRequiredPSVersionMinor) {
            $psVersionCheckResult = 'passed'
            $psVersionCheck = "(Major[$splitRequiredPSVersionMajor]; Minor[$thisPSVersionMinor] gt $($splitRequiredPSVersionMinor))"
        }
        else {
            if ($thisPSVersionPatch -ge $splitRequiredPSVersionPatch) {
                $psVersionCheckResult = 'passed'
                $psVersionCheck = "(Major[$splitRequiredPSVersionMajor]; Minor[$splitRequiredPSVersionMinor]; Patch[$thisPSVersionPatch] gt $($splitRequiredPSVersionPatch))"
            }
            else {
                $psVersionCheckResult = 'failed'
                $psVersionCheck = "(Major[$splitRequiredPSVersionMajor]; Minor[$splitRequiredPSVersionMinor]; Patch[$thisPSVersionPatch] lt $($splitRequiredPSVersionPatch))"
            }
        }
    }
    else {
        $psVersionCheckResult = 'failed'
        $psVersionCheck = "(Major[$splitRequiredPSVersionMajor] ne $($splitRequiredPSVersionMajor))"
    }

    if ($psVersionCheckResult -eq 'passed') {
        Write-Host "  PS check $psVersionCheckResult : $($psVersionCheck); (minimum supported version '$requiredPSVersion')"
        Write-Host "  PS Edition: $($PSVersionTable.PSEdition); PS Version: $($PSVersionTable.PSVersion)"
        Write-Host '  PS Version check succeeded' -ForegroundColor Green
    }
    else {
        Write-Host "  PS check $psVersionCheckResult : $($psVersionCheck)"
        Write-Host "  PS Edition: $($PSVersionTable.PSEdition); PS Version: $($PSVersionTable.PSVersion)"
        Write-Host "  Parallelization requires Powershell 'Core' version '$($requiredPSVersion)' or higher"
        Throw 'Error - check the last console output for details'
    }
}
testPowerShellVersion
#endregion testPowerShellVersion

#region filedir
function setOutput {
    #outputPath
    if (-not [IO.Path]::IsPathRooted($outputPath)) {
        $outputPath = Join-Path -Path (Get-Location).Path -ChildPath $outputPath
    }
    $outputPath = Join-Path -Path $outputPath -ChildPath '.'
    $script:outputPath = [IO.Path]::GetFullPath($outputPath)
    if (-not (Test-Path $outputPath)) {
        Write-Host "path $outputPath does not exist - please create it!" -ForegroundColor Red
        Throw 'Error - check the last console output for details'
    }
    else {
        Write-Host "Output/Files will be created in path '$outputPath'"
    }

    #fileTimestamp
    try {
        $script:fileTimestamp = (Get-Date -Format $FileTimeStampFormat)
    }
    catch {
        Write-Host "fileTimestamp format: '$($FileTimeStampFormat)' invalid; continue with default format: 'yyyyMMdd_HHmmss'" -ForegroundColor Red
        $FileTimeStampFormat = 'yyyyMMdd_HHmmss'
        $script:fileTimestamp = (Get-Date -Format $FileTimeStampFormat)
    }

    $script:executionDateTimeInternationalReadable = Get-Date -Format 'dd-MMM-yyyy HH:mm:ss'
    $script:currentTimeZone = (Get-TimeZone).Id
}
setOutput
#endregion filedir

#region verifyAzAPICall
if ($azAPICallVersion) {
    Write-Host " Verify 'AzAPICall' ($azAPICallVersion)"
}
else {
    Write-Host " Verify 'AzAPICall' (latest)"
}

do {
    $importAzAPICallModuleSuccess = $false
    try {

        if (-not $azAPICallVersion) {
            Write-Host '  Check latest module version'
            try {
                $azAPICallVersion = (Find-Module -Name AzAPICall).Version
                Write-Host "  Latest module version: $azAPICallVersion"
            }
            catch {
                Write-Host '  Check latest module version failed'
                throw
            }
        }

        try {
            $azAPICallModuleDeviation = $false
            $azAPICallModuleVersionLoaded = ((Get-Module -Name AzAPICall).Version)
            foreach ($moduleLoaded in $azAPICallModuleVersionLoaded) {
                if ($moduleLoaded.toString() -ne $azAPICallVersion) {
                    Write-Host "  Deviating loaded version found ('$($moduleLoaded.toString())' != '$($azAPICallVersion)')"
                    $azAPICallModuleDeviation = $true
                }
                else {
                    if ($azAPICallModuleVersionLoaded.count -eq 1) {
                        Write-Host "  AzAPICall module ($($moduleLoaded.toString())) is already loaded" -ForegroundColor Green
                        $importAzAPICallModuleSuccess = $true
                    }
                }
            }

            if ($azAPICallModuleDeviation) {
                $importAzAPICallModuleSuccess = $false
                try {
                    Write-Host "  Remove-Module AzAPICall ($(($azAPICallModuleVersionLoaded -join ', ').ToString()))"
                    Remove-Module -Name AzAPICall -Force
                }
                catch {
                    Write-Host '  Remove-Module AzAPICall failed'
                    throw
                }
            }
        }
        catch {
            #Write-Host '  AzAPICall module is not loaded'
        }

        if (-not $importAzAPICallModuleSuccess) {
            Write-Host "  Try importing AzAPICall module ($azAPICallVersion)"
            if (($env:SYSTEM_TEAMPROJECTID -and $env:BUILD_REPOSITORY_ID) -or $env:GITHUB_ACTIONS) {
                Import-Module ".\$($ScriptPath)\AzAPICallModule\AzAPICall\$($azAPICallVersion)\AzAPICall.psd1" -Force -ErrorAction Stop
                Write-Host "  Import PS module 'AzAPICall' ($($azAPICallVersion)) succeeded" -ForegroundColor Green
            }
            else {
                Import-Module -Name AzAPICall -RequiredVersion $azAPICallVersion -Force
                Write-Host "  Import PS module 'AzAPICall' ($($azAPICallVersion)) succeeded" -ForegroundColor Green
            }
            $importAzAPICallModuleSuccess = $true
        }
    }
    catch {
        Write-Host '  Importing AzAPICall module failed'
        if (($env:SYSTEM_TEAMPROJECTID -and $env:BUILD_REPOSITORY_ID) -or $env:GITHUB_ACTIONS) {
            Write-Host "  Saving AzAPICall module ($($azAPICallVersion))"
            try {
                $params = @{
                    Name = 'AzAPICall'
                    Path = ".\$($ScriptPath)\AzAPICallModule"
                    Force = $true
                    RequiredVersion = $azAPICallVersion
                }
                Save-Module @params
            }
            catch {
                Write-Host "  Saving AzAPICall module ($($azAPICallVersion)) failed"
                throw
            }
        }
        else {
            do {
                $installAzAPICallModuleUserChoice = Read-Host "  Do you want to install AzAPICall module ($($azAPICallVersion)) from the PowerShell Gallery? (y/n)"
                if ($installAzAPICallModuleUserChoice -eq 'y') {
                    try {
                        Install-Module -Name AzAPICall -RequiredVersion $azAPICallVersion
                    }
                    catch {
                        Write-Host "  Install-Module AzAPICall ($($azAPICallVersion)) Failed"
                        throw
                    }
                }
                elseif ($installAzAPICallModuleUserChoice -eq 'n') {
                    Write-Host '  AzAPICall module is required, please visit https://aka.ms/AZAPICall or https://www.powershellgallery.com/packages/AzAPICall'
                    throw '  AzAPICall module is required'
                }
                else {
                    Write-Host "  Accepted input 'y' or 'n'; start over.."
                }
            }
            until ($installAzAPICallModuleUserChoice -eq 'y')
        }
    }
}
until ($importAzAPICallModuleSuccess)
#endregion verifyAzAPICall

#Region initAZAPICall
Write-Host "Initialize 'AzAPICall'"
$parameters4AzAPICallModule = @{
    DebugAzAPICall = $DebugAzAPICall
    SubscriptionId4AzContext = $SubscriptionId4AzContext
    GitHubRepository = $GitHubRepository
}
$azAPICallConf = initAzAPICall @parameters4AzAPICallModule
Write-Host " Initialize 'AzAPICall' succeeded" -ForegroundColor Green
#EndRegion initAZAPICall

#region checkVersion
function checkVersion {
    try {
        $getRepoVersion = Invoke-WebRequest -Uri 'https://raw.githubusercontent.com/JulianHayward/AzADServicePrincipalInsights/master/version.txt'
        $versionThis = ($ProductVersion -split '_')[1]
        $script:versionOnRepositoryFull = $getRepoVersion.Content -replace "`n"
        $versionOnRepository = ($versionOnRepositoryFull -split '_')[1]
        $script:newerVersionAvailable = $false
        $script:newerVersionAvailableHTML = ''
        if ([int]$versionOnRepository -gt [int]$versionThis) {
            $script:newerVersionAvailable = $true
            $script:newerVersionAvailableHTML = '<span style="color:#FF5733; font-weight:bold">Get the latest ' + $Product + ' version (' + $versionOnRepositoryFull + ')!</span> <a href="https://aka.ms/AzADServicePrincipalInsights" target="_blank"><i class="fa fa-external-link" aria-hidden="true"></i></a>'
        }
        if ($newerVersionAvailable) {
            if (-not $azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions) {
                Write-Host ''
                Write-Host " * * * This $Product version ($ProductVersion) is not up to date. Get the latest $Product version ($versionOnRepositoryFull)! * * *" -ForegroundColor Green
                Write-Host 'https://aka.ms/AzADServicePrincipalInsights'
                Write-Host ' * * * * * * * * * * * * * * * * * * * * * *' -ForegroundColor Green
                Pause
            }
        }
    }
    catch {
        #skip
    }
}
checkVersion
#endregion checkVersion

if ($NoAzureRoleAssignments) {
    if ($OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes) {
        Write-Host "Reset parameter -OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes $OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes to $false due to parameter -NoAzureRoleAssignments $NoAzureRoleAssignments"
        $OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes = $false
    }
}

if (-not $ManagementGroupId) {
    $ManagementGroupId = ($azAPICallConf['checkContext']).Tenant.Id
    $runTenantRoot = $true
}
else {
    if ($ManagementGroupId.Count -gt 1) {
        if ($ManagementGroupId -contains ($azAPICallConf['checkContext']).Tenant.Id) {
            $ManagementGroupId = ($azAPICallConf['checkContext']).Tenant.Id
            $runTenantRoot = $true
        }
        else {
            $runTenantRoot = $false
        }
    }
    else {
        if ($ManagementGroupId -eq ($azAPICallConf['checkContext']).Tenant.Id) {
            $runTenantRoot = $true
        }
        else {
            $runTenantRoot = $false
        }
    }
}

Write-Host "Executing against Tenant Root Group: $runTenantRoot"
Write-Host "Executing against $($ManagementGroupID.Count) ManagementGroup(s): $($ManagementGroupID -join ', ')"

$fileNameMGRef = $ManagementGroupID -join '_'

function setTranscript {
    #region setTranscript

    if ($azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions -eq $true) {

        $script:fileNameTranscript = "AzADServiceprincipalInsights_$($fileNameMGRef)_Log.txt"
    }
    else {

        $script:fileNameTranscript = "AzADServiceprincipalInsights_$($ProductVersion)_$($fileTimestamp)_$($fileNameMGRef)_Log.txt"
    }

    Write-Host "Writing transcript: $($outputPath)$($DirectorySeparatorChar)$($fileNameTranscript)"
    Start-Transcript -Path "$($outputPath)$($DirectorySeparatorChar)$($fileNameTranscript)"
    #endregion setTranscript
}
if ($DoTranscript) {
    setTranscript
}

#region htParameters (all switch params used in foreach-object -parallel)
function addHtParameters {
    Write-Host 'Add AzADServiceprincipalInsights htParameters'
    $script:azAPICallConf['htParameters'] += [ordered]@{
        NoJsonExport = [bool]$NoJsonExport
        NoAzureRoleAssignments = [bool]$NoAzureRoleAssignments
        OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes = [bool]$OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes
        ProductVersion = $ProductVersion
    }
    Write-Host 'htParameters:'
    $azAPICallConf['htParameters'] | Format-Table -AutoSize | Out-String
    Write-Host 'Add AzADServiceprincipalInsights htParameters succeeded' -ForegroundColor Green
}
addHtParameters
#endregion htParameters

#helper file/dir, delimiter, time
#region helper
#delimiter
if ($CsvDelimiter -eq ';') {
    $CsvDelimiterOpposite = ','
}
if ($CsvDelimiter -eq ',') {
    $CsvDelimiterOpposite = ';'
}
#endregion helper

#region Function

#region getClassification
function getClassification {
    param (
        [string]$permission,
        [string]$permissionType
    )
    #Write-Host "getting classification for permission '$permission' ($permissionType)"
    $returnClassification = 'unclassified'
    $isClassified = $false
    foreach ($classification in $getClassifications.permissions.($permissionType).'classifications'.Keys) {
        if (($getClassifications.permissions.($permissionType).'classifications'.($classification).'includes').count -gt 0) {
            $currentPermissionClassification = $classification
            #Write-Host "$classification permissions to check: $(($getClassifications.permissions.($permissionType).'classifications'.($currentPermissionClassification).'includes').count)"

            foreach ($permissionToCheck in $getClassifications.permissions.($permissionType).'classifications'.($currentPermissionClassification).'includes') {
                if ($permissionToCheck.Contains('*')) {
                    if ($permission -like $permissionToCheck) {
                        #Write-Host "TRUE (like) $permissionType permission '$permission' is classified '$currentPermissionClassification'"
                        $isClassified = $true
                        $returnClassification = $classification
                    }
                }
                else {
                    if ($permission -eq $permissionToCheck) {
                        #Write-Host "TRUE (eq) $permissionType permission '$permission' is classified '$currentPermissionClassification'"
                        $isClassified = $true
                        $returnClassification = $classification
                    }
                }
            }

            foreach ($permissionToCheck in $getClassifications.permissions.($permissionType).'classifications'.($currentPermissionClassification).'excludes') {
                if ($permissionToCheck.Contains('*')) {
                    if ($permission -like $permissionToCheck) {
                        #Write-Host "excludes - TRUE (like) $permissionType permission '$permission' is excluded for classification"
                        $isClassified = $false
                        $returnClassification = 'unclassified'
                    }
                }
                else {
                    if ($permission -eq $permissionToCheck) {
                        #Write-Host "excludes - TRUE (eq) $permissionType permission '$permission' is excluded for classification"
                        $isClassified = $false
                        $returnClassification = 'unclassified'
                    }
                }
            }
        }
    }
    # if ($isClassified) {
    #     #$returnClassification = $currentPermissionClassification
    #     Write-Host $returnClassification
    # }

    return $returnClassification
}
$funcGetClassification = $function:getClassification.ToString()
#endregion getClassification

#region resolveObectsById
function resolveObectsById($objects, $targetHt) {

    $counterBatch = [PSCustomObject] @{ Value = 0 }
    $batchSize = 1000
    $ObjectIdsBatch = $objects | Group-Object -Property { [math]::Floor($counterBatch.Value++ / $batchSize) }
    $ObjectIdsBatchCount = ($ObjectIdsBatch | Measure-Object).Count
    $batchCnt = 0

    foreach ($batch in $ObjectIdsBatch) {
        $batchCnt++
        Write-Host " processing Batch #$batchCnt/$($ObjectIdsBatchCount) ($(($batch.Group).Count) ObjectIds)"

        $nonResolvedIdentitiesToCheck = '"{0}"' -f ($batch.Group -join '","')
        #Write-Host "    IdentitiesToCheck: $nonResolvedIdentitiesToCheck"

        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/directoryObjects/getByIds?`$select=userType,id,displayName"
        $method = 'POST'
        $body = @"
        {
            "ids":[$($nonResolvedIdentitiesToCheck)]
        }
"@
        $currentTask = "Resolving Identities - Batch #$batchCnt/$($ObjectIdsBatchCount) ($(($batch.Group).Count) ObjectIds)"
        $resolvedIdentities = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -body $body -currentTask $currentTask

        $t = 0
        foreach ($resolvedIdentity in $resolvedIdentities) {
            $t++
            #Write-Host $t
            $type = 'unforseen type'
            if ($resolvedIdentity.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
                $type = 'Serviceprincipal'
            }
            if ($resolvedIdentity.'@odata.type' -eq '#microsoft.graph.application') {
                $type = 'Application'
            }
            if ($resolvedIdentity.'@odata.type' -eq '#microsoft.graph.group') {
                $type = 'Group'
            }
            if ($resolvedIdentity.'@odata.type' -eq '#microsoft.graph.user') {
                $type = 'User'
            }

            if ($targetHt -eq 'htPrincipalsResolved') {
                if ([string]::IsNullOrEmpty($resolvedIdentity.userType)) {
                    $principalUserType = 'MemberSynced'
                }
                else {
                    $principalUserType = $resolvedIdentity.userType
                }
                $script:htPrincipalsResolved.($resolvedIdentity.id) = @{}
                $script:htPrincipalsResolved.($resolvedIdentity.id).full = "$($type) ($($principalUserType)), DisplayName: $($resolvedIdentity.displayName), Id: $(($resolvedIdentity.id))"
                $script:htPrincipalsResolved.($resolvedIdentity.id).typeOnly = "$($type) ($($principalUserType))"
            }

        }
        $resolvedIdentitiesCount = $resolvedIdentities.Count
        Write-Host "    $resolvedIdentitiesCount identities resolved"
    }
}
#endregion resolveObectsById

#region Function_dataCollection
function dataCollection($mgId) {
    Write-Host ' CustomDataCollection ManagementGroups'
    $startMgLoop = Get-Date

    $allManagementGroupsFromEntitiesChildOfRequestedMg = [System.Collections.ArrayList]@()
    foreach ($managementGroupIdEntry in $mgId) {
        Write-Host " -Getting child ManagementGroups for scope $managementGroupIdEntry"
        $managementGroupsFromEntitiesChildOfRequestedMg = $arrayEntitiesFromAPI.where( { $_.type -eq 'Microsoft.Management/managementGroups' -and ($_.Name -eq $managementGroupIdEntry -or $_.properties.parentNameChain -contains $managementGroupIdEntry) })
        foreach ($managementGroupFromEntitiesChildOfRequestedMg in $managementGroupsFromEntitiesChildOfRequestedMg) {
            $null = $allManagementGroupsFromEntitiesChildOfRequestedMg.Add($managementGroupFromEntitiesChildOfRequestedMg)
        }

    }
    $allManagementGroupsFromEntitiesChildOfRequestedMg = $allManagementGroupsFromEntitiesChildOfRequestedMg | Sort-Object -Property id -Unique
    $script:allManagementGroupsFromEntitiesChildOfRequestedMgCount = ($allManagementGroupsFromEntitiesChildOfRequestedMg).Count

    Write-Host " ManagementGroups ($allManagementGroupsFromEntitiesChildOfRequestedMgCount) to process:' $(($allManagementGroupsFromEntitiesChildOfRequestedMg.name | Sort-Object) -join ', ')"

    $allManagementGroupsFromEntitiesChildOfRequestedMg | ForEach-Object -Parallel {
        $mgdetail = $_
        #region UsingVARs
        #Parameters MG&Sub related
        $CsvDelimiter = $using:CsvDelimiter
        $CsvDelimiterOpposite = $using:CsvDelimiterOpposite
        #AzAPICall
        $azAPICallConf = $using:azAPICallConf
        $scriptPath = $using:ScriptPath
        #Array&HTs
        $customDataCollectionDuration = $using:customDataCollectionDuration
        $htCacheDefinitionsRole = $using:htCacheDefinitionsRole
        $htCacheAssignmentsRole = $using:htCacheAssignmentsRole
        $htCacheAssignmentsPolicy = $using:htCacheAssignmentsPolicy
        $htManagementGroupsMgPath = $using:htManagementGroupsMgPath
        $arrayEntitiesFromAPI = $using:arrayEntitiesFromAPI
        $allManagementGroupsFromEntitiesChildOfRequestedMg = $using:allManagementGroupsFromEntitiesChildOfRequestedMg
        $allManagementGroupsFromEntitiesChildOfRequestedMgCount = $using:allManagementGroupsFromEntitiesChildOfRequestedMgCount
        $arrayDataCollectionProgressMg = $using:arrayDataCollectionProgressMg
        $arrayAPICallTrackingCustomDataCollection = $using:arrayAPICallTrackingCustomDataCollection
        $htRoleAssignmentsFromAPIInheritancePrevention = $using:htRoleAssignmentsFromAPIInheritancePrevention

        if ($azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions) {
            Import-Module ".\$($scriptPath)\AzAPICallModule\AzAPICall\$($azAPICallConf['htParameters'].azAPICallModuleVersion)\AzAPICall.psd1" -Force -ErrorAction Stop
        }
        else {
            Import-Module -Name AzAPICall -RequiredVersion $azAPICallConf['htParameters'].azAPICallModuleVersion -Force -ErrorAction Stop
        }
        #endregion usingVARS

        $MgParentId = ($allManagementGroupsFromEntitiesChildOfRequestedMg.where( { $_.Name -eq $mgdetail.Name })).properties.parent.Id -replace '.*/'
        if ([string]::IsNullOrEmpty($MgParentId)) {
            $MgParentId = 'TenantRoot'
        }
        else {
        }

        $rndom = Get-Random -Minimum 10 -Maximum 750
        Start-Sleep -Millisecond $rndom
        $startMgLoopThis = Get-Date

        #MGPolicyAssignments
        $currentTask = "Policy assignments '$($mgdetail.properties.displayName)' ('$($mgdetail.Name)')"
        $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/providers/Microsoft.Management/managementgroups/$($mgdetail.Name)/providers/Microsoft.Authorization/policyAssignments?`$filter=atscope()&api-version=2021-06-01"
        $method = 'GET'
        $L0mgmtGroupPolicyAssignments = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -caller 'CustomDataCollection'

        foreach ($L0mgmtGroupPolicyAssignment in $L0mgmtGroupPolicyAssignments) {

            if (-not $htCacheAssignmentsPolicy.(($L0mgmtGroupPolicyAssignment.Id).ToLower())) {
                $script:htCacheAssignmentsPolicy.(($L0mgmtGroupPolicyAssignment.Id).ToLower()) = @{}
                $script:htCacheAssignmentsPolicy.(($L0mgmtGroupPolicyAssignment.Id).ToLower()).Assignment = $L0mgmtGroupPolicyAssignment
            }
        }

        #MGCustomRolesRoles
        $currentTask = "Custom Role definitions '$($mgdetail.properties.displayName)' ('$($mgdetail.Name)')"
        $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/providers/Microsoft.Management/managementGroups/$($mgdetail.Name)/providers/Microsoft.Authorization/roleDefinitions?api-version=2015-07-01&`$filter=type%20eq%20'CustomRole'"
        $method = 'GET'
        $mgCustomRoleDefinitions = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -caller 'CustomDataCollection'

        foreach ($mgCustomRoleDefinition in $mgCustomRoleDefinitions) {
            if (-not ($htCacheDefinitionsRole).($mgCustomRoleDefinition.name)) {

                if (
                    (
                        $mgCustomRoleDefinition.properties.permissions.actions -contains 'Microsoft.Authorization/roleassignments/write' -or
                        $mgCustomRoleDefinition.properties.permissions.actions -contains 'Microsoft.Authorization/roleassignments/*' -or
                        $mgCustomRoleDefinition.properties.permissions.actions -contains 'Microsoft.Authorization/*/write' -or
                        $mgCustomRoleDefinition.properties.permissions.actions -contains 'Microsoft.Authorization/*' -or
                        $mgCustomRoleDefinition.properties.permissions.actions -contains '*/write' -or
                        $mgCustomRoleDefinition.properties.permissions.actions -contains '*'
                    ) -and (
                        $mgCustomRoleDefinition.properties.permissions.notActions -notcontains 'Microsoft.Authorization/roleassignments/write' -and
                        $mgCustomRoleDefinition.properties.permissions.notActions -notcontains 'Microsoft.Authorization/roleassignments/*' -and
                        $mgCustomRoleDefinition.properties.permissions.notActions -notcontains 'Microsoft.Authorization/*/write' -and
                        $mgCustomRoleDefinition.properties.permissions.notActions -notcontains 'Microsoft.Authorization/*' -and
                        $mgCustomRoleDefinition.properties.permissions.notActions -notcontains '*/write' -and
                        $mgCustomRoleDefinition.properties.permissions.notActions -notcontains '*'
                    )
                ) {
                    $roleCapable4RoleAssignmentsWrite = $true
                }
                else {
                    $roleCapable4RoleAssignmentsWrite = $false
                }

                ($script:htCacheDefinitionsRole).($mgCustomRoleDefinition.name) = @{}
                ($script:htCacheDefinitionsRole).($mgCustomRoleDefinition.name).definition = $mgCustomRoleDefinition
                ($script:htCacheDefinitionsRole).($mgCustomRoleDefinition.name).roleIsCritical = $roleCapable4RoleAssignmentsWrite
                #$mgCustomRoleDefinition
            }
        }

        #PIM RoleAssignmentScheduleInstances
        $currentTask = "Role assignment schedule instances API MG '$($mgdetail.properties.displayName)' ('$($mgdetail.Name)')"
        $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/providers/Microsoft.Management/managementGroups/$($mgdetail.Name)/providers/Microsoft.Authorization/roleAssignmentScheduleInstances?api-version=2020-10-01"
        $method = 'GET'
        $roleAssignmentScheduleInstancesFromAPI = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -caller 'CustomDataCollection'

        if ($roleAssignmentScheduleInstancesFromAPI -eq 'ResourceNotOnboarded' -or $roleAssignmentScheduleInstancesFromAPI -eq 'TenantNotOnboarded' -or $roleAssignmentScheduleInstancesFromAPI -eq 'InvalidResourceType' -or $roleAssignmentScheduleInstancesFromAPI -eq 'RoleAssignmentScheduleInstancesError') {
            #Write-Host "Scope '$($childMgSubDisplayName)' ('$childMgSubId') not onboarded in PIM"
        }
        else {
            $roleAssignmentScheduleInstances = ($roleAssignmentScheduleInstancesFromAPI.where( { ($_.properties.roleAssignmentScheduleId -replace '.*/') -ne ($_.properties.originRoleAssignmentId -replace '.*/') }))
            $roleAssignmentScheduleInstancesCount = $roleAssignmentScheduleInstances.Count
            if ($roleAssignmentScheduleInstancesCount -gt 0) {
                $htRoleAssignmentsPIM = @{}
                foreach ($roleAssignmentScheduleInstance in $roleAssignmentScheduleInstances) {
                    $htRoleAssignmentsPIM.(($roleAssignmentScheduleInstance.properties.originRoleAssignmentId).tolower()) = $roleAssignmentScheduleInstance.properties
                }
            }
        }

        #RoleAssignment API (system metadata e.g. createdOn)
        $currentTask = "Role assignments API '$($mgdetail.properties.displayName)' ('$($mgdetail.Name)')"
        $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/providers/Microsoft.Management/managementGroups/$($mgdetail.Name)/providers/Microsoft.Authorization/roleAssignments?api-version=2015-07-01"
        $method = 'GET'
        $roleAssignmentsFromAPI = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -caller 'CustomDataCollection'

        if ($roleAssignmentsFromAPI.Count -gt 0) {
            foreach ($roleAssignmentFromAPI in $roleAssignmentsFromAPI) {
                if (-not ($htCacheAssignmentsRole).($roleAssignmentFromAPI.id)) {
                    $splitAssignment = ($roleAssignmentFromAPI.id).Split('/')
                    ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id) = @{}
                    ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignment = $roleAssignmentFromAPI
                    if ($roleAssignmentFromAPI.id -like '/providers/Microsoft.Authorization/roleAssignments/*') {
                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentScope = 'Ten'
                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentScopeId = ''
                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentScopeName = ''
                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentResourceType = 'Tenant'
                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentResourceName = 'Tenant'
                    }
                    else {
                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentScope = 'MG'
                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentScopeId = "/providers/Microsoft.Management/managementGroups/$($splitAssignment[4])"
                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentScopeName = "$($htManagementGroupsMgPath.($splitAssignment[4]).DisplayName)"
                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentResourceType = 'ManagementGroup'
                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentResourceName = $splitAssignment[4]
                    }

                    if ($htRoleAssignmentsPIM.(($roleAssignmentFromAPI.id).tolower())) {
                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentPIMDetails = $htRoleAssignmentsPIM.(($roleAssignmentFromAPI.id).tolower())
                    }

                    ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).roleIsCritical = ($htCacheDefinitionsRole).($roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/').roleIsCritical
                    ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).roleName = ($htCacheDefinitionsRole).($roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/').definition.properties.roleName
                    ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).roleId = $roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'
                    ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).type = ($htCacheDefinitionsRole).($roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/').definition.properties.type
                }
                if (-not $htRoleAssignmentsFromAPIInheritancePrevention.($roleAssignmentFromAPI.id -replace '.*/')) {
                    $htRoleAssignmentsFromAPIInheritancePrevention.($roleAssignmentFromAPI.id -replace '.*/') = @{}
                }
            }
        }

        $endMgLoopThis = Get-Date
        $null = $script:customDataCollectionDuration.Add([PSCustomObject]@{
                Type = 'Mg'
                Id = $mgdetail.Name
                DurationSec = (New-TimeSpan -Start $startMgLoopThis -End $endMgLoopThis).TotalSeconds
            })

        $null = $script:arrayDataCollectionProgressMg.Add($mgdetail.Name)
        $progressCount = ($arrayDataCollectionProgressMg).Count
        Write-Host "  $($progressCount)/$($allManagementGroupsFromEntitiesChildOfRequestedMgCount) ManagementGroups processed"

    } -ThrottleLimit $ThrottleLimitARM
    #[System.GC]::Collect()

    $endMgLoop = Get-Date
    Write-Host " CustomDataCollection ManagementGroups processing duration: $((New-TimeSpan -Start $startMgLoop -End $endMgLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startMgLoop -End $endMgLoop).TotalSeconds) seconds)"


    #SUBSCRIPTION

    Write-Host ' CustomDataCollection Subscriptions'
    $subsExcludedStateCount = ($outOfScopeSubscriptions | Where-Object { $_.outOfScopeReason -like 'State*' } | Measure-Object).Count
    $subsExcludedWhitelistCount = ($outOfScopeSubscriptions | Where-Object { $_.outOfScopeReason -like 'QuotaId*' } | Measure-Object).Count
    if ($subsExcludedStateCount -gt 0) {
        Write-Host "  CustomDataCollection $($subsExcludedStateCount) Subscriptions excluded (State != enabled)"
    }
    if ($subsExcludedWhitelistCount -gt 0) {
        Write-Host "  CustomDataCollection $($subsExcludedWhitelistCount) Subscriptions excluded (not in quotaId whitelist: '$($SubscriptionQuotaIdWhitelist -join ', ')' OR is AAD_ quotaId)"
    }
    Write-Host " CustomDataCollection Subscriptions will process $subsToProcessInCustomDataCollectionCount of $childrenSubscriptionsCount"

    $startSubLoop = Get-Date
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
            $startBatch = Get-Date
            $batchCnt++
            Write-Host " processing Batch #$batchCnt/$(($subscriptionsBatch | Measure-Object).Count) ($(($batch.Group | Measure-Object).Count) Subscriptions)"

            $batch.Group | ForEach-Object -Parallel {
                $startSubLoopThis = Get-Date
                $childMgSubDetail = $_
                #region UsingVARs
                #Parameters MG&Sub related
                $CsvDelimiter = $using:CsvDelimiter
                $CsvDelimiterOpposite = $using:CsvDelimiterOpposite
                #Parameters Sub related
                #AzAPICall
                $azAPICallConf = $using:azAPICallConf
                $scriptPath = $using:ScriptPath
                #Array&HTs
                $customDataCollectionDuration = $using:customDataCollectionDuration
                $htSubscriptionsMgPath = $using:htSubscriptionsMgPath
                $htManagementGroupsMgPath = $using:htManagementGroupsMgPath
                $htCacheDefinitionsRole = $using:htCacheDefinitionsRole
                $htCacheAssignmentsRole = $using:htCacheAssignmentsRole
                $htCacheAssignmentsPolicy = $using:htCacheAssignmentsPolicy
                $childrenSubscriptionsCount = $using:childrenSubscriptionsCount
                $subsToProcessInCustomDataCollectionCount = $using:subsToProcessInCustomDataCollectionCount
                $arrayDataCollectionProgressSub = $using:arrayDataCollectionProgressSub
                $htAllSubscriptionsFromAPI = $using:htAllSubscriptionsFromAPI
                $arrayEntitiesFromAPI = $using:arrayEntitiesFromAPI
                $arrayAPICallTrackingCustomDataCollection = $using:arrayAPICallTrackingCustomDataCollection
                $htRoleAssignmentsFromAPIInheritancePrevention = $using:htRoleAssignmentsFromAPIInheritancePrevention

                if ($azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions) {
                    Import-Module ".\$($scriptPath)\AzAPICallModule\AzAPICall\$($azAPICallConf['htParameters'].azAPICallModuleVersion)\AzAPICall.psd1" -Force -ErrorAction Stop
                }
                else {
                    Import-Module -Name AzAPICall -RequiredVersion $azAPICallConf['htParameters'].azAPICallModuleVersion -Force -ErrorAction Stop
                }
                #endregion UsingVARs

                $childMgSubId = $childMgSubDetail.subscriptionId
                $childMgSubDisplayName = $childMgSubDetail.subscriptionName

                $rndom = Get-Random -Minimum 10 -Maximum 750
                Start-Sleep -Millisecond $rndom

                #SubscriptionPolicyAssignments
                $currentTask = "Policy assignments '$($childMgSubDisplayName)' ('$childMgSubId')"
                $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/subscriptions/$($childMgSubId)/providers/Microsoft.Authorization/policyAssignments?api-version=2021-06-01"
                $method = 'GET'
                $L1mgmtGroupSubPolicyAssignments = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -caller 'CustomDataCollection'

                foreach ($L1mgmtGroupSubPolicyAssignment in $L1mgmtGroupSubPolicyAssignments) {

                    if (-not $htCacheAssignmentsPolicy.(($L1mgmtGroupSubPolicyAssignment.Id).ToLower())) {
                        $script:htCacheAssignmentsPolicy.(($L1mgmtGroupSubPolicyAssignment.Id).ToLower()) = @{}
                        $script:htCacheAssignmentsPolicy.(($L1mgmtGroupSubPolicyAssignment.Id).ToLower()).Assignment = $L1mgmtGroupSubPolicyAssignment
                    }
                }

                #SubscriptionRoles
                $currentTask = "Custom Role definitions '$($childMgSubDisplayName)' ('$childMgSubId')"
                $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/subscriptions/$childMgSubId/providers/Microsoft.Authorization/roleDefinitions?api-version=2015-07-01&`$filter=type%20eq%20'CustomRole'"
                $method = 'GET'
                $subCustomRoleDefinitions = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -caller 'CustomDataCollection'

                foreach ($subCustomRoleDefinition in $subCustomRoleDefinitions) {
                    if (-not ($htCacheDefinitionsRole).($subCustomRoleDefinition.name)) {

                        if (
                            (
                                $subCustomRoleDefinition.properties.permissions.actions -contains 'Microsoft.Authorization/roleassignments/write' -or
                                $subCustomRoleDefinition.properties.permissions.actions -contains 'Microsoft.Authorization/roleassignments/*' -or
                                $subCustomRoleDefinition.properties.permissions.actions -contains 'Microsoft.Authorization/*/write' -or
                                $subCustomRoleDefinition.properties.permissions.actions -contains 'Microsoft.Authorization/*' -or
                                $subCustomRoleDefinition.properties.permissions.actions -contains '*/write' -or
                                $subCustomRoleDefinition.properties.permissions.actions -contains '*'
                            ) -and (
                                $subCustomRoleDefinition.properties.permissions.notActions -notcontains 'Microsoft.Authorization/roleassignments/write' -and
                                $subCustomRoleDefinition.properties.permissions.notActions -notcontains 'Microsoft.Authorization/roleassignments/*' -and
                                $subCustomRoleDefinition.properties.permissions.notActions -notcontains 'Microsoft.Authorization/*/write' -and
                                $subCustomRoleDefinition.properties.permissions.notActions -notcontains 'Microsoft.Authorization/*' -and
                                $subCustomRoleDefinition.properties.permissions.notActions -notcontains '*/write' -and
                                $subCustomRoleDefinition.properties.permissions.notActions -notcontains '*'
                            )
                        ) {
                            $roleCapable4RoleAssignmentsWrite = $true
                        }
                        else {
                            $roleCapable4RoleAssignmentsWrite = $false
                        }

                        ($script:htCacheDefinitionsRole).($subCustomRoleDefinition.name) = @{}
                        ($script:htCacheDefinitionsRole).($subCustomRoleDefinition.name).definition = $subCustomRoleDefinition
                        ($script:htCacheDefinitionsRole).($subCustomRoleDefinition.name).roleIsCritical = $roleCapable4RoleAssignmentsWrite
                    }
                }

                #PIM RoleAssignmentScheduleInstances
                $currentTask = "Role assignment schedule instances API Sub '$($childMgSubDisplayName)' ('$childMgSubId')"
                $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/subscriptions/$childMgSubId/providers/Microsoft.Authorization/roleAssignmentScheduleInstances?api-version=2020-10-01"
                $method = 'GET'
                $roleAssignmentScheduleInstancesFromAPI = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -caller 'CustomDataCollection'

                if ($roleAssignmentScheduleInstancesFromAPI -eq 'ResourceNotOnboarded' -or $roleAssignmentScheduleInstancesFromAPI -eq 'TenantNotOnboarded' -or $roleAssignmentScheduleInstancesFromAPI -eq 'InvalidResourceType' -or $roleAssignmentScheduleInstancesFromAPI -eq 'RoleAssignmentScheduleInstancesError') {
                    #Write-Host "Scope '$($childMgSubDisplayName)' ('$childMgSubId') not onboarded in PIM"
                }
                else {
                    $roleAssignmentScheduleInstances = ($roleAssignmentScheduleInstancesFromAPI.where( { ($_.properties.roleAssignmentScheduleId -replace '.*/') -ne ($_.properties.originRoleAssignmentId -replace '.*/') }))
                    $roleAssignmentScheduleInstancesCount = $roleAssignmentScheduleInstances.Count
                    if ($roleAssignmentScheduleInstancesCount -gt 0) {
                        $htRoleAssignmentsPIM = @{}
                        foreach ($roleAssignmentScheduleInstance in $roleAssignmentScheduleInstances) {
                            $htRoleAssignmentsPIM.(($roleAssignmentScheduleInstance.properties.originRoleAssignmentId).tolower()) = $roleAssignmentScheduleInstance.properties
                        }
                    }
                }

                #SubscriptionRoleAssignments
                #RoleAssignment API (system metadata e.g. createdOn)
                $currentTask = "Role assignments API '$($childMgSubDisplayName)' ('$childMgSubId')"
                $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/subscriptions/$childMgSubId/providers/Microsoft.Authorization/roleAssignments?api-version=2015-07-01"
                $method = 'GET'
                $roleAssignmentsFromAPI = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -caller 'CustomDataCollection'

                if ($roleAssignmentsFromAPI.Count -gt 0) {
                    foreach ($roleAssignmentFromAPI in $roleAssignmentsFromAPI) {
                        if (-not $htRoleAssignmentsFromAPIInheritancePrevention.($roleAssignmentFromAPI.id -replace '.*/')) {
                            if (-not ($htCacheAssignmentsRole).($roleAssignmentFromAPI.id)) {
                                $splitAssignment = ($roleAssignmentFromAPI.id).Split('/')
                                ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id) = @{}
                                ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignment = $roleAssignmentFromAPI

                                if ($roleAssignmentFromAPI.properties.scope -like '/subscriptions/*/resourcegroups/*') {
                                    if ($roleAssignmentFromAPI.properties.scope -like '/subscriptions/*/resourcegroups/*' -and $roleAssignmentFromAPI.properties.scope -notlike '/subscriptions/*/resourcegroups/*/providers*') {
                                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentScope = 'RG'
                                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentScopeId = "$($splitAssignment[2])/$($splitAssignment[4])"
                                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentScopeName = "$($htSubscriptionsMgPath.($splitAssignment[2]).DisplayName) ($($splitAssignment[2]))/$($splitAssignment[4])"
                                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentResourceType = 'ResourceGroup'
                                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentResourceName = $splitAssignment[4]
                                    }
                                    if ($roleAssignmentFromAPI.properties.scope -like '/subscriptions/*/resourcegroups/*/providers*') {
                                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentScope = 'Res'
                                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentScopeId = "$($splitAssignment[2])/$($splitAssignment[4])/$($splitAssignment[8])"
                                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentScopeName = "$($htSubscriptionsMgPath.($splitAssignment[2]).DisplayName) ($($splitAssignment[2]))/$($splitAssignment[4])/$($splitAssignment[6])/$($splitAssignment[7])/$($splitAssignment[8])"
                                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentResourceType = 'Resource'
                                        ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentResourceName = "$($splitAssignment[6])/$($splitAssignment[7])/$($splitAssignment[8])"
                                    }
                                }
                                else {
                                    $hlperSubName = $htSubscriptionsMgPath.($splitAssignment[2]).DisplayName
                                    ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentScope = 'Sub'
                                    ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentScopeId = "/subscriptions/$($splitAssignment[2])"
                                    ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentScopeName = $hlperSubName
                                    ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentResourceType = 'Subscription'
                                    ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentResourceName = $hlperSubName
                                }

                                if ($htRoleAssignmentsPIM.(($roleAssignmentFromAPI.id).tolower())) {
                                    ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).assignmentPIMDetails = $htRoleAssignmentsPIM.(($roleAssignmentFromAPI.id).tolower())
                                }

                                ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).roleIsCritical = ($htCacheDefinitionsRole).($roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/').roleIsCritical
                                ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).roleName = ($htCacheDefinitionsRole).($roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/').definition.properties.roleName
                                ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).roleId = $roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'
                                ($script:htCacheAssignmentsRole).($roleAssignmentFromAPI.id).type = ($htCacheDefinitionsRole).($roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/').definition.properties.type
                            }
                        }
                    }
                }

                $endSubLoopThis = Get-Date
                $null = $script:customDataCollectionDuration.Add([PSCustomObject]@{
                        Type = 'SUB'
                        Id = $childMgSubId
                        DurationSec = (New-TimeSpan -Start $startSubLoopThis -End $endSubLoopThis).TotalSeconds
                    })

                $null = $script:arrayDataCollectionProgressSub.Add($childMgSubId)
                $progressCount = ($arrayDataCollectionProgressSub).Count
                Write-Host "  $($progressCount)/$($subsToProcessInCustomDataCollectionCount) Subscriptions processed"

            } -ThrottleLimit $ThrottleLimitARM

            $endBatch = Get-Date
            Write-Host " Batch #$batchCnt processing duration: $((New-TimeSpan -Start $startBatch -End $endBatch).TotalMinutes) minutes ($((New-TimeSpan -Start $startBatch -End $endBatch).TotalSeconds) seconds)"
        }
        #[System.GC]::Collect()

        $endSubLoop = Get-Date
        Write-Host " CustomDataCollection Subscriptions processing duration: $((New-TimeSpan -Start $startSubLoop -End $endSubLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startSubLoop -End $endSubLoop).TotalSeconds) seconds)"
    }
}

#endregion Function_dataCollection

#HTML

#rsu
#region TenantSummary
function summary() {
    Write-Host ' Building Summary'

    $htmlTenantSummary = [System.Text.StringBuilder]::new()


    #region SUMMARYServicePrincipals
    [void]$htmlTenantSummary.AppendLine(@'
    <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textServicePrincipal" data-content="&nbsp;Service Principals" /></button>
    <div class="content TenantSummaryContent">
'@)

    if ($cu.Count -gt 0) {
        $startCustPolLoop = Get-Date
        Write-Host '  processing Summary ServicePrincipals'

        $tfCount = $cu.Count
        $htmlTableId = 'TenantSummary_ServicePrincipals'
        $tf = "tf$($htmlTableId)"

        $categoryColorsMax = @('rgb(1,0,103)', 'rgb(213,255,0)', 'rgb(255,0,86)', 'rgb(158,0,142)', 'rgb(14,76,161)', 'rgb(255,229,2)', 'rgb(0,95,57)', 'rgb(0,255,0)', 'rgb(149,0,58)', 'rgb(255,147,126)', 'rgb(164,36,0)', 'rgb(0,21,68)', 'rgb(145,208,203)', 'rgb(98,14,0)', 'rgb(107,104,130)', 'rgb(0,0,255)', 'rgb(0,125,181)', 'rgb(106,130,108)', 'rgb(0,0,0)', 'rgb(0,174,126)', 'rgb(194,140,159)', 'rgb(190,153,112)', 'rgb(0,143,156)', 'rgb(95,173,78)', 'rgb(255,0,0)', 'rgb(255,0,246)', 'rgb(255,2,157)', 'rgb(104,61,59)', 'rgb(255,116,163)', 'rgb(150,138,232)', 'rgb(152,255,82)', 'rgb(167,87,64)', 'rgb(1,255,254)', 'rgb(255,238,232)', 'rgb(254,137,0)', 'rgb(189,198,255)', 'rgb(1,208,255)', 'rgb(187,136,0)', 'rgb(117,68,177)', 'rgb(165,255,210)', 'rgb(255,166,254)', 'rgb(119,77,0)', 'rgb(122,71,130)', 'rgb(38,52,0)', 'rgb(0,71,84)', 'rgb(67,0,44)', 'rgb(181,0,255)', 'rgb(255,177,103)', 'rgb(255,219,102)', 'rgb(144,251,146)', 'rgb(126,45,210)', 'rgb(189,211,147)', 'rgb(229,111,254)', 'rgb(222,255,116)', 'rgb(0,255,120)', 'rgb(0,155,255)', 'rgb(0,100,1)', 'rgb(0,118,255)', 'rgb(133,169,0)', 'rgb(0,185,23)', 'rgb(120,130,49)', 'rgb(0,255,198)', 'rgb(255,110,65)', 'rgb(232,94,190)')

        $groupedByOrg = $cu.SP.where( { $_.SPAppOwnerOrganizationId } ) | Group-Object -Property SPAppOwnerOrganizationId

        $arrOrgCounts = @()
        $arrOrgIds = @()
        foreach ($grp in $groupedByOrg | Sort-Object -Property count -Descending) {
            $arrOrgCounts += $grp.Count
            $arrOrgIds += $grp.Name
        }
        $OrgCounts = "'{0}'" -f ($arrOrgCounts -join "','")
        $OrgIds = "'{0}'" -f ($arrOrgIds -join "','")

        $categoryColorsOrg = ($categoryColorsMax[0..(($arrOrgIds).Count - 1)])
        $categoryColorsSeperatedOrg = "'{0}'" -f ($categoryColorsOrg -join "','")

        $groupedBySPType = $cu.ObjectType | Group-Object

        $arrSPTypeCounts = @()
        $arrSPTypes = @()
        foreach ($grp in $groupedBySPType | Sort-Object -Property count -Descending) {
            $arrSPTypeCounts += $grp.Count
            $arrSPTypes += $grp.Name
        }
        $SPTypeCounts = "'{0}'" -f ($arrSPTypeCounts -join "','")
        $SPTypes = "'{0}'" -f ($arrSPTypes -join "','")

        $categoryColorsSPType = ($categoryColorsMax[($arrOrgIds.Count)..(($arrSPTypes).Count + ($arrOrgIds.Count) - 1)])
        $categoryColorsSeperatedSPType = "'{0}'" -f ($categoryColorsSPType -join "','")

        $groupedByMIResourceType = $cu.where( { $_.ObjectType -like 'SP MI*' } ).ManagedIdentity.resourceType | Group-Object

        $arrMIResTypeCounts = @()
        $arrMIResTypes = @()
        foreach ($grp in $groupedByMIResourceType | Sort-Object -Property count -Descending) {
            $arrMIResTypeCounts += $grp.Count
            $arrMIResTypes += $grp.Name -replace 'Microsoft.'
        }
        $MIResTypeCounts = "'{0}'" -f ($arrMIResTypeCounts -join "','")
        $MIResTypes = "'{0}'" -f ($arrMIResTypes -join "','")

        $categoryColorsMIResType = ($categoryColorsMax[($arrOrgIds.Count + $arrMIResTypes.Count)..(($arrSPTypes).Count + ($arrOrgIds.Count) + ($arrMIResTypes.Count) - 1)])
        $categoryColorsSeperatedMIResType = "'{0}'" -f ($categoryColorsMIResType -join "','")

        $SPAppINT = $cu.where( { $_.ObjectType -eq 'SP APP INT' } )

        #notes
        $SPAppINTSPAppEXTSPEXT = $cu.where( { $_.ObjectType -eq 'SP APP INT' -or $_.ObjectType -eq 'SP APP EXT' -or $_.ObjectType -eq 'SP EXT' } )
        $SPAppINTSPAppEXTSPEXTCount = $SPAppINTSPAppEXTSPEXT.Count
        $notesSetSP = $SPAppINTSPAppEXTSPEXT.SP.where( { -not [string]::IsNullOrWhiteSpace($_.SPNotes) } )
        $notesNotSetSP = $SPAppINTSPAppEXTSPEXT.SP.where( { [string]::IsNullOrWhiteSpace($_.SPNotes) } )

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
                    <span>AppOwner OrgIds count: <b>$($arrOrgCounts.Count)</b></span>
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
                <div class="chartDiv">
                    <span>SP [APP INT, APP EXT, EXT] ($($SPAppINTSPAppEXTSPEXTCount)) - Notes</span>
                <canvas id="myChart5" style="height:150px; width: 250px"></canvas>
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
                            window. targetcolumn = '5'
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
                            window. targetcolumn = '6'
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
                            window. targetcolumn = '13'
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
                        window. extratargetcolumn = '6'
                        if (window. datasetitem == 0){
                            window. targetcolumn = '4'
                        }
                        if (window. datasetitem == 1){
                            window. targetcolumn = '10'
                        }
                        $($tf).clearFilters();
                        $($tf).setFilterValue((window. extratargetcolumn), (window.extratarget));
                        $($tf).setFilterValue((window. targetcolumn), (window.target));
                        $($tf).filter();

                    }
                }
});

var ctx = document.getElementById('myChart5');
var myChart = new Chart(ctx, {
    type: 'pie',
                data: {
                    datasets: [
                        {
                            data: ['$($notesSetSP.Count)', '$($notesNotSetSP.Count)'],
                            backgroundColor: ['rgb(85,194,55)', 'rgb(173,173,173)'],
                            labels: ['!notesN/A && !notesNotSet', 'notesNotSet'],
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
                            window. targetcolumn = '3'
                        }
                        $($tf).clearFilters();
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
<th>SP notes</th>
<th>SP Owners</th>
<th>SP App Owner Organization Id</th>
<th>Type</th>
<th>App object Id</th>
<th>App application (client) Id</th>
<th>App displayName</th>
<th>App Owners</th>
<th>AppReg</th>
<th>App SignIn Audience
<th>MI Resource type</th>
<th>MI Resource scope</th>
<th>MI Relict
</tr>
</thead>
<tbody>
"@)

        foreach ($sp in ($cu)) {

            $spType = $sp.ObjectType

            if ($spType -eq 'SP APP INT' -or $spType -eq 'SP APP EXT' -or $spType -eq 'SP EXT') {
                if ([string]::IsNullOrWhiteSpace($sp.SP.SPNotes)) {
                    $spNotes = 'notesNotSet'
                }
                else {
                    $spNotes = $sp.SP.SPNotes
                }
            }
            else {
                $spNotes = 'notesN/A'
            }

            $appObjectId = ''
            $appId = ''
            $appDisplayName = ''
            if ($sp.APP) {
                $appObjectId = $sp.APP.APPObjectId
                $appId = $sp.APP.APPAppClientId
                $appDisplayName = $sp.APP.APPDisplayName
            }

            $miResourceType = ''
            $miResourceScope = ''
            $miRelict = ''
            if ($sp.ManagedIdentity) {
                $miResourceType = $sp.ManagedIdentity.resourceType
                $miResourceScope = $sp.ManagedIdentity.resourceScope
                $miRelict = $sp.ManagedIdentity.relict
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

            $appSignInAudience = $null
            if (($sp.APP.APPSignInAudience)) {
                $appSignInAudience = $sp.APP.APPSignInAudience
            }

            [void]$htmlTenantSummary.AppendLine(@"
<tr>
<td>$($sp.SP.SPObjectId)</td>
<td>$($sp.SP.SPAppId)</td>
<td class="breakwordall">$($sp.SP.SPDisplayName)</td>
<td class="breakwordall">$($spNotes)</td>
<td class="breakwordall">$($spOwners)</td>
<td>$($sp.SP.SPAppOwnerOrganizationId)</td>
<td>$($spType)</td>
<td>$($appObjectId)</td>
<td>$($appId)</td>
<td class="breakwordall">$($appDisplayName)</td>
<td class="breakwordall">$($appOwners)</td>
<td>$($hasApp)</td>
<td class="breakwordall">$($appSignInAudience)</td>
<td class="breakwordall">$($miResourceType)</th>
<td class="breakwordall">$($miResourceScope)</th>
<td>$($miRelict)</th>
</tr>
"@)
        }

        [void]$htmlTenantSummary.AppendLine(@"
            </tbody>
        </table>

    <script>
        var tfConfig4$htmlTableId = {
            base_path: 'https://www.azadvertizer.net/azadserviceprincipalinsights/tablefilter/', rows_counter: true,
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
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true, linked_filters: true,
col_widths: ['6%', '6%', '7%', '8%', '8%', '6%', '7%', '6%', '6%', '8%', '8%', '5%', '6%', '4%', '6%', '4%'],
            col_5: 'select',
            col_6: 'multiple',
            col_11: 'select',
            col_12: 'select',
            col_15: 'select',
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
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring'
            ],
            watermark: ['', '', '', 'try: !notesN/A && !notesNotSet', '', '', '', '', '', '', '', '', '', '', '', ''],
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

    [void]$htmlTenantSummary.AppendLine(@'
    </div>
'@)


    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipals

    #region SUMMARYServicePrincipalOwners

    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary ServicePrincipal Owners'

    if ($cu.SPOwners.Count -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@'
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup" data-content="&nbsp;Service Principal Owners" /></button>
        <div class="content TenantSummaryContent">
'@)

        $tfCount = $cu.SPOwners.Count
        $htmlTableId = 'TenantSummary_ServicePrincipalOwners'
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

            $spType = $sp.ObjectType
            $ownerOwnedBy = ''
            foreach ($ownerinfo in $sp.SPOwners) {
                $hlpArrayDirect = @()
                $hlpArrayInDirect = @()
                $ownerDisplayName = "$($ownerinfo.displayName)"
                $ownerPrincipalType = "$($ownerinfo.principalType)"
                $ownerId = "$($ownerinfo.id)"
                $ownerApplicability = $($ownerinfo.applicability)

                if ($ownerPrincipalType -like 'SP*') {
                    $ownedBy = ($htSPOwnersFinal.($ownerinfo.id))
                    $ownedByCount = $ownedBy.Count
                    if ($ownedByCount -gt 0) {
                        foreach ($owned in $ownedBy) {
                            if ($owned.applicability -eq 'direct') {
                                $hlpArrayDirect += "$($owned.displayName) $($owned.principalType)"
                            }
                            if ($owned.applicability -eq 'indirect') {
                                $hlpArrayInDirect += "$($owned.displayName) $($owned.principalType)"
                            }
                        }
                        if ($hlpArrayDirect.Count -gt 0 -and $hlpArrayInDirect.Count -gt 0) {
                            $ownerOwnedBy = "direct $($hlpArrayDirect.Count) [$($hlpArrayDirect -Join ', ')]<br> indirect $($hlpArrayInDirect.Count) [$($hlpArrayInDirect -Join ', ')]"
                        }
                        else {
                            if ($hlpArrayDirect.Count -gt 0) {
                                $ownerOwnedBy = "direct $($hlpArrayDirect.Count) [$($hlpArrayDirect -Join ', ')]"
                            }
                        }
                    }
                    else {
                        $ownerOwnedBy = ''
                    }
                }
                else {
                    $ownerOwnedBy = ''
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
            base_path: 'https://www.azadvertizer.net/azadserviceprincipalinsights/tablefilter/', rows_counter: true,
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
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true, linked_filters: true,
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

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@'
    <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup fontGrey" data-content="&nbsp;Service Principal Owners" /></button>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalOwners

    #region SUMMARYApplicationOwners
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary Application Owners'

    if ($cu.APPAppOwners.Count -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@'
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup" data-content="&nbsp;Application Owners" /></button>
        <div class="content TenantSummaryContent">
'@)

        $tfCount = $cu.APPAppOwners.Count
        $htmlTableId = 'TenantSummary_ApplicationOwners'
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

            $spType = $sp.ObjectType

            $ownerOwnedBy = ''
            foreach ($ownerinfo in $sp.APPAppOwners) {
                $hlpArrayDirect = @()
                $hlpArrayInDirect = @()
                $ownerDisplayName = "$($ownerinfo.displayName)"
                $ownerPrincipalType = "$($ownerinfo.principalType)"
                $ownerId = "$($ownerinfo.id)"
                $ownerApplicability = $($ownerinfo.applicability)

                if ($ownerPrincipalType -like 'SP*') {
                    $ownedBy = ($htSPOwnersFinal.($ownerinfo.id))
                    $ownedByCount = $ownedBy.Count
                    if ($ownedByCount -gt 0) {
                        foreach ($owned in $ownedBy) {
                            if ($owned.applicability -eq 'direct') {
                                $hlpArrayDirect += "$($owned.displayName) $($owned.principalType)"
                            }
                            if ($owned.applicability -eq 'indirect') {
                                $hlpArrayInDirect += "$($owned.displayName) $($owned.principalType)"
                            }
                        }
                        if ($hlpArrayDirect.Count -gt 0 -and $hlpArrayInDirect.Count -gt 0) {
                            $ownerOwnedBy = "direct $($hlpArrayDirect.Count) [$($hlpArrayDirect -Join ', ')]<br> indirect $($hlpArrayInDirect.Count) [$($hlpArrayInDirect -Join ', ')]"
                        }
                        else {
                            if ($hlpArrayDirect.Count -gt 0) {
                                $ownerOwnedBy = "direct $($hlpArrayDirect.Count) [$($hlpArrayDirect -Join ', ')]"
                            }
                        }
                    }
                    else {
                        $ownerOwnedBy = ''
                    }
                }
                else {
                    $ownerOwnedBy = ''
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
            base_path: 'https://www.azadvertizer.net/azadserviceprincipalinsights/tablefilter/', rows_counter: true,
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
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true, linked_filters: true,
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

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@'
        <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup fontGrey" data-content="&nbsp;Application Owners" /></button>
'@)
    }


    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYApplicationOwners

    #region SUMMARYServicePrincipalOwnedObjects
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary ServicePrincipal Owned Objects'

    if ($cu.SPOwnedObjects.Count -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@'
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup" data-content="&nbsp;Service Principal Owned Objects" /></button>
        <div class="content TenantSummaryContent">
'@)

        $tfCount = $cu.SPOwnedObjects.Count
        $htmlTableId = 'TenantSummary_ServicePrincipalOwnedObjects'
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

            $spType = $sp.ObjectType
            $arrayOwnedObjects = @()
            foreach ($ownedObject in $sp.SPOwnedObjects | Sort-Object -Property type, typeDetailed, displayName) {
                $arrayOwnedObjects += "$($ownedObject.displayName) <b>$($ownedObject.type)</b> $($ownedObject.objectId)"
            }

            [void]$htmlTenantSummary.AppendLine(@"
<tr>
<td>$($sp.SP.SPObjectId)</td>
<td>$($sp.SP.SPAppId)</td>
<td class="breakwordall">$($sp.SP.SPDisplayName)</td>
<td>$($sp.SP.SPAppOwnerOrganizationId)</td>
<td>$($spType)</td>
<td>$($arrayOwnedObjects.Count) ($($arrayOwnedObjects -join ', '))</td>
</tr>
"@)

        }

        [void]$htmlTenantSummary.AppendLine(@"
            </tbody>
        </table>

    <script>
        var tfConfig4$htmlTableId = {
            base_path: 'https://www.azadvertizer.net/azadserviceprincipalinsights/tablefilter/', rows_counter: true,
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
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true, linked_filters: true,
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

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@'
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup fontGrey" data-content="&nbsp;Service Principal Owned Objects" /></button>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalOwnedObjects

    #region SUMMARYServicePrincipalsAADRoleAssignments
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary ServicePrincipalsAADRoleAssignments'
    $servicePrincipalsAADRoleAssignments = $cu.where( { $_.SPAADRoleAssignments.Count -ne 0 } )
    $servicePrincipalsAADRoleAssignmentsCount = $servicePrincipalsAADRoleAssignments.Count
    if ($servicePrincipalsAADRoleAssignmentsCount -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@'
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textAADRoleAssignment" data-content="&nbsp;Service Principal AAD RoleAssignments" /></button>
        <div class="content TenantSummaryContent">
'@)

        $tfCount = $servicePrincipalsAADRoleAssignmentsCount
        $htmlTableId = 'TenantSummary_ServicePrincipalsAADRoleAssignments'
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
<th>Classification</th>
<th>#</th>
<th>SP AAD RoleAssignments</th>
</tr>
</thead>
<tbody>
"@)

        $cntRow = 0
        $arrayServicePrincipalsAADRoleAssignments4CSV = [System.Collections.ArrayList]@()
        foreach ($sp in ($servicePrincipalsAADRoleAssignments)) {
            $cntRow++
            $cnt = 0

            $spType = $sp.ObjectType

            $spAADRoleAssignments = $null
            if (($sp.SPAADRoleAssignments)) {
                if (($sp.SPAADRoleAssignments.count -gt 0)) {
                    $array = @()
                    $cnt = 0
                    $roleClassification = ''
                    foreach ($ra in $sp.SPAADRoleAssignments) {
                        $cnt++

                        if ($cntRow % 2 -eq 0) {
                            if ($cnt % 2 -eq 0) {
                                $class = 'class="odd"'
                            }
                            else {
                                $class = 'class="even"'
                            }
                        }
                        else {
                            if ($cnt % 2 -eq 0) {
                                $class = 'class="even"'
                            }
                            else {
                                $class = 'class="odd"'
                            }
                        }

                        $raRoleDefinitionName = $ra.roleDefinitionName

                        if ($ra.roleType -eq 'BuiltIn') {
                            $raRoleDefinitionName = "<a class=`"externallink`" href=`"https://github.com/MicrosoftDocs/azure-docs/blob/main/articles/active-directory/roles/permissions-reference.md#$($ra.roleDefinitionName -replace ' ', '-')`" target=`"_blank`">$($ra.roleDefinitionName)</a>"
                        }

                        $faIcon = ''
                        if ($ra.roleIsCritical -eq $true) {
                            $roleClassification = 'critical'
                            $faIcon = '<i class="fa fa-exclamation-triangle" aria-hidden="true" style="color: #ff5e00; font-size: 9px;"></i> '
                        }

                        if ($ra.scopeDetail) {
                            $array += "$faIcon<span $class><b>$($ra.roleType)</b> '$($raRoleDefinitionName)' $($ra.roleDefinitionId) (scope: $($ra.scopeDetail))</span>"
                        }
                        else {
                            $array += "$faIcon<span $class><b>$($ra.roleType)</b> '$($raRoleDefinitionName)' $($ra.roleDefinitionId)</span>"
                        }

                        $null = $arrayServicePrincipalsAADRoleAssignments4CSV.Add([PSCustomObject]@{
                                SPObjectType = $sp.ObjectType
                                SPObjectId = $sp.ObjectId
                                SPDisplayName = $sp.SP.SPDisplayName
                                APPObjectId = $sp.APP.APPObjectId
                                APPAppClientId = $sp.APP.APPAppClientId
                                APPDisplayName = $sp.APP.APPDisplayName
                                MIResourceType = $sp.ManagedIdentity.resourceType
                                MIResource = $sp.ManagedIdentity.alternativeName
                                RoleAssignmentId = $ra.id
                                RoleDefinitionId = $ra.roleDefinitionId
                                RoleDefinitionName = $ra.roleDefinitionName
                                RoleDefinitionDescription = $ra.roleDefinitionDescription
                                RoleType = $ra.roleType
                                DirectoryScopeId = $ra.directoryScopeId
                                ResourceScope = $ra.resourceScope
                                ScopeDetail = $ra.scopeDetail
                                Classification = $roleClassification
                            })
                    }
                    $spAADRoleAssignments = "$($array -join '<br>')"

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
<td>$($roleClassification)</td>
<td>$(($sp.SPAADRoleAssignments).Count)</td>
<td class="breakwordall">$($spAADRoleAssignments)</td>
</tr>
"@)
        }

        if ($azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions -eq $true) {
            $fileName = "$($Product)_$($fileNameMGRef)_AADRoleAssignments_"
        }
        else {
            $fileName = "$($Product)_$($ProductVersion)_$($fileTimestamp)_$($fileNameMGRef)_AADRoleAssignments_"
        }
        $arrayServicePrincipalsAADRoleAssignments4CSV | Sort-Object -Property SPDisplayName, SPObjectId, RoleDefinitionName, ScopeDetail | Export-Csv -Path "$($outputPath)$($DirectorySeparatorChar)$($fileName).csv" -Delimiter ';' -Encoding utf8 -NoTypeInformation -UseQuotes AsNeeded
        $arrayServicePrincipalsAADRoleAssignments4CSV = $null
        [void]$htmlTenantSummary.AppendLine(@"
            </tbody>
        </table>

    <script>
        var tfConfig4$htmlTableId = {
            base_path: 'https://www.azadvertizer.net/azadserviceprincipalinsights/tablefilter/', rows_counter: true,
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
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true, linked_filters: true,
col_widths: ['10%', '10%', '10%', '10%', '10%', '10%', '3%', '37%'],
            locale: 'en-US',
            col_3: 'multiple',
            col_4: 'select',
            col_5: 'select',
            col_types: [
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'number',
                'caseinsensitivestring'
            ],
extensions: [{ name: 'sort' }]
        };
        var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
        tf.init();
    </script>
"@)

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@'
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textAADRoleAssignment fontGrey" data-content="&nbsp;Service Principal AAD RoleAssignments" /></button>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsAADRoleAssignments

    #region SUMMARYServicePrincipalsAADRoleAssignedOn
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary ServicePrincipalsAADRoleAssignedOn'
    $servicePrincipalsAADRoleAssignedOn = $cu.where( { $_.SPAAADRoleAssignedOn.Count -ne 0 } )
    $servicePrincipalsAADRoleAssignedOnCount = $servicePrincipalsAADRoleAssignedOn.Count
    if ($servicePrincipalsAADRoleAssignedOnCount -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@'
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textAADRoleAssignment" data-content="&nbsp;Service Principal AAD RoleAssignedOn" /></button>
        <div class="content TenantSummaryContent">
'@)

        $tfCount = $servicePrincipalsAADRoleAssignedOnCount
        $htmlTableId = 'TenantSummary_ServicePrincipalsAADRoleAssignedOn'
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

            $spType = $sp.ObjectType

            $SPAAADRoleAssignedOn = $null
            if (($sp.SPAAADRoleAssignedOn)) {
                if (($sp.SPAAADRoleAssignedOn.count -gt 0)) {
                    $array = @()
                    foreach ($rao in $sp.SPAAADRoleAssignedOn) {

                        $raRoleDefinitionName = $rao.roleName
                        if ($htAadRoleDefinitions.($rao.roleId)) {
                            if ($htAadRoleDefinitions.($rao.roleId).isBuiltIn -eq $true) {
                                $raRoleDefinitionName = "<a class=`"externallink`" href=`"https://github.com/MicrosoftDocs/azure-docs/blob/main/articles/active-directory/roles/permissions-reference.md#$($rao.roleName -replace ' ', '-')`" target=`"_blank`">$($rao.roleName)</a>"
                            }
                        }

                        $array += "$raRoleDefinitionName ($($rao.roleId)) on $($rao.principalDisplayName) - $($rao.principalType) ($($rao.principalId))"
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
            base_path: 'https://www.azadvertizer.net/azadserviceprincipalinsights/tablefilter/', rows_counter: true,
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
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true, linked_filters: true,
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

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@'
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textAADRoleAssignment fontGrey" data-content="&nbsp;Service Principal AAD RoleAssignedOn" /></button>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsAADRoleAssignedOn

    #region SUMMARYApplicationsAADRoleAssignedOn
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary ApplicationsAADRoleAssignedOn'
    $applicationsAADRoleAssignedOn = $cu.where( { $_.APPAAADRoleAssignedOn.Count -ne 0 } )
    $applicationsAADRoleAssignedOnCount = $applicationsAADRoleAssignedOn.Count
    if ($applicationsAADRoleAssignedOnCount -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@'
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textAADRoleAssignment" data-content="&nbsp;Application AAD RoleAssignedOn" /></button>
        <div class="content TenantSummaryContent">
'@)

        $tfCount = $applicationsAADRoleAssignedOnCount
        $htmlTableId = 'TenantSummary_ApplicationsAADRoleAssignedOn'
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

            $spType = $sp.ObjectType

            $APPAAADRoleAssignedOn = $null
            if (($sp.APPAAADRoleAssignedOn)) {
                if (($sp.APPAAADRoleAssignedOn.count -gt 0)) {
                    $array = @()
                    foreach ($rao in $sp.APPAAADRoleAssignedOn) {

                        $raRoleDefinitionName = $rao.roleName
                        if ($htAadRoleDefinitions.($rao.roleId)) {
                            if ($htAadRoleDefinitions.($rao.roleId).isBuiltIn -eq $true) {
                                $raRoleDefinitionName = "<a class=`"externallink`" href=`"https://github.com/MicrosoftDocs/azure-docs/blob/main/articles/active-directory/roles/permissions-reference.md#$($rao.roleName -replace ' ', '-')`" target=`"_blank`">$($rao.roleName)</a>"
                            }
                        }

                        $array += "$raRoleDefinitionName ($($rao.roleId)) on $($rao.principalDisplayName) - $($rao.principalType) ($($rao.principalId))"
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
            base_path: 'https://www.azadvertizer.net/azadserviceprincipalinsights/tablefilter/', rows_counter: true,
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
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true, linked_filters: true,
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

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@'
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textAADRoleAssignment fontGrey" data-content="&nbsp;Application AAD RoleAssignedOn" /></button>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYApplicationsAADRoleAssignedOn

    #region SUMMARYServicePrincipalsAppRoleAssignments
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary ServicePrincipalsAppRoleAssignments'
    $servicePrincipalsAppRoleAssignments = $cu.where( { $_.SPAppRoleAssignments.Count -ne 0 } )
    $servicePrincipalsAppRoleAssignmentsCount = $servicePrincipalsAppRoleAssignments.Count
    if ($servicePrincipalsAppRoleAssignmentsCount -gt 0) {
        $summaryClassifications = ($servicePrincipalsAppRoleAssignments.SPAppRoleAssignments.AppRolePermissionSensitivity.where( { $_ -ne 'unclassified' } ) | Sort-Object -Unique) -join ', '
        $classifiedCritical = $servicePrincipalsAppRoleAssignments.SPAppRoleAssignments.where( { $_.AppRolePermissionSensitivity -ne 'unclassified' } )
        $classifiedCriticalCount = $classifiedCritical.Count

        if ($classifiedCriticalCount -gt 0) {
            $buttonDataContent = "Service Principal App RoleAssignments (API permissions Application) [$summaryClassifications permissions: $classifiedCriticalCount]"
        }
        else {
            $buttonDataContent = 'Service Principal App RoleAssignments (API permissions Application)'
        }

        [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textAPIPermissions" data-content="&nbsp;$buttonDataContent" /></button>
        <div class="content TenantSummaryContent">
"@)

        $tfCount = $servicePrincipalsAppRoleAssignmentsCount
        $htmlTableId = 'TenantSummary_ServicePrincipalsAppRoleAssignments'
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
<th>Classification</th>
<th>SP App RoleAssignments</th>
</tr>
</thead>
<tbody>
"@)

        $arrayServicePrincipalsAppRoleAssignments4CSV = [System.Collections.ArrayList]@()
        foreach ($sp in ($servicePrincipalsAppRoleAssignments)) {

            $spType = $sp.ObjectType

            $SPAppRoleAssignments = $null
            if (($sp.SPAppRoleAssignments)) {
                $classification = 'unclassified'
                if (($sp.SPAppRoleAssignments.count -gt 0)) {
                    $array = @()
                    $classificationCollection = @()
                    foreach ($approleAss in $sp.SPAppRoleAssignments) {

                        $classification4CSV = 'unclassified'

                        if ($approleAss.AppRolePermissionSensitivity -ne 'unclassified') {
                            $classificationCollection += $approleAss.AppRolePermissionSensitivity
                            $classification = $approleAss.AppRolePermissionSensitivity
                            $classification4CSV = $approleAss.AppRolePermissionSensitivity
                            $array += "$($approleAss.AppRoleAssignmentResourceDisplayName) (<span style=`"color: $($getClassifications.permissionColors.($approleAss.AppRolePermissionSensitivity))`">$($approleAss.AppRolePermission)</span>)"
                        }
                        else {
                            $array += "$($approleAss.AppRoleAssignmentResourceDisplayName) ($($approleAss.AppRolePermission))"

                        }

                        $null = $arrayServicePrincipalsAppRoleAssignments4CSV.Add([PSCustomObject]@{
                                SPObjectType = $sp.ObjectType
                                SPObjectId = $sp.ObjectId
                                SPDisplayName = $sp.SP.SPDisplayName
                                APPObjectId = $sp.APP.APPObjectId
                                APPAppClientId = $sp.APP.APPAppClientId
                                APPDisplayName = $sp.APP.APPDisplayName
                                AppRoleAssignmentResourceDisplayName = $approleAss.AppRoleAssignmentResourceDisplayName
                                AppRoleAssignmentResourceId = $approleAss.AppRoleAssignmentResourceId
                                AppRolePermission = $approleAss.AppRolePermission
                                AppRoleDisplayName = $approleAss.AppRoleDisplayName
                                AppRoleId = $approleAss.AppRoleId
                                AppRolePermissionClassification = $classification4CSV
                            })

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
<td>$(($classificationCollection | Sort-Object -Unique) -join ', ')</td>
<td class="breakwordall">$($SPAppRoleAssignments)</td>
</tr>
"@)
        }

        if ($azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions -eq $true) {
            $fileName = "$($Product)_$($fileNameMGRef)_AppRoleAssignments_"
        }
        else {
            $fileName = "$($Product)_$($ProductVersion)_$($fileTimestamp)_$($fileNameMGRef)_AppRoleAssignments_"
        }
        $arrayServicePrincipalsAppRoleAssignments4CSV | Sort-Object -Property SPDisplayName, SPObjectId, AppRoleAssignmentResourceDisplayName, AppRolePermission | Export-Csv -Path "$($outputPath)$($DirectorySeparatorChar)$($fileName).csv" -Delimiter ';' -Encoding utf8 -NoTypeInformation -UseQuotes AsNeeded
        $arrayServicePrincipalsAppRoleAssignments4CSV = $null

        [void]$htmlTenantSummary.AppendLine(@"
            </tbody>
        </table>

    <script>
        var tfConfig4$htmlTableId = {
            base_path: 'https://www.azadvertizer.net/azadserviceprincipalinsights/tablefilter/', rows_counter: true,
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
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true, linked_filters: true,
col_widths: ['10%', '10%', '10%', '10%', '10%', '5%', '45%'],
            locale: 'en-US',
            col_3: 'multiple',
            col_4: 'select',
            col_types: [
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring'
            ],
            watermark: ['', '', '', '', '', '$($summaryClassifications)', ''],
extensions: [{ name: 'sort' }]
        };
        var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
        tf.init();
    </script>
"@)

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@'
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textAPIPermissions fontGrey" data-content="&nbsp;Service Principal App RoleAssignments (API permissions Application)" /></button>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsAppRoleAssignments

    #region SUMMARYServicePrincipalsAppRoleAssignedTo
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary ServicePrincipalsAppRoleAssignedTo'
    $servicePrincipalsAppRoleAssignedTo = $cu.where( { $_.SPAppRoleAssignedTo.Count -ne 0 -and ($_.SPAppRoleAssignedTo.principalType -like 'User*' -or $_.SPAppRoleAssignedTo.principalType -eq 'Group') } )

    #$servicePrincipalsAppRoleAssignedTo = $cu.where( { $_.SPAppRoleAssignedTo.Count -ne 0} )
    $servicePrincipalsAppRoleAssignedToCount = $servicePrincipalsAppRoleAssignedTo.Count
    if ($servicePrincipalsAppRoleAssignedToCount -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@'
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup" data-content="&nbsp;Service Principal App RoleAssignedTo (Users and Groups)" /></button>
        <div class="content TenantSummaryContent">
'@)

        $tfCount = $servicePrincipalsAppRoleAssignedToCount
        $htmlTableId = 'TenantSummary_ServicePrincipalsAppRoleAssignedTo'
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

            $spType = $sp.ObjectType

            $SPAppRoleAssignedTo = $null
            if (($sp.SPAppRoleAssignedTo)) {
                if (($sp.SPAppRoleAssignedTo.count -gt 0)) {
                    $array = @()
                    foreach ($approleAssTo in $sp.SPAppRoleAssignedTo) {
                        $array += "$($approleAssTo.principalDisplayName) - $($approleAssTo.principalType)"
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
            base_path: 'https://www.azadvertizer.net/azadserviceprincipalinsights/tablefilter/', rows_counter: true,
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
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true, linked_filters: true,
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

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@'
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup fontGrey" data-content="&nbsp;Service Principal App RoleAssignedTo (Users and Groups)" /></button>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsAppRoleAssignedTo

    #region SUMMARYServicePrincipalsOauth2PermissionGrants
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary ServicePrincipalsOauth2PermissionGrants'

    $servicePrincipalsOauth2PermissionGrants = $cu.where( { $_.SPOauth2PermissionGrants.Count -ne 0 } )
    $servicePrincipalsOauth2PermissionGrantsCount = $servicePrincipalsOauth2PermissionGrants.Count

    $summaryClassifications = ($servicePrincipalsOauth2PermissionGrants.SPOauth2PermissionGrants.permissionSensitivity.where( { $_ -ne 'unclassified' } ) | Sort-Object -Unique) -join ', '
    $classifiedCritical = $servicePrincipalsOauth2PermissionGrants.SPOauth2PermissionGrants.where( { $_.permissionSensitivity -ne 'unclassified' } )
    $classifiedCriticalCount = $classifiedCritical.Count

    if ($classifiedCriticalCount -gt 0) {
        $buttonDataContent = "Service Principal Oauth Permission grants (API permissions Delegated) [$summaryClassifications permissions: $classifiedCriticalCount]"
    }
    else {
        $buttonDataContent = 'Service Principal Oauth Permission grants (API permissions Delegated)'
    }

    if ($servicePrincipalsOauth2PermissionGrantsCount -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@"
            <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textAPIPermissions" data-content="&nbsp;$buttonDataContent" /></button>
            <div class="content TenantSummaryContent">
"@)

        $tfCount = $servicePrincipalsOauth2PermissionGrantsCount
        $htmlTableId = 'TenantSummary_ServicePrincipalsOauth2PermissionGrants'
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
<th>Classification</th>
<th>SP Oauth Permission grants</th>
</tr>
</thead>
<tbody>
"@)

        $arrayServicePrincipalsOauth2PermissionGrants4CSV = [System.Collections.ArrayList]@()
        foreach ($sp in ($servicePrincipalsOauth2PermissionGrants)) {

            $spType = $sp.ObjectType

            $SPOauth2PermissionGrants = $null
            if (($sp.SPOauth2PermissionGrants)) {
                $classification = 'unclassified'
                if (($sp.SPOauth2PermissionGrants.count -gt 0)) {
                    $array = @()
                    $classificationCollection = @()
                    foreach ($oauthGrant in $sp.SPOauth2PermissionGrants | Sort-Object -Property SPDisplayName, type, permission) {

                        $classification4CSV = 'unclassified'

                        if ($oauthGrant.permissionSensitivity -ne 'unclassified') {
                            $classificationCollection += $oauthGrant.permissionSensitivity
                            $classification = $oauthGrant.permissionSensitivity
                            $classification4CSV = $oauthGrant.permissionSensitivity
                            $array += "$($oauthGrant.SPDisplayName) (<span style=`"color: $($getClassifications.permissionColors.($oauthGrant.permissionSensitivity))`">$($oauthGrant.permission)</span> - $($oauthGrant.type))"
                        }
                        else {
                            $array += "$($oauthGrant.SPDisplayName) ($($oauthGrant.permission) - $($oauthGrant.type))"

                        }

                        $null = $arrayServicePrincipalsOauth2PermissionGrants4CSV.Add([PSCustomObject]@{
                                SPObjectType = $sp.ObjectType
                                SPObjectId = $sp.ObjectId
                                SPDisplayName = $sp.SP.SPDisplayName
                                APPObjectId = $sp.APP.APPObjectId
                                APPAppClientId = $sp.APP.APPAppClientId
                                APPDisplayName = $sp.APP.APPDisplayName
                                Oauth2PermissionGrantResourceDisplayName = $oauthGrant.SPDisplayName
                                Oauth2PermissionGrantResourceId = $oauthGrant.SPId
                                Oauth2PermissionGrantPermission = $oauthGrant.permission
                                Oauth2PermissionGrantDisplayNameUser = $oauthGrant.userConsentDisplayName
                                Oauth2PermissionGrantDisplayNameAdmin = $oauthGrant.adminConsentDisplayName
                                Oauth2PermissionGrantPermissionId = $oauthGrant.id
                                Oauth2PermissionGrantPermissionClassification = $classification4CSV
                            })
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
<td>$(($classificationCollection | Sort-Object -Unique) -join ', ')</td>
<td class="breakwordall">$($SPOauth2PermissionGrants)</td>
</tr>
"@)
        }

        if ($azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions -eq $true) {
            $fileName = "$($Product)_$($fileNameMGRef)_Oauth2PermissionGrants_"
        }
        else {
            $fileName = "$($Product)_$($ProductVersion)_$($fileTimestamp)_$($fileNameMGRef)_Oauth2PermissionGrants_"
        }
        $arrayServicePrincipalsOauth2PermissionGrants4CSV | Sort-Object -Property SPDisplayName, SPObjectId, Oauth2PermissionGrantResourceDisplayName, Oauth2PermissionGrantPermission | Export-Csv -Path "$($outputPath)$($DirectorySeparatorChar)$($fileName).csv" -Delimiter ';' -Encoding utf8 -NoTypeInformation -UseQuotes AsNeeded
        $arrayServicePrincipalsOauth2PermissionGrants4CSV = $null

        [void]$htmlTenantSummary.AppendLine(@"
            </tbody>
        </table>

    <script>
        var tfConfig4$htmlTableId = {
            base_path: 'https://www.azadvertizer.net/azadserviceprincipalinsights/tablefilter/', rows_counter: true,
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
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true, linked_filters: true,
col_widths: ['10%', '10%', '10%', '10%', '10%', '5%', '50%'],
            locale: 'en-US',
            col_3: 'multiple',
            col_4: 'select',
            col_types: [
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring'
            ],
            watermark: ['', '', '', '', '', '$($summaryClassifications)', ''],
extensions: [{ name: 'sort' }]
        };
        var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
        tf.init();
    </script>
"@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@'
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textAPIPermissions fontGrey" data-content="&nbsp;Service Principal  Oauth Permission grants (API permissions Delegated)" /></button>
'@)
    }

    [void]$htmlTenantSummary.AppendLine(@'
    </div>
'@)

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsOauth2PermissionGrants

    if (-not $NoAzureRoleAssignments) {
        #region SUMMARYServicePrincipalsAzureRoleAssignments
        $startCustPolLoop = Get-Date
        Write-Host '  processing Summary ServicePrincipalsAzureRoleAssignments'

        $servicePrincipalsAzureRoleAssignments = $cu.where( { $_.SPAzureRoleAssignments.Count -ne 0 } )
        $servicePrincipalsAzureRoleAssignmentsCount = $servicePrincipalsAzureRoleAssignments.Count

        if ($servicePrincipalsAzureRoleAssignmentsCount -gt 0) {
            [void]$htmlTenantSummary.AppendLine(@'
            <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textAzureRoleAssignment" data-content="&nbsp;Service Principal  Azure RoleAssignments" /></button>
            <div class="content TenantSummaryContent">
'@)

            $tfCount = $servicePrincipalsAzureRoleAssignmentsCount
            $htmlTableId = 'TenantSummary_ServicePrincipalsAzureRoleAssignments'
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
<th>Classification</th>
<th>#</th>
<th>SP Azure RoleAssignments</th>
</tr>
</thead>
<tbody>
"@)

            foreach ($sp in ($servicePrincipalsAzureRoleAssignments)) {

                $spType = $sp.ObjectType

                $SPAzureRoleAssignments = $null
                if (($sp.SPAzureRoleAssignments)) {
                    $RBACClassification = ''
                    $faIcon = ''
                    if (($sp.SPAzureRoleAssignments.count -gt 0)) {
                        $array = @()
                        $importance = 'ManagementGroup', 'Subscription', 'ResourceGroup', 'Resource'

                        foreach ($azureroleAss in $sp.SPAzureRoleAssignments | Sort-Object @{Expression = { $importance.IndexOf($_.roleAssignmentAssignmentResourceType) } }, @{Expression = { $_.roleAssignmentAssignmentScopeName } }, @{Expression = { $_.roleName } }) {
                            if ($azureroleAss.roleType -eq 'BuiltInRole') {
                                $roleName = "<a class=`"externallink`" href=`"https://www.azadvertizer.net/azrolesadvertizer/$($azureroleAss.roleId).html`" target=`"_blank`">$($azureroleAss.roleName)</a>"
                            }
                            else {
                                $roleName = $azureroleAss.roleName
                            }

                            if ($azureroleAss.priviledgedIdentityManagementBased -eq 'true') {
                                $pimRef = ' [PIM]'
                            }
                            else {
                                $pimRef = ''
                            }

                            if ($azureroleAss.roleAssignmentApplicability -eq 'indirect') {
                                $indirectRef = " [$($azureroleAss.roleAssignmentApplicability) - $($azureroleAss.roleAssignmentAppliesThrough)]"
                            }
                            else {
                                $indirectRef = ''
                            }

                            if ($azureroleAss.roleIsCritical -eq $true) {
                                $RBACClassification = 'critical'
                                $faIcon = '<i class="fa fa-exclamation-triangle" aria-hidden="true" style="color: #ff5e00; font-size: 9px;"></i> '
                                $array += "$($faIcon)$($roleName) (<b>$($azureroleAss.roleAssignmentAssignmentResourceType)</b> $($azureroleAss.roleAssignmentAssignmentScopeName -replace '<', '&lt;' -replace '>', '&gt;'))$($pimRef)$($indirectRef)"
                            }
                            else {
                                $array += "$($roleName) (<b>$($azureroleAss.roleAssignmentAssignmentResourceType)</b> $($azureroleAss.roleAssignmentAssignmentScopeName -replace '<', '&lt;' -replace '>', '&gt;'))$($pimRef)$($indirectRef)"
                            }

                        }
                        $SPAzureRoleAssignments = "$($array -join '<br>')"
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
<td>$RBACClassification</td>
<td>$(($sp.SPAzureRoleAssignments).Count)</td>
<td class="breakwordall">$($SPAzureRoleAssignments)</td>
</tr>
"@)
            }

            [void]$htmlTenantSummary.AppendLine(@"
            </tbody>
        </table>

    <script>
        var tfConfig4$htmlTableId = {
            base_path: 'https://www.azadvertizer.net/azadserviceprincipalinsights/tablefilter/', rows_counter: true,
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
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true, linked_filters: true,
col_widths: ['9%', '9%', '9%', '9%', '9%', '5%', '4%', '41%'],
            locale: 'en-US',
            col_3: 'multiple',
            col_4: 'select',
            col_5: 'select',
            col_types: [
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'caseinsensitivestring',
                'number',
                'caseinsensitivestring'
            ],
extensions: [{ name: 'sort' }]
        };
        var tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
        tf.init();
    </script>
"@)

            [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
        }
        else {
            [void]$htmlTenantSummary.AppendLine(@'
                <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textAzureRoleAssignment fontGrey" data-content="&nbsp;Service Principal  Azure RoleAssignments" /></button>
'@)
        }

        $endCustPolLoop = Get-Date
        Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
        #endregion SUMMARYServicePrincipalsAzureRoleAssignments
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@'
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textAzureRoleAssignment fontGrey" data-content="&nbsp;Service Principal  Azure RoleAssignments" /></button>
'@)
    }

    #region SUMMARYServicePrincipalsGroupMemberships
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary ServicePrincipalsGroupMemberships'

    $servicePrincipalsGroupMemberships = $cu.where( { $_.SPGroupMemberships.Count -ne 0 } )
    $servicePrincipalsGroupMembershipsCount = $servicePrincipalsGroupMemberships.Count

    if ($servicePrincipalsGroupMembershipsCount -gt 0) {
        [void]$htmlTenantSummary.AppendLine(@'
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup" data-content="&nbsp;Service Principal  Group memberships" /></button>
        <div class="content TenantSummaryContent">
'@)

        $tfCount = $servicePrincipalsGroupMembershipsCount
        $htmlTableId = 'TenantSummary_ServicePrincipalsGroupMemberships'
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

            $spType = $sp.ObjectType

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
                base_path: 'https://www.azadvertizer.net/azadserviceprincipalinsights/tablefilter/', rows_counter: true,
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
    btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true, linked_filters: true,
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

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@'
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textGroup fontGrey" data-content="&nbsp;Service Principal  Group memberships" /></button>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsGroupMemberships

    #region SUMMARYApplicationSecrets
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary ApplicationSecrets'

    $applicationSecrets = $cu.where( { $_.APPPasswordCredentials.Count -gt 0 } )
    $applicationSecretsCount = $applicationSecrets.Count

    if ($applicationSecretsCount -gt 0) {

        $tfCount = $applicationSecretsCount
        $htmlTableId = 'TenantSummary_ApplicationSecrets'
        $tf = "tf$($htmlTableId)"

        $applicationSecretsExpireSoon = $applicationSecrets.APPPasswordCredentials.expiryInfo.where( { $_ -like 'expires soon*' } )
        $applicationSecretsExpireSoonCount = $applicationSecretsExpireSoon.Count

        if ($applicationSecretsExpireSoonCount -gt 0) {
            [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textSecretCert" data-content="&nbsp;Application Secrets ($applicationSecretsExpireSoonCount expire soon)" /></button>
        <div class="content TenantSummaryContent">
"@)
        }
        else {
            [void]$htmlTenantSummary.AppendLine(@'
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textSecretCert" data-content="&nbsp;Application Secrets" /></button>
        <div class="content TenantSummaryContent">
'@)
        }

        $groupedExpiryNoteWorthy = $applicationSecrets.APPPasswordCredentials.expiryInfo.where( { $_ -like 'expires soon*' -or $_ -eq 'expired' -or $_ -eq "expires in more than $ApplicationSecretExpiryMax days" } ) | Group-Object
        if (($groupedExpiryNoteWorthy | Measure-Object).Count -gt 0) {
            $arrExpiryNoteWorthyCounts = @()
            $arrExpiryNoteWorthyStates = @()
            foreach ($grp in $groupedExpiryNoteWorthy | Sort-Object -Property count -Descending) {
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

        $tfCount = $applicationSecretsCount
        $htmlTableId = 'TenantSummary_ApplicationSecrets'
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

        $arrayApplicationSecrets4CSV = [System.Collections.ArrayList]@()
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
                            $null = $arrayApplicationSecrets4CSV.Add([PSCustomObject]@{
                                    SPObjectId = $sp.ObjectId
                                    SPAppId = $sp.SP.SPappId
                                    SPDisplayName = $sp.SP.SPDisplayName
                                    SPAppOwnerOrgId = $sp.SP.SPappOwnerOrganizationId
                                    SPObjectType = $sp.ObjectType
                                    APPObjectId = $sp.APP.APPObjectId
                                    APPAppClientId = $sp.APP.APPAppClientId
                                    APPDisplayName = $sp.APP.APPDisplayName
                                    APPSecretDisplayName = $secret.displayName
                                    APPSecretKeyId = $secret.keyId
                                    APPSecretExpiryInfo = $secret.expiryInfo
                                    APPSecretEndDateTimeFormated = $secret.endDateTimeFormated
                                })
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

        if ($azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions -eq $true) {
            $fileName = "$($Product)_$($fileNameMGRef)_AppSecrets_"
        }
        else {
            $fileName = "$($Product)_$($ProductVersion)_$($fileTimestamp)_$($fileNameMGRef)_AppSecrets_"
        }
        $arrayApplicationSecrets4CSV | Sort-Object -Property SPDisplayName, SPObjectId, APPSecretDisplayName, APPSecretKeyId | Export-Csv -Path "$($outputPath)$($DirectorySeparatorChar)$($fileName).csv" -Delimiter ';' -Encoding utf8 -NoTypeInformation -UseQuotes AsNeeded
        $arrayApplicationSecrets4CSV = $null

        [void]$htmlTenantSummary.AppendLine(@"
        </tbody>
    </table>

<script>
    var tfConfig4$htmlTableId = {
        base_path: 'https://www.azadvertizer.net/azadserviceprincipalinsights/tablefilter/', rows_counter: true,
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
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true, linked_filters: true,
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

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@'
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textSecretCert fontGrey" data-content="&nbsp;Application Secrets" /></button>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYApplicationSecrets

    #region SUMMARYApplicationCertificates
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary ApplicationCertificates'

    $applicationCertificates = $cu.where( { $_.APPKeyCredentials.Count -gt 0 } )
    $applicationCertificatesCount = $applicationCertificates.Count

    if ($applicationCertificatesCount -gt 0) {

        $tfCount = $applicationCertificatesCount
        $htmlTableId = 'TenantSummary_ApplicationCertificates'
        $tf = "tf$($htmlTableId)"

        $applicationCertificatesExpireSoon = $applicationCertificates.APPKeyCredentials.expiryInfo.where( { $_ -like 'expires soon*' } )
        $applicationCertificatesExpireSoonCount = $applicationCertificatesExpireSoon.Count

        if ($applicationCertificatesExpireSoonCount -gt 0) {
            [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textSecretCert" data-content="&nbsp;Application Certificates ($applicationCertificatesExpireSoonCount expire soon)" /></button>
        <div class="content TenantSummaryContent">
"@)
        }
        else {
            [void]$htmlTenantSummary.AppendLine(@'
                <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textSecretCert" data-content="&nbsp;Application Certificates" /></button>
                <div class="content TenantSummaryContent">
'@)
        }

        $groupedExpiryNoteWorthy = $applicationCertificates.APPKeyCredentials.expiryInfo.where( { $_ -like 'expires soon*' -or $_ -eq 'expired' } ) | Group-Object
        if (($groupedExpiryNoteWorthy | Measure-Object).Count -gt 0) {
            $arrExpiryNoteWorthyCounts = @()
            $arrExpiryNoteWorthyStates = @()
            foreach ($grp in $groupedExpiryNoteWorthy | Sort-Object -Property count -Descending) {
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

        $arrayApplicationCertificates4CSV = [System.Collections.ArrayList]@()
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
                            $null = $arrayApplicationCertificates4CSV.Add([PSCustomObject]@{
                                    SPObjectId = $sp.ObjectId
                                    SPAppId = $sp.SP.SPappId
                                    SPDisplayName = $sp.SP.SPDisplayName
                                    SPAppOwnerOrgId = $sp.SP.SPappOwnerOrganizationId
                                    SPObjectType = $sp.ObjectType
                                    APPObjectId = $sp.APP.APPObjectId
                                    APPAppClientId = $sp.APP.APPAppClientId
                                    APPDisplayName = $sp.APP.APPDisplayName
                                    APPCertificateDisplayName = $key.displayName
                                    APPCertificateKeyId = $key.keyId
                                    APPCertificateCuistomKeyIdentifier = $key.customKeyIdentifier
                                    APPCertificateExpiryInfo = $key.expiryInfo
                                    APPCertificateEndDateTimeFormated = $key.endDateTimeFormated
                                })
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

        if ($azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions -eq $true) {
            $fileName = "$($Product)_$($fileNameMGRef)_AppCertificates_"
        }
        else {
            $fileName = "$($Product)_$($ProductVersion)_$($fileTimestamp)_$($fileNameMGRef)_AppCertificates_"
        }
        $arrayApplicationCertificates4CSV | Sort-Object -Property SPDisplayName, SPObjectId, APPCertificateDisplayName, APPCertificateKeyId | Export-Csv -Path "$($outputPath)$($DirectorySeparatorChar)$($fileName).csv" -Delimiter ';' -Encoding utf8 -NoTypeInformation -UseQuotes AsNeeded
        $arrayApplicationCertificates4CSV = $null

        [void]$htmlTenantSummary.AppendLine(@"
        </tbody>
    </table>

<script>
    var tfConfig4$htmlTableId = {
        base_path: 'https://www.azadvertizer.net/azadserviceprincipalinsights/tablefilter/', rows_counter: true,
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
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true, linked_filters: true,
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

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@'
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textSecretCert fontGrey" data-content="&nbsp;Application Certificates" /></button>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYApplicationCertificates

    #region SUMMARYApplicationFederatedIdentityCredentials
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary ApplicationFederatedIdentityCredentials'

    $applicationFederatedIdentityCredentials = $cu.where( { $_.APPFederatedIdentityCredentials.Count -gt 0 } )
    $applicationFederatedIdentityCredentialsCount = $applicationFederatedIdentityCredentials.Count

    if ($applicationFederatedIdentityCredentialsCount -gt 0) {

        $tfCount = $applicationFederatedIdentityCredentialsCount
        $htmlTableId = 'TenantSummary_ApplicationFederatedIdentityCredentials'
        $tf = "tf$($htmlTableId)"

        [void]$htmlTenantSummary.AppendLine(@'
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textSecretCert" data-content="&nbsp;Application Federated Identity Credentials" /></button>
        <div class="content TenantSummaryContent">
'@)

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
<th>Application Federated Identity Credentials</th>
</tr>
</thead>
<tbody>
"@)
        $arrayApplicationFederatedIdentityCredentials4CSV = [System.Collections.ArrayList]@()
        foreach ($sp in ($applicationFederatedIdentityCredentials)) {
            if ($sp.APP) {

                $spType = $sp.SP.servicePrincipalType
                $appObjectId = $sp.APP.APPObjectId
                $appId = $sp.APP.APPAppClientId
                $appDisplayName = $sp.APP.APPDisplayName

                $APPFederatedIdentityCredentials = $null
                if (($sp.APPFederatedIdentityCredentials)) {
                    if (($sp.APPFederatedIdentityCredentials.count -gt 0)) {
                        $array = @()
                        foreach ($fic in $sp.APPFederatedIdentityCredentials) {
                            if ([string]::IsNullOrWhiteSpace($fic.description)) {
                                $descriptionFederatedIdentityCredential = 'not given'
                            }
                            else {
                                $descriptionFederatedIdentityCredential = $fic.description
                            }
                            $array += "$($fic.name)(id: $($fic.id)) / description: '$($descriptionFederatedIdentityCredential)' (issuer: $($fic.issuer); subject: $($fic.subject); audiences: $((($fic.audiences | Sort-Object) -join "$CsvDelimiterOpposite ")))"
                            $null = $arrayApplicationFederatedIdentityCredentials4CSV.Add([PSCustomObject]@{
                                    SPObjectId = $sp.ObjectId
                                    SPAppId = $sp.SP.SPappId
                                    SPDisplayName = $sp.SP.SPDisplayName
                                    SPAppOwnerOrgId = $sp.SP.SPappOwnerOrganizationId
                                    SPObjectType = $sp.ObjectType
                                    APPObjectId = $sp.APP.APPObjectId
                                    APPAppClientId = $sp.APP.APPAppClientId
                                    APPDisplayName = $sp.APP.APPDisplayName
                                    APPFederatedIdentityCredentialName = $fic.name
                                    APPFederatedIdentityCredentialDescription = $descriptionFederatedIdentityCredential
                                    APPFederatedIdentityCredentialId = $fic.id
                                    APPFederatedIdentityCredentialIssuer = $fic.issuer
                                    APPFederatedIdentityCredentialSubject = $fic.subject
                                    APPFederatedIdentityCredentialAudiences = (($fic.audiences | Sort-Object) -join "$CsvDelimiterOpposite ")
                                })
                        }
                        $APPFederatedIdentityCredentials = "$(($sp.APPFederatedIdentityCredentials).Count) ($($array -join "$CsvDelimiterOpposite "))"
                    }
                    else {
                        $APPFederatedIdentityCredentials = $null
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
<td class="breakwordall">$($APPFederatedIdentityCredentials)</td>
</tr>
"@)
            }
        }

        if ($azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions -eq $true) {
            $fileName = "$($Product)_$($fileNameMGRef)_FederatedIdentityCredentials_"
        }
        else {
            $fileName = "$($Product)_$($ProductVersion)_$($fileTimestamp)_$($fileNameMGRef)_FederatedIdentityCredentials_"
        }
        $arrayApplicationFederatedIdentityCredentials4CSV | Sort-Object -Property SPDisplayName, SPObjectId, APPFederatedIdentityCredentialName, APPFederatedIdentityCredentialId, APPFederatedIdentityCredentialIssuer, APPFederatedIdentityCredentialSubject | Export-Csv -Path "$($outputPath)$($DirectorySeparatorChar)$($fileName).csv" -Delimiter ';' -Encoding utf8 -NoTypeInformation -UseQuotes AsNeeded
        $arrayApplicationFederatedIdentityCredentials4CSV = $null

        [void]$htmlTenantSummary.AppendLine(@"
        </tbody>
    </table>

<script>
    var tfConfig4$htmlTableId = {
        base_path: 'https://www.azadvertizer.net/azadserviceprincipalinsights/tablefilter/', rows_counter: true,
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
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true, linked_filters: true,
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

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@'
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textSecretCert fontGrey" data-content="&nbsp;Application Federated Identity Credentials" /></button>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion ApplicationFederatedIdentityCredentials

    #region SUMMARYHipos
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary HiPo Users'

    $arrayHipos = [System.Collections.ArrayList]@()
    foreach ($object in $cu) {
        if ($object.SPOwners.principalType -contains 'User (Member)' -or $object.SPOwners.principalType -contains 'User (Guest)') {

            foreach ($user in $object.SPOwners.where({ $_.principalType -like 'User*' })) {


                #
                if ($object.SPAppRoleAssignments.count -gt 0) {
                    foreach ($SPAppRoleAssignment in $object.SPAppRoleAssignments) {
                        if ($SPAppRoleAssignment.AppRolePermissionSensitivity -ne 'unclassified') {
                            $null = $arrayHipos.Add([PSCustomObject]@{
                                    user = $user.displayName
                                    userId = $user.id
                                    userType = $user.principalType
                                    ownership = $user.applicability
                                    SPDisplayName = $object.SP.SPDisplayName
                                    SPType = $object.ObjectType
                                    SPId = $object.SP.SPObjectId
                                    SPAppId = $object.SP.SPAppId
                                    capability = 'AppRoleAssignment'
                                    classification = $SPAppRoleAssignment.AppRolePermissionSensitivity
                                    permission = "$($SPAppRoleAssignment.AppRoleAssignmentResourceDisplayName) ($($SPAppRoleAssignment.AppRolePermission))"
                                    permission4HTML = "$($SPAppRoleAssignment.AppRoleAssignmentResourceDisplayName) ($($SPAppRoleAssignment.AppRolePermission))"
                                })
                        }
                    }
                }

                if ($object.SPOauth2PermissionGrants.count -gt 0) {
                    foreach ($SPOauth2PermissionGrant in $object.SPOauth2PermissionGrants) {
                        if ($SPOauth2PermissionGrant.permissionSensitivity -ne 'unclassified') {
                            $null = $arrayHipos.Add([PSCustomObject]@{
                                    user = $user.displayName
                                    userId = $user.id
                                    userType = $user.principalType
                                    ownership = $user.applicability
                                    SPDisplayName = $object.SP.SPDisplayName
                                    SPType = $object.ObjectType
                                    SPId = $object.SP.SPObjectId
                                    SPAppId = $object.SP.SPAppId
                                    capability = 'Oauth2PermissionGrant'
                                    classification = $SPOauth2PermissionGrant.permissionSensitivity
                                    permission = "$($SPOauth2PermissionGrant.SPDisplayName) ($($SPOauth2PermissionGrant.permission))"
                                    permission4HTML = "$($SPOauth2PermissionGrant.SPDisplayName) ($($SPOauth2PermissionGrant.permission))"
                                })
                        }
                    }
                }
                #>

                if ($object.SPAzureRoleAssignments.count -gt 0) {
                    foreach ($SPAzureRoleAssignment in $object.SPAzureRoleAssignments) {
                        if ($SPAzureRoleAssignment.roleIsCritical -eq $true) {
                            if ($htCacheDefinitionsRole.($SPAzureRoleAssignment.roleId)) {
                                if ($htCacheDefinitionsRole.($SPAzureRoleAssignment.roleId).definition.properties.type -eq 'BuiltInRole') {
                                    $roleName = "<a class=`"externallink`" href=`"https://www.azadvertizer.net/azrolesadvertizer/$($SPAzureRoleAssignment.roleId).html`" target=`"_blank`">$($SPAzureRoleAssignment.roleName)</a>"
                                }
                                else {
                                    $roleName = $SPAzureRoleAssignment.roleName
                                }

                            }
                            else {
                                $roleName = $SPAzureRoleAssignment.roleName
                            }
                            $null = $arrayHipos.Add([PSCustomObject]@{
                                    user = $user.displayName
                                    userId = $user.id
                                    userType = $user.principalType
                                    ownership = $user.applicability
                                    SPDisplayName = $object.SP.SPDisplayName
                                    SPType = $object.ObjectType
                                    SPId = $object.SP.SPObjectId
                                    SPAppId = $object.SP.SPAppId
                                    capability = 'AzureRoleAssignment'
                                    classification = 'critical'
                                    permission = "$($SPAzureRoleAssignment.roleName) ($($SPAzureRoleAssignment.roleAssignmentAssignmentResourceType): $($SPAzureRoleAssignment.roleAssignmentAssignmentScopeName))"
                                    permission4HTML = "$($roleName) ($($SPAzureRoleAssignment.roleAssignmentAssignmentResourceType): $($SPAzureRoleAssignment.roleAssignmentAssignmentScopeName))"
                                })
                        }
                    }
                }

                if ($object.SPAADRoleAssignments.count -gt 0) {
                    foreach ($SPAADRoleAssignment in $object.SPAADRoleAssignments) {
                        if ($SPAADRoleAssignment.roleIsCritical -eq $true) {
                            if ($SPAADRoleAssignment.roleType -eq 'BuiltIn') {
                                $roleName = "<a class=`"externallink`" href=`"https://github.com/MicrosoftDocs/azure-docs/blob/main/articles/active-directory/roles/permissions-reference.md#$($SPAADRoleAssignment.roleDefinitionName -replace ' ', '-')`" target=`"_blank`">$($SPAADRoleAssignment.roleDefinitionName)</a>"
                            }
                            else {
                                $roleName = $SPAADRoleAssignment.roleDefinitionName
                            }
                            $null = $arrayHipos.Add([PSCustomObject]@{
                                    user = $user.displayName
                                    userId = $user.id
                                    userType = $user.principalType
                                    ownership = $user.applicability
                                    SPDisplayName = $object.SP.SPDisplayName
                                    SPType = $object.ObjectType
                                    SPId = $object.SP.SPObjectId
                                    SPAppId = $object.SP.SPAppId
                                    capability = 'AADRoleAssignment'
                                    classification = 'critical'
                                    permission = "$($SPAADRoleAssignment.roleDefinitionName) ($($SPAADRoleAssignment.roleDefinitionId))"
                                    permission4HTML = "$($roleName) ($($SPAADRoleAssignment.roleDefinitionId))"
                                })
                        }
                    }
                }

            }
        }
    }

    if ($arrayHipos.Count -gt 0) {
        $arrayHiposGrouped = $arrayHipos | Group-Object -Property userId
        $arrayHiposGroupedCount = ($arrayHiposGrouped | Measure-Object).Count
        $tfCount = $arrayHipos.Count
        $htmlTableId = 'TenantSummary_Hipos'
        $tf = "tf$($htmlTableId)"

        [void]$htmlTenantSummary.AppendLine(@"
        <button type="button" class="collapsible" id="tenantSummaryPolicy"><hr class="hr-textHiPoUsers" data-content="&nbsp;HiPo Users ($arrayHiposGroupedCount)" /></button>
        <div class="content TenantSummaryContent">
        <i class="padlx fa fa-lightbulb-o" aria-hidden="true" style="color: #FFB100"></i> A HiPo User has direct or indirect ownership on a ServicePrincipal(s) with classified permissions<br>
"@)


        [void]$htmlTenantSummary.AppendLine(@"
<i class="padlxx fa fa-table" aria-hidden="true"></i> Download CSV <a class="externallink" href="#" onclick="download_table_as_csv_semicolon('$htmlTableId');">semicolon</a> | <a class="externallink" href="#" onclick="download_table_as_csv_comma('$htmlTableId');">comma</a>
<table id="$htmlTableId" class="summaryTable">
<thead>
<tr>
<th>User</th>
<th>UserId</th>
<th>UserType</th>
<th>ownership</th>
<th>SP displayName</th>
<th>SP type</th>
<th>SP objectId</th>
<th>SP appId</th>
<th>Capability</th>
<th>Classification</th>
<th>Permissions</th>
</tr>
</thead>
<tbody>
"@)

        $arrayHiposSorted = $arrayHipos | Sort-Object -Property userId, SPId, capability, permission
        foreach ($hipo in ($arrayHiposSorted)) {

            [void]$htmlTenantSummary.AppendLine(@"
<tr>
<td>$($hipo.user)</td>
<td>$($hipo.userId)</td>
<td>$($hipo.userType)</td>
<td>$($hipo.ownership)</td>
<td class="breakwordall">$($hipo.SPDisplayName)</td>
<td>$($hipo.SPType)</td>
<td>$($hipo.SPId)</td>
<td>$($hipo.SPappId)</td>
<td>$($hipo.capability)</td>
<td>$($hipo.classification)</td>
<td>$($hipo.permission4HTML)</td>
</tr>
"@)

        }

        if ($azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions -eq $true) {
            $fileName = "$($Product)_$($fileNameMGRef)_HiPoUsers_"
        }
        else {
            $fileName = "$($Product)_$($ProductVersion)_$($fileTimestamp)_$($fileNameMGRef)_HiPoUsers_"
        }
        $arrayHiposSorted | Select-Object -ExcludeProperty permission4HTML | Export-Csv -Path "$($outputPath)$($DirectorySeparatorChar)$($fileName).csv" -Delimiter ';' -Encoding utf8 -NoTypeInformation -UseQuotes AsNeeded

        [void]$htmlTenantSummary.AppendLine(@"
        </tbody>
    </table>

<script>
    var tfConfig4$htmlTableId = {
        base_path: 'https://www.azadvertizer.net/azadserviceprincipalinsights/tablefilter/', rows_counter: true,
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
btn_reset: true, highlight_keywords: true, alternate_rows: true, auto_filter: { delay: 1100 }, no_results_message: true, linked_filters: true,
col_widths: ['10%', '10%', '5%', '5%', '10%', '5%', '10%', '10%', '10%', '5%', '20%'],
        locale: 'en-US',
        col_2: 'select',
        col_3: 'select',
        col_5: 'multiple',
        col_8: 'select',
        col_9: 'select',
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
            'caseinsensitivestring'
        ],
extensions: [{ name: 'sort' }]
    };
    var $tf = new TableFilter('$htmlTableId', tfConfig4$htmlTableId);
    $($tf).init();
</script>
"@)

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        [void]$htmlTenantSummary.AppendLine(@'
            <button type="button" class="nonCollapsible" id="tenantSummaryPolicy"><hr class="hr-textHiPoUsers fontGrey" data-content="&nbsp;HiPo Users" /></button>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYHipos

    $script:html += $htmlTenantSummary

}
#endregion TenantSummary

#endregion Function

#region verifyClassifications
Write-Host 'Verify Classifications (permissionClassification.json)'
try {
    $getClassifications = Get-Content -Raw ".\$($ScriptPath)\permissionClassification.json" | ConvertFrom-Json -AsHashtable -ErrorAction Stop
}
catch {
    Write-Host "file '.\$($ScriptPath)\permissionClassification.json' not found"
    throw
}

#validate permission assigned to ONE classification
foreach ($permissionType in $getClassifications.permissions.Keys) {
    $classifications = ($getClassifications.permissions.($permissionType).'classifications'.Keys)
    $arrayPermissions4Classification = @()
    foreach ($classification in $classifications) {
        foreach ($permission in $getClassifications.permissions.($permissionType).'classifications'.($classification).'includes') {
            $arrayPermissions4Classification += $permission
        }
    }
    if ($arrayPermissions4Classification.Count -ne ($arrayPermissions4Classification | Sort-Object -Unique).Count) {
        Write-Host "$permissionType - duplicate permissions found"
        $diff = Compare-Object -ReferenceObject $arrayPermissions4Classification -DifferenceObject ($arrayPermissions4Classification | Sort-Object -Unique)
        Write-Host ($diff.InputObject -join ', ')
        throw
    }
}
Write-Host ' Verify Classifications (permissionClassification.json) succeeded' -ForegroundColor Green
#endregion verifyClassifications

#region dataCollection

#region helper ht / collect results /save some time
if (-not $NoAzureRoleAssignments) {
    $htCacheDefinitionsRole = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htCacheAssignmentsRole = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htCacheAssignmentsPolicy = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
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

$permissionCheckResults = @()
#region validation / check 'Microsoft Graph API' Access
if ($azAPICallConf['htParameters'].onAzureDevOps -eq $true -or $azAPICallConf['checkContext'].Account.Type -eq 'ServicePrincipal') {
    Write-Host 'Checking ServicePrincipal permissions'


    $permissionsCheckFailed = $false
    $currentTask = 'Test AAD Users Read permission'
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/users?`$count=true&`$top=1"
    $method = 'GET'
    $res = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -consistencyLevel 'eventual' -validateAccess -noPaging

    if ($res -eq 'failed') {
        $permissionCheckResults += 'AAD Users Read permission check FAILED'
        $permissionsCheckFailed = $true
    }
    else {
        $permissionCheckResults += 'AAD Users Read permission check PASSED'
    }

    $currentTask = 'Test AAD Groups Read permission'
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/groups?`$count=true&`$top=1"
    $method = 'GET'
    $res = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -consistencyLevel 'eventual' -validateAccess -noPaging

    if ($res -eq 'failed') {
        $permissionCheckResults += 'AAD Groups Read permission check FAILED'
        $permissionsCheckFailed = $true
    }
    else {
        $permissionCheckResults += 'AAD Groups Read permission check PASSED'
    }

    $currentTask = 'Test AAD ServicePrincipals Read permission'
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/servicePrincipals?`$count=true&`$top=1"
    $method = 'GET'
    $res = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -consistencyLevel 'eventual' -validateAccess -noPaging

    if ($res -eq 'failed') {
        $permissionCheckResults += 'AAD ServicePrincipals Read permission check FAILED'
        $permissionsCheckFailed = $true
    }
    else {
        $permissionCheckResults += 'AAD ServicePrincipals Read permission check PASSED'
    }

    $currentTask = 'Test AAD RoleManagement Read permission'
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/roleManagement/directory/roleDefinitions"
    $method = 'GET'
    $res = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -consistencyLevel 'eventual' -validateAccess -noPaging

    if ($res -eq 'failed') {
        $permissionCheckResults += 'AAD RoleManagement Read permission check FAILED'
        $permissionsCheckFailed = $true
    }
    else {
        $permissionCheckResults += 'AAD RoleManagement Read permission check PASSED'
    }
}
#endregion validation / check 'Microsoft Graph API' Access

if (-not $NoAzureRoleAssignments) {
    Write-Host "Running $($Product) for ManagementGroupId: '$($ManagementGroupId -join ', ')'" -ForegroundColor Yellow

    foreach ($managementGroupIdEntry in $ManagementGroupId) {
        $currentTask = "Checking permissions for ManagementGroup '$managementGroupIdEntry'"
        Write-Host $currentTask
        $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/providers/Microsoft.Management/managementGroups/$($managementGroupIdEntry)?api-version=2020-05-01"
        $method = 'GET'
        $selectedManagementGroupId = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -listenOn 'Content' -validateAccess -noPaging

        if ($selectedManagementGroupId -eq 'failed') {
            $permissionCheckResults += "MG '$($managementGroupIdEntry)' Reader permission check FAILED"
            $permissionsCheckFailed = $true
        }
        else {
            $permissionCheckResults += "MG '$($managementGroupIdEntry)' Reader permission check PASSED"
        }
    }

    Write-Host 'Permission check results'
    foreach ($permissionCheckResult in $permissionCheckResults) {
        if ($permissionCheckResult -like '*PASSED*') {
            Write-Host $permissionCheckResult -ForegroundColor Green
        }
        else {
            Write-Host $permissionCheckResult -ForegroundColor DarkRed
        }
    }

    if ($permissionsCheckFailed -eq $true) {
        Write-Host "Please consult the documentation: https://$($azAPICallConf['htParameters'].gitHubRepository)#required-permissions-in-azure"
        if ($azAPICallConf['htParameters'].onAzureDevOps -eq $true) {
            Write-Error 'Error'
        }
        else {
            Throw 'Error - check the last console output for details'
        }
    }

    #region GettingEntities
    $startEntities = Get-Date
    $currentTask = 'Getting Entities'
    Write-Host "$currentTask"
    #https://management.azure.com/providers/Microsoft.Management/getEntities?api-version=2020-02-01
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/providers/Microsoft.Management/getEntities?api-version=2020-02-01"
    $method = 'POST'
    $arrayEntitiesFromAPI = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

    $htSubscriptionsMgPath = @{}
    $htManagementGroupsMgPath = @{}
    $htEntities = @{}
    $htEntitiesPlain = @{}

    foreach ($entity in $arrayEntitiesFromAPI) {
        $htEntitiesPlain.($entity.Name) = @{}
        $htEntitiesPlain.($entity.Name) = $entity
    }

    foreach ($entity in $arrayEntitiesFromAPI) {
        if ($entity.Type -eq '/subscriptions') {
            $htSubscriptionsMgPath.($entity.name) = @{}
            $htSubscriptionsMgPath.($entity.name).ParentNameChain = $entity.properties.parentNameChain
            $htSubscriptionsMgPath.($entity.name).ParentNameChainDelimited = $entity.properties.parentNameChain -join '/'
            $htSubscriptionsMgPath.($entity.name).Parent = $entity.properties.parent.Id -replace '.*/'
            $htSubscriptionsMgPath.($entity.name).ParentName = $htEntitiesPlain.($entity.properties.parent.Id -replace '.*/').properties.displayName
            $htSubscriptionsMgPath.($entity.name).DisplayName = $entity.properties.displayName
            $array = $entity.properties.parentNameChain
            $array += $entity.name
            $htSubscriptionsMgPath.($entity.name).path = $array
            $htSubscriptionsMgPath.($entity.name).pathDelimited = $array -join '/'
            $htSubscriptionsMgPath.($entity.name).level = (($entity.properties.parentNameChain).Count - 1)
        }
        if ($entity.Type -eq 'Microsoft.Management/managementGroups') {
            if ([string]::IsNullOrEmpty($entity.properties.parent.Id)) {
                $parent = '_TenantRoot_'
            }
            else {
                $parent = $entity.properties.parent.Id -replace '.*/'
            }
            $htManagementGroupsMgPath.($entity.name) = @{}
            $htManagementGroupsMgPath.($entity.name).ParentNameChain = $entity.properties.parentNameChain
            $htManagementGroupsMgPath.($entity.name).ParentNameChainDelimited = $entity.properties.parentNameChain -join '/'
            $htManagementGroupsMgPath.($entity.name).ParentNameChainCount = ($entity.properties.parentNameChain | Measure-Object).Count
            $htManagementGroupsMgPath.($entity.name).Parent = $parent
            $htManagementGroupsMgPath.($entity.name).ChildMgsAll = ($arrayEntitiesFromAPI.where( { $_.Type -eq 'Microsoft.Management/managementGroups' -and $_.properties.ParentNameChain -contains $entity.name } )).Name
            $htManagementGroupsMgPath.($entity.name).ChildMgsDirect = ($arrayEntitiesFromAPI.where( { $_.Type -eq 'Microsoft.Management/managementGroups' -and $_.properties.Parent.Id -replace '.*/' -eq $entity.name } )).Name
            $htManagementGroupsMgPath.($entity.name).DisplayName = $entity.properties.displayName
            $array = $entity.properties.parentNameChain
            $array += $entity.name
            $htManagementGroupsMgPath.($entity.name).path = $array
            $htManagementGroupsMgPath.($entity.name).pathDelimited = $array -join '/'
        }

        $htEntities.($entity.name) = @{}
        $htEntities.($entity.name).ParentNameChain = $entity.properties.parentNameChain
        $htEntities.($entity.name).Parent = $parent
        if ($parent -eq '_TenantRoot_') {
            $parentDisplayName = '_TenantRoot_'
        }
        else {
            $parentDisplayName = $htEntitiesPlain.($htEntities.($entity.name).Parent).properties.displayName
        }
        $htEntities.($entity.name).ParentDisplayName = $parentDisplayName
        $htEntities.($entity.name).DisplayName = $entity.properties.displayName
        $htEntities.($entity.name).Id = $entity.Name
    }

    $endEntities = Get-Date
    Write-Host "Getting Entities duration: $((New-TimeSpan -Start $startEntities -End $endEntities).TotalSeconds) seconds"
    #endregion GettingEntities

    #region subscriptions
    $startGetSubscriptions = Get-Date
    $currentTask = 'Getting all Subscriptions'
    Write-Host "$currentTask"
    #https://management.azure.com/subscriptions?api-version=2020-01-01
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/subscriptions?api-version=2019-10-01"
    $method = 'GET'
    $requestAllSubscriptionsAPI = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

    foreach ($subscription in $requestAllSubscriptionsAPI) {
        $htAllSubscriptionsFromAPI.($subscription.subscriptionId) = @{}
        $htAllSubscriptionsFromAPI.($subscription.subscriptionId).subDetails = $subscription
    }
    $endGetSubscriptions = Get-Date
    Write-Host "Getting all Subscriptions duration: $((New-TimeSpan -Start $startGetSubscriptions -End $endGetSubscriptions).TotalSeconds) seconds"
    #endregion subscriptions

    #region subscriptionFilter
    #API in rare cases returns duplicats, therefor sorting unique (id)
    $childrenSubscriptions = [System.Collections.ArrayList]@()
    foreach ($managementGroupIdEntry in $ManagementGroupId) {
        Write-Host " -Getting child Subscriptions for ManagementGroup scope $managementGroupIdEntry"
        foreach ($childSubsciption in ($arrayEntitiesFromAPI.where( { $_.properties.parentNameChain -contains $managementGroupIdEntry -and $_.type -eq '/subscriptions' } ) | Sort-Object -Property id -Unique)) {
            $null = $childrenSubscriptions.Add($childSubsciption)
        }
    }

    $childrenSubscriptions = $childrenSubscriptions | Sort-Object -Property id -Unique
    $childrenSubscriptionsCount = ($childrenSubscriptions).Count

    $script:subsToProcessInCustomDataCollection = [System.Collections.ArrayList]@()

    foreach ($childrenSubscription in $childrenSubscriptions) {

        $sub = $htAllSubscriptionsFromAPI.($childrenSubscription.name)
        if ($sub.subDetails.subscriptionPolicies.quotaId.startswith('AAD_', 'CurrentCultureIgnoreCase') -or $sub.subDetails.state -ne 'Enabled') {
            if (($sub.subDetails.subscriptionPolicies.quotaId).startswith('AAD_', 'CurrentCultureIgnoreCase')) {
                $null = $script:outOfScopeSubscriptions.Add([PSCustomObject]@{
                        subscriptionId = $childrenSubscription.name
                        subscriptionName = $childrenSubscription.properties.displayName
                        outOfScopeReason = "QuotaId: AAD_ (State: $($sub.subDetails.state))"
                        ManagementGroupId = $htSubscriptionsMgPath.($childrenSubscription.name).Parent
                        ManagementGroupName = $htSubscriptionsMgPath.($childrenSubscription.name).ParentName
                        Level = $htSubscriptionsMgPath.($childrenSubscription.name).level
                    })
            }
            if ($sub.subDetails.state -ne 'Enabled') {
                $null = $script:outOfScopeSubscriptions.Add([PSCustomObject]@{
                        subscriptionId = $childrenSubscription.name
                        subscriptionName = $childrenSubscription.properties.displayName
                        outOfScopeReason = "State: $($sub.subDetails.state)"
                        ManagementGroupId = $htSubscriptionsMgPath.($childrenSubscription.name).Parent
                        ManagementGroupName = $htSubscriptionsMgPath.($childrenSubscription.name).ParentName
                        Level = $htSubscriptionsMgPath.($childrenSubscription.name).level
                    })
            }
        }
        else {
            if ($SubscriptionQuotaIdWhitelist[0] -ne 'undefined') {
                $whitelistMatched = 'unknown'
                foreach ($subscriptionQuotaIdWhitelistQuotaId in $SubscriptionQuotaIdWhitelist) {
                    if (($sub.subDetails.subscriptionPolicies.quotaId).startswith($subscriptionQuotaIdWhitelistQuotaId, 'CurrentCultureIgnoreCase')) {
                        $whitelistMatched = 'inWhitelist'
                    }
                }

                if ($whitelistMatched -eq 'inWhitelist') {
                    #write-host "$($childrenSubscription.properties.displayName) in whitelist"
                    $null = $script:subsToProcessInCustomDataCollection.Add([PSCustomObject]@{
                            subscriptionId = $childrenSubscription.name
                            subscriptionName = $childrenSubscription.properties.displayName
                            subscriptionQuotaId = $sub.subDetails.subscriptionPolicies.quotaId
                        })
                }
                else {
                    #Write-Host " preCustomDataCollection: $($childrenSubscription.properties.displayName) ($($childrenSubscription.name)) Subscription Quota Id: $($sub.subDetails.subscriptionPolicies.quotaId) is out of scope for $($Product) (not in Whitelist)"
                    $null = $script:outOfScopeSubscriptions.Add([PSCustomObject]@{
                            subscriptionId = $childrenSubscription.name
                            subscriptionName = $childrenSubscription.properties.displayName
                            outOfScopeReason = "QuotaId: '$($sub.subDetails.subscriptionPolicies.quotaId)' not in Whitelist"
                            ManagementGroupId = $htSubscriptionsMgPath.($childrenSubscription.name).Parent
                            ManagementGroupName = $htSubscriptionsMgPath.($childrenSubscription.name).ParentName
                            Level = $htSubscriptionsMgPath.($childrenSubscription.name).level
                        })
                }
            }
            else {
                $null = $script:subsToProcessInCustomDataCollection.Add([PSCustomObject]@{
                        subscriptionId = $childrenSubscription.name
                        subscriptionName = $childrenSubscription.properties.displayName
                        subscriptionQuotaId = $sub.subDetails.subscriptionPolicies.quotaId
                    })
            }
        }
    }
    $subsToProcessInCustomDataCollectionCount = ($subsToProcessInCustomDataCollection | Measure-Object).Count
    #endregion subscriptionFilter

    #region dataprocessingDefinitionCaching
    $startDefinitionsCaching = Get-Date

    $currentTask = 'Caching built-in Role definitions'
    Write-Host " $currentTask"
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/subscriptions/$($azAPICallConf['checkContext'].Subscription.Id)/providers/Microsoft.Authorization/roleDefinitions?api-version=2018-07-01&`$filter=type eq 'BuiltInRole'"
    $method = 'GET'
    $requestRoleDefinitionAPI = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

    foreach ($roleDefinition in $requestRoleDefinitionAPI) {

        if (
            (
                $roleDefinition.properties.permissions.actions -contains 'Microsoft.Authorization/roleassignments/write' -or
                $roleDefinition.properties.permissions.actions -contains 'Microsoft.Authorization/roleassignments/*' -or
                $roleDefinition.properties.permissions.actions -contains 'Microsoft.Authorization/*/write' -or
                $roleDefinition.properties.permissions.actions -contains 'Microsoft.Authorization/*' -or
                $roleDefinition.properties.permissions.actions -contains '*/write' -or
                $roleDefinition.properties.permissions.actions -contains '*'
            ) -and (
                $roleDefinition.properties.permissions.notActions -notcontains 'Microsoft.Authorization/roleassignments/write' -and
                $roleDefinition.properties.permissions.notActions -notcontains 'Microsoft.Authorization/roleassignments/*' -and
                $roleDefinition.properties.permissions.notActions -notcontains 'Microsoft.Authorization/*/write' -and
                $roleDefinition.properties.permissions.notActions -notcontains 'Microsoft.Authorization/*' -and
                $roleDefinition.properties.permissions.notActions -notcontains '*/write' -and
                $roleDefinition.properties.permissions.notActions -notcontains '*'
            )
        ) {
            $roleCapable4RoleAssignmentsWrite = $true
        }
        else {
            $roleCapable4RoleAssignmentsWrite = $false
        }

        ($htCacheDefinitionsRole).($roleDefinition.name) = @{}
        ($htCacheDefinitionsRole).($roleDefinition.name).definition = ($roleDefinition)
        ($htCacheDefinitionsRole).($roleDefinition.name).linkToAzAdvertizer = "<a class=`"externallink`" href=`"https://www.azadvertizer.net/azrolesadvertizer/$($roleDefinition.name).html`" target=`"_blank`">$($roleDefinition.properties.roleName)</a>"
        ($htCacheDefinitionsRole).($roleDefinition.name).roleIsCritical = $roleCapable4RoleAssignmentsWrite
    }

    $endDefinitionsCaching = Get-Date
    Write-Host "Caching built-in definitions duration: $((New-TimeSpan -Start $startDefinitionsCaching -End $endDefinitionsCaching).TotalSeconds) seconds"
    #endregion dataprocessingDefinitionCaching


    #$arrayEntitiesFromAPISubscriptionsCount = ($arrayEntitiesFromAPI | Where-Object { $_.type -eq '/subscriptions' -and $_.properties.parentNameChain -contains $ManagementGroupId } | Sort-Object -Property id -Unique | Measure-Object).count
    #$arrayEntitiesFromAPIManagementGroupsCount = ($arrayEntitiesFromAPI | Where-Object { $_.type -eq 'Microsoft.Management/managementGroups' -and $_.properties.parentNameChain -contains $ManagementGroupId } | Sort-Object -Property id -Unique | Measure-Object).count + 1

    Write-Host 'Collecting custom data'
    $startDataCollection = Get-Date

    dataCollection -mgId $ManagementGroupId

    #region dataColletionAz summary
    $endDataCollection = Get-Date
    Write-Host "Collecting custom data duration: $((New-TimeSpan -Start $startDataCollection -End $endDataCollection).TotalMinutes) minutes ($((New-TimeSpan -Start $startDataCollection -End $endDataCollection).TotalSeconds) seconds)"

    $durationDataMG = ($customDataCollectionDuration | Where-Object { $_.Type -eq 'MG' })
    $durationDataSUB = ($customDataCollectionDuration | Where-Object { $_.Type -eq 'SUB' })
    $durationMGAverageMaxMin = ($durationDataMG.DurationSec | Measure-Object -Average -Maximum -Minimum)
    $durationSUBAverageMaxMin = ($durationDataSUB.DurationSec | Measure-Object -Average -Maximum -Minimum)
    Write-Host "Collecting custom data for $($allManagementGroupsFromEntitiesChildOfRequestedMgCount) ManagementGroups Avg/Max/Min duration in seconds: Average: $([math]::Round($durationMGAverageMaxMin.Average,4)); Maximum: $([math]::Round($durationMGAverageMaxMin.Maximum,4)); Minimum: $([math]::Round($durationMGAverageMaxMin.Minimum,4))"
    Write-Host "Collecting custom data for $($subsToProcessInCustomDataCollection.Count) Subscriptions Avg/Max/Min duration in seconds: Average: $([math]::Round($durationSUBAverageMaxMin.Average,4)); Maximum: $([math]::Round($durationSUBAverageMaxMin.Maximum,4)); Minimum: $([math]::Round($durationSUBAverageMaxMin.Minimum,4))"


    $APICallTrackingCount = ($arrayAPICallTrackingCustomDataCollection | Measure-Object).Count
    $APICallTrackingRetriesCount = ($arrayAPICallTrackingCustomDataCollection | Where-Object { $_.TryCounter -gt 0 } | Measure-Object).Count
    $APICallTrackingRestartDueToDuplicateNextlinkCounterCount = ($arrayAPICallTrackingCustomDataCollection | Where-Object { $_.RestartDueToDuplicateNextlinkCounter -gt 0 } | Measure-Object).Count
    Write-Host "Collecting custom data APICalls (Management) total count: $APICallTrackingCount ($APICallTrackingRetriesCount retries; $APICallTrackingRestartDueToDuplicateNextlinkCounterCount nextLinkReset)"
    #endregion dataColletionAz summary

}
else {

    Write-Host 'Permission check results'
    foreach ($permissionCheckResult in $permissionCheckResults) {
        if ($permissionCheckResult -like '*PASSED*') {
            Write-Host $permissionCheckResult -ForegroundColor Green
        }
        else {
            Write-Host $permissionCheckResult -ForegroundColor DarkRed
        }
    }

    if ($permissionsCheckFailed -eq $true) {
        Write-Host "Please consult the documentation: https://$($azAPICallConf['htParameters'].gitHubRepository)#required-permissions-in-azure"
        if ($azAPICallConf['htParameters'].onAzureDevOps -eq $true) {
            Write-Error 'Error'
        }
        else {
            Throw 'Error - check the last console output for details'
        }
    }
    Write-Host "Running $($Product) without resolving Role assignments in Azure" -ForegroundColor Yellow
}

#region AADSP

#PW in this region the data gets collected (search: ForEach-Object -Parallel)
#region dataColletionAADSP
$startSP = Get-Date
Write-Host 'Getting Service Principal count'
$currentTask = 'getSPCount'
$uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/servicePrincipals/`$count"
$method = 'GET'
$spCount = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -listenOn 'Content' -consistencyLevel 'eventual'

Write-Host "API `$Count returned $spCount Service Principals"

$currentTask = 'Get all Service Principals'
$uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/servicePrincipals"
$method = 'GET'
$getServicePrincipalsFromAPI = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -consistencyLevel 'eventual'

Write-Host "API returned count: $($getServicePrincipalsFromAPI.Count)"
$getServicePrincipals = $getServicePrincipalsFromAPI | Sort-Object -Property id -Unique
Write-Host "Sorting unique by Id count: $($getServicePrincipalsFromAPI.Count)"
$endSP = Get-Date
$duration = New-TimeSpan -Start $startSP -End $endSP
Write-Host "Getting $($getServicePrincipals.Count) Service Principals duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"

if ($getServicePrincipals.Count -eq 0) {
    throw 'No SPs found'
}
else {
    $htServicePrincipalsAndAppsOnlyEnriched = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htServicePrincipalsAppRoles = @{}
    $htServicePrincipalsPublishedPermissionScopes = @{}
    $htAppRoles = @{}
    $htPublishedPermissionScopes = @{}
    $htAadGroupsToResolve = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htAppRoleAssignments = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htSPOauth2PermissionGrantedTo = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htUsersAndGroupsToCheck4AppRoleAssignmentsUser = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htUsersAndGroupsToCheck4AppRoleAssignmentsGroup = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htApplications = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htSPOwners = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htAppOwners = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htFederatedIdentityCredentials = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htOwnedBy = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htProcessedTracker = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htMeanwhileDeleted = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htSpLookup = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htPrincipalsResolved = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    Write-Host 'Creating mapping AppRoles & PublishedPermissionScopes'
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

    Write-Host 'Getting all AAD Role definitions'
    $currentTask = 'get AAD RoleDefinitions'
    $htAadRoleDefinitions = @{}
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/roleManagement/directory/roleDefinitions"
    $method = 'GET'
    $aadRoleDefinitions = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

    foreach ($aadRoleDefinition in $aadRoleDefinitions) {
        $htAadRoleDefinitions.($aadRoleDefinition.id) = $aadRoleDefinition
    }

    <# Not needed
    Write-Host 'Validating Identity Governance state'
    $currentTask = 'Validate roleAssignmentScheduleInstance'
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/roleManagement/directory/roleAssignmentScheduleInstances?`$count=true&`$top=1"
    $method = 'GET'
    $getRoleAssignmentScheduleInstance = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -validateAccess -noPaging
    if ($getRoleAssignmentScheduleInstance -eq 'InvalidResource') {
        Write-Host 'Identity Governance state (roleAssignmentScheduleInstance): n/a'
        $identityGovernance = 'false'
    }
    else {
        Write-Host 'Identity Governance state (roleAssignmentScheduleInstance): available'
        $identityGovernance = 'true'
    }

    $currentTask = 'Validate roleAssignmentSchedules'
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/roleManagement/directory/roleAssignmentSchedules?`$count=true&`$top=1"
    $method = 'GET'
    $getRoleAssignmentSchedules = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -validateAccess -noPaging
    if ($getRoleAssignmentSchedules -eq 'InvalidResource') {
        Write-Host 'Identity Governance state (roleAssignmentSchedules): n/a'
        $identityGovernance = 'false'
    }
    else {
        Write-Host 'Identity Governance state (roleAssignmentSchedules): available'
        $identityGovernance = 'true'
    }
    #>

    $arraySPsAndAppsWithoutSP = [System.Collections.ArrayList]@()
    if ($OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes) {
        $spsWithAzureResourceRoleAssignmentUnique = ($htCacheAssignmentsRole).values.assignment.properties.principalId | Sort-Object -Unique
    }
    foreach ($sp in $getServicePrincipals) {
        if ($OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes) {
            if ($spsWithAzureResourceRoleAssignmentUnique -contains $sp.id) {
                $null = $arraySPsAndAppsWithoutSP.Add([PSCustomObject]@{
                        SPOrAppWithoutSP = 'SP'
                        Details = $sp
                    })
            }
        }
        else {
            $null = $arraySPsAndAppsWithoutSP.Add([PSCustomObject]@{
                    SPOrAppWithoutSP = 'SP'
                    Details = $sp
                })
        }

    }

    if ($OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes) {
        Write-Host "Process $($arraySPsAndAppsWithoutSP.Count) of $($getServicePrincipals.Count) SPs due to parameter -OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes $OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes"
    }

    if ($OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes) {
        Write-Host "Skipping AppsOnly due to parameter -OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes $OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes"
    }
    else {
        Write-Host 'Getting Applications count'
        $currentTask = 'getAppCount'
        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/applications/`$count"
        $method = 'GET'
        $appCount = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -listenOn 'Content' -consistencyLevel 'eventual'

        Write-Host "API `$Count returned $appCount Applications"
        $spWithAppCount = ($getServicePrincipals.where( { $_.servicePrincipalType -eq 'Application' -and $_.appOwnerOrganizationId -eq $azAPICallConf['checkContext'].tenant.Id } )).appid.count
        if ($appCount -gt $spWithAppCount) {
            $appsWithoutSPCount = $appCount - $spWithAppCount
            Write-Host "$($appsWithoutSPCount) Applications without ServicePrincipal present!"

            $currentTask = 'Get all Applications'
            $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/applications"
            $method = 'GET'
            $getApplicationsFromAPI = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -consistencyLevel 'eventual'

            $cnt = 0
            foreach ($application in $getApplicationsFromAPI) {
                #Write-Host "proc $application"
                if ($getServicePrincipals.appid -notcontains $application.appid) {
                    $cnt++
                    #Write-Host "Application without SP: $($application.displayName) id:$($application.id) appId:$($application.appid) ($($application.signInAudience))"
                    $null = $arraySPsAndAppsWithoutSP.Add([PSCustomObject]@{
                            SPOrAppWithoutSP = 'AppWithoutSP'
                            Details = $application
                        })
                }
            }
            Write-Host "$cnt Applications collected"
        }
    }

    Write-Host "Collecting data for $($arraySPsAndAppsWithoutSP.Count) Service Principals/Applications"
    $startForeachSP = Get-Date

    switch ($arraySPsAndAppsWithoutSP.Count) {
        { $_ -gt 0 } { $indicator = 1 }
        { $_ -gt 10 } { $indicator = 5 }
        { $_ -gt 50 } { $indicator = 10 }
        { $_ -gt 100 } { $indicator = 20 }
        { $_ -gt 250 } { $indicator = 25 }
        { $_ -gt 500 } { $indicator = 50 }
        { $_ -gt 1000 } { $indicator = 100 }
        { $_ -gt 10000 } { $indicator = 250 }
    }

    #$arraySPsAndAppsWithoutSP.where( { $_.SPOrAppWithoutSP -eq "AppWithoutSP" } ) | ForEach-Object -Parallel {
    $arraySPsAndAppsWithoutSP | ForEach-Object -Parallel {
        $spOrAppWithoutSP = $_

        #AzAPICall
        $azAPICallConf = $using:azAPICallConf
        $scriptPath = $using:ScriptPath
        #array&ht
        $htServicePrincipalsAndAppsOnlyEnriched = $using:htServicePrincipalsAndAppsOnlyEnriched
        $htServicePrincipalsAppRoles = $using:htServicePrincipalsAppRoles
        $htAppRoles = $using:htAppRoles
        $htServicePrincipalsPublishedPermissionScopes = $using:htServicePrincipalsPublishedPermissionScopes
        $htPublishedPermissionScopes = $using:htPublishedPermissionScopes
        $htAadRoleDefinitions = $using:htAadRoleDefinitions
        $htAadGroupsToResolve = $using:htAadGroupsToResolve
        $htAppRoleAssignments = $using:htAppRoleAssignments
        $htSPOauth2PermissionGrantedTo = $using:htSPOauth2PermissionGrantedTo
        $htUsersAndGroupsToCheck4AppRoleAssignmentsUser = $using:htUsersAndGroupsToCheck4AppRoleAssignmentsUser
        $htUsersAndGroupsToCheck4AppRoleAssignmentsGroup = $using:htUsersAndGroupsToCheck4AppRoleAssignmentsGroup
        $htApplications = $using:htApplications
        $indicator = $using:indicator
        $htSPOwners = $using:htSPOwners
        $htAppOwners = $using:htAppOwners
        $htFederatedIdentityCredentials = $using:htFederatedIdentityCredentials
        $htOwnedBy = $using:htOwnedBy
        $htProcessedTracker = $using:htProcessedTracker
        $htMeanwhileDeleted = $using:htMeanwhileDeleted
        $htSpLookup = $using:htSpLookup
        $htPrincipalsResolved = $using:htPrincipalsResolved

        if ($azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions) {
            Import-Module ".\$($scriptPath)\AzAPICallModule\AzAPICall\$($azAPICallConf['htParameters'].azAPICallModuleVersion)\AzAPICall.psd1" -Force -ErrorAction Stop
        }
        else {
            Import-Module -Name AzAPICall -RequiredVersion $azAPICallConf['htParameters'].azAPICallModuleVersion -Force -ErrorAction Stop
        }
        #var
        #$identityGovernance = $using:identityGovernance

        #write-host "processing $($object.id) - $($object.displayName) (type: $($object.servicePrincipalType) org: $($object.appOwnerOrganizationId))"

        $meanwhileDeleted = $false
        #write-host $spOrAppWithoutSP.SPOrAppWithoutSP
        if ($spOrAppWithoutSP.SPOrAppWithoutSP -eq 'SP') {
            $hlperType = 'SP'
            $object = $spOrAppWithoutSP.Details

            $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id) = [ordered] @{}
            #$script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).ServicePrincipal = [ordered] @{}
            $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).ServicePrincipalDetails = $object
            $script:htSpLookup.($object.id) = @{}
        }
        else {
            $hlperType = 'AppOnly'
            #   write-host "here"
            #$spOrAppWithoutSP
            $object = $spOrAppWithoutSP.Details
            $script:htServicePrincipalsAndAppsOnlyEnriched.("AppWithoutSP_$($object.id)") = [ordered] @{}
            #$script:htServicePrincipalsAndAppsOnlyEnriched.("AppWithoutSP_"$object.id).ServicePrincipal = [ordered] @{}
            #$script:htServicePrincipalsAndAppsOnlyEnriched.("AppWithoutSP_"$object.id).ServicePrincipalDetails = $object
            $script:htSpLookup.($object.id) = @{}
        }

        if ($hlperType -eq 'SP') {

            if ($object.appOwnerOrganizationId -eq $azAPICallConf['checkContext'].Tenant.Id) {
                $spTypeINTEXT = 'INT'
            }
            else {
                $spTypeINTEXT = 'EXT'
            }

            #region spownedObjects
            $currentTask = "getSP OwnedObjects $($object.id)"
            $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/servicePrincipals/$($object.id)/ownedObjects"
            $method = 'GET'
            $getSPOwnedObjects = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

            if ($getSPOwnedObjects -eq 'Request_ResourceNotFound') {
                if (-not $htMeanwhileDeleted.($object.id)) {
                    Write-Host "  $($object.displayName) ($($object.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                    $script:htMeanwhileDeleted.($object.id) = @{}
                    $meanwhileDeleted = $true
                }
            }
            else {
                if ($getSPOwnedObjects.Count -gt 0) {
                    $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).ServicePrincipalOwnedObjects = $getSPOwnedObjects | Select-Object '@odata.type', displayName, id
                }
            }
            #endregion spownedObjects

            #region spAADRoleAssignments
            #if ($identityGovernance -eq "false"){
            if (-not $meanwhileDeleted) {
                $currentTask = "getSP AADRoleAssignments $($object.id)"
                #v1 does not return principalOrganizationId, resourceScope
                $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/roleManagement/directory/roleAssignments?`$filter=principalId eq '$($object.id)'"
                $method = 'GET'
                $getSPAADRoleAssignments = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

                if ($getSPAADRoleAssignments -eq 'Request_ResourceNotFound') {
                    if (-not $htMeanwhileDeleted.($object.id)) {
                        Write-Host "  $($object.displayName) ($($object.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                        $script:htMeanwhileDeleted.($object.id) = @{}
                        $meanwhileDeleted = $true
                    }
                }
                else {
                    if ($getSPAADRoleAssignments.Count -gt 0) {
                        $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).ServicePrincipalAADRoleAssignments = $getSPAADRoleAssignments
                    }
                }
            }
            #}
            #endregion spAADRoleAssignments

            #test later
            if (1 -ne 1) {
                if ($identityGovernance -eq 'true') {
                    #region AADRoleAssignmentSchedules
                    if (-not $meanwhileDeleted) {
                        $currentTask = "getSP AADRoleAssignmentSchedules $($object.id)"
                        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/roleManagement/directory/roleAssignmentSchedules?`$filter=principalId eq '$($object.id)'"
                        $method = 'GET'
                        $getSPAADRoleAssignmentSchedules = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

                        if ($getSPAADRoleAssignmentSchedules -eq 'Request_ResourceNotFound') {
                            if (-not $htMeanwhileDeleted.($object.id)) {
                                Write-Host "  $($object.displayName) ($($object.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                                $script:htMeanwhileDeleted.($object.id) = @{}
                                $meanwhileDeleted = $true
                            }
                        }
                        else {
                            if ($getSPAADRoleAssignmentSchedules.Count -gt 0) {
                                $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).ServicePrincipalAADRoleAssignmentSchedules = $getSPAADRoleAssignmentSchedules
                            }
                        }
                    }
                    #endregion AADRoleAssignmentSchedules

                    #region AADRoleAssignmentScheduleInstances
                    if (-not $meanwhileDeleted) {
                        $currentTask = "getSP AADRoleAssignmentScheduleInstances $($object.id)"
                        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/roleManagement/directory/roleAssignmentScheduleInstances?`$filter=principalId eq '$($object.id)'"
                        $method = 'GET'
                        $getSPAADRoleAssignmentScheduleInstances = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

                        if ($getSPAADRoleAssignmentScheduleInstances -eq 'Request_ResourceNotFound') {
                            if (-not $htMeanwhileDeleted.($object.id)) {
                                Write-Host "  $($object.displayName) ($($object.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                                $script:htMeanwhileDeleted.($object.id) = @{}
                                $meanwhileDeleted = $true
                            }
                        }
                        else {
                            if ($getSPAADRoleAssignmentScheduleInstances.Count -gt 0) {
                                $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).ServicePrincipalAADRoleAssignmentScheduleInstances = $getSPAADRoleAssignmentScheduleInstances
                            }
                        }
                    }
                    #endregion AADRoleAssignmentScheduleInstances
                }
            }

            #region spAppRoleAssignments
            if (-not $meanwhileDeleted) {
                $currentTask = "getSP AppRoleAssignments $($object.id)"
                $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/servicePrincipals/$($object.id)/appRoleAssignments"
                $method = 'GET'
                $getSPAppRoleAssignments = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

                if ($getSPAppRoleAssignments -eq 'Request_ResourceNotFound') {
                    if (-not $htMeanwhileDeleted.($object.id)) {
                        Write-Host "  $($object.displayName) ($($object.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                        $script:htMeanwhileDeleted.($object.id) = @{}
                        $meanwhileDeleted = $true
                    }
                }
                else {
                    if ($getSPAppRoleAssignments.Count -gt 0) {
                        $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).ServicePrincipalAppRoleAssignments = $getSPAppRoleAssignments
                        foreach ($SPAppRoleAssignment in $getSPAppRoleAssignments) {
                            if (-not $htAppRoleAssignments.($SPAppRoleAssignment.id)) {
                                $script:htAppRoleAssignments.($SPAppRoleAssignment.id) = $SPAppRoleAssignment
                            }
                        }
                    }
                }
            }
            #endregion spAppRoleAssignments

            #region SPAADRoleAssignedOn
            if (-not $meanwhileDeleted) {
                $currentTask = "getSP AADRoleAssignedOn $($object.id)"
                $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/roleManagement/directory/roleAssignments?`$filter=resourceScope eq '/$($object.id)'&`$expand=principal"
                $method = 'GET'
                $getSPAADRoleAssignedOn = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask
                if ($getSPAADRoleAssignedOn.Count -gt 0) {
                    $tmpArray = [System.Collections.ArrayList]@()
                    foreach ($SPAADRoleAssignedOn in $getSPAADRoleAssignedOn) {
                        if ($SPAADRoleAssignedOn.principal.'@odata.type' -eq '#microsoft.graph.user') {

                            if ([string]::IsNullOrEmpty($SPAADRoleAssignedOn.principal.userType)) {
                                $principalUserType = 'MemberSynced'
                            }
                            else {
                                $principalUserType = $SPAADRoleAssignedOn.principal.userType
                            }
                            if (-not $htPrincipalsResolved.($SPAADRoleAssignedOn.principal.id)) {
                                $type = 'User'
                                $script:htPrincipalsResolved.($SPAADRoleAssignedOn.principal.id) = @{}
                                $script:htPrincipalsResolved.($SPAADRoleAssignedOn.principal.id).full = "$($type) ($($principalUserType)), DisplayName: $($SPAADRoleAssignedOn.principal.displayName), Id: $(($SPAADRoleAssignedOn.principal.id))"
                                $script:htPrincipalsResolved.($SPAADRoleAssignedOn.principal.id).typeOnly = "$($type) ($($principalUserType))"
                            }

                            $null = $tmpArray.Add([PSCustomObject]@{
                                    id = $SPAADRoleAssignedOn.id
                                    principalId = $SPAADRoleAssignedOn.principalId
                                    principalOrganizationId = $SPAADRoleAssignedOn.principalOrganizationId
                                    resourceScope = $SPAADRoleAssignedOn.resourceScope
                                    directoryScopeId = $SPAADRoleAssignedOn.directoryScopeId
                                    roleDefinitionId = $SPAADRoleAssignedOn.roleDefinitionId
                                    principalType = 'User'
                                    principalUserType = "User ($($principalUserType))"
                                    principalDisplayName = $SPAADRoleAssignedOn.principal.displayName
                                })
                        }
                        elseif ($SPAADRoleAssignedOn.principal.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
                            $null = $tmpArray.Add([PSCustomObject]@{
                                    id = $SPAADRoleAssignedOn.id
                                    principalId = $SPAADRoleAssignedOn.principalId
                                    principalOrganizationId = $SPAADRoleAssignedOn.principalOrganizationId
                                    resourceScope = $SPAADRoleAssignedOn.resourceScope
                                    directoryScopeId = $SPAADRoleAssignedOn.directoryScopeId
                                    roleDefinitionId = $SPAADRoleAssignedOn.roleDefinitionId
                                    principalType = 'ServicePrincipal'
                                    principalDisplayName = $SPAADRoleAssignedOn.principal.displayName
                                })
                        }
                        else {
                            $null = $tmpArray.Add([PSCustomObject]@{
                                    id = $SPAADRoleAssignedOn.id
                                    principalId = $SPAADRoleAssignedOn.principalId
                                    principalOrganizationId = $SPAADRoleAssignedOn.principalOrganizationId
                                    resourceScope = $SPAADRoleAssignedOn.resourceScope
                                    directoryScopeId = $SPAADRoleAssignedOn.directoryScopeId
                                    roleDefinitionId = $SPAADRoleAssignedOn.roleDefinitionId
                                    principalType = $SPAADRoleAssignedOn.principal.'@odata.type'
                                    principalDisplayName = $SPAADRoleAssignedOn.principal.displayName
                                })
                        }

                    }
                    $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).ServicePrincipalAADRoleAssignedOn = $tmpArray
                }
            }
            #endregion SPAADRoleAssignedOn

            #region spAppRoleAssignedTo
            if (-not $meanwhileDeleted) {
                $currentTask = "getSP appRoleAssignedTo $($object.id)"
                $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/servicePrincipals/$($object.id)/appRoleAssignedTo"
                $method = 'GET'
                $getSPAppRoleAssignedTo = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

                if ($getSPAppRoleAssignedTo -eq 'Request_ResourceNotFound') {
                    if (-not $htMeanwhileDeleted.($object.id)) {
                        Write-Host "  $($object.displayName) ($($object.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                        $script:htMeanwhileDeleted.($object.id) = @{}
                        $meanwhileDeleted = $true
                    }
                }
                else {
                    if ($getSPAppRoleAssignedTo.Count -gt 0) {
                        $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).ServicePrincipalAppRoleAssignedTo = $getSPAppRoleAssignedTo
                        foreach ($SPAppRoleAssignedTo in $getSPAppRoleAssignedTo) {
                            if ($SPAppRoleAssignedTo.principalType -eq 'User' -or $SPAppRoleAssignedTo.principalType -eq 'Group') {
                                if ($SPAppRoleAssignedTo.principalType -eq 'User') {
                                    if (-not $htUsersAndGroupsToCheck4AppRoleAssignmentsUser.($SPAppRoleAssignedTo.principalId)) {
                                        $script:htUsersAndGroupsToCheck4AppRoleAssignmentsUser.($SPAppRoleAssignedTo.principalId) = @{}
                                    }
                                }
                                if ($SPAppRoleAssignedTo.principalType -eq 'Group') {
                                    if (-not $htUsersAndGroupsToCheck4AppRoleAssignmentsGroup.($SPAppRoleAssignedTo.principalId)) {
                                        $script:htUsersAndGroupsToCheck4AppRoleAssignmentsGroup.($SPAppRoleAssignedTo.principalId) = @{}
                                    }
                                }
                            }
                        }
                    }
                }
            }
            #endregion spAppRoleAssignedTo

            #region spGetMemberGroups
            if (-not $meanwhileDeleted) {
                $currentTask = "getSP GroupMemberships $($object.id)"
                $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/servicePrincipals/$($object.id)/getMemberGroups"
                $method = 'POST'
                $body = @'
        {
            "securityEnabledOnly": false
        }
'@
                $getSPGroupMemberships = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -body $body -currentTask $currentTask

                if ($getSPGroupMemberships -eq 'Request_ResourceNotFound') {
                    if (-not $htMeanwhileDeleted.($object.id)) {
                        Write-Host "  $($object.displayName) ($($object.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                        $script:htMeanwhileDeleted.($object.id) = @{}
                        $meanwhileDeleted = $true
                    }
                }
                elseif ($getSPGroupMemberships -eq 'Directory_ResultSizeLimitExceeded') {
                    Write-Host 'Directory_ResultSizeLimitExceeded - skipping for now'
                }
                else {
                    if ($getSPGroupMemberships.Count -gt 0) {
                        $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).ServicePrincipalGroupMemberships = $getSPGroupMemberships
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
            if (-not $meanwhileDeleted) {
                $currentTask = "getSP oauth2PermissionGrants $($object.id)"
                #v1 does not return startTime, expiryTime
                $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/servicePrincipals/$($object.id)/oauth2PermissionGrants"
                $method = 'GET'
                $getSPOauth2PermissionGrants = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

                if ($getSPOauth2PermissionGrants -eq 'Request_ResourceNotFound') {
                    if (-not $htMeanwhileDeleted.($object.id)) {
                        Write-Host "  $($object.displayName) ($($object.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                        $script:htMeanwhileDeleted.($object.id) = @{}
                        $meanwhileDeleted = $true
                    }
                }
                else {
                    if ($getSPOauth2PermissionGrants.Count -gt 0) {
                        $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).ServicePrincipalOauth2PermissionGrants = $getSPOauth2PermissionGrants
                        foreach ($permissionGrant in $getSPOauth2PermissionGrants) {
                            $splitPermissionGrant = ($permissionGrant.scope).split(' ')
                            foreach ($permissionscope in $splitPermissionGrant) {
                                if (-not [string]::IsNullOrEmpty($permissionscope) -and -not [string]::IsNullOrWhiteSpace($permissionscope)) {
                                    $permissionGrantArray = [System.Collections.ArrayList]@()
                                    $null = $permissionGrantArray.Add([PSCustomObject]@{
                                            '@odata.id' = $permissionGrant
                                            clientId = $permissionGrant.clientId
                                            consentType = $permissionGrant.consentType
                                            expiryTime = $permissionGrant.expiryTime
                                            id = $permissionGrant.id
                                            principalId = $permissionGrant.principalId
                                            resourceId = $permissionGrant.resourceId
                                            scope = $permissionscope
                                            startTime = $permissionGrant.startTime
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
    if ($object.servicePrincipalType -eq "Application") {

        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/servicePrincipals/$($object.id)/delegatedPermissionClassifications"
        $currentTask = $uri
        $method = "GET"
        $getSPDelegatedPermissionClassifications = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -listenOn "Content"
        Write-Host "$($object.id) --> $($getSPDelegatedPermissionClassifications.Count)"
        if ($getSPDelegatedPermissionClassifications.Count -gt 0){
            foreach ($delegatedPermissionClassification in $getSPDelegatedPermissionClassifications){
                $delegatedPermissionClassification
                #Write-Host "$($object.displayName) owns: $($ownedObject.'@odata.type') - $($ownedObject.displayName) ($($ownedObject.id))"
            }
        }
    }
    #>

            #region spOwner
            if (-not $meanwhileDeleted) {
                $currentTask = "getSPOwner $($object.id)"
                $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/servicePrincipals/$($object.id)/owners"
                $method = 'GET'
                $getSPOwner = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

                if ($getSPOwner -eq 'Request_ResourceNotFound') {
                    if (-not $htMeanwhileDeleted.($object.id)) {
                        Write-Host "  $($object.displayName) ($($object.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                        $script:htMeanwhileDeleted.($object.id) = @{}
                        $meanwhileDeleted = $true
                    }
                }
                else {
                    if ($getSPOwner.Count -gt 0) {
                        foreach ($spOwner in $getSPOwner) {

                            if (-not $htOwnedBy.($object.id)) {
                                $script:htOwnedBy.($object.id) = @{}
                                $script:htOwnedBy.($object.id).ownedBy = [array]$($spOwner | Select-Object id, displayName, '@odata.type')
                            }
                            else {
                                $array = [array]($htOwnedBy.($object.id).ownedBy)
                                $array += $spOwner | Select-Object id, displayName, '@odata.type'
                                $script:htOwnedBy.($object.id).ownedBy = $array
                            }
                        }
                        if (-not $htSPOwners.($object.id)) {
                            $script:htSPOwners.($object.id) = $getSPOwner | Select-Object id, displayName, '@odata.type'
                        }
                    }
                    else {
                        $script:htOwnedBy.($object.id) = @{}
                        $script:htOwnedBy.($object.id).ownedBy = 'noOwner'
                    }
                }
            }
            #endregion spOwner
        }

        #region spApp
        if (-not $meanwhileDeleted) {
            if ($object.servicePrincipalType -eq 'Application' -or $hlperType -eq 'AppOnly') {

                if ($object.servicePrincipalType -eq 'Application') {
                    $spType = 'APP'
                }

                $currentTask = "getApp $($object.appId)"
                $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/applications?`$filter=appId eq '$($object.appId)'"
                $method = 'GET'
                $getApplication = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

                if ($getApplication -eq 'Request_ResourceNotFound') {
                    if (-not $htMeanwhileDeleted.($object.id)) {
                        Write-Host "  $($object.displayName) ($($object.id)) AppId $($object.appId) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                        $script:htMeanwhileDeleted.($object.id) = @{}
                        $meanwhileDeleted = $true
                    }
                }
                else {
                    if ($getApplication.Count -gt 0) {
                        if ($object.servicePrincipalType -eq 'Application') {
                            $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).Application = @{}
                            $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).Application.ApplicationDetails = $getApplication
                        }
                        if ($hlperType -eq 'AppOnly') {
                            $script:htServicePrincipalsAndAppsOnlyEnriched.("AppWithoutSP_$($object.id)").Application = @{}
                            $script:htServicePrincipalsAndAppsOnlyEnriched.("AppWithoutSP_$($object.id)").Application.ApplicationDetails = $getApplication
                        }

                        $script:htApplications.($getApplication.id) = $getApplication

                        #region AppAADRoleAssignedOn
                        $currentTask = "getApp AADRoleAssignedOn $($getApplication.id)"
                        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/roleManagement/directory/roleAssignments?`$filter=resourceScope eq '/$($getApplication.id)'&`$expand=principal"
                        $method = 'GET'
                        $getAppAADRoleAssignedOn = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask
                        if ($getAppAADRoleAssignedOn.Count -gt 0) {
                            $tmpArray = [System.Collections.ArrayList]@()
                            foreach ($AppAADRoleAssignedOn in $getAppAADRoleAssignedOn) {
                                if ($AppAADRoleAssignedOn.principal.'@odata.type' -eq '#microsoft.graph.user') {

                                    if ([string]::IsNullOrEmpty($AppAADRoleAssignedOn.principal.userType)) {
                                        $principalUserType = 'MemberSynced'
                                    }
                                    else {
                                        $principalUserType = $AppAADRoleAssignedOn.principal.userType
                                    }
                                    if (-not $htPrincipalsResolved.($AppAADRoleAssignedOn.principal.id)) {
                                        $type = 'User'
                                        $script:htPrincipalsResolved.($AppAADRoleAssignedOn.principal.id) = @{}
                                        $script:htPrincipalsResolved.($AppAADRoleAssignedOn.principal.id).full = "$($type) ($($principalUserType)), DisplayName: $($AppAADRoleAssignedOn.principal.displayName), Id: $(($AppAADRoleAssignedOn.principal.id))"
                                        $script:htPrincipalsResolved.($AppAADRoleAssignedOn.principal.id).typeOnly = "$($type) ($($principalUserType))"
                                    }

                                    $null = $tmpArray.Add([PSCustomObject]@{
                                            id = $AppAADRoleAssignedOn.id
                                            principalId = $AppAADRoleAssignedOn.principalId
                                            principalOrganizationId = $AppAADRoleAssignedOn.principalOrganizationId
                                            resourceScope = $AppAADRoleAssignedOn.resourceScope
                                            directoryScopeId = $AppAADRoleAssignedOn.directoryScopeId
                                            roleDefinitionId = $AppAADRoleAssignedOn.roleDefinitionId
                                            principalType = 'User'
                                            principalUserType = "User ($($principalUserType))"
                                            principalDisplayName = $AppAADRoleAssignedOn.principal.displayName
                                        })
                                }
                                elseif ($AppAADRoleAssignedOn.principal.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
                                    $null = $tmpArray.Add([PSCustomObject]@{
                                            id = $AppAADRoleAssignedOn.id
                                            principalId = $AppAADRoleAssignedOn.principalId
                                            principalOrganizationId = $AppAADRoleAssignedOn.principalOrganizationId
                                            resourceScope = $AppAADRoleAssignedOn.resourceScope
                                            directoryScopeId = $AppAADRoleAssignedOn.directoryScopeId
                                            roleDefinitionId = $AppAADRoleAssignedOn.roleDefinitionId
                                            principalType = 'ServicePrincipal'
                                            principalDisplayName = $AppAADRoleAssignedOn.principal.displayName
                                        })
                                }
                                else {
                                    $null = $tmpArray.Add([PSCustomObject]@{
                                            id = $AppAADRoleAssignedOn.id
                                            principalId = $AppAADRoleAssignedOn.principalId
                                            principalOrganizationId = $AppAADRoleAssignedOn.principalOrganizationId
                                            resourceScope = $AppAADRoleAssignedOn.resourceScope
                                            directoryScopeId = $AppAADRoleAssignedOn.directoryScopeId
                                            roleDefinitionId = $AppAADRoleAssignedOn.roleDefinitionId
                                            principalType = $AppAADRoleAssignedOn.principal.'@odata.type'
                                            principalDisplayName = $AppAADRoleAssignedOn.principal.displayName
                                        })
                                }

                            }
                            if ($object.servicePrincipalType -eq 'Application') {
                                $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).Application.ApplicationAADRoleAssignedOn = $tmpArray
                            }
                            if ($hlperType -eq 'AppOnly') {
                                $script:htServicePrincipalsAndAppsOnlyEnriched.("AppWithoutSP_$($object.id)").Application.ApplicationAADRoleAssignedOn = $tmpArray
                            }
                        }
                        #endregion AppAADRoleAssignedOn

                        #region getAppOwner
                        $currentTask = "getAppOwner $($getApplication.id)"
                        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/applications/$($getApplication.id)/owners"
                        $method = 'GET'
                        $getAppOwner = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

                        if ($getAppOwner.Count -gt 0) {
                            if (-not $htAppOwners.($getApplication.id)) {
                                $script:htAppOwners.($getApplication.id) = $getAppOwner | Select-Object id, displayName, '@odata.type'
                            }
                        }
                        #endregion getAppOwner

                        #region getFederatedIdentityCredentials
                        #"https://graph.microsoft.com/beta/applications/b8997c96-efbf-49da-93c3-fccd44834d15/federatedIdentityCredentials"
                        $currentTask = "getFederatedIdentityCredentials $($getApplication.id)"
                        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/applications/$($getApplication.id)/federatedIdentityCredentials"
                        $method = 'GET'
                        $getFederatedIdentityCredentials = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

                        if ($getFederatedIdentityCredentials.Count -gt 0) {
                            if (-not $htFederatedIdentityCredentials.($getApplication.id)) {
                                $script:htFederatedIdentityCredentials.($getApplication.id) = $getFederatedIdentityCredentials
                            }
                        }
                        #endregion getFederatedIdentityCredentials

                        #region spAppKeyCredentials
                        if (($getApplication.keyCredentials).Count -gt 0) {
                            if ($object.servicePrincipalType -eq 'Application') {
                                $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).Application.ApplicationKeyCredentials = @{}
                                foreach ($keyCredential in $getApplication.keyCredentials) {
                                    $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).Application.ApplicationKeyCredentials.($keyCredential.keyId) = $keyCredential
                                }
                            }
                            if ($hlperType -eq 'AppOnly') {
                                $script:htServicePrincipalsAndAppsOnlyEnriched.("AppWithoutSP_$($object.id)").Application.ApplicationKeyCredentials = @{}
                                foreach ($keyCredential in $getApplication.keyCredentials) {
                                    $script:htServicePrincipalsAndAppsOnlyEnriched.("AppWithoutSP_$($object.id)").Application.ApplicationKeyCredentials.($keyCredential.keyId) = $keyCredential
                                }
                            }
                        }
                        #endregion spAppKeyCredentials

                        #region spAppPasswordCredentials
                        if (($getApplication.passwordCredentials).Count -gt 0) {
                            if ($object.servicePrincipalType -eq 'Application') {
                                $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).Application.ApplicationPasswordCredentials = @{}
                                foreach ($passwordCredential in $getApplication.passwordCredentials) {
                                    $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).Application.ApplicationPasswordCredentials.($passwordCredential.keyId) = $passwordCredential
                                }
                            }
                            if ($hlperType -eq 'AppOnly') {
                                $script:htServicePrincipalsAndAppsOnlyEnriched.("AppWithoutSP_$($object.id)").Application.ApplicationPasswordCredentials = @{}
                                foreach ($passwordCredential in $getApplication.passwordCredentials) {
                                    $script:htServicePrincipalsAndAppsOnlyEnriched.("AppWithoutSP_$($object.id)").Application.ApplicationPasswordCredentials.($passwordCredential.keyId) = $passwordCredential
                                }
                            }
                        }
                        #endregion spAppPasswordCredentials
                    }
                }
            }
        }
        #endregion spApp

        if ($hlperType -eq 'SP') {
            #region spManagedIdentity
            if (-not $meanwhileDeleted) {
                if ($object.servicePrincipalType -eq 'ManagedIdentity') {
                    $spType = 'MI'

                    $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).ManagedIdentity = @{}
                    $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).ManagedIdentityDetails = $object

                    if (($object.alternativeNames).Count -gt 0) {
                        $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).ManagedIdentity.ManagedIdentityAlternativeNames = $object.alternativeNames
                    }

                    $miType = 'unknown'
                    foreach ($altName in $object.alternativeNames) {
                        if ($altName -like 'isExplicit=*') {
                            $splitAltName = $altName.split('=')
                            if ($splitAltName[1] -eq 'true') {
                                $miType = 'User assigned'
                            }
                            if ($splitAltName[1] -eq 'false') {
                                $miType = 'System assigned'
                            }
                        }
                    }

                    <#
            $miType = "unknown"
            foreach ($altName in $object.alternativeNames) {
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
            #>
                }
            }
            #endregion spManagedIdentity
        }

        if (-not $meanwhileDeleted) {
            if ($hlperType -eq 'SP') {
                $script:htSpLookup.($object.id).spDisplayName = $object.displayName
                $script:htSpLookup.($object.id).spId = $object.id
                $script:htSpLookup.($object.id).spAppId = $object.appId
                $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).SPOrAppOnly = 'SP'
                if ($spType -eq 'APP') {
                    $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).objectTypeConcatinated = "SP $($spType) $($spTypeINTEXT)"
                    $script:htSpLookup.($object.id).objectTypeConcatinated = "SP $($spType) $($spTypeINTEXT)"
                    $script:htSpLookup.($object.id).appDisplayName = $getApplication.displayName
                    $script:htSpLookup.($object.id).appId = $getApplication.id
                    $script:htSpLookup.($object.id).appAppId = $getApplication.appId
                    $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).type = $spType
                    $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).subtype = $spTypeINTEXT
                }
                elseif ($spType -eq 'MI') {
                    $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).objectTypeConcatinated = "SP $($spType) $($miType)"
                    $script:htSpLookup.($object.id).objectTypeConcatinated = "SP $($spType) $($miType)"
                    $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).type = $spType
                    $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).subtype = $miType
                }
                else {
                    $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).objectTypeConcatinated = "SP $($spTypeINTEXT)"
                    $script:htSpLookup.($object.id).objectTypeConcatinated = "SP $($spTypeINTEXT)"
                    $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).type = 'SP'
                    $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).subtype = $spTypeINTEXT
                }
            }
            if ($hlperType -eq 'AppOnly') {
                $script:htServicePrincipalsAndAppsOnlyEnriched.("AppWithoutSP_$($object.id)").SPOrAppOnly = 'AppOnly'
                $script:htSpLookup.($object.id).appDisplayName = $object.displayName
                $script:htServicePrincipalsAndAppsOnlyEnriched.("AppWithoutSP_$($object.id)").objectTypeConcatinated = 'AppOnly'
                $script:htSpLookup.($object.id).objectTypeConcatinated = 'AppOnly'
                $script:htServicePrincipalsAndAppsOnlyEnriched.("AppWithoutSP_$($object.id)").type = 'APP'
                $script:htServicePrincipalsAndAppsOnlyEnriched.("AppWithoutSP_$($object.id)").subtype = 'AppOnly'
            }
        }
        else {
            $script:htServicePrincipalsAndAppsOnlyEnriched.($object.id).MeanWhileDeleted = $true
        }

        $processedServicePrincipalsCount = ($script:htServicePrincipalsAndAppsOnlyEnriched.Keys).Count
        if ($processedServicePrincipalsCount) {
            if ($processedServicePrincipalsCount % $indicator -eq 0) {
                if (-not $script:htProcessedTracker.($processedServicePrincipalsCount)) {
                    $script:htProcessedTracker.($processedServicePrincipalsCount) = @{}
                    Write-Host " $processedServicePrincipalsCount Service Principals processed"
                }
            }
        }

    } -ThrottleLimit $ThrottleLimitGraph

    $endForeachSP = Get-Date
    $duration = New-TimeSpan -Start $startForeachSP -End $endForeachSP
    Write-Host " Collecting data for all Service Principals ($($arraySPsAndAppsWithoutSP.Count)) duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"
    Write-Host " Service Principals that have been meanwhile deleted: $($htMeanwhileDeleted.Keys.Count)"
}
$end = Get-Date
$duration = New-TimeSpan -Start $startSP -End $end
Write-Host "SP Collection duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"
#endregion dataColletionAADSP

$htUsersToResolveGuestMember = @{}

#region AppRoleAssignments4UsersAndGroups
$startAppRoleAssignments4UsersAndGroups = Get-Date

#$htUsersAndGroupsAppRoleAssignments = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
$htUsersAndGroupsAppRoleAssignmentsUser = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
$htUsersAndGroupsAppRoleAssignmentsGroup = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
if ($htUsersAndGroupsToCheck4AppRoleAssignmentsUser.Keys.Count -gt 0) {

    #UsersToResolveGuestMember
    foreach ($user in $htUsersAndGroupsToCheck4AppRoleAssignmentsUser.Keys) {
        if (-not $htUsersToResolveGuestMember.($user)) {
            #Write-Host "UsersToResolveGuestMember user added ($($user))"
            $htUsersToResolveGuestMember.($user) = @{}
        }
    }

    #$htUsersAndGroupsAppRoleAssignmentsUser = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}
    $htUsersAndGroupsToCheck4AppRoleAssignmentsUser.Keys | ForEach-Object -Parallel {
        $userObjectId = $_

        #AzAPICall
        $azAPICallConf = $using:azAPICallConf
        $scriptPath = $using:ScriptPath
        #array&ht
        $htUsersAndGroupsAppRoleAssignmentsUser = $using:htUsersAndGroupsAppRoleAssignmentsUser

        if ($azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions) {
            Import-Module ".\$($scriptPath)\AzAPICallModule\AzAPICall\$($azAPICallConf['htParameters'].azAPICallModuleVersion)\AzAPICall.psd1" -Force -ErrorAction Stop
        }
        else {
            Import-Module -Name AzAPICall -RequiredVersion $azAPICallConf['htParameters'].azAPICallModuleVersion -Force -ErrorAction Stop
        }

        $currentTask = "getUser AppRoleAssignments $($userObjectId)"
        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/users/$($userObjectId)/appRoleAssignments"
        $method = 'GET'
        $getUserAppRoleAssignments = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

        if ($getUserAppRoleAssignments.Count -gt 0) {
            foreach ($userAppRoleAssignment in $getUserAppRoleAssignments) {
                if (-not $htUsersAndGroupsAppRoleAssignmentsUser.($userObjectId).($userAppRoleAssignment.id)) {
                    if (-not $htUsersAndGroupsAppRoleAssignmentsUser.($userObjectId)) {
                        $script:htUsersAndGroupsAppRoleAssignmentsUser.($userObjectId) = @{}
                        $script:htUsersAndGroupsAppRoleAssignmentsUser.($userObjectId).($userAppRoleAssignment.id) = $userAppRoleAssignment
                    }
                    else {
                        $script:htUsersAndGroupsAppRoleAssignmentsUser.($userObjectId).($userAppRoleAssignment.id) = $userAppRoleAssignment
                    }
                }
            }
        }
    } -ThrottleLimit $ThrottleLimitGraph
}

if ($htUsersAndGroupsToCheck4AppRoleAssignmentsGroup.Keys.Count -gt 0) {
    $htUsersAndGroupsToCheck4AppRoleAssignmentsGroup.Keys | ForEach-Object -Parallel {
        $groupObjectId = $_

        #AzAPICall
        $azAPICallConf = $using:azAPICallConf
        $scriptPath = $using:ScriptPath
        #array&ht
        $htUsersAndGroupsAppRoleAssignmentsGroup = $using:htUsersAndGroupsAppRoleAssignmentsGroup

        if ($azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions) {
            Import-Module ".\$($scriptPath)\AzAPICallModule\AzAPICall\$($azAPICallConf['htParameters'].azAPICallModuleVersion)\AzAPICall.psd1" -Force -ErrorAction Stop
        }
        else {
            Import-Module -Name AzAPICall -RequiredVersion $azAPICallConf['htParameters'].azAPICallModuleVersion -Force -ErrorAction Stop
        }

        $currentTask = "getGroup AppRoleAssignments $($groupObjectId)"
        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/Groups/$($groupObjectId)/appRoleAssignments"
        $method = 'GET'
        $getGroupAppRoleAssignments = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

        if ($getGroupAppRoleAssignments.Count -gt 0) {
            foreach ($groupAppRoleAssignment in $getGroupAppRoleAssignments) {
                if (-not $htUsersAndGroupsAppRoleAssignmentsGroup.($groupObjectId).($groupAppRoleAssignment.id)) {
                    if (-not $htUsersAndGroupsAppRoleAssignmentsGroup.($groupObjectId)) {
                        $script:htUsersAndGroupsAppRoleAssignmentsGroup.($groupObjectId) = @{}
                        $script:htUsersAndGroupsAppRoleAssignmentsGroup.($groupObjectId).($groupAppRoleAssignment.id) = $groupAppRoleAssignment
                    }
                    else {
                        $script:htUsersAndGroupsAppRoleAssignmentsGroup.($groupObjectId).($groupAppRoleAssignment.id) = $groupAppRoleAssignment
                    }
                }
            }
        }
    } -ThrottleLimit $ThrottleLimitGraph
}
$end = Get-Date
$duration = New-TimeSpan -Start $startAppRoleAssignments4UsersAndGroups -End $end
Write-Host "AppRoleAssignments4UsersAndGroups duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"
#endregion AppRoleAssignments4UsersAndGroups

#region AADGroupsResolve

$htAadGroups = [System.Collections.Hashtable]::Synchronized((New-Object System.Collections.Hashtable)) #@{}

#region groupsFromSPs
$startgroupsFromSPs = Get-Date
Write-Host 'Resolving AAD Groups where any SP is memberOf'
if (($htAadGroupsToResolve.Keys).Count -gt 0) {
    Write-Host " Resolving $(($htAadGroupsToResolve.Keys).Count) AAD Groups where any SP is memberOf"
    $startgroupsFromSPs = Get-Date

    ($htAadGroupsToResolve.Keys) | ForEach-Object -Parallel {
        $aadGroupId = $_

        #AzAPICall
        $azAPICallConf = $using:azAPICallConf
        $scriptPath = $using:ScriptPath
        #array&ht
        $htAadGroups = $using:htAadGroups

        if ($azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions) {
            Import-Module ".\$($scriptPath)\AzAPICallModule\AzAPICall\$($azAPICallConf['htParameters'].azAPICallModuleVersion)\AzAPICall.psd1" -Force -ErrorAction Stop
        }
        else {
            Import-Module -Name AzAPICall -RequiredVersion $azAPICallConf['htParameters'].azAPICallModuleVersion -Force -ErrorAction Stop
        }

        #Write-Host "resolving AAD Group: $aadGroupId"
        $currentTask = "get AAD Group $($aadGroupId)"
        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/groups/$($aadGroupId)"
        $method = 'GET'
        $getAadGroup = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -listenOn 'Content'

        if ($getAadGroup -eq 'Request_UnsupportedQuery') {
            Write-Host "skipping Group $($aadGroupId)"
        }
        else {
            $script:htAadGroups.($aadGroupId) = @{}
            $script:htAadGroups.($aadGroupId).groupDetails = $getAadGroup

            #v1 does not return ServicePrincipals
            $currentTask = "get transitive members for AAD Group $($aadGroupId)"
            $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/groups/$($aadGroupId)/transitivemembers/microsoft.graph.group?`$count=true"
            $method = 'GET'
            $getNestedGroups = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -consistencyLevel 'eventual'

            if ($getNestedGroups) {
                if ($getNestedGroups -eq 'Request_UnsupportedQuery') {
                    Write-Host "skipping transitive members for Group $($aadGroupId)"
                }
                else {
                    Write-Host " $aadGroupId -> has nested Groups $($getNestedGroups.Count)"
                    $script:htAadGroups.($aadGroupId).nestedGroups = $getNestedGroups
                }
            }
        }

    } -ThrottleLimit $ThrottleLimitGraph

    $end = Get-Date
    $duration = New-TimeSpan -Start $startgroupsFromSPs -End $end
    Write-Host "AADGroupsResolve duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"
}
else {
    Write-Host " Resolving $(($htAadGroupsToResolve.Keys).Count) AAD Groups where any SP is memberOf"
}

$end = Get-Date
$duration = New-TimeSpan -Start $startgroupsFromSPs -End $end
Write-Host "Resolving AAD Groups where any SP is memberOf duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"
#endregion groupsFromSPs

#region GroupsFromAzureRoleAssignments
$startGroupsFromAzureRoleAssignments = Get-Date
#batching
$counterBatch = [PSCustomObject] @{ Value = 0 }
$batchSize = 1000
$arrayObjectIdsToProcess = [System.Collections.ArrayList]@()
$objectIdsUnique = ($htCacheAssignmentsRole).values.assignment.properties.principalId | Sort-Object -Unique
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
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/directoryObjects/getByIds"
    $method = 'POST'
    $body = @"
        {
            "ids":[$($objectIdsToCheckIfGroup)],
            "types":["group"]
        }
"@
    $resolveObjectIdsTypeGroup = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -body $body -currentTask $currentTask

    foreach ($group in $resolveObjectIdsTypeGroup) {
        $script:htAadGroups.($group.id) = @{}
        $script:htAadGroups.($group.id).groupDetails = $group

        #v1 does not return ServicePrincipals
        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/groups/$($group.id)/transitivemembers/microsoft.graph.group?`$count=true"
        $method = 'GET'
        $getNestedGroups = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -consistencyLevel 'eventual'

        if ($getNestedGroups) {
            Write-Host " -> has nested Groups $($getNestedGroups.Count)"
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

$end = Get-Date
$duration = New-TimeSpan -Start $startGroupsFromAzureRoleAssignments -End $end
Write-Host "Getting all AAD Groups duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"
#endregion GroupsFromAzureRoleAssignments

#endregion AADGroupsResolve

#region owners
Write-Host 'Processing SP/App Owners'

#UsersToResolveGuestMember
foreach ($spOwner in $htSPOwners.Values) {
    foreach ($owner in $spOwner) {
        if ($owner.'@odata.type' -eq '#microsoft.graph.user') {
            if (-not $htUsersToResolveGuestMember.($owner.id)) {
                #Write-Host "UsersToResolveGuestMember SPowner added ($($owner.id))"
                $htUsersToResolveGuestMember.($owner.id) = @{}
            }
        }
    }
}
foreach ($appOwner in $htAppOwners.Values) {
    foreach ($owner in $appOwner) {
        if ($owner.'@odata.type' -eq '#microsoft.graph.user') {
            if (-not $htUsersToResolveGuestMember.($owner.id)) {
                #Write-Host "UsersToResolveGuestMember appOwner added ($($owner.id))"
                $htUsersToResolveGuestMember.($owner.id) = @{}
            }
        }
    }
}
resolveObectsById -objects $htUsersToResolveGuestMember.Keys -targetHt 'htPrincipalsResolved'

$htOwnedByEnriched = @{}
foreach ($sp in $htOwnedBy.Keys) {
    $htOwnedByEnriched.($sp) = @{}
    foreach ($ownedBy in $htOwnedBy.($sp).ownedBy) {
        $arrayx = @()
        if ($ownedBy -ne 'noOwner') {
            foreach ($owner in $ownedBy) {
                $htTmp = [ordered] @{}
                $htTmp.id = $owner.id
                $htTmp.displayName = $owner.displayName
                $htTmp.'@odata.type' = $owner.'@odata.type'
                if ($owner.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
                    $hlpType = $htSpLookup.($owner.id).objectTypeConcatinated
                    $htTmp.spType = $hlpType
                    $htTmp.principalType = $hlpType
                }
                if ($owner.'@odata.type' -eq '#microsoft.graph.user') {
                    $htTmp.principalType = $htPrincipalsResolved.($owner.id).typeOnly
                }
                $htTmp.applicability = 'direct'
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

foreach ($sp in $htServicePrincipalsAndAppsOnlyEnriched.Keys) {

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
            foreach ($owner in $owners | Sort-Object -Property '@odata.type' -Descending) {
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
                        if ($owner -eq 'noOwner') {
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
        if ($owner.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
            $htOptInfo.spType = $htSpLookup.($owner.id).objectTypeConcatinated
            $htOptInfo.principalType = $htSpLookup.($owner.id).objectTypeConcatinated
        }
        if ($owner.'@odata.type' -eq '#microsoft.graph.user') {
            $htOptInfo.principalType = $htPrincipalsResolved.($owner.id).typeOnly
        }
        $htOptInfo.applicability = 'direct'
        $owners = $null
        if ($owner.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
            $owners = getowner -owner $owner.id
        }
        $htOptInfo.ownedBy = $owners
        $null = $arrayOwners.Add($htOptInfo)
    }

    foreach ($owner in $htSPOwnersTmp.($sp).indirect) {
        if ($owner -eq 'noOwner' -or $owner.'@odata.type' -eq '#microsoft.graph.user') {
            if ($owner.'@odata.type' -eq '#microsoft.graph.user') {
                if (($arrayOwners.where({ $_.applicability -eq 'indirect' })).id -notcontains $owner.id) {
                    $htOptInfo = [ordered] @{}
                    $htOptInfo.id = $($owner.id)
                    $htOptInfo.displayName = $($owner.displayName)
                    $htOptInfo.type = $($owner.'@odata.type')
                    $htOptInfo.principalType = $htPrincipalsResolved.($owner.id).typeOnly
                    $htOptInfo.applicability = 'indirect'
                    $null = $arrayOwners.Add($htOptInfo)
                }
            }
        }
        else {
            $htOptInfo = [ordered] @{}
            $htOptInfo.id = $($owner.id)
            $htOptInfo.displayName = $($owner.displayName)
            $htOptInfo.type = $($owner.'@odata.type')
            $htOptInfo.applicability = 'indirect'
            if ($owner.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
                $htOptInfo.principalType = $htSpLookup.($owner.id).objectTypeConcatinated
            }
            if ($owner.'@odata.type' -eq '#microsoft.graph.user') {
                $htOptInfo.principalType = $htPrincipalsResolved.($owner.id).typeOnly
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
            $htOpt.principalType = $htPrincipalsResolved.($owner.id).typeOnly
            $array += $htOpt
        }
        else {
            $htOpt = [ordered] @{}
            $htOpt.id = $owner.id
            $htOpt.displayName = $owner.displayName
            $htOpt.type = $owner.'@odata.type'
            $htOpt.spType = $htSpLookup.($owner.id).objectTypeConcatinated
            $htOpt.principalType = $htSpLookup.($owner.id).objectTypeConcatinated
            $htOpt.ownedBy = $htSPOwnersFinal.($owner.id)
            $array += $htOpt
        }
    }
    $htAppOwnersFinal.($app) = $array
}

#endregion owners

if (-not $NoAzureRoleAssignments) {
    #region AzureRoleAssignmentMapping
    $startAzureRoleAssignmentMapping = Get-Date

    #resolving createdby/updatedby
    $htCreatedByUpdatedByObjectIdsToBeResolved = @{}
    foreach ($createdByItem in ($htCacheAssignmentsRole).values.assignment.properties.createdBy | Sort-Object -Unique) {

        if ([guid]::TryParse(($createdByItem), $([ref][guid]::Empty))) {
            if (-not $htPrincipalsResolved.($createdByItem)) {
                if ($getServicePrincipals.id -contains $createdByItem) {
                    #
                }
                else {
                    if (-not $htCreatedByUpdatedByObjectIdsToBeResolved.($createdByItem)) {
                        $htCreatedByUpdatedByObjectIdsToBeResolved.($createdByItem) = @{}
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
        resolveObectsById -objects $arrayUnresolvedIdentities -targetHt 'htPrincipalsResolved'
    }

    if (($htCacheAssignmentsRole).Keys.Count -gt 0) {
        # $htAssignmentsByPrincipalId = @{}
        # $htAssignmentsByPrincipalId.'servicePrincipals' = @{}
        # $htAssignmentsByPrincipalId.'groups' = @{}

        $htAssignmentsByPrincipalIdServicePrincipals = @{}
        $htAssignmentsByPrincipalIdGroups = @{}
        foreach ($assignment in ($htCacheAssignmentsRole).values) {
            #todo sp created ra in azure
            if (-not [string]::IsNullOrEmpty($assignment.assignment.properties.createdBy)) {
                if ($htPrincipalsResolved.($assignment.assignment.properties.createdBy)) {
                    $assignment.assignment.properties.createdBy = $htPrincipalsResolved.($assignment.assignment.properties.createdBy).full
                }
                else {
                    if ($htServicePrincipalsAndAppsOnlyEnriched.($assignment.assignment.properties.createdBy)) {
                        $hlper = $htServicePrincipalsAndAppsOnlyEnriched.($assignment.assignment.properties.createdBy)
                        $assignment.assignment.properties.createdBy = "$($hlper.objectTypeConcatinated), DisplayName: $($hlper.ServicePrincipalDetails.displayName), Id: $($assignment.assignment.properties.createdBy)"
                    }
                }
            }
            if ($getServicePrincipals.id -contains $assignment.assignment.properties.principalId) {
                if (-not $htAssignmentsByPrincipalIdServicePrincipals.($assignment.assignment.properties.principalId)) {
                    $htAssignmentsByPrincipalIdServicePrincipals.($assignment.assignment.properties.principalId) = [array]$assignment
                }
                else {
                    $htAssignmentsByPrincipalIdServicePrincipals.($assignment.assignment.properties.principalId) += $assignment
                }
            }
            if ($htAadGroups.Keys -contains $assignment.assignment.properties.principalId) {
                if (-not $htAssignmentsByPrincipalIdGroups.($assignment.assignment.properties.principalId)) {
                    $htAssignmentsByPrincipalIdGroups.($assignment.assignment.properties.principalId) = [array]$assignment
                }
                else {
                    $htAssignmentsByPrincipalIdGroups.($assignment.assignment.properties.principalId) += $assignment
                }
            }
        }
    }
    else {
        Write-Host ' No RoleAssignments?!'
        break
    }
    $end = Get-Date
    $duration = New-TimeSpan -Start $startAzureRoleAssignmentMapping -End $end
    Write-Host "AzureRoleAssignmentMapping duration: $(($duration).TotalMinutes) minutes ($(($duration).TotalSeconds) seconds)"
    #endregion AzureRoleAssignmentMapping
}

#region enrichedAADSPData
Write-Host 'Enrichment starting prep'
#$cu = [System.Collections.ArrayList]@()
$cu = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))
$appPasswordCredentialsExpiredCount = 0
$appPasswordCredentialsGracePeriodExpiryCount = 0
$appPasswordCredentialsExpiryOKCount = 0
$appPasswordCredentialsExpiryOKMoreThanMaxCount = 0
$appKeyCredentialsExpiredCount = 0
$appKeyCredentialsGracePeriodExpiryCount = 0
$appKeyCredentialsExpiryOKCount = 0
$appKeyCredentialsExpiryOKMoreThanMaxCount = 0

$htSPandAPPHelper4AADRoleAssignmentsWithScope = @{}
foreach ($aadRoleAssignment in $htServicePrincipalsAndAppsOnlyEnriched.values.ServicePrincipalAADRoleAssignments) {
    if ($aadRoleAssignment.resourceScope -ne '/') {

        if ($htApplications.($aadRoleAssignment.resourceScope -replace '/')) {
            if (-not $htSPandAPPHelper4AADRoleAssignmentsWithScope.($aadRoleAssignment.resourceScope -replace '/')) {
                $hlp = $htApplications.($aadRoleAssignment.resourceScope -replace '/')
                $htSPandAPPHelper4AADRoleAssignmentsWithScope.($aadRoleAssignment.resourceScope -replace '/') = "Application: $($hlp.displayname) ($($hlp.id))"
            }

        }
        else {
            if ($htServicePrincipalsAndAppsOnlyEnriched.($aadRoleAssignment.resourceScope -replace '/')) {
                if (-not $htSPandAPPHelper4AADRoleAssignmentsWithScope.($aadRoleAssignment.resourceScope -replace '/')) {
                    $hlp = $htServicePrincipalsAndAppsOnlyEnriched.($aadRoleAssignment.resourceScope -replace '/').ServicePrincipalDetails
                    $htSPandAPPHelper4AADRoleAssignmentsWithScope.($aadRoleAssignment.resourceScope -replace '/') = "ServicePrincipal: $($hlp.displayname) ($($hlp.id))"
                }
            }
        }
    }
}
Write-Host 'Enrichment completed prep'

Write-Host 'Enrichment starting'
$enrichmentProcessCounter = [pscustomobject]@{counter = 0 }
$enrichmentProcessindicator = 100
$processedServicePrincipalsCount = 0
$startEnrichmentSP = Get-Date
$arrayPerformanceTracking = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))
($htServicePrincipalsAndAppsOnlyEnriched.values).where( { -not $_.MeanWhileDeleted } ) | ForEach-Object -Parallel {
    #parallel
    $spOrAppWithoutSP = $_
    $cu = $using:cu
    $enrichmentProcessCounter = $using:enrichmentProcessCounter
    $enrichmentProcessindicator = $using:enrichmentProcessindicator
    $arrayPerformanceTracking = $using:arrayPerformanceTracking
    $htSPandAPPHelper4AADRoleAssignmentsWithScope = $using:htSPandAPPHelper4AADRoleAssignmentsWithScope
    $htSPOwnersFinal = $using:htSPOwnersFinal
    #vars
    $appPasswordCredentialsExpiredCount = $using:appPasswordCredentialsExpiredCount
    $appPasswordCredentialsGracePeriodExpiryCount = $using:appPasswordCredentialsGracePeriodExpiryCount
    $appPasswordCredentialsExpiryOKCount = $using:appPasswordCredentialsExpiryOKCount
    $appPasswordCredentialsExpiryOKMoreThanMaxCount = $using:appPasswordCredentialsExpiryOKMoreThanMaxCount
    $appKeyCredentialsExpiredCount = $using:appKeyCredentialsExpiredCount
    $appKeyCredentialsGracePeriodExpiryCount = $using:appKeyCredentialsGracePeriodExpiryCount
    $appKeyCredentialsExpiryOKCount = $using:appKeyCredentialsExpiryOKCount
    $appKeyCredentialsExpiryOKMoreThanMaxCount = $using:appKeyCredentialsExpiryOKMoreThanMaxCount
    $NoAzureRoleAssignments = $using:NoAzureRoleAssignments
    $ApplicationSecretExpiryWarning = $using:ApplicationSecretExpiryWarning
    $ApplicationSecretExpiryMax = $using:ApplicationSecretExpiryMax
    $ApplicationCertificateExpiryWarning = $using:ApplicationCertificateExpiryWarning
    $ApplicationCertificateExpiryMax = $using:ApplicationCertificateExpiryMax
    $runTenantRoot = $using:runTenantRoot

    $htCacheAssignmentsPolicy = $using:htCacheAssignmentsPolicy
    $htAadRoleDefinitions = $using:htAadRoleDefinitions
    $htPublishedPermissionScopes = $using:htPublishedPermissionScopes
    $htSPOauth2PermissionGrantedTo = $using:htSPOauth2PermissionGrantedTo
    $htAppRoles = $using:htAppRoles
    $htPrincipalsResolved = $using:htPrincipalsResolved
    $htAppRoleAssignments = $using:htAppRoleAssignments
    $htUsersAndGroupsAppRoleAssignmentsUser = $using:htUsersAndGroupsAppRoleAssignmentsUser
    $htUsersAndGroupsAppRoleAssignmentsGroup = $using:htUsersAndGroupsAppRoleAssignmentsGroup
    $htAaDGroups = $using:htAaDGroups
    $htAssignmentsByPrincipalIdGroups = $using:htAssignmentsByPrincipalIdGroups
    $htAssignmentsByPrincipalIdServicePrincipals = $using:htAssignmentsByPrincipalIdServicePrincipals
    $htAppOwners = $using:htAppOwners
    $htServicePrincipalsPublishedPermissionScopes = $using:htServicePrincipalsPublishedPermissionScopes
    $htSpLookup = $using:htSpLookup
    $getClassifications = $using:getClassifications
    $htFederatedIdentityCredentials = $using:htFederatedIdentityCredentials
    $CriticalAADRoles = $using:CriticalAADRoles
    #functions
    $function:getClassification = $using:funcGetClassification

    $object = $spOrAppWithoutSP
    if ($spOrAppWithoutSP.SPOrAppOnly -eq 'SP') {
        $spId = $object.ServicePrincipalDetails.id
        #Write-host "processing SP:" $object.ServicePrincipalDetails.displayName "objId: $($spId)" "appId: $($object.ServicePrincipalDetails.appId)"
    }
    elseif ($spOrAppWithoutSP.SPOrAppOnly -eq 'AppOnly') {
        $objId = $object.Application.ApplicationDetails.id
        #Write-host "processing AppOnly:" $object.Application.ApplicationDetails.displayName "objId: $($objId)" "appId: $($object.Application.ApplicationDetails.appId)"
    }
    else {
        Write-Host 'unexpected'
        throw
    }

    if ($spOrAppWithoutSP.SPOrAppOnly -eq 'SP') {

        #region ServicePrincipalOwnedObjects
        $start = Get-Date
        $arrayServicePrincipalOwnedObjectsOpt = [System.Collections.ArrayList]@()
        if (($object.ServicePrincipalOwnedObjects).Count -gt 0) {
            foreach ($ownedObject in $object.ServicePrincipalOwnedObjects | Sort-Object -Property '@odata.type', id) {

                $type = 'unforseen type'
                if ($ownedObject.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
                    $type = 'Serviceprincipal'
                }
                if ($ownedObject.'@odata.type' -eq '#microsoft.graph.application') {
                    $type = 'Application'
                }
                if ($ownedObject.'@odata.type' -eq '#microsoft.graph.group') {
                    $type = 'Group'
                }
                $htOptInfo = [ordered] @{}
                $htOptInfo.type = $type
                if ($type -eq 'Serviceprincipal') {
                    $htOptInfo.typeDetailed = $htSpLookup.($ownedObject.id).objectTypeConcatinated
                }
                $htOptInfo.displayName = $ownedObject.displayName
                $htOptInfo.objectId = $ownedObject.id
                $null = $arrayServicePrincipalOwnedObjectsOpt.Add($htOptInfo)
                #Write-Host "SP OwnedObjects             : $($type) $($ownedObject.displayName) ($($ownedObject.id))"
            }
            $durationPerfTrackServicePrincipalOwnedObjects = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)
        }
        #endregion ServicePrincipalOwnedObjects

        #region ServicePrincipalOwners
        $start = Get-Date
        $arrayServicePrincipalOwnerOpt = [System.Collections.ArrayList]@()
        if ($htSPOwnersFinal.($spId)) {
            foreach ($servicePrincipalOwner in $htSPOwnersFinal.($spId)) {
                $htOptInfo = [ordered] @{}
                $htOptInfo.id = $servicePrincipalOwner.id
                $htOptInfo.displayName = $servicePrincipalOwner.displayName
                $htOptInfo.principalType = $servicePrincipalOwner.principalType
                $htOptInfo.applicability = $servicePrincipalOwner.applicability
                $arrayOwnedBy = @()

                foreach ($owner in $servicePrincipalOwner.ownedBy) {
                    if ($owner -ne 'noOwner') {
                        if ($htSPOwnersFinal.($owner.id)) {
                            $arrayOwnedBy += ($htSPOwnersFinal.($owner.id))
                        }
                        else {
                            $arrayOwnedBy += ($owner)
                        }
                    }
                    else {
                        $arrayOwnedBy += ($owner)
                    }

                }
                if ($servicePrincipalOwner.type -ne '#microsoft.graph.user') {
                    $htOptInfo.ownedBy = $arrayOwnedBy
                }

                $null = $arrayServicePrincipalOwnerOpt.Add($htOptInfo)
            }
            $durationPerfTrackServicePrincipalOwners = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)
        }
        #endregion ServicePrincipalOwners

        #region ServicePrincipalAADRoleAssignments
        $start = Get-Date
        $arrayServicePrincipalAADRoleAssignmentsOpt = [System.Collections.ArrayList]@()
        if ($object.ServicePrincipalAADRoleAssignments) {
            foreach ($servicePrincipalAADRoleAssignment in $object.ServicePrincipalAADRoleAssignments) {
                $hlper = $htAadRoleDefinitions.($servicePrincipalAADRoleAssignment.roleDefinitionId)
                if ($hlper.isBuiltIn) {
                    $roleType = 'BuiltIn'
                }
                else {
                    $roleType = 'Custom'
                }

                $aadRoleIsCritical = $false
                if ($CriticalAADRoles -contains $servicePrincipalAADRoleAssignment.roleDefinitionId) {
                    $aadRoleIsCritical = $true
                }

                $htOptInfo = [ordered] @{}
                $htOptInfo.id = $servicePrincipalAADRoleAssignment.id
                $htOptInfo.roleDefinitionId = $servicePrincipalAADRoleAssignment.roleDefinitionId
                $htOptInfo.roleDefinitionName = $hlper.displayName
                $htOptInfo.roleDefinitionDescription = $hlper.description
                $htOptInfo.roleType = $roleType
                $htOptInfo.roleIsCritical = $aadRoleIsCritical
                $htOptInfo.directoryScopeId = $servicePrincipalAADRoleAssignment.directoryScopeId
                $htOptInfo.resourceScope = $servicePrincipalAADRoleAssignment.resourceScope
                if ($servicePrincipalAADRoleAssignment.resourceScope -ne '/') {
                    if ($htSPandAPPHelper4AADRoleAssignmentsWithScope.($servicePrincipalAADRoleAssignment.resourceScope -replace '/')) {
                        $htOptInfo.scopeDetail = $htSPandAPPHelper4AADRoleAssignmentsWithScope.($servicePrincipalAADRoleAssignment.resourceScope -replace '/')
                    }
                }
                $null = $arrayServicePrincipalAADRoleAssignmentsOpt.Add($htOptInfo)
            }
            $durationPerfTrackServicePrincipalAADRoleAssignments = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)
        }
        #endregion ServicePrincipalAADRoleAssignments

        <#region ServicePrincipalAADRoleAssignmentScheduleInstances
        $arrayServicePrincipalAADRoleAssignmentScheduleInstancesOpt = [System.Collections.ArrayList]@()
        if ($object.ServicePrincipalAADRoleAssignmentScheduleInstances) {
            foreach ($servicePrincipalAADRoleAssignmentScheduleInstance in $object.ServicePrincipalAADRoleAssignmentScheduleInstances) {
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

        <#
        #region ServicePrincipalAADRoleAssignedOn
        $start = get-date
        $arrayServicePrincipalAADRoleAssignedOnOpt = [System.Collections.ArrayList]@()
        if ($htAADRoleAssignmentOnSPOrAPP.SP.($spId)) {
            foreach ($aadRoleAssignedOn in $htAADRoleAssignmentOnSPOrAPP.SP.($spId)) {
                $hlperAaDRoleDefinition = $htAadRoleDefinitions.($aadRoleAssignedOn.roleDefinitionId)
                $hlperSP = $htSpLookup.($aadRoleAssignedOn.principalId)
                $htOptInfo = [ordered] @{}
                $htOptInfo.id = $aadRoleAssignedOn.id
                $htOptInfo.roleName = $hlperAaDRoleDefinition.displayName
                $htOptInfo.roleId = $aadRoleAssignedOn.roleDefinitionId
                $htOptInfo.roleDescription = $hlperAaDRoleDefinition.description
                $htOptInfo.principalId = $aadRoleAssignedOn.principalId
                $htOptInfo.principalDisplayName = $hlperSP.spDisplayName
                $htOptInfo.principalType = $hlperSP.objectTypeConcatinated
                $null = $arrayServicePrincipalAADRoleAssignedOnOpt.Add($htOptInfo)
            }
            $durationPerfTrackServicePrincipalAADRoleAssignedOn = V(NEW-TIMESPAN -Start $start -End (Get-Date)).TotalMilliseconds)
        }
        #endregion ServicePrincipalAADRoleAssignedOn
        #>

        #region ServicePrincipalAADRoleAssignedOn
        $start = Get-Date
        $arrayServicePrincipalAADRoleAssignedOnOpt = [System.Collections.ArrayList]@()
        if ($object.ServicePrincipalAADRoleAssignedOn) {
            #foreach ($aadRoleAssignedOn in $htAADRoleAssignmentOnSPOrAPP.SP.($spId)) {
            foreach ($aadRoleAssignedOn in $object.ServicePrincipalAADRoleAssignedOn | Sort-Object -Property roleName, id) {
                $hlperAaDRoleDefinition = $htAadRoleDefinitions.($aadRoleAssignedOn.roleDefinitionId)

                $htOptInfo = [ordered] @{}
                $htOptInfo.id = $aadRoleAssignedOn.id
                $htOptInfo.roleName = $hlperAaDRoleDefinition.displayName
                $htOptInfo.roleId = $aadRoleAssignedOn.roleDefinitionId
                $htOptInfo.roleDescription = $hlperAaDRoleDefinition.description
                $htOptInfo.principalId = $aadRoleAssignedOn.principalId
                $htOptInfo.principalDisplayName = $aadRoleAssignedOn.principalDisplayName
                if ($aadRoleAssignedOn.principalType -eq 'User') {
                    $htOptInfo.principalType = $aadRoleAssignedOn.principalUserType
                }
                elseif ($aadRoleAssignedOn.principalType -eq 'ServicePrincipal') {
                    $hlperSP = $htSpLookup.($aadRoleAssignedOn.principalId)
                    $htOptInfo.principalType = $hlperSP.objectTypeConcatinated
                }
                else {
                    $htOptInfo.principalType = $aadRoleAssignedOn.principalType
                }

                $null = $arrayServicePrincipalAADRoleAssignedOnOpt.Add($htOptInfo)
            }
            $durationPerfTrackServicePrincipalAADRoleAssignedOn = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)
        }
        #endregion ServicePrincipalAADRoleAssignedOn

        #region ServicePrincipalOauth2PermissionGrants
        $start = Get-Date
        $arrayServicePrincipalOauth2PermissionGrantsOpt = [System.Collections.ArrayList]@()
        if ($object.ServicePrincipalOauth2PermissionGrants) {
            foreach ($servicePrincipalOauth2PermissionGrant in $object.ServicePrincipalOauth2PermissionGrants | Sort-Object -Property resourceId) {
                $multipleScopes = $servicePrincipalOauth2PermissionGrant.scope.split(' ')
                foreach ($scope in $multipleScopes | Sort-Object) {
                    if (-not [string]::IsNullOrEmpty($scope) -and -not [string]::IsNullOrWhiteSpace($scope)) {
                        $hlperServicePrincipalsPublishedPermissionScope = $htServicePrincipalsPublishedPermissionScopes.($servicePrincipalOauth2PermissionGrant.resourceId).spdetails
                        $hlperPublishedPermissionScope = $htPublishedPermissionScopes.($servicePrincipalOauth2PermissionGrant.resourceId).($scope)

                        $htOptInfo = [ordered] @{}
                        $htOptInfo.SPId = $hlperServicePrincipalsPublishedPermissionScope.id
                        $htOptInfo.SPAppId = $hlperServicePrincipalsPublishedPermissionScope.appId
                        $htOptInfo.SPDisplayName = $hlperServicePrincipalsPublishedPermissionScope.displayName
                        $htOptInfo.scope = $scope
                        $htOptInfo.permission = $hlperPublishedPermissionScope.value
                        $oauth2PermissionSensitivity = 'unclassified'
                        <#
                        if (
                            #$hlperPublishedPermissionScope.value -eq "Application.ReadWrite.All" -or
                            #$hlperPublishedPermissionScope.value -eq "Directory.ReadWrite.All" -or
                            #$hlperPublishedPermissionScope.value -like "Domain.ReadWrite.All*" -or
                            #$hlperPublishedPermissionScope.value -like "EduRoster.ReadWrite.All*" -or
                            #$hlperPublishedPermissionScope.value -eq "Group.ReadWrite.All" -or
                            $hlperPublishedPermissionScope.value -like 'Member.Read.Hidden*' -or
                            $hlperPublishedPermissionScope.value -eq 'RoleManagement.ReadWrite.Directory' -or
                            #$hlperPublishedPermissionScope.value -like "User.ReadWrite.All*" -or
                            $hlperPublishedPermissionScope.value -eq 'User.ManageCreds.All' -or
                            $hlperPublishedPermissionScope.value -like '*Write.All*' -or
                            $hlperPublishedPermissionScope.value -like '*Write'
                        ) {
                            $oauth2PermissionSensitivity = 'critical'
                        }
                        #>
                        <#
                        Application.ReadWrite.All
                        Directory.ReadWrite.All
                        Domain.ReadWrite.All*
                        EduRoster.ReadWrite.All*
                        Group.ReadWrite.All
                        Member.Read.Hidden*
                        RoleManagement.ReadWrite.Directory
                        User.ReadWrite.All*
                        User.ManageCreds.All
                        All other AppOnly permissions that allow write access
                        #>
                        $oauth2PermissionSensitivity = getClassification -permission $hlperPublishedPermissionScope.value -permissionType 'oauth2Permissions'

                        $htOptInfo.permissionSensitivity = $oauth2PermissionSensitivity
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
            $durationPerfTrackServicePrincipalOauth2PermissionGrants = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)
        }
        #endregion ServicePrincipalOauth2PermissionGrants

        #region SPOauth2PermissionGrantedTo
        $start = Get-Date
        $arraySPOauth2PermissionGrantedTo = [System.Collections.ArrayList]@()
        if ($htSPOauth2PermissionGrantedTo.($spId)) {
            foreach ($SPOauth2PermissionGrantedTo in $htSPOauth2PermissionGrantedTo.($spId) <#| Sort-Object -Property clientId, id#>) {
                foreach ($SPOauth2PermissionGrantedToScope in $SPOauth2PermissionGrantedTo.scope <#| Sort-Object#>) {
                    #$hlper = $htServicePrincipalsAndAppsOnlyEnriched.($SPOauth2PermissionGrantedTo.clientId).ServicePrincipal
                    #$spHlper = $hlper.ServicePrincipalDetails #| Select-Object displayName, id, appId
                    #$appHlperApplicationDetails = $hlper.Application.ApplicationDetails #| Select-Object displayName, id, appId
                    $hlper = $htSpLookup.($SPOauth2PermissionGrantedTo.clientId)
                    #$appHlperApplicationDetails = $appHlper.ApplicationDetails
                    $htOptInfo = [ordered] @{}
                    $htOptInfo.servicePrincipalDisplayName = $hlper.spDisplayName
                    $htOptInfo.servicePrincipalObjectId = $hlper.spId
                    $htOptInfo.servicePrincipalAppId = $hlper.spAppId
                    $htOptInfo.applicationDisplayName = $hlper.appDisplayName
                    $htOptInfo.applicationObjectId = $hlper.appId
                    $htOptInfo.applicationAppId = $hlper.appAppId
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
            $durationPerfTrackSPOauth2PermissionGrantedTo = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)
        }
        #endregion SPOauth2PermissionGrantedTo

        #region ServicePrincipalAppRoleAssignments
        $start = Get-Date
        $arrayServicePrincipalAppRoleAssignmentsOpt = [System.Collections.ArrayList]@()
        if ($object.ServicePrincipalAppRoleAssignments) {
            foreach ($servicePrincipalAppRoleAssignment in $object.ServicePrincipalAppRoleAssignments) {
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
                #Critical permissions
                #https://m365internals.com/2021/07/24/everything-about-service-principals-applications-and-api-permissions/ -> What applications are considered critical?
                #https://www.youtube.com/watch?v=T-ZnAUt1IP8 - Monitoring and Incident Response in Azure AD
                #https://docs.microsoft.com/en-us/security/compass/incident-response-playbook-app-consent#classifying-risky-permissions
                $appRolePermissionSensitivity = 'unclassified'
                <#
                if (
                    ($hlper.value -like 'Mail.*' -and $hlper.value -notlike 'Mail.ReadBasic*') -or
                    $hlper.value -like 'Contacts.*' -or
                    $hlper.value -like 'MailboxSettings.*' -or
                    $hlper.value -like 'People.*' -or
                    $hlper.value -like 'Files.*' -or
                    $hlper.value -like 'Notes.*' -or
                    $hlper.value -eq 'Directory.AccessAsUser.All' -or
                    $hlper.value -eq 'User_Impersonation' -or
                    $hlper.value -like '*Write.All*' -or
                    $hlper.value -like '*Write'
                ) {
                    $appRolePermissionSensitivity = 'critical'
                }
                #>
                <#
                Mail.* (including Mail.Send*, but not Mail.ReadBasic*)
                Contacts. *
                MailboxSettings.*
                People.*
                Files.*
                Notes.*
                Directory.AccessAsUser.All
                User_Impersonation
                #>
                $appRolePermissionSensitivity = getClassification -permission $hlper.value -permissionType 'appRolePermissions'

                $htOptInfo.AppRolePermissionSensitivity = $appRolePermissionSensitivity
                $htOptInfo.AppRoleDisplayName = $hlper.displayName
                $htOptInfo.AppRoleDescription = $hlper.description
                $null = $arrayServicePrincipalAppRoleAssignmentsOpt.Add($htOptInfo)
            }
            $durationPerfTrackServicePrincipalAppRoleAssignments = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)
        }
        #endregion ServicePrincipalAppRoleAssignments

        #region ServicePrincipalAppRoleAssignedTo
        $start = Get-Date
        $arrayServicePrincipalAppRoleAssignedToOpt = [System.Collections.ArrayList]@()
        if ($object.ServicePrincipalAppRoleAssignedTo) {

            foreach ($servicePrincipalAppRoleAssignedTo in $object.ServicePrincipalAppRoleAssignedTo) {

                $htOptInfo = [ordered] @{}
                $htOptInfo.principalDisplayName = $servicePrincipalAppRoleAssignedTo.principalDisplayName
                $htOptInfo.principalId = $servicePrincipalAppRoleAssignedTo.principalId
                if ($servicePrincipalAppRoleAssignedTo.principalType -eq 'User') {
                    if ($htPrincipalsResolved.($servicePrincipalAppRoleAssignedTo.principalId)) {
                        $htOptInfo.principalType = $htPrincipalsResolved.($servicePrincipalAppRoleAssignedTo.principalId).typeOnly
                    }
                    else {
                        $htOptInfo.principalType = $servicePrincipalAppRoleAssignedTo.principalType
                    }
                }
                else {
                    $htOptInfo.principalType = $servicePrincipalAppRoleAssignedTo.principalType
                }
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
                    if ($servicePrincipalAppRoleAssignedTo.principalType -eq 'User') {
                        if ($htUsersAndGroupsAppRoleAssignmentsUser.($servicePrincipalAppRoleAssignedTo.principalId).($servicePrincipalAppRoleAssignedTo.id)) {
                            $appRoleId = $htUsersAndGroupsAppRoleAssignmentsUser.($servicePrincipalAppRoleAssignedTo.principalId).($servicePrincipalAppRoleAssignedTo.id).appRoleId
                            if ($htAppRoles.($appRoleId)) {
                                $hlper = $htAppRoles.($appRoleId)
                                $htOptInfo.roleId = $appRoleId
                                $htOptInfo.roleOrigin = $hlper.origin
                                $htOptInfo.roleAllowedMemberTypes = $hlper.allowedMemberTypes
                                $htOptInfo.roleDisplayName = $hlper.displayName
                                $htOptInfo.roleDescription = $hlper.description
                                $htOptInfo.roleValue = $hlper.value
                            }
                            else {
                                $htOptInfo.roleId = $appRoleId
                            }
                        }
                    }
                    if ($servicePrincipalAppRoleAssignedTo.principalType -eq 'Group') {
                        if ($htUsersAndGroupsAppRoleAssignmentsGroup.($servicePrincipalAppRoleAssignedTo.principalId).($servicePrincipalAppRoleAssignedTo.id)) {
                            $appRoleId = $htUsersAndGroupsAppRoleAssignmentsGroup.($servicePrincipalAppRoleAssignedTo.principalId).($servicePrincipalAppRoleAssignedTo.id).appRoleId
                            if ($htAppRoles.($appRoleId)) {
                                $hlper = $htAppRoles.($appRoleId)
                                $htOptInfo.roleId = $appRoleId
                                $htOptInfo.roleOrigin = $hlper.origin
                                $htOptInfo.roleAllowedMemberTypes = $hlper.allowedMemberTypes
                                $htOptInfo.roleDisplayName = $hlper.displayName
                                $htOptInfo.roleDescription = $hlper.description
                                $htOptInfo.roleValue = $hlper.value
                            }
                            else {
                                $htOptInfo.roleId = $appRoleId
                            }
                        }
                    }
                }
                $null = $arrayServicePrincipalAppRoleAssignedToOpt.Add($htOptInfo)
            }
            $durationPerfTrackServicePrincipalAppRoleAssignedTo = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)
        }
        #endregion ServicePrincipalAppRoleAssignedTo


        if (-not $NoAzureRoleAssignments) {

            $start = Get-Date
            $htSPAzureRoleAssignments = @{}

            #region AzureRoleAssignmentsPrep
            $arrayServicePrincipalGroupMembershipsOpt = [System.Collections.ArrayList]@()
            if ($object.ServicePrincipalGroupMemberships) {

                foreach ($servicePrincipalGroupMembership in $object.ServicePrincipalGroupMemberships | Sort-Object) {
                    $htOptInfo = [ordered] @{}
                    if ($htAaDGroups.($servicePrincipalGroupMembership)) {
                        $htOptInfo.DisplayName = $htAaDGroups.($servicePrincipalGroupMembership).groupDetails.displayName
                    }
                    else {
                        $htOptInfo.DisplayName = '<n/a>'
                    }
                    $htOptInfo.ObjectId = $servicePrincipalGroupMembership
                    $null = $arrayServicePrincipalGroupMembershipsOpt.Add($htOptInfo)

                    if ($htAssignmentsByPrincipalIdGroups.($servicePrincipalGroupMembership)) {
                        foreach ($roleAssignmentSPThroughGroup in $htAssignmentsByPrincipalIdGroups.($servicePrincipalGroupMembership)) {
                            $roleAssignmentSPThroughGroup_assignment_id = $roleAssignmentSPThroughGroup.assignment.id
                            if (-not $htSPAzureRoleAssignments.($roleAssignmentSPThroughGroup_assignment_id)) {
                                $htSPAzureRoleAssignments.($roleAssignmentSPThroughGroup_assignment_id) = @{}
                                $htSPAzureRoleAssignments.($roleAssignmentSPThroughGroup_assignment_id).results = [System.Collections.ArrayList]@()
                            }
                            $htTemp = @{}
                            $htTemp.roleAssignment = $roleAssignmentSPThroughGroup_assignment_id
                            $htTemp.roleAssignmentFull = $roleAssignmentSPThroughGroup
                            $htTemp.appliesThrough = "$($htAaDGroups.($servicePrincipalGroupMembership).groupDetails.displayName) ($servicePrincipalGroupMembership)"
                            $htTemp.applicability = 'indirect'
                            $null = ($htSPAzureRoleAssignments.($roleAssignmentSPThroughGroup_assignment_id).results).Add($htTemp)
                        }
                    }
                }
                $durationPerfTrackAzureRoleAssignmentsPrep = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)
            }
            #endregion AzureRoleAssignmentsPrep

            #region AzureRoleAssignmentsOpt
            $start = Get-Date
            if ($htAssignmentsByPrincipalIdServicePrincipals.($spId)) {
                foreach ($roleAssignmentSP in $htAssignmentsByPrincipalIdServicePrincipals.($spId)) {
                    $roleAssignmentSP_assignment_id = $roleAssignmentSP.assignment.id
                    if (-not $htSPAzureRoleAssignments.($roleAssignmentSP_assignment_id)) {
                        $htSPAzureRoleAssignments.($roleAssignmentSP_assignment_id) = @{}
                        $htSPAzureRoleAssignments.($roleAssignmentSP_assignment_id).results = [System.Collections.ArrayList]@()
                    }
                    $htTemp = @{}
                    $htTemp.roleAssignment = $roleAssignmentSP_assignment_id
                    $htTemp.roleAssignmentFull = $roleAssignmentSP
                    $htTemp.appliesThrough = ''
                    $htTemp.applicability = 'direct'
                    $null = ($htSPAzureRoleAssignments.($roleAssignmentSP_assignment_id).results).Add($htTemp)
                }
                $durationPerfTrackAzureRoleAssignmentsOpt1 = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)
            }

            $start = Get-Date
            $arrayServicePrincipalAzureRoleAssignmentsOpt = [System.Collections.ArrayList]@()
            if ($htSPAzureRoleAssignments.Keys.Count -gt 0) {
                foreach ($roleAssignment in $htSPAzureRoleAssignments.Values.results | Sort-Object -Property roleAssignment) {

                    $hlproleAssignmentFull = $roleAssignment.roleAssignmentFull
                    $htOptInfo = [ordered] @{}

                    if ($hlproleAssignmentFull.assignmentPIMDetails) {
                        $pimBased = $true
                    }
                    else {
                        $pimBased = $false
                    }

                    $htOptInfo.priviledgedIdentityManagementBased = $pimBased
                    $htOptInfo.roleAssignmentId = $roleAssignment.roleAssignment
                    $htOptInfo.roleIsCritical = $hlproleAssignmentFull.roleIsCritical
                    $htOptInfo.roleName = $hlproleAssignmentFull.roleName
                    $htOptInfo.roleId = $hlproleAssignmentFull.roleId
                    $htOptInfo.roleType = $hlproleAssignmentFull.type
                    $htOptInfo.roleAssignmentApplicability = $roleAssignment.applicability
                    $htOptInfo.roleAssignmentAppliesThrough = $roleAssignment.appliesThrough
                    $htOptInfo.roleAssignmentAssignmentScope = $hlproleAssignmentFull.assignmentScope
                    $htOptInfo.roleAssignmentAssignmentScopeId = $hlproleAssignmentFull.assignmentScopeId
                    $htOptInfo.roleAssignmentAssignmentScopeName = $hlproleAssignmentFull.assignmentScopeName
                    $htOptInfo.roleAssignmentAssignmentResourceName = $hlproleAssignmentFull.assignmentResourceName
                    $htOptInfo.roleAssignmentAssignmentResourceType = $hlproleAssignmentFull.assignmentResourceType
                    $htOptInfo.roleAssignment = $hlproleAssignmentFull.assignment.properties
                    if ($pimBased) {
                        $htOptInfo.priviledgedIdentityManagement = [ordered] @{}
                        $hlproleAssignmentFullAssignmentPIMDetails = $hlproleAssignmentFull.assignmentPIMDetails
                        $htOptInfo.priviledgedIdentityManagement.assignmentType = $hlproleAssignmentFullAssignmentPIMDetails.assignmentType
                        $htOptInfo.priviledgedIdentityManagement.startDateTime = $hlproleAssignmentFullAssignmentPIMDetails.startDateTime
                        $htOptInfo.priviledgedIdentityManagement.endDateTime = $hlproleAssignmentFullAssignmentPIMDetails.endDateTime
                        $htOptInfo.priviledgedIdentityManagement.createdOn = $hlproleAssignmentFullAssignmentPIMDetails.createdOn
                        $htOptInfo.priviledgedIdentityManagement.updatedOn = $hlproleAssignmentFullAssignmentPIMDetails.updatedOn
                    }
                    $null = $arrayServicePrincipalAzureRoleAssignmentsOpt.Add($htOptInfo)
                }
                $durationPerfTrackAzureRoleAssignmentsOpt2 = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)
            }
            #endregion AzureRoleAssignmentsOpt
        }
        else {
            $arrayServicePrincipalAzureRoleAssignmentsOpt = $null

            $arrayServicePrincipalGroupMembershipsOpt = [System.Collections.ArrayList]@()
            if ($object.ServicePrincipalGroupMemberships) {
                foreach ($servicePrincipalGroupMembership in $object.ServicePrincipalGroupMemberships | Sort-Object) {
                    $htOptInfo = [ordered] @{}
                    if ($htAaDGroups.($servicePrincipalGroupMembership)) {
                        #Write-Host "SP GroupMembership      :" $htAaDGroups.($servicePrincipalGroupMembership).groupDetails.displayName "($($servicePrincipalGroupMembership))"
                        $htOptInfo.DisplayName = $htAaDGroups.($servicePrincipalGroupMembership).groupDetails.displayName
                        $htOptInfo.ObjectId = $servicePrincipalGroupMembership
                    }
                    else {
                        #Write-Host "SP GroupMembership      :" "notResolved" "($($servicePrincipalGroupMembership))"
                        $htOptInfo.DisplayName = '<n/a>'
                        $htOptInfo.ObjectId = $servicePrincipalGroupMembership
                    }
                    $null = $arrayServicePrincipalGroupMembershipsOpt.Add($htOptInfo)
                }
            }
        }

        #region ManagedIdentity
        $start = Get-Date
        $arrayManagedIdentityOpt = [System.Collections.ArrayList]@()
        if ($object.ManagedIdentity) {

            foreach ($altName in $object.ManagedIdentity.ManagedIdentityAlternativeNames) {

                $relict = $false
                if ($altName -notlike 'isExplicit=*') {
                    $s1 = $altName -replace '.*/providers/'
                    $rm = $s1 -replace '.*/'
                    $resourceType = $s1 -replace "/$($rm)"

                    $altNameSplit = $altName.split('/')
                    if ($altName -like '/subscriptions/*') {
                        if ($resourceType -eq 'Microsoft.Authorization/policyAssignments') {
                            if ($runTenantRoot) {
                                if (-not $NoAzureRoleAssignments) {
                                    if (-not $htCacheAssignmentsPolicy.($altname.ToLower())) {
                                        $relict = $true
                                    }
                                }
                            }
                            if ($altName -like '/subscriptions/*/resourceGroups/*') {
                                $miResourceScope = "Sub $($altNameSplit[2]) RG $($altNameSplit[4])"
                            }
                            else {
                                $miResourceScope = "Sub $($altNameSplit[2])"
                            }
                        }
                        else {
                            $miResourceScope = "Sub $($altNameSplit[2])"
                        }
                    }
                    else {
                        if ($resourceType -eq 'Microsoft.Authorization/policyAssignments') {
                            if ($runTenantRoot) {
                                if (-not $NoAzureRoleAssignments) {
                                    if (-not $htCacheAssignmentsPolicy.($altname.ToLower())) {
                                        $relict = $true
                                    }
                                }
                            }
                            $miResourceScope = "MG $($altNameSplit[4])"
                        }
                        else {
                            $miResourceScope = "MG $($altNameSplit[4])"
                        }
                    }

                    if ($relict) {
                        Write-Host "Relict (MI PolicyAssignment) found: Name/ObjectId:'$($object.ServicePrincipalDetails.displayName)/$($object.ServicePrincipalDetails.id)' - $altName"
                    }
                }
            }

            $htOptInfo = [ordered]@{}
            #$hlper = $htServicePrincipalsAndAppsOnlyEnriched.($spId)
            $htOptInfo.type = $object.subtype
            $htOptInfo.alternativeName = $altname
            $htOptInfo.resourceType = $resourceType
            $htOptInfo.resourceScope = $miResourceScope
            $htOptInfo.relict = $relict
            $null = $arrayManagedIdentityOpt.Add($htOptInfo)
            $durationPerfTrackManagedIdentity = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)
        }
        #endregion ManagedIdentity
    }

    #region Application
    if ($object.Application) {
        #Write-host "SP type:                : Application - objId: $($object.Application.ApplicationDetails.id) appId: $($object.Application.ApplicationDetails.appId)"

        #region ApplicationAADRoleAssignedOn
        $start = Get-Date
        $arrayApplicationAADRoleAssignedOnOpt = [System.Collections.ArrayList]@()
        if ($object.Application.ApplicationAADRoleAssignedOn) {
            foreach ($aadRoleAssignedOn in $object.Application.ApplicationAADRoleAssignedOn | Sort-Object -Property roleName, id) {
                $hlperAadRoleDefinition = $htAadRoleDefinitions.($aadRoleAssignedOn.roleDefinitionId)

                $htOptInfo = [ordered] @{}
                $htOptInfo.id = $aadRoleAssignedOn.id
                $htOptInfo.roleName = $hlperAadRoleDefinition.displayName
                $htOptInfo.roleId = $aadRoleAssignedOn.roleDefinitionId
                $htOptInfo.roleDescription = $hlperAadRoleDefinition.description
                $htOptInfo.principalId = $aadRoleAssignedOn.principalId
                $htOptInfo.principalDisplayName = $aadRoleAssignedOn.principalDisplayName

                if ($aadRoleAssignedOn.principalType -eq 'User') {
                    $htOptInfo.principalType = $aadRoleAssignedOn.principalUserType
                }
                elseif ($aadRoleAssignedOn.principalType -eq 'ServicePrincipal') {
                    $hlperSP = $htSpLookup.($aadRoleAssignedOn.principalId)
                    $htOptInfo.principalType = $hlperSP.objectTypeConcatinated
                }
                else {
                    $htOptInfo.principalType = $aadRoleAssignedOn.principalType
                }
                $null = $arrayApplicationAADRoleAssignedOnOpt.Add($htOptInfo)
            }
            $durationPerfTrackApplicationAADRoleAssignedOn = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)
        }
        #endregion ApplicationAADRoleAssignedOn

        #region ApplicationOwner
        $start = Get-Date
        $arrayApplicationOwnerOpt = [System.Collections.ArrayList]@()
        if ($htAppOwners.($object.Application.ApplicationDetails.id)) {
            foreach ($appOwner in $htAppOwners.($object.Application.ApplicationDetails.id)) {
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
                            if ($owner -ne 'noOwner') {
                                if ($htSPOwnersFinal.($owner.id)) {
                                    $arrayOwnedBy += ($htSPOwnersFinal.($owner.id))
                                }
                                else {
                                    $arrayOwnedBy += ($owner)
                                }
                            }
                            else {
                                $arrayOwnedBy += ($owner)
                            }

                        }
                        if ($servicePrincipalOwner.type -ne '#microsoft.graph.user') {
                            $htOptInfo.ownedBy = $arrayOwnedBy
                        }
                        $null = $arrayApplicationOwner.Add($htOptInfo)
                    }

                }
                $htOptInfo = [ordered] @{}
                $htOptInfo.id = $appOwner.id
                $htOptInfo.displayName = $appOwner.displayName
                if ($appOwner.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
                    $htOptInfo.principalType = $htSpLookup.($appOwner.id).objectTypeConcatinated
                }
                if ($appOwner.'@odata.type' -eq '#microsoft.graph.user') {
                    $htOptInfo.principalType = $htPrincipalsResolved.($appOwner.id).typeOnly
                }
                $htOptInfo.applicability = 'direct'
                if ($appOwner.'@odata.type' -ne '#microsoft.graph.user') {
                    $htOptInfo.ownedBy = $arrayApplicationOwner
                }
                $null = $arrayApplicationOwnerOpt.Add($htOptInfo)
            }
            $durationPerfTrackApplicationOwner = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)
        }
        #endregion ApplicationOwner

        #region ApplicationFederatedIdentityCredentials
        $start = Get-Date
        $arrayFederatedIdentityCredentialsOpt = [System.Collections.ArrayList]@()
        if ($htFederatedIdentityCredentials.($object.Application.ApplicationDetails.id)) {

            foreach ($federatedIdentityCredential in $htFederatedIdentityCredentials.($object.Application.ApplicationDetails.id)) {
                $htOptInfo = [ordered] @{}
                $htOptInfo.name = $federatedIdentityCredential.name
                $htOptInfo.description = $federatedIdentityCredential.description
                $htOptInfo.id = $federatedIdentityCredential.id
                $htOptInfo.issuer = $federatedIdentityCredential.issuer
                $htOptInfo.subject = $federatedIdentityCredential.subject
                $htOptInfo.audiences = $federatedIdentityCredential.audiences
                $null = $arrayFederatedIdentityCredentialsOpt.Add($htOptInfo)
            }
            $durationPerfTrackFederatedIdentityCredentials = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)
        }
        #endregion ApplicationFederatedIdentityCredentials

        #region ApplicationSecrets
        $start = Get-Date
        $currentDateUTC = (Get-Date).ToUniversalTime()
        $arrayApplicationPasswordCredentialsOpt = [System.Collections.ArrayList]@()
        if ($object.Application.ApplicationPasswordCredentials) {
            $appPasswordCredentialsCount = ($object.Application.ApplicationPasswordCredentials).count
            if ($appPasswordCredentialsCount -gt 0) {
                foreach ($appPasswordCredential in $object.Application.ApplicationPasswordCredentials.Values | Sort-Object -Property keyId) {
                    $hlperApplicationPasswordCredential = $appPasswordCredential
                    if ($hlperApplicationPasswordCredential.displayName) {
                        $displayName = $hlperApplicationPasswordCredential.displayName
                    }
                    else {
                        $displayName = 'notGiven'
                    }

                    $passwordCredentialExpiryTotalDays = (New-TimeSpan -Start $currentDateUTC -End $hlperApplicationPasswordCredential.endDateTime).TotalDays
                    $expiryApplicationPasswordCredential = [math]::Round($passwordCredentialExpiryTotalDays, 0)
                    if ($passwordCredentialExpiryTotalDays -lt 0) {
                        $expiryApplicationPasswordCredential = 'expired'
                        $appPasswordCredentialsExpiredCount++
                    }
                    elseif ($passwordCredentialExpiryTotalDays -lt $ApplicationSecretExpiryWarning) {
                        $appPasswordCredentialsGracePeriodExpiryCount++
                        $expiryApplicationPasswordCredential = "expires soon (less than grace period $ApplicationSecretExpiryWarning)"
                    }
                    else {
                        if ($passwordCredentialExpiryTotalDays -gt $ApplicationSecretExpiryMax) {
                            $appPasswordCredentialsExpiryOKMoreThanMaxCount++
                            $expiryApplicationPasswordCredential = "expires in more than $ApplicationSecretExpiryMax days"
                        }
                        else {
                            $appPasswordCredentialsExpiryOKCount++
                            $expiryApplicationPasswordCredential = "expires in $ApplicationSecretExpiryWarning to $ApplicationSecretExpiryMax days"
                        }
                    }

                    $htOptInfo = [ordered] @{}
                    $htOptInfo.keyId = $hlperApplicationPasswordCredential.keyId
                    $htOptInfo.displayName = $displayName
                    $htOptInfo.expiryInfo = $expiryApplicationPasswordCredential
                    $htOptInfo.endDateTime = $hlperApplicationPasswordCredential.endDateTime
                    $htOptInfo.endDateTimeFormated = ($hlperApplicationPasswordCredential.endDateTime).ToString('dd-MMM-yyyy HH:mm:ss')
                    $null = $arrayApplicationPasswordCredentialsOpt.Add($htOptInfo)
                }
            }
            $durationPerfTrackApplicationSecrets = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)
        }
        #endregion ApplicationSecrets

        #region ApplicationCertificates
        $start = Get-Date
        $arrayApplicationKeyCredentialsOpt = [System.Collections.ArrayList]@()
        if ($object.Application.ApplicationKeyCredentials) {
            $appKeyCredentialsCount = ($object.Application.ApplicationKeyCredentials).count
            if ($appKeyCredentialsCount -gt 0) {

                foreach ($appKeyCredential in $object.Application.ApplicationKeyCredentials.Values | Sort-Object -Property keyId) {
                    $hlperApplicationKeyCredential = $appKeyCredential

                    $keyCredentialExpiryTotalDays = (New-TimeSpan -Start $currentDateUTC -End $hlperApplicationKeyCredential.endDateTime).TotalDays
                    $expiryApplicationKeyCredential = [math]::Round($keyCredentialExpiryTotalDays, 0)

                    if ($keyCredentialExpiryTotalDays -lt 0) {
                        $expiryApplicationKeyCredential = 'expired'
                        $appKeyCredentialsExpiredCount++
                    }
                    elseif ($keyCredentialExpiryTotalDays -lt $ApplicationCertificateExpiryWarning) {
                        $expiryApplicationKeyCredential = "expires soon (less than grace period $ApplicationCertificateExpiryWarning)"
                        $appKeyCredentialsGracePeriodExpiryCount++
                    }
                    else {
                        if ($keyCredentialExpiryTotalDays -gt $ApplicationCertificateExpiryMax) {
                            $expiryApplicationKeyCredential = "expires in more than $ApplicationCertificateExpiryMax days"
                            $appKeyCredentialsExpiryOKMoreThanMaxCount++
                        }
                        else {
                            $expiryApplicationKeyCredential = "expires in $ApplicationCertificateExpiryWarning to $ApplicationCertificateExpiryMax days"
                            $appKeyCredentialsExpiryOKCount++
                        }
                    }

                    $htOptInfo = [ordered] @{}
                    $htOptInfo.keyId = $hlperApplicationKeyCredential.keyId
                    $htOptInfo.displayName = $hlperApplicationKeyCredential.displayName
                    $htOptInfo.customKeyIdentifier = $hlperApplicationKeyCredential.customKeyIdentifier
                    $htOptInfo.expiryInfo = $expiryApplicationKeyCredential
                    $htOptInfo.endDateTime = $hlperApplicationKeyCredential.endDateTime
                    $htOptInfo.endDateTimeFormated = $hlperApplicationKeyCredential.endDateTime.ToString('dd-MMM-yyyy HH:mm:ss')
                    $null = $arrayApplicationKeyCredentialsOpt.Add($htOptInfo)
                }
            }
            $durationPerfTrackApplicationCertificates = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)
        }
        #endregion ApplicationCertificates
    }
    #endregion Application


    #region finalArray
    $start = Get-Date
    $spArray = [System.Collections.ArrayList]@()
    if ($spOrAppWithoutSP.SPOrAppOnly -eq 'SP') {
        if ($object.ServicePrincipalDetails.appRoles.Count -gt 0) {
            $hlpxAppRoles = $object.ServicePrincipalDetails.appRoles | Sort-Object -Property value
        }
        else {
            $hlpxAppRoles = $object.ServicePrincipalDetails.appRoles
        }
        if ($object.ServicePrincipalDetails.oauth2PermissionScopes.Count -gt 0) {
            $hlpxOauth2PermissionScopes = $object.ServicePrincipalDetails.oauth2PermissionScopes | Sort-Object -Property value
        }
        else {
            $hlpxOauth2PermissionScopes = $object.ServicePrincipalDetails.oauth2PermissionScopes
        }
        $null = $spArray.Add([PSCustomObject]@{
                SPObjectId = $spId
                SPAppId = $object.ServicePrincipalDetails.appId
                SPDisplayName = $object.ServicePrincipalDetails.displayName
                SPDescription = $object.ServicePrincipalDetails.description
                SPNotes = $object.ServicePrincipalDetails.notes
                SPAppOwnerOrganizationId = $object.ServicePrincipalDetails.appOwnerOrganizationId
                SPServicePrincipalType = $object.ServicePrincipalDetails.servicePrincipalType
                SPAccountEnabled = $object.ServicePrincipalDetails.accountEnabled
                SPCreatedDateTime = $object.ServicePrincipalDetails.createdDateTime
                #SPPublisherName             = $object.ServicePrincipalDetails.publisherName
                SPVerifiedPublisher = $object.ServicePrincipalDetails.verifiedPublisher
                SPHomepage = $object.ServicePrincipalDetails.homepage
                SPErrorUrl = $object.ServicePrincipalDetails.errorUrl
                SPLoginUrl = $object.ServicePrincipalDetails.loginUrl
                SPLogoutUrl = $object.ServicePrincipalDetails.logoutUrl
                SPPreferredSingleSignOnMode = $object.ServicePrincipalDetails.preferredSingleSignOnMode
                SPTags = $object.ServicePrincipalDetails.tags
                SPAppRoles = $hlpxAppRoles
                SPOauth2PermissionScopes = $hlpxOauth2PermissionScopes
            })
    }

    if ($object.Application) {
        #Write-Host "$($object.ServicePrincipalDetails.displayName) is App"

        $appArray = [System.Collections.ArrayList]@()
        $null = $appArray.Add([PSCustomObject]@{
                APPObjectId = $object.Application.ApplicationDetails.id
                APPAppClientId = $object.Application.ApplicationDetails.appId
                APPDisplayName = $object.Application.ApplicationDetails.displayName
                APPDescription = $object.Application.ApplicationDetails.description
                APPNotes = $object.Application.ApplicationDetails.notes
                APPTags = $object.Application.ApplicationDetails.tags
                APPCreatedDateTime = $object.Application.ApplicationDetails.createdDateTime
                APPSignInAudience = $object.Application.ApplicationDetails.signInAudience
                APPPublisherDomain = $object.Application.ApplicationDetails.publisherDomain
                APPVerifiedPublisher = $object.Application.ApplicationDetails.verifiedPublisher
                APPGroupMembershipClaims = $object.Application.ApplicationDetails.groupMembershipClaims
                APPDefaultRedirectUri = $object.Application.ApplicationDetails.defaultRedirectUri
                APPRequiredResourceAccess = $object.Application.ApplicationDetails.requiredResourceAccess
            })

        if ($spOrAppWithoutSP.SPOrAppOnly -eq 'SP') {
            if ($arraySPOauth2PermissionGrantedTo.Count -gt 0) {
                $arraySPOauth2PermissionGrantedTo = ($arraySPOauth2PermissionGrantedTo | Sort-Object { $_.servicePrincipalDisplayName }, { $_.scope }, { $_.permissionId }, { $_.consentType })
            }

            $null = $script:cu.Add([PSCustomObject]@{
                    #SPObjId                     = $spId
                    #SPDisplayName               = $object.ServicePrincipalDetails.displayName
                    #SPType                      = $object.ServicePrincipalDetails.servicePrincipalType
                    #SPAppRoles                  = $object.ServicePrincipalDetails.appRoles
                    #SPpublishedPermissionScopes = $object.ServicePrincipalDetails.publishedPermissionScopes
                    ObjectType = $object.objectTypeConcatinated
                    ObjectId = $spId
                    #SP                          = $object.ServicePrincipalDetails | Select-Object -ExcludeProperty '@odata.id'
                    SP = $spArray
                    SPOwners = $arrayServicePrincipalOwnerOpt
                    SPOwnedObjects = $arrayServicePrincipalOwnedObjectsOpt
                    SPAADRoleAssignments = $arrayServicePrincipalAADRoleAssignmentsOpt
                    SPAAADRoleAssignedOn = $arrayServicePrincipalAADRoleAssignedOnOpt
                    SPOauth2PermissionGrants = $arrayServicePrincipalOauth2PermissionGrantsOpt
                    SPOauth2PermissionGrantedTo = $arraySPOauth2PermissionGrantedTo
                    SPAppRoleAssignments = $arrayServicePrincipalAppRoleAssignmentsOpt
                    SPAppRoleAssignedTo = $arrayServicePrincipalAppRoleAssignedToOpt
                    SPGroupMemberships = $arrayServicePrincipalGroupMembershipsOpt
                    SPAzureRoleAssignments = $arrayServicePrincipalAzureRoleAssignmentsOpt
                    #APP                         = $object.Application.ApplicationDetails | Select-Object -ExcludeProperty '@odata.id'
                    APP = $appArray
                    APPAAADRoleAssignedOn = $arrayApplicationAADRoleAssignedOnOpt
                    #approles always inherited from sp
                    #APPAppRoles                 = $object.Application.ApplicationDetails.appRoles
                    APPAppOwners = $arrayApplicationOwnerOpt
                    APPPasswordCredentials = $arrayApplicationPasswordCredentialsOpt
                    APPKeyCredentials = $arrayApplicationKeyCredentialsOpt
                    APPFederatedIdentityCredentials = $arrayFederatedIdentityCredentialsOpt
                })
        }
        if ($spOrAppWithoutSP.SPOrAppOnly -eq 'AppOnly') {
            $null = $script:cu.Add([PSCustomObject]@{
                    #SPObjId                     = $spId
                    #SPDisplayName               = $object.ServicePrincipalDetails.displayName
                    #SPType                      = $object.ServicePrincipalDetails.servicePrincipalType
                    #SPAppRoles                  = $object.ServicePrincipalDetails.appRoles
                    #SPpublishedPermissionScopes = $object.ServicePrincipalDetails.publishedPermissionScopes
                    ObjectType = $object.objectTypeConcatinated
                    ObjectId = $object.Application.ApplicationDetails.id
                    #SP                          = $object.ServicePrincipalDetails | Select-Object -ExcludeProperty '@odata.id'
                    #APP                         = $object.Application.ApplicationDetails | Select-Object -ExcludeProperty '@odata.id'
                    APP = $appArray
                    APPAAADRoleAssignedOn = $arrayApplicationAADRoleAssignedOnOpt
                    #approles always inherited from sp
                    #APPAppRoles                 = $object.Application.ApplicationDetails.appRoles
                    APPAppOwners = $arrayApplicationOwnerOpt
                    APPPasswordCredentials = $arrayApplicationPasswordCredentialsOpt
                    APPKeyCredentials = $arrayApplicationKeyCredentialsOpt
                    APPFederatedIdentityCredentials = $arrayFederatedIdentityCredentialsOpt
                })
        }
    }
    elseif ($object.ManagedIdentity) {
        #Write-Host "$($object.ServicePrincipalDetails.displayName) is MI"
        if ($arraySPOauth2PermissionGrantedTo.Count -gt 0) {
            $arraySPOauth2PermissionGrantedTo = ($arraySPOauth2PermissionGrantedTo | Sort-Object { $_.servicePrincipalDisplayName }, { $_.scope }, { $_.permissionId }, { $_.consentType })
        }

        $null = $script:cu.Add([PSCustomObject]@{
                #SPObjId                     = $spId
                #SPDisplayName               = $object.ServicePrincipalDetails.displayName
                #SPType                      = $object.ServicePrincipalDetails.servicePrincipalType
                #SPAppRoles                  = $object.ServicePrincipalDetails.appRoles
                #SPpublishedPermissionScopes = $object.ServicePrincipalDetails.publishedPermissionScopes
                ObjectType = $object.objectTypeConcatinated
                ObjectId = $spId
                #SP                          = $object.ServicePrincipalDetails | Select-Object -ExcludeProperty '@odata.id'
                SP = $spArray
                SPOwners = $arrayServicePrincipalOwnerOpt
                SPOwnedObjects = $arrayServicePrincipalOwnedObjectsOpt
                SPAADRoleAssignments = $arrayServicePrincipalAADRoleAssignmentsOpt
                SPAAADRoleAssignedOn = $arrayServicePrincipalAADRoleAssignedOnOpt
                SPOauth2PermissionGrants = $arrayServicePrincipalOauth2PermissionGrantsOpt
                SPOauth2PermissionGrantedTo = $arraySPOauth2PermissionGrantedTo
                SPAppRoleAssignments = $arrayServicePrincipalAppRoleAssignmentsOpt
                SPAppRoleAssignedTo = $arrayServicePrincipalAppRoleAssignedToOpt
                SPGroupMemberships = $arrayServicePrincipalGroupMembershipsOpt
                SPAzureRoleAssignments = $arrayServicePrincipalAzureRoleAssignmentsOpt
                ManagedIdentity = $arrayManagedIdentityOpt
            })
    }
    else {
        #Write-Host "$($object.ServicePrincipalDetails.displayName) is neither App, nore MI"
        if ($arraySPOauth2PermissionGrantedTo.Count -gt 0) {
            $arraySPOauth2PermissionGrantedTo = ($arraySPOauth2PermissionGrantedTo | Sort-Object { $_.servicePrincipalDisplayName }, { $_.scope }, { $_.permissionId }, { $_.consentType })
        }

        $null = $script:cu.Add([PSCustomObject]@{
                #SPObjId                     = $spId
                #SPDisplayName               = $object.ServicePrincipalDetails.displayName
                #SPType                      = $object.ServicePrincipalDetails.servicePrincipalType
                #SPAppRoles                  = $object.ServicePrincipalDetails.appRoles
                #SPpublishedPermissionScopes = $object.ServicePrincipalDetails.publishedPermissionScopes
                ObjectType = $object.objectTypeConcatinated
                ObjectId = $spId
                #SP                          = $object.ServicePrincipalDetails | Select-Object -ExcludeProperty '@odata.id'
                SP = $spArray
                SPOwners = $arrayServicePrincipalOwnerOpt
                SPOwnedObjects = $arrayServicePrincipalOwnedObjectsOpt
                SPAADRoleAssignments = $arrayServicePrincipalAADRoleAssignmentsOpt
                SPAAADRoleAssignedOn = $arrayServicePrincipalAADRoleAssignedOnOpt
                SPOauth2PermissionGrants = $arrayServicePrincipalOauth2PermissionGrantsOpt
                SPOauth2PermissionGrantedTo = $arraySPOauth2PermissionGrantedTo
                SPAppRoleAssignments = $arrayServicePrincipalAppRoleAssignmentsOpt
                SPAppRoleAssignedTo = $arrayServicePrincipalAppRoleAssignedToOpt
                SPGroupMemberships = $arrayServicePrincipalGroupMembershipsOpt
                SPAzureRoleAssignments = $arrayServicePrincipalAzureRoleAssignmentsOpt
            })
    }
    $durationPerfTrackFinalArray = [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds)

    #endregion finalArray

    $processedServicePrincipalsCount++
    $null = $script:arrayPerformanceTracking.Add([PSCustomObject]@{
            Type = $spOrAppWithoutSP.SPOrAppOnly
            ServicePrincipalId = $spId
            ServicePrincipalDisplayName = $object.ServicePrincipalDetails.displayName
            ApplicationId = $object.Application.ApplicationDetails.id
            ApplicationDisplayName = $object.Application.ApplicationDetails.displayName
            ProcessedSequenceCount = $processedServicePrincipalsCount
            ServicePrincipalOwnedObjects = $durationPerfTrackServicePrincipalOwnedObjects
            ServicePrincipalOwners = $durationPerfTrackServicePrincipalOwners
            ServicePrincipalAADRoleAssignments = $durationPerfTrackServicePrincipalAADRoleAssignments
            ServicePrincipalAADRoleAssignedOn = $durationPerfTrackServicePrincipalAADRoleAssignedOn
            ServicePrincipalOauth2PermissionGrants = $durationPerfTrackServicePrincipalOauth2PermissionGrants
            SPOauth2PermissionGrantedTo = $durationPerfTrackSPOauth2PermissionGrantedTo
            ServicePrincipalAppRoleAssignments = $durationPerfTrackServicePrincipalAppRoleAssignments
            ServicePrincipalAppRoleAssignedTo = $durationPerfTrackServicePrincipalAppRoleAssignedTo
            AzureRoleAssignmentsPrep = $durationPerfTrackAzureRoleAssignmentsPrep
            AzureRoleAssignmentsOpt1 = $durationPerfTrackAzureRoleAssignmentsOpt1
            AzureRoleAssignmentsOpt2 = $durationPerfTrackAzureRoleAssignmentsOpt2
            ApplicationAADRoleAssignedOn = $durationPerfTrackApplicationAADRoleAssignedOn
            ApplicationOwner = $durationPerfTrackApplicationOwner
            ApplicationFederatedIdentityCredentials = $durationPerfTrackFederatedIdentityCredentials
            ApplicationSecrets = $durationPerfTrackApplicationSecrets
            ApplicationCertificates = $durationPerfTrackApplicationCertificates
            ManagedIdentity = $durationPerfTrackManagedIdentity
            FinalArray = $durationPerfTrackFinalArray
        })


    $durationPerfTrackServicePrincipalOwnedObjects = $null
    $durationPerfTrackServicePrincipalOwners = $null
    $durationPerfTrackServicePrincipalAADRoleAssignments = $null
    $durationPerfTrackServicePrincipalAADRoleAssignedOn = $null
    $durationPerfTrackServicePrincipalOauth2PermissionGrants = $null
    $durationPerfTrackSPOauth2PermissionGrantedTo = $null
    $durationPerfTrackServicePrincipalAppRoleAssignments = $null
    $durationPerfTrackServicePrincipalAppRoleAssignedTo = $null
    $durationPerfTrackAzureRoleAssignmentsPrep = $null
    $durationPerfTrackAzureRoleAssignmentsOpt1 = $null
    $durationPerfTrackAzureRoleAssignmentsOpt2 = $null
    $durationPerfTrackApplicationAADRoleAssignedOn = $null
    $durationPerfTrackApplicationOwner = $null
    $durationPerfTrackFederatedIdentityCredentials = $null
    $durationPerfTrackApplicationSecrets = $null
    $durationPerfTrackApplicationCertificates = $null
    $durationPerfTrackManagedIdentity = $null
    $durationPerfTrackFinalArray = $null

    ($enrichmentProcessCounter).counter++
    if (($enrichmentProcessCounter).counter % $enrichmentProcessindicator -eq 0) {
        Write-Host "processed: $(($enrichmentProcessCounter).counter)"
    }

} -ThrottleLimit $ThrottleLimitLocal
Write-Host "Enrichment completed: $processedServicePrincipalsCount ServicePrincipals processed"
$endEnrichmentSP = Get-Date
$duration = New-TimeSpan -Start $startEnrichmentSP -End $endEnrichmentSP
Write-Host "Service Principals enrichment duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"

if ($azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions) {
    $JSONPath = "JSON_SP_$($fileNameMGRef)"
    if (Test-Path -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($JSONPath)") {
        Write-Host ' Cleaning old state (Pipeline only)'
        Remove-Item -Recurse -Force "$($outputPath)$($DirectorySeparatorChar)$($JSONPath)"
    }
}
else {
    $fileTimestamp = (Get-Date -Format $FileTimeStampFormat)
    $JSONPath = "JSON_SP_$($fileNameMGRef)_$($fileTimestamp)"
    Write-Host " Creating new state ($($JSONPath)) (local only))"
}

$null = New-Item -Name $JSONPath -ItemType directory -Path $outputPath
foreach ($entry in $cu) {
    $entry | ConvertTo-Json -Depth 99 | Set-Content -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($JSONPath)$($DirectorySeparatorChar)$($entry.ObjectId)_$($entry.ObjectType -replace ' ', '-').json" -Encoding utf8 -Force
}
#endregion enrichedAADSPData

Write-Host 'Processing totals per capability (in ms)'
Write-Host ' ServicePrincipalOwnedObjects:' ($arrayPerformanceTracking.ServicePrincipalOwnedObjects | Measure-Object -Sum).Sum
Write-Host ' ServicePrincipalOwners:' ($arrayPerformanceTracking.ServicePrincipalOwners | Measure-Object -Sum).Sum
Write-Host ' ServicePrincipalAADRoleAssignments:' ($arrayPerformanceTracking.ServicePrincipalAADRoleAssignments | Measure-Object -Sum).Sum
Write-Host ' ServicePrincipalAADRoleAssignedOn:' ($arrayPerformanceTracking.ServicePrincipalAADRoleAssignedOn | Measure-Object -Sum).Sum
Write-Host ' ServicePrincipalOauth2PermissionGrants:' ($arrayPerformanceTracking.ServicePrincipalOauth2PermissionGrants | Measure-Object -Sum).Sum
Write-Host ' SPOauth2PermissionGrantedTo:' ($arrayPerformanceTracking.SPOauth2PermissionGrantedTo | Measure-Object -Sum).Sum
Write-Host ' ServicePrincipalAppRoleAssignments:' ($arrayPerformanceTracking.ServicePrincipalAppRoleAssignments | Measure-Object -Sum).Sum
Write-Host ' ServicePrincipalAppRoleAssignedTo:' ($arrayPerformanceTracking.ServicePrincipalAppRoleAssignedTo | Measure-Object -Sum).Sum
Write-Host ' AzureRoleAssignmentsPrep:' ($arrayPerformanceTracking.AzureRoleAssignmentsPrep | Measure-Object -Sum).Sum
Write-Host ' AzureRoleAssignmentsOpt1:' ($arrayPerformanceTracking.AzureRoleAssignmentsOpt1 | Measure-Object -Sum).Sum
Write-Host ' AzureRoleAssignmentsOpt2:' ($arrayPerformanceTracking.AzureRoleAssignmentsOpt2 | Measure-Object -Sum).Sum
Write-Host ' ApplicationAADRoleAssignedOn:' ($arrayPerformanceTracking.ApplicationAADRoleAssignedOn | Measure-Object -Sum).Sum
Write-Host ' ApplicationOwner:' ($arrayPerformanceTracking.ApplicationOwner | Measure-Object -Sum).Sum
Write-Host ' ApplicationFederatedIdentityCredentials:' ($arrayPerformanceTracking.ApplicationFederatedIdentityCredentials | Measure-Object -Sum).Sum
Write-Host ' ApplicationSecrets:' ($arrayPerformanceTracking.ApplicationSecrets | Measure-Object -Sum).Sum
Write-Host ' ApplicationCertificates:' ($arrayPerformanceTracking.ApplicationCertificates | Measure-Object -Sum).Sum
Write-Host ' ManagedIdentity:' ($arrayPerformanceTracking.ManagedIdentity | Measure-Object -Sum).Sum
Write-Host ' FinalArray:' ($arrayPerformanceTracking.FinalArray | Measure-Object -Sum).Sum


#endregion AADSP

#endregion dataCollection

#region createoutputs

#region BuildHTML
#testhelper
#$fileTimestamp = (Get-Date -Format $FileTimeStampFormat)

$startBuildHTML = Get-Date

#filename
if ($azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions -eq $true) {
    $fileName = "$($Product)_$($fileNameMGRef)"
}
else {
    $fileName = "$($Product)_$($ProductVersion)_$($fileTimestamp)_$($fileNameMGRef)"
}

#Export perf csv
$arrayPerformanceTracking | Export-Csv -Path "$($outputPath)$($DirectorySeparatorChar)$($fileName)_perf.csv" -Delimiter "$csvDelimiter" -NoTypeInformation


Write-Host 'Building HTML'

$html = $null
$html += @"
<!doctype html>
<html lang="en" style="height: 100%">
<head>
    <meta charset="utf-8" />
    <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
    <meta http-equiv="Pragma" content="no-cache" />
    <meta http-equiv="Expires" content="0" />
    <title>$($Product)</title>
    <link rel="stylesheet" type="text/css" href="https://www.azadvertizer.net/azadserviceprincipalinsights/css/azadserviceprincipalinsightsmain_001_007.css">
    <script src="https://www.azadvertizer.net/azadserviceprincipalinsights/js/jquery-3.6.0.min.js"></script>
    <script src="https://www.azadvertizer.net/azadserviceprincipalinsights/js/jquery-ui-1.13.0.min.js"></script>
    <script src="https://www.azadvertizer.net/azadserviceprincipalinsights/js/fontawesome-0c0b5cbde8.js"></script>
    <script src="https://www.azadvertizer.net/azadserviceprincipalinsights/tablefilter/tablefilter.js"></script>
    <script src="https://www.azadvertizer.net/azadserviceprincipalinsights/js/chartjs-2.8.0.min.js"></script>

    <script>
        `$(window).on('load', function () {
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
    <div class="summary" id="summary"><p class="pbordered">Azure Active Directory Service Principal Insights ($($ProductVersion))</p>
"@

$startSummary = Get-Date

summary
#[System.GC]::Collect()

$endSummary = Get-Date
Write-Host " Building Summary duration: $((New-TimeSpan -Start $startSummary -End $endSummary).TotalMinutes) minutes ($((New-TimeSpan -Start $startSummary -End $endSummary).TotalSeconds) seconds)"


$html += @'
    </div><!--summary-->
    </div><!--summprnt-->
'@


$html += @'
    <div class="footer">
    <div class="VersionDiv VersionLatest"></div>
    <div class="VersionDiv VersionThis"></div>
    <div class="VersionAlert"></div>
'@


$html += @"
        <abbr style="text-decoration:none" title="$($paramsUsed)"><i class="fa fa-question-circle" aria-hidden="true"></i></abbr> $newerVersionAvailableHTML
        <hr>
"@

$html += @'
    </div>
    <script src="https://www.azadvertizer.net/azadserviceprincipalinsights/js/toggle_v004_004.js"></script>
    <script src="https://www.azadvertizer.net/azadserviceprincipalinsights/js/collapsetable_v004_002.js"></script>
    <script src="https://www.azadvertizer.net/azadserviceprincipalinsights/js/version_v001_003.js"></script>
</body>
</html>
'@

$html | Set-Content -Path "$($outputPath)$($DirectorySeparatorChar)$($fileName).html" -Encoding utf8 -Force

$endBuildHTML = Get-Date
Write-Host "Building HTML total duration: $((New-TimeSpan -Start $startBuildHTML -End $endBuildHTML).TotalMinutes) minutes ($((New-TimeSpan -Start $startBuildHTML -End $endBuildHTML).TotalSeconds) seconds)"
#endregion BuildHTML

#endregion createoutputs

#APITracking
$APICallTrackingCount = ($arrayAPICallTracking | Measure-Object).Count
$APICallTrackingManagementCount = ($arrayAPICallTracking | Where-Object { $_.TargetEndpoint -eq 'ManagementAPI' } | Measure-Object).Count
$APICallTrackingGraphCount = ($arrayAPICallTracking | Where-Object { $_.TargetEndpoint -eq 'MSGraphAPI' } | Measure-Object).Count
$APICallTrackingRetriesCount = ($arrayAPICallTracking | Where-Object { $_.TryCounter -gt 0 } | Measure-Object).Count
$APICallTrackingRestartDueToDuplicateNextlinkCounterCount = ($arrayAPICallTracking | Where-Object { $_.RestartDueToDuplicateNextlinkCounter -gt 0 } | Measure-Object).Count
Write-Host "$($Product) APICalls total count: $APICallTrackingCount ($APICallTrackingManagementCount ManagementAPI; $APICallTrackingGraphCount MSGraphAPI; $APICallTrackingRetriesCount retries; $APICallTrackingRestartDueToDuplicateNextlinkCounterCount nextLinkReset)"

$endProduct = Get-Date
$durationProduct = New-TimeSpan -Start $startProduct -End $endProduct
Write-Host "$($Product) duration: $(($durationProduct).TotalMinutes) minutes ($(($durationProduct).TotalSeconds) seconds)"

#end
$endTime = Get-Date -Format 'dd-MMM-yyyy HH:mm:ss'
Write-Host "End $($Product) $endTime"

Write-Host 'Checking for errors'
if ($Error.Count -gt 0) {
    Write-Host "Dumping $($Error.Count) Errors (handled by $($Product)):" -ForegroundColor Yellow
    $Error | Out-Host
}
else {
    Write-Host 'Error count is 0'
}

#region Stats
if (-not $StatsOptOut) {

    if ($azAPICallConf['htParameters'].onAzureDevOps) {
        if ($env:BUILD_REPOSITORY_ID) {
            $hashTenantIdOrRepositoryId = [string]($env:BUILD_REPOSITORY_ID)
        }
        else {
            $hashTenantIdOrRepositoryId = [string]($azAPICallConf['checkContext'].Tenant.Id)
        }
    }
    else {
        $hashTenantIdOrRepositoryId = [string]($azAPICallConf['checkContext'].Tenant.Id)
    }

    $hashAccId = [string]($azAPICallConf['checkContext'].Account.Id)

    $hasher384 = [System.Security.Cryptography.HashAlgorithm]::Create('sha384')
    $hasher512 = [System.Security.Cryptography.HashAlgorithm]::Create('sha512')

    $hashTenantIdOrRepositoryIdSplit = $hashTenantIdOrRepositoryId.split('-')
    $hashAccIdSplit = $hashAccId.split('-')

    if (($hashTenantIdOrRepositoryIdSplit[0])[0] -match '[a-z]') {
        $hashTenantIdOrRepositoryIdUse = "$(($hashTenantIdOrRepositoryIdSplit[0]).substring(2))$($hashAccIdSplit[2])"
        $hashTenantIdOrRepositoryIdUse = $hasher512.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($hashTenantIdOrRepositoryIdUse))
        $hashTenantIdOrRepositoryIdUse = "$(([System.BitConverter]::ToString($hashTenantIdOrRepositoryIdUse)) -replace '-')"
    }
    else {
        $hashTenantIdOrRepositoryIdUse = "$(($hashTenantIdOrRepositoryIdSplit[4]).substring(6))$($hashAccIdSplit[1])"
        $hashTenantIdOrRepositoryIdUse = $hasher384.ComputeHash([System.Text.Encoding]::UTF8.GetBytes($hashTenantIdOrRepositoryIdUse))
        $hashTenantIdOrRepositoryIdUse = "$(([System.BitConverter]::ToString($hashTenantIdOrRepositoryIdUse)) -replace '-')"
    }

    if (($hashAccIdSplit[0])[0] -match '[a-z]') {
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

    $accountInfo = "$($azAPICallConf['checkContext'].Account.Type)$($azAPICallConf['userType'])"
    if ($azAPICallConf['checkContext'].Account.Type -eq 'ServicePrincipal' -or $azAPICallConf['checkContext'].Account.Type -eq 'ManagedService' -or $azAPICallConf['checkContext'].Account.Type -eq 'ClientAssertion') {
        $accountInfo = $azAPICallConf['checkContext'].Account.Type
    }

    $statsCountSubscriptions = 'less than 100'
    if (($htSubscriptionsMgPath.Keys).Count -ge 100) {
        $statsCountSubscriptions = 'more than 100'
    }

    $statsCountSPs = 'less than 1000'
    if ($cu.Count -ge 1000) {
        $statsCountSPs = 'more than 1000'
    }

    $tryCounter = 0
    do {
        if ($tryCounter -gt 0) {
            Start-Sleep -Seconds ($tryCounter * 3)
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
                "azCloud": "$($azAPICallConf['checkContext'].Environment.Name)",
                "identifier": "$($identifier)",
                "platform": "$($azAPICallConf['htParameters'].codeRunPlatform)",
                "productVersion": "$($ProductVersion)",
                "psAzAccountsVersion": "$($azAPICallConf['htParameters'].azAccountsVersion)",
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
            $stats = Invoke-WebRequest -Uri 'https://dc.services.visualstudio.com/v2/track' -Method 'POST' -Body $statusBody
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
            Start-Sleep -Seconds ($tryCounter * 3)
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
            $stats = Invoke-WebRequest -Uri 'https://dc.services.visualstudio.com/v2/track' -Method 'POST' -Body $statusBody
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

Write-Host ''
Write-Host '--------------------'
Write-Host 'Completed successful' -ForegroundColor Green
if ($Error.Count -gt 0) {
    Write-Host "Don't bother about dumped errors"
}