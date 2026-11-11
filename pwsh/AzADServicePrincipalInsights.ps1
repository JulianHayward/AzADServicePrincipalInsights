[CmdletBinding()]
param
(
    [string]$Product = 'AzADServicePrincipalInsights',
    [string]$ScriptPath = 'pwsh',
    [string]$ProductVersion = 'v2_20261111_0',
    [string][ValidateSet('AzAPICall', 'AzAPICallBeta')]$AzAPICallModuleName = 'AzAPICall',
    [string]$azAPICallVersion = '1.4.2',
    [string]$GitHubRepository = 'aka.ms/AzADServicePrincipalInsights',
    [switch]$DebugAzAPICall,
    #one or more management group IDs; empty: the Tenant Root Group ([string[]]: pipelines/workflows pass a comma separated list as list)
    [string[]]$ManagementGroupId,
    [switch]$NoCsvExport,
    [ValidateSet(';', ',')][string]$CsvDelimiter = ';',
    [switch]$CsvExportUseQuotesAsNeeded,
    [string]$OutputPath,
    [array]$SubscriptionQuotaIdWhitelist = @('undefined'),
    [switch]$DoTranscript,
    [ValidateRange(1, 1000)][int]$ThrottleLimitARM = 10,
    [ValidateRange(1, 1000)][int]$ThrottleLimitGraph = 25,
    [ValidateRange(1, 1000)][int]$ThrottleLimitLocal = 100,
    [switch]$PerfTracking,
    [string]$SubscriptionId4AzContext = 'undefined',
    [string]$FileTimeStampFormat = 'yyyyMMdd_HHmmss',
    [switch]$NoJsonExport,
    [switch]$NoAzureResourceSideRelations,
    [switch]$StatsOptOut,
    [int]$ApplicationSecretExpiryWarning = 14,
    [int]$ApplicationSecretExpiryMax = 730,
    [int]$ApplicationCertificateExpiryWarning = 14,
    [int]$ApplicationCertificateExpiryMax = 730,
    [string]$DirectorySeparatorChar = [IO.Path]::DirectorySeparatorChar,
    [switch]$OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes,
    #Entra roles treated as critical in addition to the roles Microsoft marks as privileged (roleDefinition isPrivileged), e.g. custom roles
    [array]$CriticalAADRoles = @('62e90394-69f5-4237-9190-012177145e10', 'e8611ab8-c189-46e8-94e1-60213ab1f814', '7be44c8a-adaf-4e2a-84d6-ab2649e08a13'), #Global Administrator, Privileged Role Administrator, Privileged Authentication Administrator
    #change history (README: Change history): on by default; the report shows the changes of the last ChangeHistoryDays days
    [switch]$NoChangeHistory,
    [ValidateRange(1, 3650)][int]$ChangeHistoryDays = 30
)

$Error.clear()
$ErrorActionPreference = 'Stop'

#parameters of this run, shown in the 'Run parameters' dialog of the report (top bar). Computed here at script level
#($PSBoundParameters inside a function holds that function's parameters); encoded by the registry (JSON)
$paramsUsed = @($PSBoundParameters.GetEnumerator() | Sort-Object { $_.Key } | ForEach-Object { [ordered]@{ name = "-$($_.Key)"; value = "$(@($_.Value) -join ', ')" } })

#removeNoise
$ProgressPreference = 'SilentlyContinue'
Set-Item Env:\SuppressAzurePowerShellBreakingChangeWarnings 'true'

$startProduct = Get-Date
#InvariantCulture: month names must not depend on the machine's culture (e.g. 'Nov.'/'Mai' with de-DE)
$startTime = (Get-Date).ToString('dd-MMM-yyyy HH:mm:ss', [cultureinfo]::InvariantCulture)
Write-Host "Start $($Product) $($startTime) (#$($ProductVersion))"

#region testPowerShellVersion
function testPowerShellVersion {

    Write-Host ' Checking PowerShell edition and version'
    $requiredPSVersion = [version]'7.0.3'
    #compare as [version] (Major.Minor.Patch; prerelease labels ignored). The former check required Major to *equal* 7,
    #so PowerShell 8 would have been rejected
    $thisPSVersion = [version]('{0}.{1}.{2}' -f $PSVersionTable.PSVersion.Major, $PSVersionTable.PSVersion.Minor, $PSVersionTable.PSVersion.Patch)

    if ($PSVersionTable.PSEdition -eq 'Core' -and $thisPSVersion -ge $requiredPSVersion) {
        Write-Host "  PS check passed : ($thisPSVersion ge $requiredPSVersion); (minimum supported version '$requiredPSVersion')"
        Write-Host "  PS Edition: $($PSVersionTable.PSEdition); PS Version: $($PSVersionTable.PSVersion)"
        Write-Host '  PS Version check succeeded' -ForegroundColor Green
    }
    else {
        Write-Host "  PS check failed : (Edition '$($PSVersionTable.PSEdition)', version $thisPSVersion; required: Edition 'Core', version $requiredPSVersion or higher)"
        Write-Host "  PS Edition: $($PSVersionTable.PSEdition); PS Version: $($PSVersionTable.PSVersion)"
        Write-Host "  Parallelization requires Powershell 'Core' version '$($requiredPSVersion)' or higher"
        throw 'Error - check the last console output for details'
    }
}

#prompts (Read-Host / Pause) block or fail in unattended runs (pipelines, Azure Automation, scheduled tasks, 'pwsh -NonInteractive')
function testInteractiveSession {
    if (-not [Environment]::UserInteractive) {
        return $false
    }
    if ([Console]::IsInputRedirected) {
        return $false
    }
    if ([Environment]::GetCommandLineArgs() | Where-Object { $_ -match '^-noni' }) {
        return $false
    }
    return $true
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
        throw 'Error - check the last console output for details'
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
    #the timestamp is part of file names: e.g. 'yyyy/MM/dd' or 'HH:mm' would produce invalid paths
    if ($fileTimestamp.IndexOfAny([IO.Path]::GetInvalidFileNameChars()) -ge 0 -or $fileTimestamp.IndexOfAny([char[]]'/\:*?"<>|') -ge 0) {
        Write-Host "fileTimestamp format: '$($FileTimeStampFormat)' produces characters that are invalid in file names; continue with default format: 'yyyyMMdd_HHmmss'" -ForegroundColor Red
        $FileTimeStampFormat = 'yyyyMMdd_HHmmss'
        $script:fileTimestamp = (Get-Date -Format $FileTimeStampFormat)
    }

    $script:executionDateTimeInternationalReadable = (Get-Date).ToString('dd-MMM-yyyy HH:mm:ss', [cultureinfo]::InvariantCulture)
    $script:currentTimeZone = (Get-TimeZone).Id
}
setOutput
#endregion filedir

function verifyModules3rd {
    [CmdletBinding()]param(
        [object]$modules
    )

    foreach ($module in $modules) {
        $moduleName = $module.ModuleName
        $moduleVersion = $module.ModuleVersion

        if ($moduleVersion) {
            Write-Host " Verify '$($moduleName)' ($moduleVersion)"
        }
        else {
            Write-Host " Verify '$($moduleName)' (latest)"
            Write-Host '  Check latest module version'
            try {
                $moduleVersion = (Find-Module -Name $moduleName).Version
                Write-Host "  Latest module version: $moduleVersion"
            }
            catch {
                Write-Host '  Check latest module version failed'
                throw
            }
        }

        #Get-InstalledModule -Name <name> only returns the newest installed version, so with side-by-side versions
        #(e.g. 1.4.1 and 1.4.2) the required one was reported as 'deviating' and the run ended at the install prompt.
        #Look for the exact version among all installed ones and load it; without Import-Module the later autoload
        #would pick the newest version anyway.
        $maxRetry = 3
        $tryCount = 0
        $moduleImported = $false
        $installApproved = ($env:SYSTEM_TEAMPROJECTID -and $env:BUILD_REPOSITORY_ID) -or $env:GITHUB_ACTIONS
        do {
            $tryCount++
            $installed = Get-Module -ListAvailable -Name $moduleName | Where-Object { "$($_.Version)" -eq "$moduleVersion" }
            if ($installed) {
                try {
                    Import-Module -Name $moduleName -RequiredVersion $moduleVersion -Force -ErrorAction Stop
                    $moduleImported = $true
                    Write-Host "  '$($moduleName) $moduleVersion' loaded"
                }
                catch {
                    Write-Host "  'Import-Module -Name $($moduleName) -RequiredVersion $moduleVersion' failed (try $tryCount/$maxRetry): $($_.Exception.Message)"
                }
            }
            else {
                Write-Host "  '$($moduleName) $moduleVersion' not installed"
                if (-not $installApproved -and -not (testInteractiveSession)) {
                    #unattended run: there is nobody to answer the prompt (Read-Host fails or loops)
                    Write-Host "  Install it first: Install-Module -Name $($moduleName) -RequiredVersion $moduleVersion (or run interactively)"
                    throw "  $($moduleName) module ($($moduleVersion)) is required"
                }
                if (-not $installApproved) {
                    do {
                        $installModuleUserChoice = Read-Host "  Do you want to install $($moduleName) module ($($moduleVersion)) from the PowerShell Gallery? (y/n)"
                        if ($installModuleUserChoice -eq 'n') {
                            Write-Host "  $($moduleName) module is required, please visit https://aka.ms/$($module.ModuleProductName) or https://www.powershellgallery.com/packages/$($module.ModuleProductName)"
                            throw "  $($moduleName) module is required"
                        }
                        if ($installModuleUserChoice -ne 'y') {
                            Write-Host "  Accepted input 'y' or 'n'; start over.."
                        }
                    }
                    until ($installModuleUserChoice -eq 'y')
                    $installApproved = $true
                }
                Write-Host "  Installing $($moduleName) module ($($moduleVersion))"
                try {
                    Install-Module -Name $moduleName -RequiredVersion $moduleVersion -Force -ErrorAction Stop
                }
                catch {
                    Write-Host "  'Install-Module -Name $($moduleName) -RequiredVersion $moduleVersion' failed (try $tryCount/$maxRetry): $($_.Exception.Message)"
                }
            }
        }
        until ($moduleImported -or $tryCount -ge $maxRetry)

        if (-not $moduleImported) {
            Write-Host " Managing '$($moduleName)' failed (tried $($tryCount)x)"
            throw " Managing '$($moduleName)' failed"
        }
    }
}

$modules = [System.Collections.ArrayList]@()
$null = $modules.Add([PSCustomObject]@{
        ModuleName = $AzAPICallModuleName
        ModuleVersion = $AzAPICallVersion
        ModuleProductName = 'AzAPICall'
        ModulePathPipeline = 'AzAPICallModule'
    })
verifyModules3rd -modules $modules

#the AzAPICall module loaded by verifyModules3rd: the runspaces of ForEach-Object -Parallel import it from this path (review R-7:
#they autoloaded the newest installed version, not the pinned -AzAPICallVersion)
#review F-19: the module whose AzAPICall command the main runspace resolves (with two loaded versions - a run with another
#-AzAPICallVersion in the same session - the newest loaded one is not necessarily the pinned one)
$azAPICallModuleLoaded = (Get-Command -Name 'AzAPICall' -Module $AzAPICallModuleName -ErrorAction SilentlyContinue | Select-Object -First 1).Module
if (-not $azAPICallModuleLoaded) { $azAPICallModuleLoaded = Get-Module -Name $AzAPICallModuleName | Sort-Object -Property Version -Descending | Select-Object -First 1 }
$azAPICallModuleImportPath = Join-Path -Path $azAPICallModuleLoaded.ModuleBase -ChildPath "$($azAPICallModuleLoaded.Name).psd1"
if (-not (Test-Path -LiteralPath $azAPICallModuleImportPath)) { $azAPICallModuleImportPath = $azAPICallModuleLoaded.Path }
Write-Host " Parallel runspaces use '$($azAPICallModuleLoaded.Name)' $($azAPICallModuleLoaded.Version)"

#Region initAZAPICall
Write-Host "Initialize 'AzAPICall'"
if ($NoAzureResourceSideRelations) {
    $parameters4AzAPICallModule = @{
        DebugAzAPICall = $DebugAzAPICall
        SkipAzContextSubscriptionValidation = $true
        GitHubRepository = $GitHubRepository
    }
}
else {
    $parameters4AzAPICallModule = @{
        DebugAzAPICall = $DebugAzAPICall
        SubscriptionId4AzContext = $SubscriptionId4AzContext
        GitHubRepository = $GitHubRepository
    }
    #review R-34: an identity without a subscription in its context (e.g. management group Reader only, or a tenant without
    #subscriptions): AzAPICall would stop at its subscription check, although the management group data can be collected
    if ($SubscriptionId4AzContext -eq 'undefined' -and -not (Get-AzContext).Subscription) {
        Write-Host ' No subscription in the Az context - AzAPICall subscription validation skipped (management group data only from the management groups)' -ForegroundColor Yellow
        $parameters4AzAPICallModule.Remove('SubscriptionId4AzContext')
        $parameters4AzAPICallModule.SkipAzContextSubscriptionValidation = $true
    }
}

$azAPICallConf = initAzAPICall @parameters4AzAPICallModule
Write-Host " Initialize 'AzAPICall' succeeded" -ForegroundColor Green
#EndRegion initAZAPICall

#region checkVersion
function checkVersion {
    try {
        $getRepoVersion = Invoke-WebRequest -Uri 'https://raw.githubusercontent.com/JulianHayward/AzADServicePrincipalInsights/master/version.txt' -TimeoutSec 10
        $versionThis = ($ProductVersion -split '_')[1]
        $buildThis = ($ProductVersion -split '_')[2]
        $script:versionOnRepositoryFull = ([string]$getRepoVersion.Content).Trim()
        $versionOnRepository = ($versionOnRepositoryFull -split '_')[1]
        $buildOnRepository = ($versionOnRepositoryFull -split '_')[2]
        $script:newerVersionAvailable = $false
        $script:newerVersionAvailableHTML = ''
        #the remote content is written into the HTML report: only accept the expected format (v1_yyyyMMdd_n)
        if ($versionOnRepositoryFull -notmatch '^v\d+_\d{8}_\d+$') {
            Write-Host '  Version check: unexpected content of the remote version.txt - skipping'
        }
        #date first, then the build suffix (v1_yyyyMMdd_<build>; a newer build of the same day was not detected)
        elseif ([int]$versionOnRepository -gt [int]$versionThis -or ([int]$versionOnRepository -eq [int]$versionThis -and [int]$buildOnRepository -gt [int]$buildThis)) {
            $script:newerVersionAvailable = $true
            $script:newerVersionAvailableHTML = '<a class="az-update" href="https://aka.ms/AzADServicePrincipalInsights" target="_blank" rel="noopener" title="This report was built with ' + [HtmlSafe]::Encode($ProductVersion) + '">Get the latest version (' + $versionOnRepositoryFull + ')</a>'
        }
        if ($newerVersionAvailable) {
            if (-not $azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions) {
                Write-Host ''
                Write-Host " * * * This $Product version ($ProductVersion) is not up to date. Get the latest $Product version ($versionOnRepositoryFull)! * * *" -ForegroundColor Green
                Write-Host 'https://aka.ms/AzADServicePrincipalInsights'
                Write-Host ' * * * * * * * * * * * * * * * * * * * * * *' -ForegroundColor Green
                if (testInteractiveSession) {
                    Pause
                }
            }
        }
    }
    catch {
        #the version check is optional (e.g. no internet access)
        Write-Verbose "version check skipped: $($_.Exception.Message)"
    }
}
checkVersion
#endregion checkVersion

if ($NoAzureResourceSideRelations) {
    if ($OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes) {
        Write-Host "Reset parameter -OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes $OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes to $false due to parameter -NoAzureResourceSideRelations $NoAzureResourceSideRelations"
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

        $script:fileNameTranscript = "$($Product)_$($fileNameMGRef)_Log.txt"
        #renamed (was AzADServiceprincipalInsights_...): remove the old file, a case-only difference would collide on case-insensitive checkouts
        $fileNameTranscriptOld = "AzADServiceprincipalInsights_$($fileNameMGRef)_Log.txt"
        if ($fileNameTranscriptOld -cne $fileNameTranscript -and (Get-ChildItem -LiteralPath $outputPath -File | Where-Object { $_.Name -ceq $fileNameTranscriptOld })) {
            Remove-Item -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileNameTranscriptOld)"
        }
    }
    else {

        $script:fileNameTranscript = "$($Product)_$($ProductVersion)_$($fileTimestamp)_$($fileNameMGRef)_Log.txt"
    }

    Write-Host "Writing transcript: $($outputPath)$($DirectorySeparatorChar)$($fileNameTranscript)"
    Start-Transcript -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileNameTranscript)"
    #endregion setTranscript
}
if ($DoTranscript) {
    setTranscript
}
#if the script fails, stop the transcript too (it stayed open in the calling session); 'break' passes the error on unchanged
trap {
    if ($DoTranscript) {
        Stop-Transcript -ErrorAction SilentlyContinue | Out-Null
    }
    break
}

#region htParameters (all switch params used in foreach-object -parallel)
function addHtParameters {
    Write-Host 'Add AzADServicePrincipalInsights htParameters'
    #set per key: '+=' throws 'Item has already been added' if a future AzAPICall version defines one of these keys
    $addParameters = [ordered]@{
        NoJsonExport = [bool]$NoJsonExport
        NoAzureResourceSideRelations = [bool]$NoAzureResourceSideRelations
        OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes = [bool]$OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes
        ProductVersion = $ProductVersion
    }
    foreach ($key in $addParameters.Keys) {
        $script:azAPICallConf['htParameters'][$key] = $addParameters[$key]
    }
    Write-Host 'htParameters:'
    #Write-Host: 'Format-Table | Out-String' alone wrote to the success (output) stream of the script
    Write-Host ($azAPICallConf['htParameters'] | Format-Table -AutoSize | Out-String)
    Write-Host 'Add AzADServicePrincipalInsights htParameters succeeded' -ForegroundColor Green
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

#region parallel processing helpers (review P1-21)
#many small batches instead of exactly $throttleLimit large ones: a slow item no longer holds up 1/N of all items
function getParallelBatchSize([int]$itemCount, [int]$throttleLimit) {
    return [math]::Max(1, [math]::Min(50, [math]::Ceiling($itemCount / ($throttleLimit * 4))))
}

#review P-8: the items in batches of $BatchSize (objects with .Group, as Group-Object returned them) by index slices;
#'Group-Object { [math]::Floor($counter.Value++ / $size) }' grew faster than linear (100k items: 11-16 s)
function getParallelBatches($Items, [int]$BatchSize) {
    $list = @($Items)
    $BatchSize = [math]::Max(1, $BatchSize)
    $batches = [System.Collections.Generic.List[object]]::new()
    for ($start = 0; $start -lt $list.Count; $start += $BatchSize) {
        $end = [math]::Min($start + $BatchSize, $list.Count) - 1
        $batches.Add([PSCustomObject]@{ Group = @($list[$start..$end]) })
    }
    #enumerated (not ', $array'): piped into ForEach-Object -Parallel each batch is one item
    return $batches
}

#items that failed in a parallel phase (key 'phase|item'); each parallel block catches per item and records here
$htParallelFailures = [System.Collections.Hashtable]::Synchronized(@{})

#after a parallel phase: report failed items; stop if so many failed that the report would be misleading
#(e.g. an expired token or missing permission makes every item fail)
#a warning in the run summary of Azure DevOps / GitHub Actions (console only elsewhere); one line, no workflow command injection
function writePipelineWarning([string]$Message) {
    $line = ($Message -replace '[\r\n]+', ' ').Trim()
    if ($env:SYSTEM_TEAMPROJECTID -and $env:BUILD_REPOSITORY_ID) {
        $adoLine = $line -replace ';', ',' -replace '\]', ')' -replace '\[', '('
        Write-Host "##vso[task.logissue type=warning]$adoLine"
    }
    elseif ($env:GITHUB_ACTIONS) {
        Write-Host "::warning::$($line -replace '%', '%25')"
    }
}

#review F-17: tenant-controlled names (display names of apps, SPs, subscriptions, management groups, roles) in console lines are
#neutralised with -replace '##', '# #' -replace '[\r\n]+', ' ' - the Azure DevOps agent runs '##vso[...]' logging commands it finds
#in the output (e.g. an app named 'x ##vso[task.setvariable ...]' that is deleted during a run and printed as 'meanwhile deleted')
function checkParallelFailures([string]$phase, [int]$itemCount) {
    $failures = @($htParallelFailures.Values | Where-Object { $_.Phase -eq $phase })
    if ($failures.Count -gt 0) {
        Write-Host " $($phase): $($failures.Count) of $itemCount items failed and were skipped" -ForegroundColor Yellow
        #review R-4: visible in the pipeline run summary as well (the report shows a warning on the overview)
        writePipelineWarning "$($Product) $($phase): $($failures.Count) of $itemCount items failed and were skipped - the output is incomplete (see the console output)"
        foreach ($failure in $failures) {
            Write-Host "  $($failure.Item -replace '##', '# #' -replace '[\r\n]+', ' '): $($failure.Error -replace '##', '# #' -replace '[\r\n]+', ' ')" -ForegroundColor Yellow
        }
        #review R-4: with 10 items or fewer max(10, 5%) never stopped, so a phase could fail completely (e.g. all subscriptions
        #of a small tenant) and the run still ended successfully with an incomplete report
        if ($itemCount -gt 0 -and $failures.Count -ge $itemCount) {
            throw "$($phase): all $itemCount items failed - stopping"
        }
        $maxFailures = [math]::Max(10, [math]::Ceiling($itemCount * 0.05))
        if ($failures.Count -gt $maxFailures) {
            throw "$($phase): too many failed items ($($failures.Count) of $itemCount, maximum $maxFailures) - stopping"
        }
    }
}
#endregion parallel processing helpers

#region CSV export helpers
#same quoting for all CSV files: by default every value is quoted, -CsvExportUseQuotesAsNeeded quotes only where needed
#(before, 8 of the 19 files used 'AsNeeded' and the others 'Always', and the parameter had no effect)
$csvUseQuotes = if ($CsvExportUseQuotesAsNeeded) { 'AsNeeded' } else { 'Always' }

#CSV injection: spreadsheet apps evaluate values that start with = + - @ (or tab / carriage return) as formulas.
#Such values get a leading ' (OWASP recommendation). Use in the pipeline right before Export-Csv
filter protectCsvFormula {
    #review P-10: most rows have no such value - passed on as they are (a new object per row cost ~150 us); the check can only
    #give false positives (e.g. a negative number), which take the path below and stay unchanged there
    if ((@($_.PSObject.Properties.Value) -join "`u{1}") -notmatch '(^|\x01)[=+\-@\t\r]') {
        return $_
    }
    $protected = [ordered]@{}
    foreach ($property in $_.PSObject.Properties) {
        $value = $property.Value
        if ($value -is [string] -and $value.Length -gt 0 -and "=+-@`t`r".IndexOf($value[0]) -ge 0) {
            $value = "'" + $value
        }
        $protected[$property.Name] = $value
    }
    [PSCustomObject]$protected
}
#endregion CSV export helpers

#region HtmlSafe
#[HtmlSafe]::Encode($value) encodes a value for HTML element content and quoted attribute values. Every tenant- or
#publisher-controlled value (displayNames, notes, tags, credential descriptions, role names, ...) must pass through it
#before it is written to the report, otherwise e.g. an app named '<img src=x onerror=...>' runs script when the report is opened.
#Only the 5 significant characters are encoded (the report is UTF-8, so other characters stay as they are).
#[string] conversion matches "$(...)" interpolation: $null -> '', arrays are joined with a space.
#A static class method instead of a function: it is called ~25 times per object, and function calls are ~3x slower.
class HtmlSafe {
    static [char[]] $Special = [char[]]'&<>"'''

    static [string] Encode([object]$Value) {
        if ($null -eq $Value) {
            return ''
        }
        $s = [string]$Value
        if ($s.IndexOfAny([HtmlSafe]::Special) -lt 0) {
            return $s
        }
        return $s.Replace('&', '&amp;').Replace('<', '&lt;').Replace('>', '&gt;').Replace('"', '&quot;').Replace("'", '&#39;')
    }
}
#endregion HtmlSafe

#region getClassification
function getClassification {
    param (
        [string]$permission,
        [string]$permissionType
    )
    #A classification applies if one of its 'includes' matches and none of its 'excludes' matches ('*' = wildcard, case-insensitive).
    #Classifications are checked in a fixed priority ('critical' first, then the others in file order); the first that applies wins.
    #Before, the last match won (so the result depended on the key order, which is not preserved by ConvertFrom-Json -AsHashtable
    #before PowerShell 7.3), and an 'excludes' of one classification reset a match of another one to 'unclassified'.
    #(must be self-contained: it is passed to the parallel runspaces as text)
    $classifications = $getClassifications.permissions[$permissionType].'classifications'
    if (-not $classifications) {
        return 'unclassified'
    }
    #review P-12: the order per permission type once per runspace (was two pipelines per call, ~200 us; called per permission row)
    if (-not $script:classificationOrderCache) { $script:classificationOrderCache = @{} }
    $cached = $script:classificationOrderCache[$permissionType]
    if ($cached -and [object]::ReferenceEquals($cached.source, $classifications)) {
        $orderedNames = $cached.names
    }
    else {
        $orderedNames = @(@($classifications.Keys) | Where-Object { $_ -eq 'critical' }) + @(@($classifications.Keys) | Where-Object { $_ -ne 'critical' })
        $script:classificationOrderCache[$permissionType] = @{ source = $classifications; names = $orderedNames }
    }
    foreach ($classification in $orderedNames) {
        $isIncluded = $false
        foreach ($permissionToCheck in $classifications[$classification].'includes') {
            if (($permissionToCheck.Contains('*') -and $permission -like $permissionToCheck) -or $permission -eq $permissionToCheck) {
                $isIncluded = $true
                break
            }
        }
        if (-not $isIncluded) {
            continue
        }
        $isExcluded = $false
        foreach ($permissionToCheck in $classifications[$classification].'excludes') {
            if (($permissionToCheck.Contains('*') -and $permission -like $permissionToCheck) -or $permission -eq $permissionToCheck) {
                $isExcluded = $true
                break
            }
        }
        if (-not $isExcluded) {
            return $classification
        }
    }
    return 'unclassified'
}
$funcGetClassification = $function:getClassification.ToString()
#endregion getClassification

#region resolveObjectsById
function resolveObjectsById($objects, $targetHt) {

    $batchSize = 1000
    $ObjectIdsBatch = @(getParallelBatches -Items $objects -BatchSize $batchSize)
    $ObjectIdsBatchCount = $ObjectIdsBatch.Count
    #beta with explicit types (as AzGovViz ResolveObjectIds, issue Azure/Azure-Governance-Visualizer#66): foreign principals
    #(directoryObjectPartnerReference, e.g. CSP/GDAP partner groups) are only returned when the type is requested
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/directoryObjects/getByIds?`$select=id,displayName,userType,objectType,externalPartnerTenantId"
    $bodyTemplate = '{ "ids":[{0}], "types":["user","group","servicePrincipal","application","device","directoryObjectPartnerReference"] }'
    #review P-15: several batches (100k ids = 100 POSTs of 1-2 s) are requested in parallel; one batch inline
    $resultsByBatch = @{}
    if ($ObjectIdsBatchCount -gt 1) {
        $htBatchResults = [System.Collections.Hashtable]::Synchronized(@{})
        $batchNumbers = 0..($ObjectIdsBatchCount - 1)
        $batchNumbers | ForEach-Object -Parallel {
            #R-7: the AzAPICall version of the main runspace (an autoload would take the newest installed version)
            if (-not (Get-Module -Name $using:AzAPICallModuleName)) { Import-Module -Name $using:azAPICallModuleImportPath -ErrorAction Stop }
            $batch = ($using:ObjectIdsBatch)[$_]
            $body = ($using:bodyTemplate).Replace('{0}', ('"{0}"' -f ($batch.Group -join '","')))
            $result = AzAPICall -AzAPICallConfiguration $using:azAPICallConf -uri $using:uri -method 'POST' -body $body -currentTask "Resolving Identities - Batch #$($_ + 1)/$($using:ObjectIdsBatchCount) ($(($batch.Group).Count) ObjectIds)"
            ($using:htBatchResults)[$_] = @($result)
        } -ThrottleLimit ([math]::Min(10, $ThrottleLimitGraph))
        foreach ($key in $htBatchResults.Keys) { $resultsByBatch[$key] = $htBatchResults[$key] }
    }
    $batchCnt = 0

    foreach ($batch in $ObjectIdsBatch) {
        $batchCnt++
        Write-Host " processing Batch #$batchCnt/$($ObjectIdsBatchCount) ($(($batch.Group).Count) ObjectIds)"
        if ($ObjectIdsBatchCount -gt 1) {
            $resolvedIdentities = $resultsByBatch[$batchCnt - 1]
        }
        else {
            $body = $bodyTemplate.Replace('{0}', ('"{0}"' -f ($batch.Group -join '","')))
            $currentTask = "Resolving Identities - Batch #$batchCnt/$($ObjectIdsBatchCount) ($(($batch.Group).Count) ObjectIds)"
            $resolvedIdentities = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method 'POST' -body $body -currentTask $currentTask
        }
        if ($resolvedIdentities -is [string] -or ($resolvedIdentities.Count -eq 1 -and $resolvedIdentities[0] -is [string])) {
            Write-Host "    batch not resolved ($($resolvedIdentities))" -ForegroundColor Yellow
            continue
        }

        $t = 0
        foreach ($resolvedIdentity in $resolvedIdentities) {
            $t++
            $type = 'unforeseen type'
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
            if ($resolvedIdentity.'@odata.type' -eq '#microsoft.graph.device') {
                $type = 'Device'
            }
            $partnerTenant = ''
            if ($resolvedIdentity.'@odata.type' -eq '#microsoft.graph.directoryObjectPartnerReference') {
                #foreign principal, e.g. a CSP/GDAP partner group: 'Foreign Group' (AzGovViz: "Foreign $($identity.objectType)")
                $type = if ($resolvedIdentity.objectType) { "Foreign $($resolvedIdentity.objectType)" } else { 'Foreign principal' }
                if ($resolvedIdentity.externalPartnerTenantId) {
                    $partnerTenant = ", PartnerTenantId: $($resolvedIdentity.externalPartnerTenantId)"
                }
            }

            if ($targetHt -eq 'htPrincipalsResolved') {
                if ($type -eq 'User') {
                    #userType only exists for users (it was empty for SPs, groups and apps, which then showed as '(MemberSynced)');
                    #an empty userType of a user says nothing about on-premises sync (onPremisesSyncEnabled) - shown as such, not as 'MemberSynced'
                    if ([string]::IsNullOrEmpty($resolvedIdentity.userType)) {
                        $principalUserType = 'userType not set'
                    }
                    else {
                        $principalUserType = $resolvedIdentity.userType
                    }
                    $typeLabel = "$($type) ($($principalUserType))"
                }
                else {
                    $typeLabel = $type
                }
                $script:htPrincipalsResolved[$resolvedIdentity.id] = @{
                    full = "$($typeLabel), DisplayName: $($resolvedIdentity.displayName), Id: $(($resolvedIdentity.id))$($partnerTenant)"
                    typeOnly = $typeLabel
                }

            }

        }
        $resolvedIdentitiesCount = $resolvedIdentities.Count
        Write-Host "    $resolvedIdentitiesCount identities resolved"
    }
}
#endregion resolveObjectsById

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
        #R-7: the AzAPICall version of the main runspace (an autoload would take the newest installed version)
        if (-not (Get-Module -Name $using:AzAPICallModuleName)) { Import-Module -Name $using:azAPICallModuleImportPath -ErrorAction Stop }
        #P1-21: one failing item must not abort the whole run (a throw inside -Parallel stops the script under ErrorActionPreference Stop)
        try {
            $htParallelFailures = $using:htParallelFailures
            $mgdetail = $_
            #region UsingVARs
            #Parameters MG&Sub related
            #AzAPICall
            $azAPICallConf = $using:azAPICallConf
            #Array&HTs
            $customDataCollectionDuration = $using:customDataCollectionDuration
            $htCacheDefinitionsRole = $using:htCacheDefinitionsRole
            $htCacheAssignmentsRole = $using:htCacheAssignmentsRole
            $htCacheAssignmentsPolicy = $using:htCacheAssignmentsPolicy
            $roleDefinitionsApiVersion = $using:roleDefinitionsApiVersion
            $htManagementGroupsMgPath = $using:htManagementGroupsMgPath
            $allManagementGroupsFromEntitiesChildOfRequestedMg = $using:allManagementGroupsFromEntitiesChildOfRequestedMg
            $allManagementGroupsFromEntitiesChildOfRequestedMgCount = $using:allManagementGroupsFromEntitiesChildOfRequestedMgCount
            $arrayDataCollectionProgressMg = $using:arrayDataCollectionProgressMg
            $htRoleAssignmentsFromAPIInheritancePrevention = $using:htRoleAssignmentsFromAPIInheritancePrevention
            #endregion usingVARS

            #review P-17: the entity itself has its parent (was a search over all management groups per management group)
            $MgParentId = $mgdetail.properties.parent.Id -replace '.*/'
            if ([string]::IsNullOrEmpty($MgParentId)) {
                $MgParentId = 'TenantRoot'
            }
            else {
            }

            $rndom = Get-Random -Minimum 10 -Maximum 750
            Start-Sleep -Millisecond $rndom
            $startMgLoopThis = Get-Date

            #MGPolicyAssignments
            $currentTask = "Policy assignments '$($mgdetail.properties.displayName -replace '##', '# #' -replace '[\r\n]+', ' ')' ('$($mgdetail.Name)')"
            #only the assignments of this management group (atExactScope()); the management groups of -ManagementGroupId keep atscope():
            #they also deliver the assignments above the scope. Every management group below returned all assignments above it again
            #(each one parsed and dropped as a duplicate); the cached set is the same (AzGovViz mg-leaf concept review, 2026-11-10:
            #atExactScope() on management groups confirmed with api-version 2026-07-01). Subscriptions stay unfiltered (assignments on
            #resource groups and resources).
            $policyAssignmentFilter = $(if ($mgdetail.Name -in @($using:mgId)) { 'atscope()' } else { 'atExactScope()' })
            $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/providers/Microsoft.Management/managementgroups/$($mgdetail.Name)/providers/Microsoft.Authorization/policyAssignments?`$filter=$($policyAssignmentFilter)&api-version=2026-07-01"
            $method = 'GET'
            $L0mgmtGroupPolicyAssignments = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -caller 'CustomDataCollection'

            foreach ($L0mgmtGroupPolicyAssignment in $L0mgmtGroupPolicyAssignments) {

                if (-not $htCacheAssignmentsPolicy[($L0mgmtGroupPolicyAssignment.Id).ToLower()]) {
                    $script:htCacheAssignmentsPolicy[($L0mgmtGroupPolicyAssignment.Id).ToLower()] = @{
                        Assignment = $L0mgmtGroupPolicyAssignment
                    }
                }
            }

            #MGCustomRolesRoles
            $currentTask = "Custom Role definitions '$($mgdetail.properties.displayName -replace '##', '# #' -replace '[\r\n]+', ' ')' ('$($mgdetail.Name)')"
            $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/providers/Microsoft.Management/managementGroups/$($mgdetail.Name)/providers/Microsoft.Authorization/roleDefinitions?api-version=$($roleDefinitionsApiVersion)&`$filter=type%20eq%20'CustomRole'"
            $method = 'GET'
            $mgCustomRoleDefinitions = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -caller 'CustomDataCollection'

            foreach ($mgCustomRoleDefinition in $mgCustomRoleDefinitions) {
                if (-not ($htCacheDefinitionsRole)[$mgCustomRoleDefinition.name]) {

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

                    ($script:htCacheDefinitionsRole)[$mgCustomRoleDefinition.name] = @{
                        definition = $mgCustomRoleDefinition
                        roleIsCritical = $roleCapable4RoleAssignmentsWrite
                    }

                    #$mgCustomRoleDefinition
                }
            }

            #PIM RoleAssignmentScheduleInstances
            #always defined (empty without PIM data, e.g. no Entra ID P2 license): it is read below with $htRoleAssignmentsPIM[...]
            $htRoleAssignmentsPIM = @{}
            $htDoARMRoleAssignmentScheduleInstances = $using:htDoARMRoleAssignmentScheduleInstances
            if ($htDoARMRoleAssignmentScheduleInstances.Do -eq $true) {
                $currentTask = "Role assignment schedule instances API MG '$($mgdetail.properties.displayName -replace '##', '# #' -replace '[\r\n]+', ' ')' ('$($mgdetail.Name)')"
                $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/providers/Microsoft.Management/managementGroups/$($mgdetail.Name)/providers/Microsoft.Authorization/roleAssignmentScheduleInstances?api-version=2020-10-01"
                $method = 'GET'
                $roleAssignmentScheduleInstancesFromAPI = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -caller 'CustomDataCollection'

                if ($roleAssignmentScheduleInstancesFromAPI -eq 'ResourceNotOnboarded' -or $roleAssignmentScheduleInstancesFromAPI -eq 'TenantNotOnboarded' -or $roleAssignmentScheduleInstancesFromAPI -eq 'InvalidResourceType' -or $roleAssignmentScheduleInstancesFromAPI -eq 'RoleAssignmentScheduleInstancesError' -or $roleAssignmentScheduleInstancesFromAPI -eq 'AadPremiumLicenseRequired') {
                    if ($roleAssignmentScheduleInstancesFromAPI -eq 'AadPremiumLicenseRequired') {
                        Write-Host "    -> Setting 'htDoARMRoleAssignmentScheduleInstances.Do' to false (AadPremiumLicenseRequired)"
                        $script:htDoARMRoleAssignmentScheduleInstances.Do = $false
                    }
                }
                else {
                    $roleAssignmentScheduleInstances = ($roleAssignmentScheduleInstancesFromAPI.where( { ($_.properties.roleAssignmentScheduleId -replace '.*/') -ne ($_.properties.originRoleAssignmentId -replace '.*/') }))
                    $roleAssignmentScheduleInstancesCount = $roleAssignmentScheduleInstances.Count
                    if ($roleAssignmentScheduleInstancesCount -gt 0) {
                        $htRoleAssignmentsPIM = @{}
                        foreach ($roleAssignmentScheduleInstance in $roleAssignmentScheduleInstances) {
                            $htRoleAssignmentsPIM[($roleAssignmentScheduleInstance.properties.originRoleAssignmentId).tolower()] = $roleAssignmentScheduleInstance.properties
                        }
                    }
                }
            }

            #RoleAssignment API (system metadata e.g. createdOn)
            $currentTask = "Role assignments API '$($mgdetail.properties.displayName -replace '##', '# #' -replace '[\r\n]+', ' ')' ('$($mgdetail.Name)')"
            $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/providers/Microsoft.Management/managementGroups/$($mgdetail.Name)/providers/Microsoft.Authorization/roleAssignments?api-version=2022-04-01"
            $method = 'GET'
            $roleAssignmentsFromAPI = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -caller 'CustomDataCollection'

            if ($roleAssignmentsFromAPI.Count -gt 0) {
                foreach ($roleAssignmentFromAPI in $roleAssignmentsFromAPI) {
                    if (-not ($htCacheAssignmentsRole)[$roleAssignmentFromAPI.id]) {
                        $splitAssignment = ($roleAssignmentFromAPI.id).Split('/')

                        if ($roleAssignmentFromAPI.id -like '/providers/Microsoft.Authorization/roleAssignments/*') {
                            ($script:htCacheAssignmentsRole)[$roleAssignmentFromAPI.id] = @{
                                assignment = $roleAssignmentFromAPI
                                assignmentScope = 'Ten'
                                assignmentScopeId = ''
                                assignmentScopeName = ''
                                assignmentResourceType = 'Tenant'
                                assignmentResourceName = 'Tenant'
                                roleIsCritical = ($htCacheDefinitionsRole)[$roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'].roleIsCritical
                                roleName = ($htCacheDefinitionsRole)[$roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'].definition.properties.roleName
                                roleId = $roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'
                                type = ($htCacheDefinitionsRole)[$roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'].definition.properties.type
                            }

                        }
                        else {
                            ($script:htCacheAssignmentsRole)[$roleAssignmentFromAPI.id] = @{
                                assignment = $roleAssignmentFromAPI
                                assignmentScope = 'MG'
                                assignmentScopeId = "/providers/Microsoft.Management/managementGroups/$($splitAssignment[4])"
                                assignmentScopeName = "$($htManagementGroupsMgPath[$splitAssignment[4]].DisplayName)"
                                assignmentResourceType = 'ManagementGroup'
                                assignmentResourceName = $splitAssignment[4]
                                roleIsCritical = ($htCacheDefinitionsRole)[$roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'].roleIsCritical
                                roleName = ($htCacheDefinitionsRole)[$roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'].definition.properties.roleName
                                roleId = $roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'
                                type = ($htCacheDefinitionsRole)[$roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'].definition.properties.type
                            }

                        }

                        if ($htRoleAssignmentsPIM[($roleAssignmentFromAPI.id).tolower()]) {
                            ($script:htCacheAssignmentsRole)[$roleAssignmentFromAPI.id].assignmentPIMDetails = $htRoleAssignmentsPIM[($roleAssignmentFromAPI.id).tolower()]
                        }


                    }
                    if (-not $htRoleAssignmentsFromAPIInheritancePrevention[$roleAssignmentFromAPI.id -replace '.*/']) {
                        $htRoleAssignmentsFromAPIInheritancePrevention[$roleAssignmentFromAPI.id -replace '.*/'] = @{}
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

        }
        catch {
            $failedItem = "$($mgdetail.properties.displayName -replace '##', '# #' -replace '[\r\n]+', ' ') ($($mgdetail.Name))"
            $htParallelFailures["ManagementGroups custom data|$failedItem"] = [PSCustomObject]@{ Phase = 'ManagementGroups custom data'; Item = $failedItem; Error = $_.Exception.Message }
            Write-Host "  ManagementGroups custom data: '$failedItem' failed - skipped: $($_.Exception.Message)" -ForegroundColor Yellow
        }
    } -ThrottleLimit $ThrottleLimitARM
    checkParallelFailures -phase 'ManagementGroups custom data' -itemCount ($allManagementGroupsFromEntitiesChildOfRequestedMgCount)
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

        $batchSize = getParallelBatchSize -itemCount ($subsToProcessInCustomDataCollectionCount) -throttleLimit $ThrottleLimitARM
        Write-Host "Optimal batch size: $($batchSize)"
        $subsToProcessInCustomDataCollectionBatch = getParallelBatches -Items $subsToProcessInCustomDataCollection -BatchSize $batchSize
        Write-Host "Processing data in $($subsToProcessInCustomDataCollectionBatch.Count) batches"

        #foreach ($batch in $subscriptionsBatch) {
        #[System.GC]::Collect()
        # $startBatch = Get-Date
        # $batchCnt++
        # Write-Host " processing Batch #$batchCnt/$(($subscriptionsBatch | Measure-Object).Count) ($(($batch.Group | Measure-Object).Count) Subscriptions)"

        $subsToProcessInCustomDataCollectionBatch | ForEach-Object -Parallel {
            #R-7: the AzAPICall version of the main runspace (an autoload would take the newest installed version)
            if (-not (Get-Module -Name $using:AzAPICallModuleName)) { Import-Module -Name $using:azAPICallModuleImportPath -ErrorAction Stop }
            $htParallelFailures = $using:htParallelFailures

            $startSubLoopThis = Get-Date
            #$childMgSubDetail = $_
            #region UsingVARs
            #Parameters MG&Sub related
            #Parameters Sub related
            #AzAPICall
            $azAPICallConf = $using:azAPICallConf
            #Array&HTs
            $customDataCollectionDuration = $using:customDataCollectionDuration
            $htSubscriptionsMgPath = $using:htSubscriptionsMgPath
            $htManagementGroupsMgPath = $using:htManagementGroupsMgPath   #R-5: names of management groups for assignments the MG phase missed
            $htCacheDefinitionsRole = $using:htCacheDefinitionsRole
            $htCacheAssignmentsRole = $using:htCacheAssignmentsRole
            $htCacheAssignmentsPolicy = $using:htCacheAssignmentsPolicy
            $roleDefinitionsApiVersion = $using:roleDefinitionsApiVersion
            $subsToProcessInCustomDataCollectionCount = $using:subsToProcessInCustomDataCollectionCount
            $arrayDataCollectionProgressSub = $using:arrayDataCollectionProgressSub
            $htAllSubscriptionsFromAPI = $using:htAllSubscriptionsFromAPI
            $htRoleAssignmentsFromAPIInheritancePrevention = $using:htRoleAssignmentsFromAPIInheritancePrevention
            $arrayUserAssignedIdentities4Resources = $using:arrayUserAssignedIdentities4Resources
            $htDoARMRoleAssignmentScheduleInstances = $using:htDoARMRoleAssignmentScheduleInstances
            $arrayMIUserAssignedFederatedCreds = $using:arrayMIUserAssignedFederatedCreds
            $arrayUserAssignedIdentityIds4FederatedCreds = $using:arrayUserAssignedIdentityIds4FederatedCreds
            $htUserAssignedIdentitiesSettings = $using:htUserAssignedIdentitiesSettings
            #endregion UsingVARs

            foreach ($childMgSubDetail in $_.Group) {
                #P1-21: one failing item must not abort the whole run (a throw inside -Parallel stops the script under ErrorActionPreference Stop)
                try {
                    #region parallel
                    $childMgSubId = $childMgSubDetail.subscriptionId
                    $childMgSubDisplayName = $childMgSubDetail.subscriptionName

                    # $rndom = Get-Random -Minimum 10 -Maximum 750
                    # Start-Sleep -Millisecond $rndom

                    #SubscriptionPolicyAssignments
                    $currentTask = "Policy assignments '$($childMgSubDisplayName -replace '##', '# #' -replace '[\r\n]+', ' ')' ('$childMgSubId')"
                    $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/subscriptions/$($childMgSubId)/providers/Microsoft.Authorization/policyAssignments?api-version=2026-07-01"
                    $method = 'GET'
                    $L1mgmtGroupSubPolicyAssignments = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -caller 'CustomDataCollection'

                    foreach ($L1mgmtGroupSubPolicyAssignment in $L1mgmtGroupSubPolicyAssignments) {

                        if (-not $htCacheAssignmentsPolicy[($L1mgmtGroupSubPolicyAssignment.Id).ToLower()]) {
                            $script:htCacheAssignmentsPolicy[($L1mgmtGroupSubPolicyAssignment.Id).ToLower()] = @{
                                Assignment = $L1mgmtGroupSubPolicyAssignment
                            }
                        }
                    }

                    #SubscriptionRoles
                    $currentTask = "Custom Role definitions '$($childMgSubDisplayName -replace '##', '# #' -replace '[\r\n]+', ' ')' ('$childMgSubId')"
                    $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/subscriptions/$childMgSubId/providers/Microsoft.Authorization/roleDefinitions?api-version=$($roleDefinitionsApiVersion)&`$filter=type%20eq%20'CustomRole'"
                    $method = 'GET'
                    $subCustomRoleDefinitions = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -caller 'CustomDataCollection'

                    foreach ($subCustomRoleDefinition in $subCustomRoleDefinitions) {
                        if (-not ($htCacheDefinitionsRole)[$subCustomRoleDefinition.name]) {

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

                            ($script:htCacheDefinitionsRole)[$subCustomRoleDefinition.name] = @{
                                definition = $subCustomRoleDefinition
                                roleIsCritical = $roleCapable4RoleAssignmentsWrite
                            }

                        }
                    }

                    #PIM RoleAssignmentScheduleInstances
                    #reset per subscription (it kept the previous subscription's entries in the same batch) and always defined
                    #(empty without PIM data, e.g. no Entra ID P2 license): it is read below with $htRoleAssignmentsPIM[...]
                    $htRoleAssignmentsPIM = @{}
                    if ($htDoARMRoleAssignmentScheduleInstances.Do -eq $true) {
                        $currentTask = "Role assignment schedule instances API Sub '$($childMgSubDisplayName -replace '##', '# #' -replace '[\r\n]+', ' ')' ('$childMgSubId')"
                        $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/subscriptions/$childMgSubId/providers/Microsoft.Authorization/roleAssignmentScheduleInstances?api-version=2020-10-01"
                        $method = 'GET'
                        $roleAssignmentScheduleInstancesFromAPI = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -caller 'CustomDataCollection'

                        if ($roleAssignmentScheduleInstancesFromAPI -eq 'ResourceNotOnboarded' -or $roleAssignmentScheduleInstancesFromAPI -eq 'TenantNotOnboarded' -or $roleAssignmentScheduleInstancesFromAPI -eq 'InvalidResourceType' -or $roleAssignmentScheduleInstancesFromAPI -eq 'RoleAssignmentScheduleInstancesError' -or $roleAssignmentScheduleInstancesFromAPI -eq 'AadPremiumLicenseRequired') {

                            if ($roleAssignmentScheduleInstancesFromAPI -eq 'AadPremiumLicenseRequired') {
                                Write-Host "    -> Setting 'htDoARMRoleAssignmentScheduleInstances.Do' to false (AadPremiumLicenseRequired)"
                                $script:htDoARMRoleAssignmentScheduleInstances.Do = $false
                            }
                        }
                        else {
                            $roleAssignmentScheduleInstances = ($roleAssignmentScheduleInstancesFromAPI.where( { ($_.properties.roleAssignmentScheduleId -replace '.*/') -ne ($_.properties.originRoleAssignmentId -replace '.*/') }))
                            $roleAssignmentScheduleInstancesCount = $roleAssignmentScheduleInstances.Count
                            if ($roleAssignmentScheduleInstancesCount -gt 0) {
                                $htRoleAssignmentsPIM = @{}
                                foreach ($roleAssignmentScheduleInstance in $roleAssignmentScheduleInstances) {
                                    $htRoleAssignmentsPIM[($roleAssignmentScheduleInstance.properties.originRoleAssignmentId).tolower()] = $roleAssignmentScheduleInstance.properties
                                }
                            }
                        }
                    }

                    #SubscriptionRoleAssignments
                    #RoleAssignment API (system metadata e.g. createdOn)
                    $currentTask = "Role assignments API '$($childMgSubDisplayName -replace '##', '# #' -replace '[\r\n]+', ' ')' ('$childMgSubId')"
                    $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/subscriptions/$childMgSubId/providers/Microsoft.Authorization/roleAssignments?api-version=2022-04-01"
                    $method = 'GET'
                    $roleAssignmentsFromAPI = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -caller 'CustomDataCollection'

                    if ($roleAssignmentsFromAPI.Count -gt 0) {
                        foreach ($roleAssignmentFromAPI in $roleAssignmentsFromAPI) {
                            if (-not $htRoleAssignmentsFromAPIInheritancePrevention[$roleAssignmentFromAPI.id -replace '.*/']) {
                                if (-not ($htCacheAssignmentsRole)[$roleAssignmentFromAPI.id]) {
                                    $splitAssignment = ($roleAssignmentFromAPI.id).Split('/')
                                    #
                                    ($script:htCacheAssignmentsRole)[$roleAssignmentFromAPI.id] = @{
                                        assignment = $roleAssignmentFromAPI
                                    }

                                    if ($roleAssignmentFromAPI.properties.scope -like '/subscriptions/*/resourcegroups/*') {
                                        if ($roleAssignmentFromAPI.properties.scope -like '/subscriptions/*/resourcegroups/*' -and $roleAssignmentFromAPI.properties.scope -notlike '/subscriptions/*/resourcegroups/*/providers*') {
                                            ($script:htCacheAssignmentsRole)[$roleAssignmentFromAPI.id] = @{
                                                assignment = $roleAssignmentFromAPI
                                                assignmentScope = 'RG'
                                                assignmentScopeId = "$($splitAssignment[2])/$($splitAssignment[4])"
                                                assignmentScopeName = "$($htSubscriptionsMgPath[$splitAssignment[2]].DisplayName) ($($splitAssignment[2]))/$($splitAssignment[4])"
                                                assignmentResourceType = 'ResourceGroup'
                                                assignmentResourceName = $splitAssignment[4]
                                                roleIsCritical = ($htCacheDefinitionsRole)[$roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'].roleIsCritical
                                                roleName = ($htCacheDefinitionsRole)[$roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'].definition.properties.roleName
                                                roleId = $roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'
                                                type = ($htCacheDefinitionsRole)[$roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'].definition.properties.type
                                            }

                                        }
                                        if ($roleAssignmentFromAPI.properties.scope -like '/subscriptions/*/resourcegroups/*/providers*') {
                                            #resource part from the scope ('<namespace>/<type>/<name>[/<childType>/<childName>...]'): fixed Split('/')
                                            #positions 6-8 credited assignments on nested resources (e.g. a storage container) to the parent resource.
                                            #For a non-nested resource the values are the same as before
                                            $resourcePath = $roleAssignmentFromAPI.properties.scope -replace '(?i)^/subscriptions/[^/]+/resourcegroups/[^/]+/providers/', ''
                                            $resourcePathSegments = $resourcePath.Split('/')
                                            $resourceNames = @(for ($i = 2; $i -lt $resourcePathSegments.Count; $i += 2) { $resourcePathSegments[$i] }) -join '/'
                                            ($script:htCacheAssignmentsRole)[$roleAssignmentFromAPI.id] = @{
                                                assignment = $roleAssignmentFromAPI
                                                assignmentScope = 'Res'
                                                assignmentScopeId = "$($splitAssignment[2])/$($splitAssignment[4])/$($resourceNames)"
                                                assignmentScopeName = "$($htSubscriptionsMgPath[$splitAssignment[2]].DisplayName) ($($splitAssignment[2]))/$($splitAssignment[4])/$($resourcePath)"
                                                assignmentResourceType = 'Resource'
                                                assignmentResourceName = $resourcePath
                                                roleIsCritical = ($htCacheDefinitionsRole)[$roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'].roleIsCritical
                                                roleName = ($htCacheDefinitionsRole)[$roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'].definition.properties.roleName
                                                roleId = $roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'
                                                type = ($htCacheDefinitionsRole)[$roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'].definition.properties.type
                                            }

                                        }
                                    }
                                    else {
                                        #review R-5: by the scope of the assignment. An assignment inherited from a management group or the tenant
                                        #root is normally known from the management group phase; one that phase missed (a failed item, created
                                        #during the run) was labelled 'Sub' with assignmentScopeId '/subscriptions/Microsoft.Management'
                                        $roleAssignmentScope = [string]$roleAssignmentFromAPI.properties.scope
                                        $roleDefinitionCacheId = $roleAssignmentFromAPI.properties.roleDefinitionId -replace '.*/'
                                        $scopeLabels = $null
                                        if ($roleAssignmentScope -eq '/') {
                                            $scopeLabels = @{ assignmentScope = 'Ten'; assignmentScopeId = ''; assignmentScopeName = ''; assignmentResourceType = 'Tenant'; assignmentResourceName = 'Tenant' }
                                        }
                                        elseif ($roleAssignmentScope -match '^/providers/Microsoft\.Management/managementGroups/([^/]+)$') {
                                            $scopeMgId = $Matches[1]
                                            $scopeLabels = @{ assignmentScope = 'MG'; assignmentScopeId = "/providers/Microsoft.Management/managementGroups/$($scopeMgId)"; assignmentScopeName = "$($htManagementGroupsMgPath[$scopeMgId].DisplayName)"; assignmentResourceType = 'ManagementGroup'; assignmentResourceName = $scopeMgId }
                                        }
                                        else {
                                            if ($roleAssignmentScope -notmatch "^/subscriptions/$([regex]::Escape($childMgSubId))$") {
                                                Write-Host "    role assignment '$($roleAssignmentFromAPI.id)': unexpected scope '$($roleAssignmentScope)' - listed as subscription assignment" -ForegroundColor Yellow
                                            }
                                            $hlperSubName = $htSubscriptionsMgPath[$splitAssignment[2]].DisplayName
                                            $scopeLabels = @{ assignmentScope = 'Sub'; assignmentScopeId = "/subscriptions/$($splitAssignment[2])"; assignmentScopeName = $hlperSubName; assignmentResourceType = 'Subscription'; assignmentResourceName = $hlperSubName }
                                        }
                                        ($script:htCacheAssignmentsRole)[$roleAssignmentFromAPI.id] = @{
                                            assignment = $roleAssignmentFromAPI
                                            assignmentScope = $scopeLabels.assignmentScope
                                            assignmentScopeId = $scopeLabels.assignmentScopeId
                                            assignmentScopeName = $scopeLabels.assignmentScopeName
                                            assignmentResourceType = $scopeLabels.assignmentResourceType
                                            assignmentResourceName = $scopeLabels.assignmentResourceName
                                            roleIsCritical = ($htCacheDefinitionsRole)[$roleDefinitionCacheId].roleIsCritical
                                            roleName = ($htCacheDefinitionsRole)[$roleDefinitionCacheId].definition.properties.roleName
                                            roleId = $roleDefinitionCacheId
                                            type = ($htCacheDefinitionsRole)[$roleDefinitionCacheId].definition.properties.type
                                        }

                                    }

                                    if ($htRoleAssignmentsPIM[($roleAssignmentFromAPI.id).tolower()]) {
                                        ($script:htCacheAssignmentsRole)[$roleAssignmentFromAPI.id].assignmentPIMDetails = $htRoleAssignmentsPIM[($roleAssignmentFromAPI.id).tolower()]
                                    }

                                }
                            }
                        }
                    }

                    function dataCollectionResources {
                        [CmdletBinding()]param(
                            [string]$scopeId,
                            [string]$scopeDisplayName,
                            $ChildMgMgPath,
                            $subscriptionQuotaId
                        )

                        $currentTask = "Getting ResourceTypes for Subscription: '$($scopeDisplayName -replace '##', '# #' -replace '[\r\n]+', ' ')' ('$scopeId') [quotaId:'$subscriptionQuotaId']"
                        #review R-20: without $expand=createdTime,changedTime (never read) and without grouping by type (the consumers
                        #sort / aggregate; the array is filled by parallel items anyway)
                        $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/subscriptions/$($scopeId)/resources?api-version=2025-04-01"
                        $method = 'GET'
                        $resourcesSubscriptionResult = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -caller 'CustomDataCollection'

                        $subscriptionHasUserAssignedIdentities = $false
                        foreach ($resource in @($resourcesSubscriptionResult)) {

                            if ($resource.identity.userAssignedIdentities) {
                                $resource.identity.userAssignedIdentities.psobject.properties | ForEach-Object {
                                    if ((-not [string]::IsNullOrEmpty($resource.Id)) -and (-not [string]::IsNullOrEmpty($_.Value.principalId))) {
                                        $hlp = ($_.Name.split('/'))
                                        $hlpMiSubId = $hlp[2]
                                        $null = $script:arrayUserAssignedIdentities4Resources.Add([PSCustomObject]@{
                                                resourceId = $resource.Id
                                                resourceName = $resource.name
                                                resourceMgPath = $childMgMgPath
                                                resourceSubscriptionName = $scopeDisplayName
                                                resourceSubscriptionId = $scopeId
                                                resourceResourceGroupName = ($resource.Id -split ('/'))[4]
                                                resourceType = $resource.type
                                                resourceLocation = $resource.location
                                                miPrincipalId = $_.Value.principalId
                                                miClientId = $_.Value.clientId
                                                miMgPath = $htSubscriptionsMgPath[$hlpMiSubId].pathDelimited
                                                miSubscriptionName = $htSubscriptionsMgPath[$hlpMiSubId].DisplayName
                                                miSubscriptionId = $hlpMiSubId
                                                miResourceGroupName = $hlp[4]
                                                miResourceId = $_.Name
                                                miResourceName = $_.Name -replace '.*/'
                                            })
                                    }
                                }
                            }

                            if ($resource.type -eq 'Microsoft.ManagedIdentity/userAssignedIdentities') {
                                $subscriptionHasUserAssignedIdentities = $true
                                #review P-14: the federated credentials of the UAMIs are read in their own parallel phase after the
                                #collection (one request after another here made a subscription with many UAMIs the long tail)
                                $null = $script:arrayUserAssignedIdentityIds4FederatedCreds.Add([PSCustomObject]@{ id = $resource.Id; name = $resource.name; subscriptionName = $scopeDisplayName; subscriptionId = $scopeId })
                            }
                        }


                        #isolation scope and assignment restrictions of the user-assigned managed identities: the resources listing does not
                        #return them. One call per subscription that has user-assigned identities. assignmentRestrictions needs the preview
                        #api-version (isolationScope is in 2024-11-30); a cloud without the preview version falls back to 2024-11-30
                        if ($subscriptionHasUserAssignedIdentities) {
                            $userAssignedIdentitiesSubscription = @()
                            $userAssignedIdentitiesApiVersion = $null
                            foreach ($apiVersion in '2025-05-31-preview', '2024-11-30') {
                                $currentTask = "Getting user-assigned managed identities (api-version $apiVersion) for Subscription: '$($scopeDisplayName -replace '##', '# #' -replace '[\r\n]+', ' ')' ('$scopeId') [quotaId:'$subscriptionQuotaId']"
                                $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/subscriptions/$($scopeId)/providers/Microsoft.ManagedIdentity/userAssignedIdentities?api-version=$apiVersion"
                                $userAssignedIdentitiesSubscription = @(AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method 'GET' -currentTask $currentTask -caller 'CustomDataCollection' -skipOnErrorCode @(400, 404) -unhandledErrorAction 'ContinueQuiet' | Where-Object { $_.id })
                                if ($userAssignedIdentitiesSubscription.Count -gt 0) {
                                    $userAssignedIdentitiesApiVersion = $apiVersion
                                    break
                                }
                            }
                            foreach ($userAssignedIdentity in $userAssignedIdentitiesSubscription) {
                                $restrictions = $userAssignedIdentity.properties.assignmentRestrictions
                                $script:htUserAssignedIdentitiesSettings[$userAssignedIdentity.id.ToLower()] = [PSCustomObject]@{
                                    id = $userAssignedIdentity.id
                                    name = $userAssignedIdentity.name
                                    location = $userAssignedIdentity.location
                                    subscriptionId = $scopeId
                                    subscriptionName = $scopeDisplayName
                                    mgPath = $childMgMgPath
                                    resourceGroup = ($userAssignedIdentity.id -split '/')[4]
                                    principalId = $userAssignedIdentity.properties.principalId
                                    clientId = $userAssignedIdentity.properties.clientId
                                    isolationScope = $userAssignedIdentity.properties.isolationScope
                                    #$false: the api-version did not return the property (not collected), not 'unrestricted'
                                    assignmentRestrictionsCollected = ($null -ne $restrictions)
                                    assignmentRestrictions = @($restrictions.providers | Where-Object { $_ } | Sort-Object)
                                    apiVersion = $userAssignedIdentitiesApiVersion
                                }
                            }
                        }
                    }
                    $hierarchyInfo = $htSubscriptionsMgPath[$childMgSubId]
                    $childMgMgPath = $hierarchyInfo.pathDelimited
                    $currentSubscription = $htAllSubscriptionsFromAPI[$childMgSubId].subDetails
                    $subscriptionQuotaId = $currentSubscription.subscriptionPolicies.quotaId
                    dataCollectionResources -scopeId $childMgSubId -scopeDisplayName $childMgSubDisplayName -ChildMgMgPath $childMgMgPath -subscriptionQuotaId $subscriptionQuotaId

                    $endSubLoopThis = Get-Date
                    $null = $script:customDataCollectionDuration.Add([PSCustomObject]@{
                            Type = 'SUB'
                            Id = $childMgSubId
                            DurationSec = (New-TimeSpan -Start $startSubLoopThis -End $endSubLoopThis).TotalSeconds
                        })

                    $null = $script:arrayDataCollectionProgressSub.Add($childMgSubId)
                    $progressCount = ($arrayDataCollectionProgressSub).Count
                    Write-Host "  $($progressCount)/$($subsToProcessInCustomDataCollectionCount) Subscriptions processed"
                    #endregion parallel
                }
                catch {
                    $failedItem = "$($childMgSubDetail.subscriptionName -replace '##', '# #' -replace '[\r\n]+', ' ') ($($childMgSubDetail.subscriptionId))"
                    $htParallelFailures["Subscriptions custom data|$failedItem"] = [PSCustomObject]@{ Phase = 'Subscriptions custom data'; Item = $failedItem; Error = $_.Exception.Message }
                    Write-Host "  Subscriptions custom data: '$failedItem' failed - skipped: $($_.Exception.Message)" -ForegroundColor Yellow
                }
            }



        } -ThrottleLimit $ThrottleLimitARM
        checkParallelFailures -phase 'Subscriptions custom data' -itemCount ($subsToProcessInCustomDataCollectionCount)

        # $endBatch = Get-Date
        # Write-Host " Batch #$batchCnt processing duration: $((New-TimeSpan -Start $startBatch -End $endBatch).TotalMinutes) minutes ($((New-TimeSpan -Start $startBatch -End $endBatch).TotalSeconds) seconds)"
        #}
        #[System.GC]::Collect()

        $endSubLoop = Get-Date
        Write-Host " CustomDataCollection Subscriptions processing duration: $((New-TimeSpan -Start $startSubLoop -End $endSubLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startSubLoop -End $endSubLoop).TotalSeconds) seconds)"
    }
}

#endregion Function_dataCollection

#region Service Principal type label (review R-8)
#the type label of a Service Principal ('SP APP INT', 'SP MI System assigned', 'SP EXT', ...) as the collection sets it
#(objectTypeConcatinated); for a Service Principal that was not processed it is derived from the listed object
function getServicePrincipalTypeLabel($ServicePrincipal, [string]$TenantId) {
    $intExt = $(if ($ServicePrincipal.appOwnerOrganizationId -eq $TenantId) { 'INT' } else { 'EXT' })
    if ($ServicePrincipal.servicePrincipalType -eq 'ManagedIdentity') {
        $miType = 'unknown'
        foreach ($altName in @($ServicePrincipal.alternativeNames)) {
            if ($altName -eq 'isExplicit=True') { $miType = 'User assigned' }
            if ($altName -eq 'isExplicit=False') { $miType = 'System assigned' }
        }
        return "SP MI $($miType)"
    }
    if ($ServicePrincipal.servicePrincipalType -eq 'Application') {
        return "SP APP $($intExt)"
    }
    return "SP $($intExt)"
}
function getServicePrincipalTypeLabelById([string]$Id) {
    if ($htSpLookup -and $htSpLookup[$Id].objectTypeConcatinated) { return $htSpLookup[$Id].objectTypeConcatinated }
    if ($htAllServicePrincipalsById -and $htAllServicePrincipalsById[$Id]) { return getServicePrincipalTypeLabel -ServicePrincipal $htAllServicePrincipalsById[$Id] -TenantId $azAPICallConf['checkContext'].Tenant.Id }
    return $null
}
#endregion Service Principal type label

#region resolveMissingRoleDefinitions (review R-6)
#can the role write role assignments (same rule as the definition caching above): the 'critical' flag of an Azure role
function testRoleCapableForRoleAssignmentsWrite($RoleDefinition) {
    $actions = @($RoleDefinition.properties.permissions.actions)
    $notActions = @($RoleDefinition.properties.permissions.notActions)
    $writeActions = @('Microsoft.Authorization/roleassignments/write', 'Microsoft.Authorization/roleassignments/*', 'Microsoft.Authorization/*/write', 'Microsoft.Authorization/*', '*/write', '*')
    return [bool](@($writeActions.where({ $actions -contains $_ })).Count -gt 0 -and @($writeActions.where({ $notActions -contains $_ })).Count -eq 0)
}

#region Azure role conditions
#A condition can limit which roles a 'Microsoft.Authorization/roleAssignments/write' may hand out: on the role definition (built-in
#roles such as Key Vault Data Access Administrator may only assign Key Vault data roles; returned since api-version 2022-05-01-preview)
#or on the role assignment (the 'constrain roles' delegation of Role Based Access Control Administrator / User Access Administrator).
#A role that can write role assignments is critical only if a critical role is among the roles it may assign.

#one permission block of a role definition grants role assignment write
function testPermissionCanWriteRoleAssignments($Permission) {
    $actions = @($Permission.actions)
    $notActions = @($Permission.notActions)
    $writeActions = @('Microsoft.Authorization/roleassignments/write', 'Microsoft.Authorization/roleassignments/*', 'Microsoft.Authorization/*/write', 'Microsoft.Authorization/*', '*/write', '*')
    return [bool](@($writeActions.where({ $actions -contains $_ })).Count -gt 0 -and @($writeActions.where({ $notActions -contains $_ })).Count -eq 0)
}

#the roles a condition lets a role assignment write hand out: allow (only these ids) and/or deny (all but these ids), ids lower case
#without hyphens. Read conservatively, otherwise evaluated = $false (the role stays critical):
#- every RoleDefinitionId clause of the request is 'ForAnyOfAnyValues:GuidEquals {ids}' or 'ForAnyOfAllValues:GuidNotEquals {ids}';
#- every OR is the guard that leaves other actions unaffected: (!(ActionMatches{'...'})) OR (...).
#Several allow lists are united (combined with AND the real set is smaller: never understated).
function getRoleConditionAssignableRoles([string]$Condition) {
    $notEvaluated = @{ evaluated = $false; allow = $null; deny = $null }
    if ([string]::IsNullOrWhiteSpace($Condition)) { return $notEvaluated }
    $text = $Condition -replace '\s+', ' '
    $attribute = '@Request\[Microsoft\.Authorization/roleAssignments:RoleDefinitionId\]'
    $clauses = [regex]::Matches($text, "(?i)$($attribute) ?(ForAnyOfAnyValues:GuidEquals|ForAnyOfAllValues:GuidNotEquals) ?\{([^}]*)\}")
    if ($clauses.Count -eq 0 -or $clauses.Count -ne [regex]::Matches($text, "(?i)$($attribute)").Count) { return $notEvaluated }
    $orCount = [regex]::Matches($text, '(?i)(?<![\w/.])OR(?![\w/.])|\|\|').Count
    $guardOrCount = [regex]::Matches($text, "(?i)!\s?\(\s?ActionMatches\s?\{'[^']*'\}\s?\)(\s?\))*\s?(OR(?![\w/.])|\|\|)").Count
    if ($orCount -ne $guardOrCount) { return $notEvaluated }
    $result = @{ evaluated = $true; allow = $null; deny = $null }
    foreach ($clause in $clauses) {
        $ids = [System.Collections.Generic.HashSet[string]]::new()
        foreach ($value in ($clause.Groups[2].Value -split ',')) {
            $id = ($value.Trim().Trim("'", '"') -replace '-', '').ToLowerInvariant()
            if ($id -notmatch '^[0-9a-f]{32}$') { return $notEvaluated }
            $null = $ids.Add($id)
        }
        $kind = $(if ($clause.Groups[1].Value -like 'ForAnyOfAnyValues*') { 'allow' } else { 'deny' })
        if ($null -eq $result[$kind]) { $result[$kind] = [System.Collections.Generic.HashSet[string]]::new() }
        $result[$kind].UnionWith($ids)
    }
    return $result
}

#the effect of the conditions on a role that can write role assignments. $Conditions: @{ source = 'role definition' | 'role
#assignment'; text }; $htRoles: id (lower case, no hyphens) -> @{ name; critical }; $CriticalRoleIds: the ids of the critical roles.
#effect: 'limited' (only non-critical roles can be assigned: not critical), 'allows critical' (critical), 'not evaluated' (critical)
#$SelfRoleId: the role itself when its own definition carries the condition - assigning it again gives nothing beyond that limit
#(Azure Sphere Owner may assign Azure Sphere Owner); not for a condition on the assignment only (the new assignment would lack it)
function getRoleConditionEffect($Conditions, $htRoles, $CriticalRoleIds, [string]$SelfRoleId) {
    $effect = [ordered]@{ critical = $true; effect = ''; sources = ''; assignableRoles = ''; criticalRoles = '' }
    $limits = [System.Collections.Generic.List[object]]::new()
    $sources = [System.Collections.Generic.List[string]]::new()
    foreach ($condition in @($Conditions)) {
        if (-not $condition -or [string]::IsNullOrWhiteSpace([string]$condition.text)) { continue }
        if (-not $sources.Contains($condition.source)) { $sources.Add($condition.source) }
        $parsed = getRoleConditionAssignableRoles -Condition $condition.text
        #a condition that cannot be read is left out: all conditions must hold, it can only limit further
        if ($parsed.evaluated) { $limits.Add($parsed) }
    }
    if ($sources.Count -eq 0) { return $effect }
    $effect.sources = $sources -join ' and '
    if ($limits.Count -eq 0) { $effect.effect = 'not evaluated'; return $effect }
    $nameOf = { param($id) if ($htRoles[$id]) { [string]$htRoles[$id].name } else { "role $(([guid]$id).ToString())" } }
    $isAllowed = {
        param($id)
        foreach ($limit in $limits) {
            if ($null -ne $limit.allow -and -not $limit.allow.Contains($id)) { return $false }
            if ($null -ne $limit.deny -and $limit.deny.Contains($id)) { return $false }
        }
        return $true
    }
    $criticalAllowed = [System.Collections.Generic.List[string]]::new()
    foreach ($id in @($CriticalRoleIds)) { if ($id -ne $SelfRoleId -and (& $isAllowed $id)) { $criticalAllowed.Add($id) } }
    $allowLimits = @($limits.where({ $null -ne $_.allow }))
    foreach ($limit in $allowLimits) {
        foreach ($id in $limit.allow) {
            #a role that is not known (e.g. a custom role of a scope that was not collected) counts as critical
            if (-not $htRoles.ContainsKey($id) -and -not $criticalAllowed.Contains($id) -and (& $isAllowed $id)) { $criticalAllowed.Add($id) }
        }
    }
    if ($allowLimits.Count -gt 0) {
        $effect.assignableRoles = 'only ' + (@(@($allowLimits[0].allow).where({ & $isAllowed $_ }) | ForEach-Object { & $nameOf $_ } | Sort-Object) -join ', ')
    }
    else {
        $denied = [System.Collections.Generic.HashSet[string]]::new()
        foreach ($limit in $limits) { if ($null -ne $limit.deny) { $denied.UnionWith($limit.deny) } }
        $effect.assignableRoles = 'all roles except ' + (@($denied | ForEach-Object { & $nameOf $_ } | Sort-Object) -join ', ')
    }
    if ($criticalAllowed.Count -gt 0) {
        $effect.effect = 'allows critical'
        $effect.criticalRoles = @($criticalAllowed | ForEach-Object { & $nameOf $_ } | Sort-Object) -join ', '
    }
    else {
        $effect.critical = $false
        $effect.effect = 'limited'
    }
    return $effect
}

#the text of an assignment's condition effect for the report, CSV and effective privilege ('' without condition effect)
function getAzureRoleConditionText($Assignment) {
    switch ([string]$Assignment.roleConditionEffect) {
        'limited' { return "limited by $($Assignment.roleConditionSources) condition: can assign $($Assignment.roleConditionAssignableRoles)" }
        'allows critical' { return "$($Assignment.roleConditionSources) condition allows $($Assignment.roleConditionCriticalRoles)" }
        'not evaluated' { return "$($Assignment.roleConditionSources) condition not evaluated - rated critical" }
    }
    return ''
}

#after the collection (and resolveMissingRoleDefinitions): the critical flag of every role assignment from the conditions
function applyAzureRoleConditions {
    #1 the roles by their actions
    $htRoles = @{}
    foreach ($roleId in @($htCacheDefinitionsRole.Keys)) {
        $htRoles[([string]$roleId -replace '-', '').ToLowerInvariant()] = @{ name = [string]$htCacheDefinitionsRole[$roleId].definition.properties.roleName; critical = [bool]$htCacheDefinitionsRole[$roleId].roleIsCritical }
    }
    #2 role definitions whose role assignment write is limited by a condition
    $conditionalDefinitionIds = [System.Collections.Generic.List[string]]::new()
    foreach ($roleId in @($htCacheDefinitionsRole.Keys)) {
        $cached = $htCacheDefinitionsRole[$roleId]
        if (-not $cached.roleIsCritical) { continue }
        $granting = @(@($cached.definition.properties.permissions).where({ $_ -and (testPermissionCanWriteRoleAssignments $_) }))
        #a granting permission without condition grants it without limit
        if ($granting.Count -eq 0 -or @($granting.where({ [string]::IsNullOrWhiteSpace([string]$_.condition) })).Count -gt 0) { continue }
        #several granting permissions are alternatives (OR): read as one condition with an OR that is no guard - not evaluated
        $cached.roleDefinitionCondition = @($granting | ForEach-Object { [string]$_.condition }) -join ' OR '
        $conditionalDefinitionIds.Add($roleId)
    }
    $definitionsWithCondition = $conditionalDefinitionIds.Count
    #the roles they may assign can be limited by their own condition (Azure Local Migrate Owner may assign Azure Local Migrate Execute
    #Expert, which may only assign storage roles): evaluated until nothing changes - every role starts critical (by its actions) and
    #only becomes not critical when all roles it may assign are not critical (never understated; ends after at most n rounds)
    $htRolesEffective = @{}
    foreach ($key in $htRoles.Keys) { $htRolesEffective[$key] = $htRoles[$key] }
    for ($round = 0; $round -le $conditionalDefinitionIds.Count; $round++) {
        $criticalRoleIdsEffective = @(@($htRolesEffective.Keys).where({ $htRolesEffective[$_].critical }))
        $changed = $false
        foreach ($roleId in $conditionalDefinitionIds) {
            $cached = $htCacheDefinitionsRole[$roleId]
            $key = ([string]$roleId -replace '-', '').ToLowerInvariant()
            $cached.definitionConditionEffect = getRoleConditionEffect -Conditions @(@{ source = 'role definition'; text = $cached.roleDefinitionCondition }) -htRoles $htRolesEffective -CriticalRoleIds $criticalRoleIdsEffective -SelfRoleId $key
            if (-not $cached.definitionConditionEffect.critical -and $htRolesEffective[$key].critical) {
                $htRolesEffective[$key] = @{ name = $htRoles[$key].name; critical = $false }
                $changed = $true
            }
        }
        if (-not $changed) { break }
    }
    #3 the role assignments (a role limited by its own definition condition is not critical as an assignable role)
    $criticalRoleIdsEffective = @(@($htRolesEffective.Keys).where({ $htRolesEffective[$_].critical }))
    $assignmentsWithEffect = 0
    $assignmentsLimited = 0
    foreach ($assignment in @($htCacheAssignmentsRole.Values)) {
        $cached = $htCacheDefinitionsRole[$assignment.roleId]
        $assignment.roleAssignmentCondition = [string]$assignment.assignment.properties.condition
        $assignment.roleDefinitionCondition = $(if ($cached -and $cached.roleDefinitionCondition) { [string]$cached.roleDefinitionCondition } else { '' })
        $assignment.roleConditionEffect = ''
        $assignment.roleConditionSources = ''
        $assignment.roleConditionAssignableRoles = ''
        $assignment.roleConditionCriticalRoles = ''
        if (-not $cached -or -not $cached.roleIsCritical) { continue }
        $conditions = @(
            if ($assignment.roleDefinitionCondition) { @{ source = 'role definition'; text = $assignment.roleDefinitionCondition } }
            if ($assignment.roleAssignmentCondition) { @{ source = 'role assignment'; text = $assignment.roleAssignmentCondition } }
        )
        if ($conditions.Count -eq 0) { continue }
        $effect = getRoleConditionEffect -Conditions $conditions -htRoles $htRolesEffective -CriticalRoleIds $criticalRoleIdsEffective -SelfRoleId $(if ($assignment.roleDefinitionCondition) { ([string]$assignment.roleId -replace '-', '').ToLowerInvariant() } else { '' })
        $assignmentsWithEffect++
        $assignment.roleIsCritical = $effect.critical
        $assignment.roleConditionEffect = $effect.effect
        $assignment.roleConditionSources = $effect.sources
        $assignment.roleConditionAssignableRoles = $effect.assignableRoles
        $assignment.roleConditionCriticalRoles = $effect.criticalRoles
        if ($effect.effect -eq 'limited') { $assignmentsLimited++ }
    }
    Write-Host " Azure role conditions: $($definitionsWithCondition) role definitions limit role assignment write by a condition; $($assignmentsWithEffect) role assignments with such a condition, $($assignmentsLimited) of them limited to non-critical roles (not critical)"
}
#endregion Azure role conditions

#role definitions of role assignments that the custom role listings of management groups and subscriptions ('at scope and above')
#did not return: a custom role assignable only to resource groups or resources, or a deleted definition with an orphaned
#assignment. Without the definition such an assignment had no role name, type and critical flag (an escalation role was missed).
#Each definition is read by its id (the assignment's roleDefinitionId, then the assignment's scope); unresolved ones are listed.
function resolveMissingRoleDefinitions {
    $missing = @($htCacheAssignmentsRole.Values.where({ $_.roleId -and -not $htCacheDefinitionsRole[$_.roleId] }))
    if ($missing.Count -eq 0) {
        Write-Host ' Role definitions of all role assignments known'
        return
    }
    #the api-version of the role definition listings (Azure role conditions); 2022-04-01 when called without it (tests)
    if (-not $roleDefinitionsApiVersion) { $roleDefinitionsApiVersion = '2022-04-01' }
    $byRoleId = @($missing | Group-Object -Property { $_.roleId })
    Write-Host " Role definitions not returned by the custom role listings: $($byRoleId.Count) ($($missing.Count) role assignments) - reading them by id"
    $unresolved = [System.Collections.ArrayList]@()
    foreach ($roleGroup in $byRoleId) {
        #the scope of the roleDefinitionId of the assignment ('' for the tenant-level form), then the scope of the assignment
        $candidateScopes = @(@(foreach ($cached in $roleGroup.Group) {
                    ([string]$cached.assignment.properties.roleDefinitionId) -replace '(?i)/providers/Microsoft\.Authorization/roleDefinitions/[^/]+$', ''
                    ([string]$cached.assignment.properties.scope).TrimEnd('/')
                }) | Select-Object -Unique)
        $definition = $null
        foreach ($candidateScope in $candidateScopes) {
            $result = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri "$($azAPICallConf['azAPIEndpointUrls'].ARM)$($candidateScope)/providers/Microsoft.Authorization/roleDefinitions/$($roleGroup.Name)?api-version=$($roleDefinitionsApiVersion)" -method 'GET' -currentTask "Role definition '$($roleGroup.Name)' at '$($candidateScope)'" -listenOn 'Content' -skipOnErrorCode @(400, 403, 404)
            if ($result -and $result -isnot [string] -and $result.properties.roleName) {
                $definition = $result
                break
            }
        }
        if (-not $definition) {
            $null = $unresolved.Add("$($roleGroup.Name) ($($roleGroup.Count) role assignments)")
            #an orphaned assignment (its definition was deleted): visible as such instead of an empty role name
            foreach ($cached in $roleGroup.Group) {
                $cached.roleName = 'role definition not found (deleted?)'
                $cached.type = 'unknown'
            }
            continue
        }
        $roleIsCritical = testRoleCapableForRoleAssignmentsWrite -RoleDefinition $definition
        $htCacheDefinitionsRole[$roleGroup.Name] = @{
            definition = $definition
            roleIsCritical = $roleIsCritical
        }
        foreach ($cached in $roleGroup.Group) {
            $cached.roleIsCritical = $roleIsCritical
            $cached.roleName = $definition.properties.roleName
            $cached.type = $definition.properties.type
        }
        Write-Host "  '$($definition.properties.roleName -replace '##', '# #' -replace '[\r\n]+', ' ')' ($($roleGroup.Name)): $($roleGroup.Count) role assignments$(if ($roleIsCritical) { ', critical' })"
    }
    if ($unresolved.Count -gt 0) {
        Write-Host "  not found (deleted definitions?): $($unresolved -join ', ')" -ForegroundColor Yellow
    }
}
#endregion resolveMissingRoleDefinitions

#HTML

#rsu
#region TenantSummary
#region insight helpers (derived from the collected data, no API calls)
#expiry text of a credential, same wording as the app secrets/certificates
function getCredentialExpiryInfo([datetime]$endDateTime, [int]$warningDays, [int]$maxDays) {
    $totalDays = (New-TimeSpan -Start (Get-Date).ToUniversalTime() -End $endDateTime.ToUniversalTime()).TotalDays
    if ($totalDays -lt 0) {
        return 'expired'
    }
    if ($totalDays -lt $warningDays) {
        return "expires soon (less than grace period $warningDays days)"
    }
    if ($totalDays -gt $maxDays) {
        return "expires in more than $maxDays days"
    }
    return "expires in $warningDays to $maxDays days"
}

#who can sign in with a federated identity credential (issuer + subject)
#$claimsMatchingExpression (review R-27): a flexible federated identity credential has no subject but an expression over the token
#claims (e.g. claims['sub'] matches 'repo:contoso/*'); it trusts every token the expression matches
#$signInAudience/$cloudEnvironment: the deprecated Azure DevOps issuer (retirement 2027-07-01, https://aka.ms/azdo-issuer) concerns
#single-tenant apps and managed identities in the Azure public cloud; multitenant apps and other clouds keep the Azure DevOps issuer
function getFederatedCredentialTrust([string]$issuer, [string]$subject, [string]$claimsMatchingExpression, [string]$signInAudience, [string]$cloudEnvironment) {
    $trust = [ordered]@{ issuerType = 'Other OIDC issuer'; trust = $subject; finding = '' }
    if (-not $subject -and $claimsMatchingExpression) {
        $trust.trust = "claims expression: $claimsMatchingExpression"
    }
    switch -Regex ($issuer) {
        #review F-4: every pattern ends at the host (a look-alike issuer such as https://token.actions.githubusercontent.com.attacker.example
        #was classified as the provider and lost the 'custom issuer' finding)
        '^https://token\.actions\.githubusercontent\.com(/|$)' {
            $trust.issuerType = 'GitHub Actions'
            if ($subject -match '^repo:(?<repo>[^:]+):(?<rest>.+)$') {
                $repo = $Matches['repo']; $rest = $Matches['rest']
                $trust.trust = switch -Regex ($rest) {
                    '^ref:refs/heads/(?<b>.+)$' { "repository $repo, branch $($Matches['b'])"; break }
                    '^ref:refs/tags/(?<t>.+)$' { "repository $repo, tag $($Matches['t'])"; break }
                    '^environment:(?<e>.+)$' { "repository $repo, environment $($Matches['e'])"; break }
                    '^pull_request$' { "repository $repo, any pull request"; break }
                    default { "repository $repo, $rest" }
                }
                if ($rest -eq 'pull_request') {
                    $trust.finding = 'broad: any pull request of the repository can sign in'
                }
            }
            break
        }
        '^https://(vstoken\.dev\.azure\.com|app\.vstoken\.visualstudio\.com)(/|$)' {
            $trust.issuerType = 'Azure DevOps'
            if ($subject -match '^sc://(?<org>[^/]+)/(?<project>[^/]+)/(?<sc>.+)$') {
                $trust.trust = "organization $($Matches['org']), project $($Matches['project']), service connection $($Matches['sc'])"
            }
            if ($cloudEnvironment -in '', 'AzureCloud' -and $signInAudience -notin 'AzureADMultipleOrgs', 'AzureADandPersonalMicrosoftAccount') {
                $trust.finding = 'deprecated Azure DevOps issuer (retirement 2027-07-01): convert the service connection to the Microsoft Entra issuer (https://aka.ms/azdo-issuer)'
            }
            break
        }
        '^https://([a-z0-9-]+\.)?(oic|oidc)\.prod-aks\.azure\.com/' {
            $trust.issuerType = 'Kubernetes (AKS)'
            if ($subject -match '^system:serviceaccount:(?<ns>[^:]+):(?<sa>.+)$') {
                $trust.trust = "namespace $($Matches['ns']), service account $($Matches['sa'])"
            }
            break
        }
        '^https://gitlab\.com(/|$)' {
            $trust.issuerType = 'GitLab'
            break
        }
        '^https://app\.terraform\.io(/|$)' {
            $trust.issuerType = 'Terraform Cloud'
            break
        }
        '^https://accounts\.google\.com(/|$)' {
            $trust.issuerType = 'Google Cloud'
            break
        }
        '^https://login\.microsoftonline\.com/[^/]+/v2\.0(/|$)' {
            $trust.issuerType = 'Microsoft Entra ID (managed identity or app of a tenant)'
            #Azure DevOps service connection with the Microsoft Entra issuer: /eid1/c/pub/t/<tenant>/a/<app>/sc/<organization id>/<service connection id>,
            #tenant and app as base64url GUIDs; the app is Azure DevOps (499b84ac-1321-427f-aa17-267ca6975798)
            if ($subject -cmatch '^/eid1/c/pub/t/[A-Za-z0-9_-]{22}/a/(?<app>[A-Za-z0-9_-]{22})/sc/(?<org>[0-9a-fA-F-]{36})/(?<sc>[0-9a-fA-F-]{36})$') {
                $azureDevOpsAppIdBase64Url = [Convert]::ToBase64String(([guid]'499b84ac-1321-427f-aa17-267ca6975798').ToByteArray()).TrimEnd('=').Replace('+', '-').Replace('/', '_')
                if ($Matches['app'] -ceq $azureDevOpsAppIdBase64Url) {
                    $trust.issuerType = 'Azure DevOps (Microsoft Entra issuer)'
                    $trust.trust = "organization id $($Matches['org']), service connection id $($Matches['sc'])"
                }
            }
            break
        }
        default {
            $trust.finding = 'custom issuer: check who controls it'
        }
    }
    if (-not $subject -and $claimsMatchingExpression) {
        $flexibleFinding = 'flexible: every token whose claims match the expression can sign in - check the pattern (wildcards)'
        $trust.finding = $(if ($trust.finding) { "$($trust.finding); $flexibleFinding" } else { $flexibleFinding })
    }
    return $trust
}

#converting an Azure DevOps service connection to the Microsoft Entra issuer adds a credential and keeps the old one (test tenant 2026-11-10:
#new credential 'migratedFromOldADOIssuer'); $trusts: the getFederatedCredentialTrust results of one identity. The new subject has ids
#only (no organization/project/connection names), so the old credential is matched per identity, not per service connection
function setAzureDevOpsConvertedFinding($trusts) {
    if (@($trusts).where({ $_.issuerType -eq 'Azure DevOps (Microsoft Entra issuer)' }).Count -eq 0) {
        return
    }
    foreach ($trust in @($trusts).where({ $_.finding -like 'deprecated Azure DevOps issuer*' })) {
        $trust.finding = 'deprecated Azure DevOps issuer, converted: the identity also has an Azure DevOps credential with the Microsoft Entra issuer - remove this credential if no other service connection still uses it (https://aka.ms/azdo-issuer)'
    }
}

#owner consent (permissionGrantPoliciesAssigned 'ManagePermissionGrantsForOwnedResource.<policy>'): the Teams/chat resource-specific
#consent (RSC) states of Set-MgBetaTeamRscConfiguration / Set-MgBetaChatRscConfiguration, one policy per state and scope:
#microsoft-dynamically-managed-permissions-for-<scope> = ManagedByMicrosoft (the default; Microsoft: 'by default, this state enables RSC
#for all unblocked apps'), microsoft-all-application-permissions-for-<scope> = EnabledForAllApps, microsoft-pre-approval-apps-for-<scope> =
#EnabledForPreApprovedAppsOnly, none = DisabledForAllApps. Other owner policies (e.g. for groups) are listed by id.
function getOwnedResourceConsentSetting([string[]]$PermissionGrantPoliciesAssigned) {
    $policies = @(@($PermissionGrantPoliciesAssigned).where({ $_ -like 'ManagePermissionGrantsForOwnedResource.*' }) | ForEach-Object { $_ -replace '^ManagePermissionGrantsForOwnedResource\.' })
    if ($policies.Count -eq 0) {
        return [ordered]@{ Value = 'no'; Note = 'team owners and chat members cannot consent to apps for their team or chat (resource-specific consent disabled)' }
    }
    $parts = [System.Collections.Generic.List[string]]::new()
    foreach ($scope in @(@{ id = 'team'; label = 'teams' }, @{ id = 'chat'; label = 'chats' })) {
        $scopePolicies = @($policies.where({ $_ -like "*-for-$($scope.id)" }))
        $state = $(if ($scopePolicies.Count -eq 0) { 'not allowed' }
            elseif ($scopePolicies -contains "microsoft-dynamically-managed-permissions-for-$($scope.id)") { 'Microsoft-managed (default)' }
            elseif ($scopePolicies -contains "microsoft-all-application-permissions-for-$($scope.id)") { 'all apps' }
            elseif ($scopePolicies -contains "microsoft-pre-approval-apps-for-$($scope.id)") { 'pre-approved apps only' }
            else { $scopePolicies -join ', ' })
        $parts.Add("$($scope.label): $state")
    }
    $otherPolicies = @($policies.where({ $_ -notlike '*-for-team' -and $_ -notlike '*-for-chat' }))
    if ($otherPolicies.Count -gt 0) { $parts.Add("other: $($otherPolicies -join ', ')") }
    $note = 'resource-specific consent: team owners and chat members can give apps permissions on their team or chat (e.g. read its messages)'
    if ($policies.where({ $_ -like 'microsoft-dynamically-managed-permissions-for-*' })) {
        $note += "; 'Microsoft-managed' currently allows it for all apps that are not blocked in Teams, and Microsoft can change it (Set-MgBetaTeamRscConfiguration, Set-MgBetaChatRscConfiguration)"
    }
    return [ordered]@{ Value = "yes - $($parts -join '; ')"; Note = $note }
}

#redirect URI findings (scheme, wildcard, Azure-hosted domains that can be taken over when the resource is deleted)
function getRedirectUriFindings([string]$platform, [string]$uri) {
    $findings = [System.Collections.ArrayList]@()
    $isLocalhost = $uri -match '^https?://(localhost|127\.0\.0\.1|\[::1\])([:/]|$)'
    if ($uri -like 'http://*' -and -not $isLocalhost) {
        $null = $findings.Add([ordered]@{ severity = 'high'; finding = 'redirect URI without https' })
    }
    if ($uri.Contains('*')) {
        $null = $findings.Add([ordered]@{ severity = 'high'; finding = 'wildcard redirect URI' })
    }
    if ($uri -match '^https?://[^/]*\.(azurewebsites\.net|cloudapp\.net|cloudapp\.azure\.com|trafficmanager\.net|azurefd\.net|azureedge\.net|azure-api\.net|azurecontainerapps\.io|azurestaticapps\.net|blob\.core\.windows\.net|web\.core\.windows\.net)([:/?#]|$)') {
        $null = $findings.Add([ordered]@{ severity = 'medium'; finding = 'Azure-hosted domain: check the resource still exists (otherwise the name can be registered by others)' })
    }
    if ($isLocalhost -and $platform -eq 'web') {
        $null = $findings.Add([ordered]@{ severity = 'info'; finding = 'localhost redirect URI on the web platform' })
    }
    #items, not the list (the caller collects them with @())
    $findings
}

#why a row of the Risk page is considered risky (report column 'Why it is risky'; not in the CSV). Keys: the reason/finding
#text of the row builders. Wording for readers who are not identity specialists.
$script:spiRiskExplanations = @{
    AppConfiguration = [ordered]@{
        'redirect URI without https' = 'Sign-in results (tokens, authorization codes) are sent to this address unencrypted: anyone on the network path can read them and use them to act as the user or the app.'
        'wildcard redirect URI' = 'Every address that matches the wildcard receives the sign-in results, including subdomains or paths that someone else may control. Use exact redirect URIs.'
        'Azure-hosted domain*' = 'If the Azure resource behind this name (web app, storage account, CDN, ...) was deleted, anyone can create a resource with the same name and receive the sign-in results for this app (subdomain takeover).'
        'localhost redirect URI on the web platform' = 'Usually left over from development: the sign-in result goes to whatever listens on the user''s machine. Remove it if it is not needed.'
        'implicit grant: access tokens' = 'Access tokens are returned in the browser address (implicit flow) and can leak through browser history, logs or referrers. The implicit flow is deprecated; use the authorization code flow with PKCE.'
        'implicit grant: ID tokens' = 'ID tokens through the implicit flow are only needed by older sign-in libraries. Prefer the authorization code flow; remove it when the app does not need it.'
        'public client flows allowed' = 'The app accepts sign-in flows without its own credential (device code, username/password, integrated Windows authentication). Device code phishing and password flows weaken the sign-in protection.'
    }
    EffectivePrivilege = [ordered]@{
        'Entra role|critical' = 'A privileged Microsoft Entra role for the whole tenant: the Service Principal can change tenant settings, identities or permissions without a signed-in user.'
        'Entra role|high' = 'A privileged Microsoft Entra role limited to a scope (administrative unit or object): control over the identities or objects in that scope.'
        'App role (can escalate)|*' = 'This application permission lets the Service Principal give itself or others more rights (assign roles or app permissions, add credentials to any app, change group membership or policies), up to control over the tenant.'
        'App role|*' = 'Application.ReadWrite.OwnedBy: the Service Principal can add credentials to the apps it owns and so act with their permissions.'
        'App role (critical data access)|*' = 'An application permission classified as critical: access to data of all users or of the whole tenant (for example all mail or files) without a signed-in user.'
        'Delegated permission (admin consent for all users)|*' = 'A critical delegated permission an administrator consented to for all users: the app can use it on behalf of every user who signs in to it.'
        'Azure role|critical' = 'A critical Azure role on the tenant root, a management group or a subscription: control over all resources below it and over who has access to them.'
        'Azure role|high' = 'A critical Azure role on a resource group or resource: control over these resources and over who has access to them.'
        'Azure role (limited by condition)|*' = 'An Azure role that can assign roles, limited by a condition (on the role definition or on the role assignment) to the roles listed: the Service Principal can give itself or others these roles, but none that can assign roles (such as Owner or User Access Administrator).'
    }
    EffectivePrivilegeVia = [ordered]@{
        'owner' = ' Reached through ownership: an owner can add a credential to the owned app and sign in as it, so it effectively holds these rights.'
        'group' = ' Held through membership of a group: whoever can change the group''s members can give these rights to others.'
    }
    CredentialRisk = [ordered]@{
        'secret on a privileged identity*' = 'A secret is a password: it can be copied from scripts, pipelines or configuration files and used from anywhere, with all permissions of this identity.'
        '* active secrets' = 'Several active secrets: more places where one can leak, and it is unclear which one is still used.'
        'credential with a long lifetime' = 'A leaked long-lived credential stays usable until it expires.'
        'credential on the service principal*' = 'Not visible under Certificates & secrets of the app registration, so it is easily missed in reviews.'
    }
    CredentialRiskNone = 'No credential finding. Listed because the identity is privileged: its credentials are worth a review.'
    Cleanup = [ordered]@{
        'disabled, still holds permissions' = 'Disabled today, but whoever enables it again gets all its permissions back at once. Unused permissions should be removed.'
        'managed identity of a deleted resource, still holds permissions' = 'The resource is gone, but the identity keeps its permissions and nobody watches it. A user-assigned identity can be attached to a new resource by anyone allowed to, who then gets these permissions.'
        'privileged, no owner' = 'Nobody is responsible for this privileged app: no one reviews its permissions and credentials, and changes go unnoticed.'
        'expired credentials still present' = 'No direct risk (an expired credential cannot sign in), but clutter: it hides which credentials are in use and suggests the app is not maintained.'
        'app registration without a service principal in this tenant has active credentials' = 'The app is not used in this tenant, yet it has active credentials: a multi-tenant app can use them in other tenants, and unused credentials are a leak risk without purpose.'
    }
    ConsentSettings = [ordered]@{
        'Users can consent to apps' = 'User consent lets any user give an app access to their data. Attackers use this with consent phishing (illicit consent grant). Recommended: allow it only for verified publishers and low-impact permissions, or not at all.'
        'Group/team owners can consent to apps accessing their data' = 'Owners of groups and teams can give apps access to the group''s files, chats or mail without an administrator.'
        'Users can register applications' = 'Any user can create app registrations and becomes their owner: such apps can ask for consent and hold credentials outside of the administrators'' control.'
        'Admin consent workflow' = 'With the workflow, users request apps they cannot consent to and administrators review each request; without it, users look for workarounds.'
        'Custom permission grant policies' = 'Custom policies decide which permissions users may consent to: review what they allow.'
    }
}
#the explanation for a key: exact match first, then the wildcard patterns of the table
function getRiskExplanation([string]$Table, [string]$Key) {
    $entries = $script:spiRiskExplanations[$Table]
    if ($entries.Contains($Key)) { return $entries[$Key] }
    foreach ($pattern in $entries.Keys) {
        if ($Key -like $pattern) { return $entries[$pattern] }
    }
    return ''
}
#section notes: the reasoning is the same for every row of these sections
$script:spiRiskSectionNotes = @{
    ThirdPartyAppExposure = 'Why it is risky: these apps are published by another organization (App Owner Organization Id is not this tenant). Their code, credentials and changes are controlled by that organization: if it is compromised or acts in bad faith, it uses the permissions granted here (app roles without a signed-in user, delegated permissions on behalf of users). A publisher that is not verified has not proven its identity to Microsoft. Microsoft first-party apps are listed for completeness and are usually expected. Review whether each app is still needed and whether it needs these permissions.'
}

#'Owner OwnedBy' of an owner SP: who controls it, direct (its owners) and indirect (with the path). CSV: the former text
#'direct n [name type, ...], indirect m [...]' (the separator is $CsvDelimiterOpposite); HTML: two labelled lists.
#$SelfId: the Service Principal of the row; in an ownership cycle it controls its own owner, which the cell says
function getOwnerOwnedByCell($OwnerEntries, [string]$SelfId) {
    $direct = @(@($OwnerEntries).where({ $_ -and $_.applicability -eq 'direct' }))
    $indirect = @(@($OwnerEntries).where({ $_ -and $_.applicability -eq 'indirect' }))
    $csvParts = @()
    if ($direct.Count) { $csvParts += "direct $($direct.Count) [$(@($direct | ForEach-Object { "$($_.displayName) $($_.principalType)" }) -join ', ')]" }
    if ($indirect.Count) { $csvParts += "indirect $($indirect.Count) [$(@($indirect | ForEach-Object { "$($_.displayName) $($_.principalType)" }) -join ', ')]" }
    $item = { param($entry, [switch]$WithVia) "<li>$([HtmlSafe]::Encode($entry.displayName)) <span class=`"spiDim`">$([HtmlSafe]::Encode($entry.principalType))$(if ($WithVia -and $entry.via) { ' &middot; ' + [HtmlSafe]::Encode($entry.via) })$(if ($SelfId -and $entry.id -eq $SelfId) { ' &middot; this Service Principal (ownership cycle)' })</span></li>" }
    $html = ''
    if ($direct.Count) { $html += "<b>direct ($($direct.Count))</b><ul class=`"spiList`">$(@($direct | ForEach-Object { & $item $_ }) -join '')</ul>" }
    if ($indirect.Count) { $html += "<b>indirect ($($indirect.Count))</b><ul class=`"spiList`">$(@($indirect | ForEach-Object { & $item $_ -WithVia }) -join '')</ul>" }
    return @{ Csv = ($csvParts -join "$($CsvDelimiterOpposite) "); Html = $html }
}

#credit above the grids that build on the work of Thomas Naunheim (maintainer decision 2026-11-06): privileged Entra roles as
#Microsoft classifies them (isPrivileged) and Entra roles through role-assignable groups, adopted from the review of EntraOps
#and AzurePrivilegedIAM (theme 21). $What: what the grid takes from it (text)
function getThomasCreditHtml([string]$What) {
    return "<div class=`"az-note az-note-credit`"><i class=`"fa fa-handshake`" aria-hidden=`"true`"></i> $([HtmlSafe]::Encode($What)): based on the work of Thomas Naunheim (<a class=`"externallink`" href=`"https://github.com/Cloud-Architekt`" target=`"_blank`" rel=`"noopener`">Cloud-Architekt</a>): <a class=`"externallink`" href=`"https://github.com/Cloud-Architekt/EntraOps`" target=`"_blank`" rel=`"noopener`">EntraOps</a> and <a class=`"externallink`" href=`"https://github.com/Cloud-Architekt/AzurePrivilegedIAM`" target=`"_blank`" rel=`"noopener`">AzurePrivilegedIAM</a>.</div>"
}

#one report section (card with an AG Grid) plus its CSV; called from function summary
#$Columns: @{ Header = '...'; Property = '...'; Select = $true/$false; Number = $true (number filter and sort, review F-37); Csv = $false (report only, e.g. the explanation column) }
#$Description: a note above the grid (text)
#$CreditHtml: a credit note above the grid (html, e.g. getThomasCreditHtml)
#$Warning: a problem with the data of the section (text); shown also when there are no rows (review F-35)
function addReportSection([string]$Region, [string]$Title, [object[]]$Rows, [object[]]$Columns, [string]$NotCollectedReason, [string]$Description, [string]$CreditHtml, [string]$Warning) {
    $startSection = Get-Date
    Write-Host "  processing Summary $Region"
    $Rows = @($Rows)
    $rowCount = $Rows.Count
    $encodedTitle = [HtmlSafe]::Encode($Title)
    if ($NotCollectedReason) {
        $builder = useSpiSection -Id $Region -Kind note -NoteState disabled
        [void]$builder.AppendLine("<i class=`"fa fa-ban`" aria-hidden=`"true`"></i> <span>$encodedTitle (not collected: $([HtmlSafe]::Encode($NotCollectedReason)))</span>")
        return
    }
    if ($rowCount -eq 0) {
        $builder = useSpiSection -Id $Region -Kind note
        [void]$builder.AppendLine("<i class=`"fa fa-ban`" aria-hidden=`"true`"></i> <span>$encodedTitle$(if ($Warning) { " - $([HtmlSafe]::Encode($Warning))" })</span>")
        return
    }
    $htmlTableId = "TenantSummary_$Region"
    #the count of the section is shown by the card (registry), no longer in the title
    $builder = useSpiSection -Id $Region
    [void]$builder.AppendLine((getSpiCardHeadHtml -TitleHtml $encodedTitle))
    [void]$builder.AppendLine('<div class="az-card-body">')
    if ($CreditHtml) {
        [void]$builder.AppendLine($CreditHtml)
    }
    if ($Description) {
        [void]$builder.AppendLine("<div class=`"az-note az-note-info`"><i class=`"fa fa-circle-info`" aria-hidden=`"true`"></i> $([HtmlSafe]::Encode($Description))</div>")
    }
    if ($Warning) {
        [void]$builder.AppendLine("<div class=`"az-note az-note-warning`"><i class=`"fa fa-triangle-exclamation`" aria-hidden=`"true`"></i> $([HtmlSafe]::Encode($Warning))</div>")
    }
    $gridColumns = @(foreach ($column in $Columns) { @{ header = $column.Header; property = $column.Property; htmlProperty = $column.HtmlProperty; portal = $column.Portal; appIdProperty = $column.AppIdProperty; filter = $(if ($column.Select) { 'select' } elseif ($column.Number) { 'number' }) } })
    [void]$builder.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $Rows -PopoutTitle $Title -ColumnDefinitions $gridColumns))
    [void]$builder.AppendLine('</div>')
    if (-not $NoCsvExport) {
        $Rows | Select-Object -Property @($Columns.where({ $_.Csv -ne $false }).Property) | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_$($Region).csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
    }
    Write-Host "   processing duration: $((New-TimeSpan -Start $startSection -End (Get-Date)).TotalSeconds) seconds"
}

#columns shared by the SP based sections
function getSPColumns {
    @(
        @{ Header = 'SP object Id'; Property = 'SPObjectId'; Portal = 'servicePrincipal'; AppIdProperty = 'SPAppId' }
        @{ Header = 'SP application Id'; Property = 'SPAppId' }
        @{ Header = 'SP displayName'; Property = 'SPDisplayName' }
        @{ Header = 'SP type'; Property = 'SPType'; Select = $true }
        @{ Header = 'SP App Owner Organization Id'; Property = 'SPAppOwnerOrganizationId'; Select = $true }
    )
}
function getSPRowBase($entry) {
    [ordered]@{
        SPObjectId = @($entry.SP)[0].SPObjectId
        SPAppId = @($entry.SP)[0].SPAppId
        SPDisplayName = $(if ($entry.SP) { @($entry.SP)[0].SPDisplayName } else { @($entry.APP)[0].APPDisplayName })
        SPType = $entry.ObjectType
        SPAppOwnerOrganizationId = @($entry.SP)[0].SPAppOwnerOrganizationId
    }
}
#user-assigned managed identities: isolation scope, assignment restrictions (preview) and the resources the identity is attached to.
#Azure refuses a restriction that an existing attachment would break (verified 2026-11-06: 'Existing assignments: [...]. Please
#remove the user-assigned managed identity's existing assignment(s) ...'), so the suggestion is what the identity can be restricted to
#without changing its attachments. Attachments in subscriptions outside the scanned scope are not seen.
#$Settings: values of $htUserAssignedIdentitiesSettings; $Attachments: $arrayUserAssignedIdentities4Resources;
#$FicByMI: $htMIUserAssignedFederatedCreds (keyed by MI resource id); $Delimiter joins lists in a cell
$script:spiUamiSettingsNote = 'Assignment restrictions (preview) limit the resource types a user-assigned managed identity can be attached to; isolation scope Regional limits it to resources in its own region. Both limit where the identity can be used from, not what it can access. Unrestricted is the default. Azure refuses a setting that an existing attachment would break, so the suggestion only uses what the identity is attached to today: resources and policy assignments in the scanned scope (attachments elsewhere are not seen).'
#$PolicyAssignments: policy assignment objects ($htCacheAssignmentsPolicy.Values.Assignment): a policy assignment with a user-assigned
#identity counts as an attachment (type Microsoft.Authorization/policyAssignments); the resources listing does not return them
function getUserAssignedIdentitySettingsRows($Settings, $Attachments, $FicByMI, $PolicyAssignments, [string]$Delimiter = ', ') {
    $attachmentsAll = [System.Collections.ArrayList]@(@($Attachments) | Where-Object { $_ })
    foreach ($policyAssignment in @($PolicyAssignments)) {
        if (-not $policyAssignment.identity.userAssignedIdentities) { continue }
        foreach ($miResourceId in $policyAssignment.identity.userAssignedIdentities.psobject.Properties.Name) {
            $null = $attachmentsAll.Add([PSCustomObject]@{ miResourceId = $miResourceId; resourceType = 'Microsoft.Authorization/policyAssignments'; resourceLocation = $policyAssignment.location; resourceId = $policyAssignment.id })
        }
    }
    $attachmentsByMI = @{}
    foreach ($attachment in $attachmentsAll) {
        if (-not $attachment) { continue }
        $key = ([string]$attachment.miResourceId).ToLower()
        if (-not $attachmentsByMI[$key]) {
            $attachmentsByMI[$key] = [System.Collections.ArrayList]@()
        }
        $null = $attachmentsByMI[$key].Add($attachment)
    }
    $ficCountByMI = @{}
    if ($FicByMI) {
        foreach ($key in $FicByMI.Keys) {
            $ficCountByMI[([string]$key).ToLower()] = @($FicByMI[$key].federatedCredentials).Count
        }
    }
    foreach ($mi in @($Settings | Where-Object { $_ } | Sort-Object -Property name, id)) {
        $miKey = ([string]$mi.id).ToLower()
        $attached = $(if ($attachmentsByMI[$miKey]) { @($attachmentsByMI[$miKey]) } else { @() })
        $ficCount = [int]$ficCountByMI[$miKey]
        $miLocation = ([string]$mi.location -replace '\s', '').ToLower()
        $isolationScope = $(if ($mi.isolationScope) { [string]$mi.isolationScope } else { 'None' })
        $isRegional = $isolationScope -eq 'Regional'
        $isUnrestricted = $mi.assignmentRestrictionsCollected -and @($mi.assignmentRestrictions).Count -eq 0
        $restrictionsText = $(if (-not $mi.assignmentRestrictionsCollected) { "not collected (api-version $($mi.apiVersion))" } elseif ($isUnrestricted) { 'none (unrestricted)' } else { @($mi.assignmentRestrictions) -join $Delimiter })

        $suggestions = [System.Collections.ArrayList]@()
        if ($attached.Count -eq 0) {
            if ($isUnrestricted) {
                if ($ficCount -gt 0) {
                    #restrictions do not apply to the use through federated identity credentials; an empty list means unrestricted
                    $null = $suggestions.Add('Used through federated identity credentials only (restrictions do not apply to that use); restrict it to the resource types it may be attached to.')
                }
                else {
                    $null = $suggestions.Add('Restrict it to the resource types it is meant for before it is attached.')
                }
            }
            if (-not $isRegional) {
                $null = $suggestions.Add('Consider isolation scope Regional before it is attached.')
            }
        }
        else {
            if ($isUnrestricted) {
                $types = @($attached.resourceType | Sort-Object -Unique)
                $null = $suggestions.Add("Restrict to $($types -join $Delimiter).")
            }
            if (-not $isRegional) {
                $otherRegion = @($attached | Where-Object { ([string]$_.resourceLocation -replace '\s', '').ToLower() -ne $miLocation })
                if ($otherRegion.Count -eq 0) {
                    $null = $suggestions.Add("Set isolation scope Regional (all attached resources are in $($mi.location)).")
                }
                else {
                    $null = $suggestions.Add("Regional not possible without changes: $($otherRegion.Count) of $($attached.Count) attached resources are in other regions ($(@($otherRegion.resourceLocation | Sort-Object -Unique) -join $Delimiter)).")
                }
            }
        }

        [PSCustomObject][ordered]@{
            MIName = $mi.name
            MISubscriptionName = $mi.subscriptionName
            MISubscriptionId = $mi.subscriptionId
            MIResourceGroup = $mi.resourceGroup
            MILocation = $mi.location
            MIResourceId = $mi.id
            MIAADSPObjectId = $mi.principalId
            IsolationScope = $isolationScope
            AssignmentRestrictions = $restrictionsText
            AttachedResourcesCount = $attached.Count
            AttachedResourceTypes = @($attached | Group-Object -Property resourceType | Sort-Object -Property Name | ForEach-Object { "$($_.Name) ($($_.Count))" }) -join $Delimiter
            AttachedRegions = @($attached.resourceLocation | Where-Object { $_ } | Sort-Object -Unique) -join $Delimiter
            FederatedIdentityCredentialsCount = $ficCount
            Usage = $(if ($attached.Count -gt 0) { 'attached to resources' } elseif ($ficCount -gt 0) { 'federated identity credentials only' } else { 'not attached (scanned scope)' })
            Suggestion = $(if ($suggestions.Count -gt 0) { $suggestions -join ' ' } elseif (-not $mi.assignmentRestrictionsCollected) { '' } else { 'Restricted and Regional.' })
        }
    }
}
#effective privilege per SP: its own privileged permissions plus those of the SPs/apps it controls as owner (an owner can add
#credentials and sign in as the owned app). Levels: critical (tenant or Azure management group/subscription control), high, none.
#App roles that allow an app to escalate its own privileges (curated list; to be replaced by a maintained tier model, see review P1-24)
$script:escalationAppRoles = @{
    '00000003-0000-0000-c000-000000000000' = @('AppRoleAssignment.ReadWrite.All', 'Application.ReadWrite.All', 'RoleManagement.ReadWrite.Directory', 'RoleAssignmentSchedule.ReadWrite.Directory', 'RoleEligibilitySchedule.ReadWrite.Directory', 'RoleManagementPolicy.ReadWrite.Directory',
        'PrivilegedAccess.ReadWrite.AzureADGroup', 'PrivilegedAssignmentSchedule.ReadWrite.AzureADGroup', 'PrivilegedEligibilitySchedule.ReadWrite.AzureADGroup', 'Directory.ReadWrite.All', 'Group.ReadWrite.All', 'GroupMember.ReadWrite.All',
        'DelegatedPermissionGrant.ReadWrite.All', 'Policy.ReadWrite.ConditionalAccess', 'Policy.ReadWrite.PermissionGrant', 'Policy.ReadWrite.AuthenticationMethod', 'UserAuthenticationMethod.ReadWrite.All', 'Domain.ReadWrite.All', 'EntitlementManagement.ReadWrite.All') #Microsoft Graph
    '00000002-0000-0000-c000-000000000000' = @('Application.ReadWrite.All', 'Directory.ReadWrite.All') #Azure AD Graph
}
function getEffectivePrivileges($entries) {
    $levelRank = @{ none = 0; high = 1; critical = 2 }
    $htAppIdBySPObjectId = @{}
    $htEntryBySPObjectId = @{}
    foreach ($entry in $entries) {
        if ($entry.SP) {
            $htAppIdBySPObjectId[@($entry.SP)[0].SPObjectId] = @($entry.SP)[0].SPAppId
            $htEntryBySPObjectId[@($entry.SP)[0].SPObjectId] = $entry
        }
    }
    #own reasons
    $htOwnReasons = @{}
    foreach ($id in $htEntryBySPObjectId.Keys) {
        $entry = $htEntryBySPObjectId[$id]
        $reasons = [System.Collections.ArrayList]@()
        #review F-30: the lists are checked before the .where() scriptblocks (most SPs have none of them)
        if ($entry.SPAADRoleAssignments) {
            foreach ($ra in @($entry.SPAADRoleAssignments).where({ $_ -and $_.roleIsCritical -eq $true })) {
                #review F-7: directoryScopeId (resourceScope is deprecated in Graph) with resourceScope as fallback
                $raScope = $(if ($ra.directoryScopeId) { $ra.directoryScopeId } else { $ra.resourceScope })
                $scoped = $raScope -and $raScope -ne '/'
                $null = $reasons.Add([ordered]@{ level = $(if ($scoped) { 'high' } else { 'critical' }); category = 'Entra role'; detail = "$($ra.roleDefinitionName)$(if ($scoped) { " (scope: $(if ($ra.scopeDetail) { $ra.scopeDetail } else { $raScope }))" })"; via = $(if ($ra.roleAssignmentApplicability -eq 'indirect') { "group $($ra.roleAssignmentAppliesThrough)" } else { '' }) })
            }
        }
        if ($entry.SPAppRoleAssignments) {
            foreach ($ar in @($entry.SPAppRoleAssignments).where({ $_ })) {
                $resourceAppId = $htAppIdBySPObjectId[$ar.AppRoleAssignmentResourceId]
                #review R-8: the resource SP (e.g. Microsoft Graph) is not in $entries with -OnlyProcessSPs...: its appId from all SPs
                if (-not $resourceAppId -and $htServicePrincipalsAppRoleIdsToAppId) { $resourceAppId = $htServicePrincipalsAppRoleIdsToAppId["$($ar.AppRoleAssignmentResourceId)_$($ar.AppRoleId)"] }
                if ($resourceAppId -and $script:escalationAppRoles[$resourceAppId] -contains $ar.AppRolePermission) {
                    $null = $reasons.Add([ordered]@{ level = 'critical'; category = 'App role (can escalate)'; detail = "$($ar.AppRoleAssignmentResourceDisplayName) $($ar.AppRolePermission)"; via = '' })
                }
                elseif ($ar.AppRolePermission -eq 'Application.ReadWrite.OwnedBy') {
                    $null = $reasons.Add([ordered]@{ level = 'high'; category = 'App role'; detail = "$($ar.AppRoleAssignmentResourceDisplayName) $($ar.AppRolePermission) (controls the apps it owns)"; via = '' })
                }
                elseif ($ar.AppRolePermissionSensitivity -eq 'critical') {
                    $null = $reasons.Add([ordered]@{ level = 'high'; category = 'App role (critical data access)'; detail = "$($ar.AppRoleAssignmentResourceDisplayName) $($ar.AppRolePermission)"; via = '' })
                }
            }
        }
        #review R-1: consentType (AllPrincipals = for all users), not the scope type (who may consent)
        if ($entry.SPOauth2PermissionGrants) {
            $grantReasonKeys = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
            foreach ($grant in @($entry.SPOauth2PermissionGrants).where({ $_ -and $_.consentType -eq 'AllPrincipals' -and $_.permissionSensitivity -eq 'critical' })) {
                if (-not $grantReasonKeys.Add("$($grant.SPId)|$($grant.permission)")) { continue }
                $null = $reasons.Add([ordered]@{ level = 'high'; category = 'Delegated permission (admin consent for all users)'; detail = "$($grant.SPDisplayName) $($grant.permission)"; via = '' })
            }
        }
        if ($entry.SPAzureRoleAssignments) {
            foreach ($ra in @($entry.SPAzureRoleAssignments).where({ $_ -and $_.roleIsCritical -eq $true })) {
                $null = $reasons.Add([ordered]@{ level = $(if ($ra.roleAssignmentAssignmentScope -in 'Ten', 'MG', 'Sub') { 'critical' } else { 'high' }); category = 'Azure role'; detail = "$($ra.roleName) on $($ra.roleAssignmentAssignmentResourceType) $($ra.roleAssignmentAssignmentScopeName)$(if ($ra.roleConditionEffect) { " ($(getAzureRoleConditionText $ra))" })"; via = $(if ($ra.roleAssignmentApplicability -eq 'indirect') { "group $($ra.roleAssignmentAppliesThrough)" } else { '' }) })
            }
            #Azure role conditions: a role that can write role assignments, limited to non-critical roles - high, not critical
            foreach ($ra in @($entry.SPAzureRoleAssignments).where({ $_ -and $_.roleConditionEffect -eq 'limited' })) {
                $null = $reasons.Add([ordered]@{ level = 'high'; category = 'Azure role (limited by condition)'; detail = "$($ra.roleName) on $($ra.roleAssignmentAssignmentResourceType) $($ra.roleAssignmentAssignmentScopeName) ($(getAzureRoleConditionText $ra))"; via = $(if ($ra.roleAssignmentApplicability -eq 'indirect') { "group $($ra.roleAssignmentAppliesThrough)" } else { '' }) })
            }
        }
        $htOwnReasons[$id] = $reasons
    }
    #ownership edges: owner SP -> owned SP (owner of the SP or of its app registration)
    $htControls = @{}
    foreach ($id in $htEntryBySPObjectId.Keys) {
        $entry = $htEntryBySPObjectId[$id]
        if (-not $entry.SPOwners -and -not $entry.APPAppOwners) { continue }
        foreach ($owner in @(@($entry.SPOwners) + @($entry.APPAppOwners)).where({ $_ -and $_.applicability -eq 'direct' -and $_.principalType -like 'SP*' -and $_.id -ne $id })) {
            if (-not $htControls[$owner.id]) {
                $htControls[$owner.id] = [System.Collections.Generic.HashSet[string]]::new()
            }
            $null = $htControls[$owner.id].Add($id)
        }
    }
    $result = @{}
    foreach ($id in $htEntryBySPObjectId.Keys) {
        #review F-30: an SP that owns no SP has only its own reasons (no walk, no visited set, no queue)
        if (-not $htControls[$id]) {
            $reasons = $htOwnReasons[$id]
            $level = 'none'
            foreach ($reason in $reasons) { if ($levelRank[$reason.level] -gt $levelRank[$level]) { $level = $reason.level } }
            $result[$id] = [ordered]@{ level = $level; reasons = @($reasons) }
            continue
        }
        $reasons = [System.Collections.ArrayList]@($htOwnReasons[$id])
        #breadth-first over the owned SPs (visited set: terminates on cycles)
        $visited = [System.Collections.Generic.HashSet[string]]::new(); $null = $visited.Add($id)
        $queue = [System.Collections.Generic.Queue[object]]::new()
        foreach ($ownedId in @($htControls[$id])) { if ($ownedId) { $queue.Enqueue(@($ownedId, "owner of '$(@($htEntryBySPObjectId[$ownedId].SP)[0].SPDisplayName)'")) } }
        while ($queue.Count -gt 0) {
            $item = $queue.Dequeue(); $ownedId = $item[0]; $path = $item[1]
            if (-not $visited.Add($ownedId)) { continue }
            foreach ($ownedReason in $htOwnReasons[$ownedId]) {
                $null = $reasons.Add([ordered]@{ level = $ownedReason.level; category = $ownedReason.category; detail = $ownedReason.detail; via = (@($path, $ownedReason.via).where({ $_ }) -join ' > ') })
            }
            foreach ($nextId in @($htControls[$ownedId])) { if ($nextId) { $queue.Enqueue(@($nextId, "$path > owner of '$(@($htEntryBySPObjectId[$nextId].SP)[0].SPDisplayName)'")) } }
        }
        $level = 'none'
        foreach ($reason in $reasons) { if ($levelRank[$reason.level] -gt $levelRank[$level]) { $level = $reason.level } }
        $result[$id] = [ordered]@{ level = $level; reasons = @($reasons) }
    }
    return $result
}

#credential risk of privileged SPs: which credential types they use, and what to improve
function getCredentialRiskRows($entries) {
    foreach ($entry in @($entries).where({ $_.SPEffectivePrivilege.level -in 'critical', 'high' })) {
        $isMI = $entry.ObjectType -like 'SP MI*'
        $isSaml = @($entry.SP)[0].SPPreferredSingleSignOnMode -eq 'saml'
        $active = { param($list) @(@($list).where({ $_ -and $_.expiryInfo -ne 'expired' })) }
        $appSecrets = @(& $active $entry.APPPasswordCredentials)
        $appCerts = @(& $active $entry.APPKeyCredentials)
        $spSecrets = @(if ($isMI -or $isSaml) { } else { & $active $entry.SPPasswordCredentials })
        $spCerts = @(if ($isMI -or $isSaml) { } else { & $active $entry.SPKeyCredentials })
        $fics = @(@(@($entry.APPFederatedIdentityCredentials) + @($entry.ManagedIdentityFederatedIdentityCredentials)).where({ $_ }))
        $credentials = if ($isMI) { 'managed identity' } else { "app secrets $($appSecrets.Count), app certificates $($appCerts.Count), SP secrets $($spSecrets.Count), SP certificates $($spCerts.Count)" }
        if ($fics.Count -gt 0) { $credentials += ", federated credentials $($fics.Count)" }
        $findings = [System.Collections.ArrayList]@()
        $secrets = $appSecrets.Count + $spSecrets.Count
        if ($secrets -gt 0) { $null = $findings.Add('secret on a privileged identity (prefer a certificate, federated credential or managed identity)') }
        if ($secrets -gt 1) { $null = $findings.Add("$secrets active secrets") }
        #review R-13: the lifetime (end - start), not the time left until expiry; without a start date (older output): the time left
        $secretMaxDays = $(if ($ApplicationSecretExpiryMax) { $ApplicationSecretExpiryMax } else { 730 })
        $certificateMaxDays = $(if ($ApplicationCertificateExpiryMax) { $ApplicationCertificateExpiryMax } else { 730 })
        $isLongLived = {
            param($credential, [int]$maxDays)
            if ($credential.startDateTime -and $credential.endDateTime) {
                return (([datetime]$credential.endDateTime) - ([datetime]$credential.startDateTime)).TotalDays -gt $maxDays
            }
            return $credential.expiryInfo -like 'expires in more than*'
        }
        if (@(@($appSecrets + $spSecrets).where({ & $isLongLived $_ $secretMaxDays })).Count + @(@($appCerts + $spCerts).where({ & $isLongLived $_ $certificateMaxDays })).Count -gt 0) { $null = $findings.Add('credential with a long lifetime') }
        if ($spSecrets.Count + $spCerts.Count -gt 0) { $null = $findings.Add('credential on the service principal (not visible in the app registration)') }
        $row = getSPRowBase $entry
        $row.Level = $entry.SPEffectivePrivilege.level
        $row.Credentials = $credentials
        $row.Findings = $findings -join '; '
        $row.Why = $(if ($findings.Count) { @($findings | ForEach-Object { getRiskExplanation -Table 'CredentialRisk' -Key $_ }).where({ $_ }) -join ' ' } else { $script:spiRiskExplanations.CredentialRiskNone })
        [PSCustomObject]$row
    }
}

#multi-tenant apps of other organizations with sensitive permissions in this tenant
function getThirdPartyExposureRows($entries, [string]$tenantId) {
    $microsoftTenants = @('f8cdef31-a31e-4b4a-93e4-5f571e91255a', '72f988bf-86f1-41af-91ab-2d7cd011db47')
    foreach ($entry in @($entries).where({ $_.SP -and @($_.SP)[0].SPAppOwnerOrganizationId -and @($_.SP)[0].SPAppOwnerOrganizationId -ne $tenantId })) {
        #review P-11: the pipelines only for non-empty lists (most apps of other organizations have none of these)
        $appRoleRows = @(@($entry.SPAppRoleAssignments).where({ $_ -and $_.AppRolePermissionSensitivity -in 'critical', 'medium' }))
        #review R-1: tenant-wide (consentType AllPrincipals) vs for single users (Principal); was: the type of the published scope
        $adminConsentRows = @(@($entry.SPOauth2PermissionGrants).where({ $_ -and $_.consentType -eq 'AllPrincipals' -and $_.permissionSensitivity -in 'critical', 'medium' }))
        $userConsent = @(@($entry.SPOauth2PermissionGrants).where({ $_ -and $_.consentType -eq 'Principal' -and $_.permissionSensitivity -eq 'critical' }))
        if ($appRoleRows.Count + $adminConsentRows.Count + $userConsent.Count -eq 0) { continue }
        $appRoles = @(if ($appRoleRows.Count) { $appRoleRows | ForEach-Object { "$($_.AppRolePermission) ($($_.AppRolePermissionSensitivity))" } | Sort-Object -Unique })
        $adminConsent = @(if ($adminConsentRows.Count) { $adminConsentRows | ForEach-Object { "$($_.permission) ($($_.permissionSensitivity))" } | Sort-Object -Unique })
        $userConsentUserCount = @($userConsent.principalId | Where-Object { $_ } | Sort-Object -Unique).Count
        $sp = @($entry.SP)[0]
        $row = getSPRowBase $entry
        $row.Publisher = $(if ($sp.SPVerifiedPublisher.displayName) { "verified: $($sp.SPVerifiedPublisher.displayName)" } else { 'not verified' })
        $row.MicrosoftFirstParty = $(if ($sp.SPAppOwnerOrganizationId -in $microsoftTenants) { 'yes' } else { 'no' })
        $row.AppRoles = $appRoles -join ', '
        $row.DelegatedAdminConsent = $adminConsent -join ', '
        $row.DelegatedUserConsent = $(if ($userConsent.Count) { "$((@($userConsent.permission) | Sort-Object -Unique) -join ', ') ($($userConsentUserCount) $(if ($userConsentUserCount -eq 1) { 'user' } else { 'users' }))" } else { '' })
        $row.Level = $entry.SPEffectivePrivilege.level
        [PSCustomObject]$row
    }
}

#objects to review for removal
function getCleanupCandidateRows($entries) {
    foreach ($entry in $entries) {
        $isMI = $entry.ObjectType -like 'SP MI*'
        $reasons = [System.Collections.ArrayList]@()
        $permissionCount = @($entry.SPAppRoleAssignments).where({ $_ }).Count + @($entry.SPAADRoleAssignments).where({ $_ }).Count + @($entry.SPAzureRoleAssignments).where({ $_ }).Count + @($entry.SPOauth2PermissionGrants).where({ $_ }).Count
        if ($entry.SP -and @($entry.SP)[0].SPAccountEnabled -eq $false -and $permissionCount -gt 0) {
            $null = $reasons.Add(@('disabled, still holds permissions', "$permissionCount app role assignments / Entra roles / Azure roles / delegated grants"))
        }
        if ($isMI -and @($entry.ManagedIdentity)[0].relict -eq $true -and $permissionCount -gt 0) {
            $null = $reasons.Add(@('managed identity of a deleted resource, still holds permissions', "$permissionCount app role assignments / Entra roles / Azure roles / delegated grants"))
        }
        if ($entry.ObjectType -in 'SP APP INT', 'AppOnly' -and $entry.SPEffectivePrivilege.level -in 'critical', 'high' -and @(@($entry.SPOwners) + @($entry.APPAppOwners)).where({ $_ }).Count -eq 0) {
            $null = $reasons.Add(@('privileged, no owner', "effective privilege $($entry.SPEffectivePrivilege.level)"))
        }
        $expired = [ordered]@{
            'app secrets' = @($entry.APPPasswordCredentials).where({ $_ -and $_.expiryInfo -eq 'expired' }).Count
            'app certificates' = @($entry.APPKeyCredentials).where({ $_ -and $_.expiryInfo -eq 'expired' }).Count
            'SP secrets' = $(if ($isMI) { 0 } else { @($entry.SPPasswordCredentials).where({ $_ -and $_.expiryInfo -eq 'expired' }).Count })
            'SP certificates' = $(if ($isMI) { 0 } else { @($entry.SPKeyCredentials).where({ $_ -and $_.expiryInfo -eq 'expired' }).Count })
        }
        $expiredText = @($expired.Keys.where({ $expired[$_] -gt 0 }) | ForEach-Object { "$($expired[$_]) $_" }) -join ', '
        if ($expiredText) {
            $null = $reasons.Add(@('expired credentials still present', $expiredText))
        }
        if ($entry.ObjectType -eq 'AppOnly' -and @(@($entry.APPPasswordCredentials) + @($entry.APPKeyCredentials)).where({ $_ -and $_.expiryInfo -ne 'expired' }).Count -gt 0) {
            $null = $reasons.Add(@('app registration without a service principal in this tenant has active credentials', 'used in other tenants, or not used at all?'))
        }
        foreach ($reason in $reasons) {
            $row = getSPRowBase $entry
            $row.Reason = $reason[0]
            $row.Detail = $reason[1]
            $row.Why = getRiskExplanation -Table 'Cleanup' -Key $reason[0]
            [PSCustomObject]$row
        }
    }
}
#rows of the insight sections (computed once; the overview at the top of the report counts them)
function getSPCredentialRows($entries) {
    #managed identities: certificates managed by Azure, not listed
    foreach ($sp in @($entries).where( { $_.ObjectType -notlike 'SP MI*' -and ($_.SPPasswordCredentials.Count -gt 0 -or $_.SPKeyCredentials.Count -gt 0) } )) {
        $isSaml = @($sp.SP)[0].SPPreferredSingleSignOnMode -eq 'saml'
        foreach ($credential in @(@($sp.SPPasswordCredentials) | ForEach-Object { [ordered]@{ kind = 'secret'; c = $_ } }) + @(@($sp.SPKeyCredentials) | ForEach-Object { [ordered]@{ kind = "certificate ($($_.usage))"; c = $_ } })) {
            $row = getSPRowBase $sp
            $row.CredentialType = $credential.kind
            $row.KeyId = $credential.c.keyId
            $row.DisplayName = $credential.c.displayName
            $row.ExpiryInfo = $credential.c.expiryInfo
            $row.EndDateTime = $credential.c.endDateTimeFormated
            $row.Note = $(if ($isSaml) { 'SAML single sign-on signing credential (expected)' } else { 'not visible in the app registration' })
            [PSCustomObject]$row
        }
    }
}
function getAppConfigurationFindingRows($entries) {
    foreach ($app in @($entries).where( { $_.APP } )) {
        $appObject = @($app.APP)[0]
        $findings = @(foreach ($redirectUri in @($appObject.APPRedirectUris)) {
                foreach ($finding in @($redirectUri.findings)) {
                    [ordered]@{ severity = $finding.severity; finding = $finding.finding; detail = "$($redirectUri.platform): $($redirectUri.uri)" }
                }
            })
        if ($appObject.APPImplicitGrantAccessTokenIssuance) {
            $findings += [ordered]@{ severity = 'high'; finding = 'implicit grant: access tokens'; detail = 'web.implicitGrantSettings.enableAccessTokenIssuance' }
        }
        if ($appObject.APPImplicitGrantIdTokenIssuance) {
            $findings += [ordered]@{ severity = 'info'; finding = 'implicit grant: ID tokens'; detail = 'web.implicitGrantSettings.enableIdTokenIssuance' }
        }
        if ($appObject.APPIsFallbackPublicClient) {
            $findings += [ordered]@{ severity = 'medium'; finding = 'public client flows allowed'; detail = 'isFallbackPublicClient' }
        }
        foreach ($finding in $findings) {
            $row = getSPRowBase $app
            $row.APPObjectId = $appObject.APPObjectId
            $row.APPDisplayName = $appObject.APPDisplayName
            $row.Severity = $finding.severity
            $row.Finding = $finding.finding
            $row.Detail = $finding.detail
            $row.Why = getRiskExplanation -Table 'AppConfiguration' -Key $finding.finding
            [PSCustomObject]$row
        }
    }
}
function getFederatedCredentialTrustRows($entries) {
    foreach ($sp in @($entries).where( { $_.APPFederatedIdentityCredentials.Count -gt 0 -or $_.ManagedIdentityFederatedIdentityCredentials.Count -gt 0 } )) {
        foreach ($fic in @(@($sp.APPFederatedIdentityCredentials).where({ $_ }) | ForEach-Object { [ordered]@{ name = $_.name; issuer = $_.issuer; subject = $_.subject; issuerType = $_.issuerType; trust = $_.trust; finding = $_.trustFinding; on = 'app registration' } }) +
            @(@($sp.ManagedIdentityFederatedIdentityCredentials).where({ $_ }) | ForEach-Object { [ordered]@{ name = $_.name; issuer = $_.properties.issuer; subject = $_.properties.subject; issuerType = $_.issuerType; trust = $_.trust; finding = $_.trustFinding; on = 'managed identity' } })) {
            $row = getSPRowBase $sp
            $row.CredentialOn = $fic.on
            $row.Name = $fic.name
            $row.IssuerType = $fic.issuerType
            $row.Trust = $fic.trust
            $row.Issuer = $fic.issuer
            $row.Subject = $fic.subject
            $row.Finding = $fic.finding
            [PSCustomObject]$row
        }
    }
}
function getEffectivePrivilegeRows($entries) {
    #one row per reason
    foreach ($sp in @($entries).where( { $_.SPEffectivePrivilege.level -in 'critical', 'high' } )) {
        foreach ($reason in $sp.SPEffectivePrivilege.reasons) {
            $row = getSPRowBase $sp
            $row.EffectiveLevel = $sp.SPEffectivePrivilege.level
            $row.Level = $reason.level
            $row.Category = $reason.category
            $row.Detail = $reason.detail
            $row.Via = $reason.via
            $row.Why = (getRiskExplanation -Table 'EffectivePrivilege' -Key "$($reason.category)|$($reason.level)") + $(if ($reason.via -like 'owner of*') { $script:spiRiskExplanations.EffectivePrivilegeVia.owner } elseif ($reason.via -like 'group *') { $script:spiRiskExplanations.EffectivePrivilegeVia.group } else { '' })
            [PSCustomObject]$row
        }
    }
}
function getAgentObjectRows($entries, $htAgentObjects) {
    $htEntryById = @{}
    foreach ($entry in $entries) {
        if ($entry.SP) { $htEntryById[@($entry.SP)[0].SPObjectId] = $entry }
        if ($entry.APP) { $htEntryById[@($entry.APP)[0].APPObjectId] = $entry }
    }
    foreach ($id in @($htAgentObjects.Keys)) {
        $entry = $htEntryById[$id]
        [PSCustomObject][ordered]@{
            AgentType = $htAgentObjects[$id].agentType
            ObjectId = $id
            DisplayName = $htAgentObjects[$id].displayName
            InThisReport = $(if ($entry) { "yes ($($entry.ObjectType))" } elseif ($htAgentObjects[$id].agentType -eq 'AgentUser') { 'no (user object)' } else { 'no' })
            EffectiveLevel = $(if ($entry.SPEffectivePrivilege) { $entry.SPEffectivePrivilege.level } else { '' })
        }
    }
}
function getInsightRows($entries, [string]$tenantId, $htAgentObjects) {
    $levelOrder = @('critical', 'high')
    [ordered]@{
        SPCredentials = @(getSPCredentialRows -entries $entries | Sort-Object -Property SPDisplayName, SPObjectId, CredentialType, KeyId)
        AppConfiguration = @(getAppConfigurationFindingRows -entries $entries | Sort-Object -Property @{ Expression = { @('high', 'medium', 'info').IndexOf($_.Severity) } }, APPDisplayName, APPObjectId, Finding, Detail)
        FederatedCredentialTrust = @(getFederatedCredentialTrustRows -entries $entries | Sort-Object -Property IssuerType, SPDisplayName, SPObjectId, Name, Subject)
        EffectivePrivilege = @(getEffectivePrivilegeRows -entries $entries | Sort-Object -Property @{ Expression = { $levelOrder.IndexOf($_.EffectiveLevel) } }, SPDisplayName, SPObjectId, @{ Expression = { $levelOrder.IndexOf($_.Level) } }, Category, Detail, Via)
        CredentialRisk = @(getCredentialRiskRows -entries $entries | Sort-Object -Property @{ Expression = { $levelOrder.IndexOf($_.Level) } }, @{ Expression = { -not $_.Findings } }, SPDisplayName, SPObjectId)
        ThirdParty = @(getThirdPartyExposureRows -entries $entries -tenantId $tenantId | Sort-Object -Property MicrosoftFirstParty, Publisher, SPDisplayName, SPObjectId)
        Cleanup = @(getCleanupCandidateRows -entries $entries | Sort-Object -Property Reason, SPDisplayName, SPObjectId, Detail)
        AgentObjects = @(if ($htAgentObjects) { getAgentObjectRows -entries $entries -htAgentObjects $htAgentObjects | Sort-Object -Property AgentType, DisplayName })
    }
}

#findings overview at the top of the report: counts, linking to the sections
function getFindingsOverviewRows($insightRows, $consentSettings, $htEntraRecommendations, $htAgentObjects, $changeHistoryRows, [int]$changeHistoryDays, $changeHistoryRunRecord, [string]$changeHistoryIssue, [bool]$changeHistoryNotRead) {
    $distinct = { param($rows) @(@($rows).SPObjectId | Sort-Object -Unique).Count }
    $spCredentialsNotSaml = @($insightRows.SPCredentials.where({ $_.Note -notlike 'SAML*' }))
    $userConsent = @($consentSettings).where({ $_.Setting -eq 'Users can consent to apps' })
    @(
        [ordered]@{ finding = 'Service Principals with critical effective privilege'; count = (& $distinct @($insightRows.EffectivePrivilege.where({ $_.EffectiveLevel -eq 'critical' }))); detail = 'tenant control: privileged Entra roles, app roles that allow escalation, Azure roles on management groups/subscriptions, or ownership of such'; section = 'ServicePrincipalEffectivePrivilege' }
        [ordered]@{ finding = 'Service Principals with high effective privilege'; count = (& $distinct @($insightRows.EffectivePrivilege.where({ $_.EffectiveLevel -eq 'high' }))); detail = 'scoped roles, critical data access, Azure roles below subscription'; section = 'ServicePrincipalEffectivePrivilege' }
        [ordered]@{ finding = 'Privileged Service Principals with an active secret'; count = @($insightRows.CredentialRisk.where({ $_.Findings -like '*secret on a privileged identity*' })).Count; detail = 'prefer a certificate, federated credential or managed identity'; section = 'ServicePrincipalCredentialRisk' }
        [ordered]@{ finding = 'Credentials on the service principal (not on the app registration)'; count = $spCredentialsNotSaml.Count; detail = "$(@($spCredentialsNotSaml.where({ $_.ExpiryInfo -ne 'expired' })).Count) active; not visible in the app registration"; section = 'ServicePrincipalCredentials' }
        [ordered]@{ finding = 'Multi-tenant apps of other organizations with sensitive permissions'; count = $insightRows.ThirdParty.Count; detail = "$(@($insightRows.ThirdParty.where({ $_.Publisher -eq 'not verified' })).Count) not verified, $(@($insightRows.ThirdParty.where({ $_.MicrosoftFirstParty -eq 'no' })).Count) not Microsoft"; section = 'ThirdPartyAppExposure' }
        [ordered]@{ finding = 'Application configuration findings (high, medium)'; count = @($insightRows.AppConfiguration.where({ $_.Severity -in 'high', 'medium' })).Count; detail = "$(@($insightRows.AppConfiguration.where({ $_.Severity -eq 'info' })).Count) info"; section = 'ApplicationConfigurationFindings' }
        [ordered]@{ finding = 'Federated identity credentials with a finding'; count = @($insightRows.FederatedCredentialTrust.where({ $_.Finding })).Count; detail = "$($insightRows.FederatedCredentialTrust.Count) federated identity credentials; $(@($insightRows.FederatedCredentialTrust.where({ $_.Finding -like 'deprecated Azure DevOps issuer*' })).Count) with the deprecated Azure DevOps issuer (retirement 2027-07-01; $(@($insightRows.FederatedCredentialTrust.where({ $_.Finding -like 'deprecated Azure DevOps issuer, converted*' })).Count) already converted)"; section = 'FederatedIdentityCredentialTrust' }
        [ordered]@{ finding = 'Cleanup candidates'; count = $insightRows.Cleanup.Count; detail = (@($insightRows.Cleanup | Group-Object -Property Reason | ForEach-Object { "$($_.Count) $($_.Name)" }) -join '; '); section = 'CleanupCandidates' }
        [ordered]@{ finding = 'Service Principals with an active Entra recommendation'; count = $(if ($null -eq $htEntraRecommendations) { 'not collected' } else { $htEntraRecommendations.Count }); detail = 'optional permission DirectoryRecommendations.Read.All'; section = 'ServicePrincipalsEntraRecommendations' }
        [ordered]@{ finding = 'Agent objects (Microsoft Entra Agent ID)'; count = $(if ($null -eq $htAgentObjects) { 'not collected' } else { $htAgentObjects.Count }); detail = 'agent identities, blueprints, agent users'; section = 'AgentObjects' }
        [ordered]@{ finding = 'Users can consent to apps'; count = $(if ($userConsent) { $userConsent[0].Value } else { 'not collected' }); detail = $(if ($userConsent) { $userConsent[0].Note } else { 'optional permission Policy.Read.All' }); section = 'TenantConsentSettings' }
        $(if ($null -eq $changeHistoryRows) {
                [ordered]@{ finding = 'Changes since previous runs'; count = 'not collected'; detail = 'parameter NoChangeHistory'; section = 'ChangeHistory' }
            }
            else {
                $objectChanges = @($changeHistoryRows.where({ $_.Category -ne 'run' }))
                #review F-35: a history that could not be read is not 'no changes'
                [ordered]@{ finding = "Changes in the last $changeHistoryDays days"; count = $(if ($changeHistoryNotRead) { 'not read' } else { $objectChanges.Count }); state = $(if ($changeHistoryIssue) { 'warning' } else { 'info' }); detail = "$(if ($changeHistoryNotRead) { $changeHistoryIssue } else { "$(@($objectChanges.where({ $_.Category -eq 'security' })).Count) security, $(@($objectChanges.where({ $_.Category -eq 'configuration' })).Count) configuration$(if ($changeHistoryRunRecord) { "; this run: $($changeHistoryRunRecord.change)" })$(if ($changeHistoryIssue) { "; $($changeHistoryIssue)" })" })"; section = 'ChangeHistory' }
            })
    )
}
#$collectionFailures: the items that failed during the collection ($htParallelFailures values: Phase, Item, Error); review R-4:
#such a run continues within limits, its output is incomplete - shown on the overview, not only in the console
#$outOfScopeSubscriptions (review F-18): subscriptions that were not collected (state, AAD_ quota, quota whitelist)
function addFindingsOverview($overviewRows, $keyFigures, $collectionFailures, $outOfScopeSubscriptions) {
    $builder = useSpiPage -Id 'Overview'
    $collectionFailures = @(@($collectionFailures).where({ $_ }))
    if ($collectionFailures.Count -gt 0) {
        $byPhase = @($collectionFailures | Group-Object -Property Phase | Sort-Object -Property Name | ForEach-Object { "$($_.Name): $($_.Count)" }) -join '; '
        [void]$builder.AppendLine("<div class=`"az-note az-note-warning`" id=`"spiCollectionFailures`"><i class=`"fa fa-triangle-exclamation`" aria-hidden=`"true`"></i> <b>Incomplete data:</b> $($collectionFailures.Count) item(s) failed during the collection and were skipped ($([HtmlSafe]::Encode($byPhase))). The affected objects are missing or partial in this report and in the CSV/JSON output; the next run collects them again.")
        [void]$builder.AppendLine('<details><summary>Failed items</summary><ul>')
        foreach ($failure in @($collectionFailures | Sort-Object -Property Phase, Item)) {
            [void]$builder.AppendLine("<li>$([HtmlSafe]::Encode($failure.Phase)): $([HtmlSafe]::Encode($failure.Item)) - $([HtmlSafe]::Encode($failure.Error))</li>")
        }
        [void]$builder.AppendLine('</ul></details></div>')
    }
    $outOfScopeSubscriptions = @(@($outOfScopeSubscriptions).where({ $_ }))
    if ($outOfScopeSubscriptions.Count -gt 0) {
        $byReason = @($outOfScopeSubscriptions | Group-Object -Property { $_.outOfScopeReason -replace ':.*$' } | Sort-Object -Property Name | ForEach-Object { "$($_.Name): $($_.Count)" }) -join '; '
        [void]$builder.AppendLine("<div class=`"az-note az-note-info`" id=`"spiOutOfScopeSubscriptions`"><i class=`"fa fa-circle-info`" aria-hidden=`"true`"></i> <b>Not collected:</b> $($outOfScopeSubscriptions.Count) subscription(s) are out of scope ($([HtmlSafe]::Encode($byReason))) - their Azure role assignments, resources and managed identity attachments are not in this report.")
        [void]$builder.AppendLine('<details><summary>Subscriptions</summary><ul>')
        foreach ($outOfScopeSubscription in @($outOfScopeSubscriptions | Sort-Object -Property subscriptionName, subscriptionId)) {
            [void]$builder.AppendLine("<li>$([HtmlSafe]::Encode($outOfScopeSubscription.subscriptionName)) ($([HtmlSafe]::Encode($outOfScopeSubscription.subscriptionId))) - $([HtmlSafe]::Encode($outOfScopeSubscription.outOfScopeReason))</li>")
        }
        [void]$builder.AppendLine('</ul></details></div>')
    }
    #key figures of the tenant and the run
    [void]$builder.AppendLine('<div class="az-essentials"><dl>')
    foreach ($keyFigure in $keyFigures) {
        [void]$builder.AppendLine("<dt>$([HtmlSafe]::Encode($keyFigure.name))</dt><dd>$(if ($keyFigure.html) { $keyFigure.html } else { [HtmlSafe]::Encode($keyFigure.value) })</dd>")
    }
    [void]$builder.AppendLine('</dl></div>')
    #one tile per finding, linking to its section
    [void]$builder.AppendLine('<div class="az-section-title">Findings</div>')
    [void]$builder.AppendLine('<div class="az-kpis">')
    foreach ($row in $overviewRows) {
        $isNumber = $row.count -is [int] -or $row.count -is [long]
        $state = $(if ($row.state) { $row.state } elseif ($isNumber) { if ($row.count -gt 0) { 'warning' } else { 'ok' } } elseif ($row.count -eq 'not collected') { 'disabled' } else { 'info' })
        [void]$builder.AppendLine("<a class=`"az-kpi-tile az-kpi-$state`" href=`"#$($row.section)`" title=`"$([HtmlSafe]::Encode($row.detail))`"><span class=`"az-kpi-value$(if (-not $isNumber) { ' az-kpi-text' })`">$([HtmlSafe]::Encode($row.count))</span><span class=`"az-kpi-label`">$([HtmlSafe]::Encode($row.finding))</span></a>")
    }
    [void]$builder.AppendLine('</div>')
    $builder = useSpiSection -Id 'FindingsOverview' -Open
    [void]$builder.AppendLine((getSpiCardHeadHtml -TitleHtml 'Findings overview' -Open))
    [void]$builder.AppendLine(@'
    <div class="az-card-body">
    <table class="summaryTable az-table" id="TenantSummary_FindingsOverview">
    <thead><tr><th>Finding</th><th>Count</th><th>Detail</th></tr></thead>
    <tbody>
'@)
    foreach ($row in $overviewRows) {
        [void]$builder.AppendLine("    <tr><td><a href=`"#$($row.section)`">$([HtmlSafe]::Encode($row.finding))</a></td><td class=`"az-num`">$([HtmlSafe]::Encode($row.count))</td><td class=`"breakwordall`">$([HtmlSafe]::Encode($row.detail))</td></tr>")
    }
    [void]$builder.AppendLine(@'
    </tbody>
    </table>
    </div>
'@)
}
#endregion insight helpers

#region change history (feature A7)
#Every run compares its state with the state of the previous run (same output folder and management group scope) and appends the
#changes to <Product>_<scope>_ChangeHistory.jsonl (one JSON line per change, kept without time limit); the state is
#<Product>_<scope>_ChangeHistoryState.json. Items are compared by their IDs, never by derived labels (sensitivity, classification),
#which can change with a new script version. Not recorded (noise): Microsoft first-party apps appearing/disappearing, certificates of
#managed identities (rotated by Azure), Azure roles that are not critical, indirect owners/roles, expiry status.
#format 2 (review R-1): delegated permissions keyed by consent (all users / single users), resource and scope
#format 3 (Azure role conditions): roles limited to non-critical roles by a condition are not critical any more; the conditions of
#roles that can assign roles are compared (a new baseline instead of 'critical Azure role removed' for each limited role)
$script:changeHistoryFormatVersion = 3
$script:changeHistoryMicrosoftTenants = @('f8cdef31-a31e-4b4a-93e4-5f571e91255a', '72f988bf-86f1-41af-91ab-2d7cd011db47')

#the comparable state of one output object (an entry of $cu, same shape as its JSON file)
function getChangeHistoryObjectState($entry) {
    $sp = @($entry.SP)[0]
    $app = @($entry.APP)[0]
    $isManagedIdentity = $entry.ObjectType -like 'SP MI*'
    $state = [ordered]@{
        type = [string]$entry.ObjectType
        spName = [string]$sp.SPDisplayName
        appName = [string]$app.APPDisplayName
        microsoftFirstParty = [bool]($sp -and $sp.SPAppOwnerOrganizationId -in $script:changeHistoryMicrosoftTenants)
        level = [string]$entry.SPEffectivePrivilege.level
        enabled = $(if ($sp) { [string]$sp.SPAccountEnabled } else { '' })
        signInAudience = [string]$app.APPSignInAudience
        entraRoles = [ordered]@{}
        azureRoles = [ordered]@{}
        azureRoleConditions = [ordered]@{}
        appPermissions = [ordered]@{}
        delegatedPermissions = [ordered]@{}
        credentials = [ordered]@{}
        federatedCredentials = [ordered]@{}
        owners = [ordered]@{}
        groups = [ordered]@{}
        redirectUris = [ordered]@{}
        identifierUris = [ordered]@{}
    }
    foreach ($item in @($entry.SPAADRoleAssignments).where({ $_ })) {
        $state.entraRoles["$($item.id)|$($item.roleAssignmentApplicability)"] = "$($item.roleDefinitionName)$(if ($item.roleIsPrivileged) { ' (privileged)' })$(if ($item.roleAssignmentApplicability -and $item.roleAssignmentApplicability -ne 'direct') { ", $($item.roleAssignmentApplicability)" })$(if ($item.directoryScopeId -and $item.directoryScopeId -ne '/') { ", scope $($item.directoryScopeId)" })"
    }
    foreach ($item in @($entry.SPAzureRoleAssignments).where({ $_ -and $_.roleIsCritical })) {
        #review R-24: the scope itself (type and name), not only its level ('on MG')
        $state.azureRoles["$($item.roleAssignmentId)|$($item.roleAssignmentApplicability)"] = "$($item.roleName) on $(@($item.roleAssignmentAssignmentResourceType, $item.roleAssignmentAssignmentScopeName).where({ $_ }) -join ' ')$(if ($item.roleAssignmentApplicability -and $item.roleAssignmentApplicability -ne 'direct') { " ($($item.roleAssignmentApplicability))" })"
    }
    #Azure role conditions: the effect of the condition of a role that can assign roles (a delegation narrowed or widened)
    foreach ($item in @($entry.SPAzureRoleAssignments).where({ $_ -and $_.roleConditionEffect })) {
        $state.azureRoleConditions["$($item.roleAssignmentId)|$($item.roleAssignmentApplicability)"] = "$($item.roleName) on $(@($item.roleAssignmentAssignmentResourceType, $item.roleAssignmentAssignmentScopeName).where({ $_ }) -join ' '): $(getAzureRoleConditionText $item)"
    }
    foreach ($item in @($entry.SPAppRoleAssignments).where({ $_ })) {
        $state.appPermissions[[string]$item.AppRoleAssignmentId] = "$($item.AppRolePermission) ($($item.AppRoleAssignmentResourceDisplayName))$(if ($item.AppRolePermissionSensitivity) { ", $($item.AppRolePermissionSensitivity)" })"
    }
    #review R-1: one item per consent kind (all users / single users), resource and scope; single-user grants are counted, not
    #recorded per user (a new user consent is not a change, a first one or a switch to all users is)
    #(review P-11: first item per key without Group-Object; the [ordered] keys are case-insensitive like Group-Object)
    foreach ($item in @($entry.SPOauth2PermissionGrants)) {
        if (-not $item) { continue }
        $grantKey = "$(if ($item.consentType -eq 'AllPrincipals') { 'all' } else { 'user' })|$($item.SPId)|$($item.scope)"
        if ($state.delegatedPermissions.Contains($grantKey)) { continue }
        $consent = $(if ($item.consentType -eq 'AllPrincipals') { 'all users' } else { 'single users' })
        $state.delegatedPermissions[$grantKey] = "$($item.permission) ($($item.SPDisplayName), $($consent))$(if ($item.permissionSensitivity) { ", $($item.permissionSensitivity)" })"
    }
    #credentials of managed identities are rotated by Azure: not recorded
    if (-not $isManagedIdentity) {
        foreach ($source in @(
                @{ items = $entry.APPPasswordCredentials; prefix = 'APP'; kind = 'application secret' }
                @{ items = $entry.APPKeyCredentials; prefix = 'APP'; kind = 'application certificate' }
                @{ items = $entry.SPPasswordCredentials; prefix = 'SP'; kind = 'service principal secret' }
                @{ items = $entry.SPKeyCredentials; prefix = 'SP'; kind = 'service principal certificate' }
            )) {
            foreach ($item in @($source.items).where({ $_ -and $_.keyId })) {
                $state.credentials["$($source.prefix):$($item.keyId)"] = "$($source.kind)$(if ($item.displayName) { " '$($item.displayName)'" }), ends $($item.endDateTimeFormated)"
            }
        }
    }
    foreach ($item in @($entry.APPFederatedIdentityCredentials).where({ $_ })) {
        $state.federatedCredentials["APP:$($item.id)"] = "$($item.name): issuer $($item.issuer), subject $($item.subject)$(if ($item.claimsMatchingExpression.value) { ", claims expression $($item.claimsMatchingExpression.value)" })"
    }
    foreach ($item in @($entry.ManagedIdentityFederatedIdentityCredentials).where({ $_ })) {
        $state.federatedCredentials["MI:$($item.id)"] = "$($item.name): issuer $($item.properties.issuer), subject $($item.properties.subject)"
    }
    #direct owners only (indirect owners follow from them)
    foreach ($item in @($entry.SPOwners).where({ $_ -and $_.applicability -eq 'direct' })) {
        $state.owners["SP:$($item.id)"] = "$($item.displayName) ($($item.principalType)), owner of the service principal"
    }
    foreach ($item in @($entry.APPAppOwners).where({ $_ -and $_.applicability -eq 'direct' })) {
        $state.owners["APP:$($item.id)"] = "$($item.displayName) ($($item.principalType)), owner of the application"
    }
    foreach ($item in @($entry.SPGroupMemberships).where({ $_ })) {
        $state.groups[[string]$item.ObjectId] = [string]$item.DisplayName
    }
    foreach ($item in @($app.APPRedirectUris).where({ $_ })) {
        $value = $(if ($item -is [string]) { $item } else { "$($item.uri) ($($item.platform))" })
        $state.redirectUris[$value] = $value
    }
    foreach ($item in @($app.APPIdentifierUris).where({ $_ })) {
        $state.identifierUris[[string]$item] = [string]$item
    }
    return $state
}

function getChangeHistoryState($entries) {
    $objects = [ordered]@{}
    foreach ($entry in $entries) {
        $objects[[string]$entry.ObjectId] = getChangeHistoryObjectState -entry $entry
    }
    return $objects
}

#the changes between two states (ordered dictionaries objectId -> object state); $VersionChanged: the script version differs from the
#previous run (the effective privilege level is derived and can change with the classification of a new version)
function compareChangeHistoryState($Previous, $Current, [string]$Time, [string]$Version, [bool]$VersionChanged) {
    $records = [System.Collections.Generic.List[object]]::new()
    $add = {
        param($category, $change, $objectId, $state, $detail, $note)
        $records.Add([ordered]@{
                time = $Time; version = $Version; category = $category; change = $change
                objectId = $objectId; objectType = $state.type
                displayName = $(if ($state.spName) { $state.spName } else { $state.appName })
                detail = $detail; note = $note
            })
    }
    $relations = @(
        @{ property = 'entraRoles'; category = 'security'; added = 'Entra role added'; removed = 'Entra role removed' }
        @{ property = 'azureRoles'; category = 'security'; added = 'critical Azure role added'; removed = 'critical Azure role removed' }
        @{ property = 'azureRoleConditions'; category = 'security'; added = 'Azure role condition added'; removed = 'Azure role condition removed'; changed = 'Azure role condition changed' }
        @{ property = 'appPermissions'; category = 'security'; added = 'application permission added'; removed = 'application permission removed' }
        @{ property = 'delegatedPermissions'; category = 'security'; added = 'delegated permission granted'; removed = 'delegated permission removed' }
        @{ property = 'credentials'; category = 'security'; added = 'credential added'; removed = 'credential removed' }
        @{ property = 'federatedCredentials'; category = 'security'; added = 'federated credential added'; removed = 'federated credential removed'; changed = 'federated credential changed' }
        @{ property = 'owners'; category = 'security'; added = 'owner added'; removed = 'owner removed' }
        @{ property = 'groups'; category = 'configuration'; added = 'group membership added'; removed = 'group membership removed' }
        @{ property = 'redirectUris'; category = 'configuration'; added = 'redirect URI added'; removed = 'redirect URI removed' }
        @{ property = 'identifierUris'; category = 'configuration'; added = 'identifier URI added'; removed = 'identifier URI removed' }
    )
    foreach ($objectId in $Current.Keys) {
        $now = $Current[$objectId]
        $before = $Previous[$objectId]
        if ($null -eq $before) {
            if (-not $now.microsoftFirstParty) {
                & $add 'security' 'object added' $objectId $now "$($now.type)$(if ($now.level) { ", effective privilege $($now.level)" })" $null
            }
            continue
        }
        if ($before.spName -ne $now.spName -and $before.spName -and $now.spName) {
            & $add 'configuration' 'renamed' $objectId $now "service principal: '$($before.spName)' -> '$($now.spName)'" $null
        }
        if ($before.appName -ne $now.appName -and $before.appName -and $now.appName) {
            & $add 'configuration' 'renamed' $objectId $now "application: '$($before.appName)' -> '$($now.appName)'" $null
        }
        if ($before.enabled -ne $now.enabled -and $before.enabled -and $now.enabled) {
            & $add 'configuration' $(if ($now.enabled -eq 'True') { 'enabled' } else { 'disabled' }) $objectId $now "accountEnabled: $($before.enabled) -> $($now.enabled)" $null
        }
        if ($before.signInAudience -ne $now.signInAudience -and $before.signInAudience -and $now.signInAudience) {
            & $add 'configuration' 'sign-in audience changed' $objectId $now "$($before.signInAudience) -> $($now.signInAudience)" $null
        }
        if ($before.level -ne $now.level -and $before.level -and $now.level) {
            & $add 'security' 'effective privilege changed' $objectId $now "$($before.level) -> $($now.level)" $(if ($VersionChanged) { 'script version changed (classification may differ)' })
        }
        foreach ($relation in $relations) {
            $beforeMap = $before[$relation.property]
            $nowMap = $now[$relation.property]
            if ($null -eq $beforeMap) { $beforeMap = @{} }
            elseif ($beforeMap.Count -gt 0) {
                #review F-26: keys compared case-insensitively (the previous state is read case-sensitively)
                $beforeMapCaseInsensitive = [System.Collections.Specialized.OrderedDictionary]::new([System.StringComparer]::OrdinalIgnoreCase)
                foreach ($beforeEntry in $beforeMap.GetEnumerator()) { $beforeMapCaseInsensitive[$beforeEntry.Key] = $beforeEntry.Value }
                $beforeMap = $beforeMapCaseInsensitive
            }
            if ($null -eq $nowMap) { $nowMap = @{} }
            foreach ($key in $nowMap.Keys) {
                if (-not $beforeMap.Contains($key)) {
                    & $add $relation.category $relation.added $objectId $now $nowMap[$key] $null
                }
                elseif ($relation.changed -and $beforeMap[$key] -ne $nowMap[$key]) {
                    & $add $relation.category $relation.changed $objectId $now "$($beforeMap[$key]) -> $($nowMap[$key])" $null
                }
            }
            foreach ($key in $beforeMap.Keys) {
                if (-not $nowMap.Contains($key)) {
                    & $add $relation.category $relation.removed $objectId $now $beforeMap[$key] $null
                }
            }
        }
    }
    foreach ($objectId in $Previous.Keys) {
        if (-not $Current.Contains($objectId)) {
            $before = $Previous[$objectId]
            if (-not $before.microsoftFirstParty) {
                & $add 'security' 'object removed' $objectId $before "$($before.type)$(if ($before.level) { ", effective privilege was $($before.level)" })" $null
            }
        }
    }
    return $records
}

#the scope of a run: two runs are only compared when it is the same
function getChangeHistoryScope([string]$TenantId, [string[]]$ManagementGroupIds, [string[]]$SubscriptionQuotaIdWhitelist, [bool]$NoAzureResourceSideRelations, [bool]$OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes) {
    [ordered]@{
        tenantId = $TenantId
        managementGroups = (@($ManagementGroupIds | Sort-Object -Unique) -join ',')
        subscriptionQuotaIdWhitelist = (@($SubscriptionQuotaIdWhitelist | Sort-Object -Unique) -join ',')
        noAzureResourceSideRelations = $NoAzureResourceSideRelations
        onlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes = $OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes
    }
}

#compares with the previous state, appends the changes to the log, writes the new state; returns the run record (what was done)
#$Incomplete: nothing is compared and the previous state is kept; the next complete run reports the changes.
#Review R-9: collection errors of single objects no longer skip the whole comparison. $CarryForwardObjects (object id -> reason):
#objects with failed or incomplete data keep their previous state (no change reported for them, also not 'removed');
#$CarryForwardRelations ('azureRoles', 'level', 'groups', ...): parts that are incomplete for all objects (e.g. a failed
#subscription) keep their previous value. The next complete run reports the changes of these objects / parts.
function updateChangeHistory([string]$StatePath, [string]$LogPath, $Scope, $CurrentState, [string]$Version, [bool]$Incomplete, [string]$Time, [hashtable]$CarryForwardObjects = @{}, [string[]]$CarryForwardRelations = @()) {
    $runRecord = { param($change, $detail) [ordered]@{ time = $Time; version = $Version; category = 'run'; change = $change; objectId = ''; objectType = ''; displayName = ''; detail = $detail; note = $null } }
    $writeState = {
        $stateDocument = [ordered]@{ formatVersion = $script:changeHistoryFormatVersion; time = $Time; version = $Version; scope = $Scope; objects = $CurrentState }
        #review R-25: written to a temporary file and moved, so an interrupted run never leaves a truncated state
        $temporaryStatePath = "$($StatePath).tmp"
        [System.IO.File]::WriteAllText($temporaryStatePath, (ConvertTo-Json -InputObject $stateDocument -Depth 8 -Compress), [System.Text.UTF8Encoding]::new($false))
        [System.IO.File]::Move($temporaryStatePath, $StatePath, $true)
    }
    $records = [System.Collections.Generic.List[object]]::new()
    $stateToWrite = $false
    $previousDocument = $null
    $reason = $null
    if (Test-Path -LiteralPath $StatePath -PathType Leaf) {
        try {
            $previousDocument = Get-Content -LiteralPath $StatePath -Raw | ConvertFrom-Json -AsHashtable -NoEnumerate -ErrorAction Stop
        }
        catch {
            $reason = "the state file of the previous run cannot be read ($($_.Exception.Message))"
        }
    }
    if ($Incomplete) {
        $records.Add((& $runRecord 'comparison skipped' 'the run had collection errors - the previous state is kept, the next complete run reports the changes'))
    }
    elseif (-not $previousDocument -and -not $reason) {
        $records.Add((& $runRecord 'baseline created' "first run in this output folder: $($CurrentState.Count) objects; changes are reported from the next run on"))
        $stateToWrite = $true
    }
    else {
        #review R-25: valid JSON of another shape (hand edit, other tool) threw below on every run - now a baseline reset
        $previousTime = $null
        if (-not $reason -and $previousDocument -isnot [System.Collections.IDictionary]) {
            $reason = 'the state file of the previous run has an unexpected content'
        }
        if (-not $reason -and $previousDocument['formatVersion'] -ne $script:changeHistoryFormatVersion) {
            $reason = "the state format changed ($($previousDocument['formatVersion']) -> $($script:changeHistoryFormatVersion))"
        }
        if (-not $reason -and ($previousDocument['scope'] -isnot [System.Collections.IDictionary] -or $previousDocument['objects'] -isnot [System.Collections.IDictionary] -or
                -not $(try { $previousTime = convertChangeHistoryTime -Value $previousDocument['time']; $true } catch { $false }))) {
            $reason = 'the state file of the previous run has an unexpected content'
        }
        if (-not $reason) {
            $differences = @(foreach ($key in $Scope.Keys) { if ([string]$previousDocument['scope'][$key] -ne [string]$Scope[$key]) { "$($key): '$($previousDocument['scope'][$key])' -> '$($Scope[$key])'" } })
            if ($differences.Count -gt 0) {
                $reason = "the scope of the run changed ($($differences -join '; '))"
            }
        }
        if ($reason) {
            $records.Add((& $runRecord 'baseline reset' "$reason - no comparison with the previous run; changes are reported from the next run on"))
        }
        else {
            #review F-26: ConvertFrom-Json -AsHashtable is case-sensitive, the current state ([ordered]) is not - a key that differs only
            #in case (ARM ids: resourceGroups / resourcegroups) was 'added' without 'removed'
            $previousObjects = [System.Collections.Specialized.OrderedDictionary]::new([System.StringComparer]::OrdinalIgnoreCase)
            foreach ($previousObject in $previousDocument['objects'].GetEnumerator()) { $previousObjects[$previousObject.Key] = $previousObject.Value }
            #review R-9: objects / parts with collection errors keep their previous state (also in the state written below)
            $notComparedObjects = 0
            foreach ($objectId in @($CarryForwardObjects.Keys)) {
                if ($previousObjects.Contains($objectId)) {
                    $CurrentState[$objectId] = $previousObjects[$objectId]
                    $notComparedObjects++
                }
            }
            if ($CarryForwardRelations.Count -gt 0) {
                foreach ($objectId in @($CurrentState.Keys)) {
                    if ($CarryForwardObjects.ContainsKey($objectId) -or -not $previousObjects.Contains($objectId)) { continue }
                    foreach ($relation in $CarryForwardRelations) {
                        $CurrentState[$objectId][$relation] = $previousObjects[$objectId][$relation]
                    }
                }
            }
            $changes = @(compareChangeHistoryState -Previous $previousObjects -Current $CurrentState -Time $Time -Version $Version -VersionChanged ([string]$previousDocument['version'] -ne $Version))
            foreach ($change in $changes) { $records.Add($change) }
            $previousTimeText = $previousTime.ToString("yyyy-MM-dd HH:mm:ss 'UTC'", [cultureinfo]::InvariantCulture)
            $notCompared = @(
                $(if ($notComparedObjects -gt 0) { "$($notComparedObjects) objects with collection errors" })
                $(if ($CarryForwardRelations.Count -gt 0) { "$($CarryForwardRelations -join ', ') of all objects (collection errors)" })
            ).where({ $_ })
            $records.Add((& $runRecord 'compared' "$($changes.Count) changes since the run of $($previousTimeText) ($($previousDocument['version']))$(if ($notCompared.Count) { "; not compared (previous state kept, reported by the next complete run): $($notCompared -join '; ')" })"))
        }
        $stateToWrite = $true
    }
    $lines = @(foreach ($record in $records) { ConvertTo-Json -InputObject $record -Depth 4 -Compress })
    #review F-27: a last line without line end (a run that was stopped while writing) is ended first - the first new record
    #was glued to it and both stayed unreadable
    if ([System.IO.File]::Exists($LogPath)) {
        $logStream = [System.IO.File]::Open($LogPath, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
        try {
            if ($logStream.Length -gt 0) {
                $null = $logStream.Seek(-1, [System.IO.SeekOrigin]::End)
                if ($logStream.ReadByte() -ne 10) { $lines = @('') + $lines }
            }
        }
        finally {
            $logStream.Dispose()
        }
    }
    #review F-5: the log first, then the state - a failed append (log opened by another process) left the new state written and
    #the records lost (the next run compared against the new state); now the state stays and the next run reports the changes
    [System.IO.File]::AppendAllLines($LogPath, [string[]]$lines, [System.Text.UTF8Encoding]::new($false))
    if ($stateToWrite) {
        & $writeState
    }
    return $records
}

#ConvertFrom-Json of PowerShell 7 returns ISO 8601 strings as DateTime: both forms to a UTC DateTime
function convertChangeHistoryTime($Value) {
    if ($Value -is [datetime]) {
        return $Value.ToUniversalTime()
    }
    return [datetime]::Parse([string]$Value, [cultureinfo]::InvariantCulture, [System.Globalization.DateTimeStyles]::AdjustToUniversal -bor [System.Globalization.DateTimeStyles]::AssumeUniversal)
}

#review R-11: the byte offset of the first line of the log that is not before $SinceLine ('{"time":"<ISO 8601 UTC, 'o'>'). The log
#is appended in time order and its lines start with the time, which sorts as text: a binary search over the byte offsets reads
#about 26 short lines instead of every line (a PowerShell loop costs ~30 us per line even without parsing; memory stays flat).
#A line without that prefix counts as 'not before' - the search can only start too early (more lines parsed), never too late.
function getChangeHistoryLogOffset([string]$LogPath, [string]$SinceLine) {
    $linePrefix = '{"time":"'
    $stream = [System.IO.File]::Open($LogPath, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    try {
        $length = $stream.Length
        $buffer = [byte[]]::new(256)
        #the start of the first line that begins at or after $position
        $lineStartAt = {
            param([long]$position)
            if ($position -le 0) { return [long]0 }
            $null = $stream.Seek($position - 1, [System.IO.SeekOrigin]::Begin)
            while ($true) {
                $byte = $stream.ReadByte()
                if ($byte -eq -1) { return $length }
                if ($byte -eq 10) { return $stream.Position }
            }
        }
        #true: the line at $lineStart is not before $SinceLine (or there is no line)
        $notBefore = {
            param([long]$lineStart)
            if ($lineStart -ge $length) { return $true }
            $null = $stream.Seek($lineStart, [System.IO.SeekOrigin]::Begin)
            $read = $stream.Read($buffer, 0, $buffer.Length)
            $text = [System.Text.Encoding]::UTF8.GetString($buffer, 0, $read).TrimStart([char]0xFEFF)
            $newline = $text.IndexOf("`n")
            if ($newline -ge 0) { $text = $text.Substring(0, $newline) }
            if (-not $text.StartsWith($linePrefix, [System.StringComparison]::Ordinal)) { return $true }
            return [string]::CompareOrdinal($text, $SinceLine) -ge 0
        }
        $low = [long]0
        $high = $length
        while ($low -lt $high) {
            $middle = [long][math]::Floor(($low + $high) / 2)
            if (& $notBefore (& $lineStartAt $middle)) { $high = $middle } else { $low = $middle + 1 }
        }
        return (& $lineStartAt $low)
    }
    finally {
        $stream.Dispose()
    }
}

#the report rows: the records of the last $Days days, newest first. A line that cannot be read (no JSON, no object, no
#valid 'time': e.g. a hand or merge edit of the log) is skipped and counted in $script:changeHistoryUnreadLines (review R-10:
#an invalid time threw outside the try and stopped the report on every later run)
function getChangeHistoryRows([string]$LogPath, [int]$Days, [datetime]$NowUtc) {
    $script:changeHistoryUnreadLines = 0
    if (-not (Test-Path -LiteralPath $LogPath -PathType Leaf)) {
        return @()
    }
    $since = $NowUtc.AddDays(-$Days)
    #review R-11: the log grows without limit (every line was parsed: 200,000 lines ~16 s); only the lines from the first one of the
    #period on are read (getChangeHistoryLogOffset), older lines among them are still filtered by their parsed time below
    $offset = getChangeHistoryLogOffset -LogPath $LogPath -SinceLine ('{"time":"' + $since.ToUniversalTime().ToString('o', [cultureinfo]::InvariantCulture))
    $logStream = [System.IO.File]::Open($LogPath, [System.IO.FileMode]::Open, [System.IO.FileAccess]::Read, [System.IO.FileShare]::ReadWrite)
    $null = $logStream.Seek($offset, [System.IO.SeekOrigin]::Begin)
    $logReader = [System.IO.StreamReader]::new($logStream, [System.Text.UTF8Encoding]::new($false), ($offset -eq 0))
    $logLines = [System.Collections.Generic.List[string]]::new()
    try {
        while ($null -ne ($logLine = $logReader.ReadLine())) { $logLines.Add($logLine) }
    }
    finally {
        $logReader.Dispose()
    }
    #all lines in one parse (one JSON array); if a line is no valid JSON, line by line (that line is skipped)
    $nonEmptyLines = @($logLines.Where({ -not [string]::IsNullOrWhiteSpace($_) }))
    $parsedRecords = $null
    try {
        $parsedRecords = ConvertFrom-Json -InputObject ('[' + ($nonEmptyLines -join ',') + ']') -NoEnumerate -ErrorAction Stop
    }
    catch {
        $parsedRecords = foreach ($line in $nonEmptyLines) {
            try { , (ConvertFrom-Json -InputObject $line -NoEnumerate -ErrorAction Stop) } catch { , 'unreadable line' }
        }
    }
    $rows = foreach ($record in @($parsedRecords)) {
        if ($record -isnot [System.Management.Automation.PSCustomObject] -or $null -eq $record.time -or [string]::IsNullOrWhiteSpace([string]$record.time)) {
            $script:changeHistoryUnreadLines++
            continue
        }
        #ConvertFrom-Json of PowerShell 7 returns the ISO 8601 time as DateTime (convertChangeHistoryTime for other forms)
        $time = $(if ($record.time -is [datetime]) { $record.time.ToUniversalTime() } else { try { convertChangeHistoryTime -Value $record.time } catch { $null } })
        if ($null -eq $time) {
            $script:changeHistoryUnreadLines++
            continue
        }
        if ($time -lt $since) { continue }
        [PSCustomObject]@{
            Time = $time.ToString("yyyy-MM-dd HH:mm:ss 'UTC'", [cultureinfo]::InvariantCulture)
            SortTime = $time
            Category = $record.category
            Change = $record.change
            ObjectType = $record.objectType
            DisplayName = $record.displayName
            ObjectId = $record.objectId
            Detail = $record.detail
            Note = $record.note
            ScriptVersion = $record.version
        }
    }
    return @($rows | Sort-Object -Property @{ Expression = 'SortTime'; Descending = $true }, Category, DisplayName, Change, Detail)
}
#endregion change history


#region AG Grid emitter
#Ported from Azure Governance Visualizer 7.0.0 (pwsh/dev/functions/buildAgGridScript.ps1, agGridAssets.ps1,
#compressGridDictionaries.ps1; MIT License, Copyright (c) 2020 Julian Hayward). One report file: one string table,
#one grid definition per column signature.
$script:htSpiGrid = @{
    definitions = [ordered]@{}
    fragments = @()
    #review P-5: a hashtable (indexer lookups); Dictionary.ContainsKey per value was a method call per value (~20 us each here)
    stringIndex = [System.Collections.Hashtable]::new([System.StringComparer]::Ordinal)
}

#markup that the report repeats across many grid values (links, icons): replaced by one private use character that
#references the fragment table; the browser (spiLoadStrings) expands them again
$script:spiGridDictionaryFragments = @(
    '<a class="externallink" href="https://www.azadvertizer.net/azentraidroles/'
    '<a class="externallink" href="https://www.azadvertizer.net/azrolesadvertizer/'
    '<a class="externallink" href="https://www.azadvertizer.net/azEntraIdAPIpermissionsAdvertizer.html?targetAppId='
    '" target="_blank">'
    '</a>'
    '<i class="fa fa-exclamation-triangle" aria-hidden="true" style="color: #ff5e00; font-size: 9px;"></i> '
    '<span class="odd">'
    '<span class="even">'
    '</span>'
    '<br>'
    '<b>'
    '</b>'
    '<a class="externallink" href="https://www.azadvertizer.net/azresourcetypes/'
    '<a class="externallink" href="https://portal.azure.com/#@'
    '<a class="externallink" href="https://portal.azure.com/#view/Microsoft_AAD_UsersAndTenants/UserProfileMenuBlade/~/overview/userId/'
    '<a class="externallink" href="https://portal.azure.com/#view/Microsoft_AAD_IAM/GroupDetailsMenuBlade/~/Overview/groupId/'
    '<a class="externallink" href="https://portal.azure.com/#view/Microsoft_AAD_IAM/ManagedAppMenuBlade/~/Overview/objectId/'
    '<a class="externallink" href="https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/~/Overview/appId/'
    '<a class="externallink" href="https://www.azadvertizer.net/azentraidapipermissions/'
)

#review R-16: the browser expands U+E000.. in every string of the table into a fragment; a private use character that is part of the
#data (tenant text: names, tags, credential names - HtmlSafe does not encode it) would become report markup. Such a character is
#written as U+E0FF + the character (U+E0FF: escape, never a fragment token) and the browser restores it
$script:spiGridEscapeChar = [char]0xE0FF
$script:spiGridPrivateUseChars = [char[]]@(foreach ($code in 0xE000..0xE0FF) { [char]$code })
#the JSON text of the string table with the private use characters of the data escaped (review R-16) and the markup fragments as
#tokens. Review P-5: once on the whole text instead of 19 .Replace per html value. A fragment cannot match across two strings: an
#unescaped quote (string boundary) is always followed by , ] or : - never by what follows \" inside a fragment
function compressSpiGridStringsJson([string]$StringsJson) {
    $fragments = @($script:spiGridDictionaryFragments)
    if ($fragments.Count -ge 0xFF) { throw "compressSpiGridStringsJson: $($fragments.Count) fragments - at most 255 (U+E0FF is the escape)" }
    $text = $StringsJson -replace '([\uE000-\uE0FF])', ([string]$script:spiGridEscapeChar + '$1')
    for ($i = 0; $i -lt $fragments.Count; $i++) {
        $fragmentJson = ConvertTo-Json -InputObject $fragments[$i] -Compress
        $text = $text.Replace($fragmentJson.Substring(1, $fragmentJson.Length - 2), [string][char](0xE000 + $i))
    }
    $script:htSpiGrid.fragments = $fragments
    return $text
}

function addSpiGridStrings([AllowEmptyCollection()][string[]]$Values) {
    $indices = New-Object 'int[]' $Values.Count
    $stringIndex = $script:htSpiGrid.stringIndex
    for ($i = 0; $i -lt $Values.Count; $i++) {
        $value = $Values[$i]
        if ($null -eq $value) { $value = '' }
        $index = $stringIndex[$value]
        if ($null -eq $index) {
            #review F-32: the table is built from the index once (buildSpiGridAssetsBlock), no List.Add per string
            $index = $stringIndex.Count
            $stringIndex[$value] = $index
        }
        $indices[$i] = $index
    }
    return , $indices
}

#gzip + base64; the browser inflates it with DecompressionStream('gzip')
function compressSpiGridText([AllowEmptyString()][string]$Text) {
    $bytes = [System.Text.Encoding]::UTF8.GetBytes($Text)
    $memory = [System.IO.MemoryStream]::new()
    $gzip = [System.IO.Compression.GZipStream]::new($memory, [System.IO.Compression.CompressionLevel]::Optimal, $true)
    $gzip.Write($bytes, 0, $bytes.Length)
    $gzip.Dispose()
    $base64 = [Convert]::ToBase64String($memory.ToArray())
    $memory.Dispose()
    return $base64
}

#the grid definitions and the string table, written once at the end of the report (the grids are created on demand)
function buildSpiGridAssetsBlock {
    $builder = [System.Text.StringBuilder]::new()
    foreach ($key in $script:htSpiGrid.definitions.Keys) {
        [void]$builder.Append("<script id=`"spiGridDef_$key`">`n")
        [void]$builder.Append($script:htSpiGrid.definitions[$key])
        [void]$builder.Append("`n</script>`n")
    }
    $strings = New-Object 'string[]' $script:htSpiGrid.stringIndex.Count
    foreach ($pair in $script:htSpiGrid.stringIndex.GetEnumerator()) { $strings[$pair.Value] = $pair.Key }
    $stringsJson = if ($strings.Count -eq 0) { '[]' } else { compressSpiGridStringsJson -StringsJson (ConvertTo-Json -InputObject $strings -Compress -Depth 1) }
    $fragmentsJson = if (@($script:htSpiGrid.fragments).Count -eq 0) { '[]' } else { ConvertTo-Json -InputObject @($script:htSpiGrid.fragments) -Compress }
    [void]$builder.Append("<script type=`"application/octet-stream`" id=`"spiStrings`">")
    [void]$builder.Append((compressSpiGridText -Text ('{"fragments":' + $fragmentsJson + ',"strings":' + $stringsJson + '}')))
    [void]$builder.Append("</script>`n")
    return $builder.ToString()
}

function buildSpiGrid {
    <#
    .SYNOPSIS
    The html of an AG Grid: toolbar host, grid element, dictionary encoded and compressed row data, registration.
    A column definition is a hashtable:
      header       - the column header
      property     - the property of a row object that is used for sorting, filtering and the CSV export (plain text)
      htmlProperty - optional, the property that is rendered instead of 'property' (encoded html fragment)
      filter       - optional, 'select', 'selectMulti' (multi value cell, items joined by $CsvDelimiterOpposite), 'number', 'date'
      portal       - optional, 'servicePrincipal' or 'application': the id is linked to its Microsoft Entra page in the Azure portal
      appIdProperty - with portal: the property with the application (client) id (servicePrincipal: required; application: the
                     column itself when omitted)
      typeProperty - with portal 'principal': the property with the principal type ('User ...' / 'Group ...' are linked)
      tooltip      - optional, the header tooltip
      hide         - optional, the column is available but not shown
    #>
    param(
        [Parameter(Mandatory)][string]$HtmlTableId,
        [Parameter(Mandatory)][AllowEmptyCollection()][object[]]$Rows,
        [Parameter(Mandatory)][object[]]$ColumnDefinitions,
        [Parameter(Mandatory)][AllowEmptyString()][string]$PopoutTitle
    )
    if ([string]::IsNullOrWhiteSpace($PopoutTitle)) {
        $PopoutTitle = $HtmlTableId
    }
    #portal: the grid belongs to the open report section; the rows of its first grid are the count of the section
    if ($script:spiCurrentSection) {
        $script:spiCurrentSection.grids.Add($HtmlTableId)
        if ($null -eq $script:spiCurrentSection.count) {
            $script:spiCurrentSection.count = $Rows.Count
        }
    }
    $gridColumns = [System.Collections.Generic.List[string]]::new()
    foreach ($columnDefinition in $ColumnDefinitions) {
        if ($columnDefinition.htmlProperty) {
            $gridColumns.Add($columnDefinition.htmlProperty)
        }
        $gridColumns.Add($columnDefinition.property)
    }
    $gridColumnCount = $gridColumns.Count

    #review F-51: '<' as \u003c - a '</script>' in a title or header must not end the inline script
    $jsString = { param([string]$text) $text.Replace('\', '\\').Replace("'", "\'").Replace("`r", '').Replace("`n", '\n').Replace('<', '\u003c') }
    $columnDefsJs = @(foreach ($columnDefinition in $ColumnDefinitions) {
            $valueIndex = $gridColumns.IndexOf($columnDefinition.property)
            $parts = [System.Collections.Generic.List[string]]::new()
            $parts.Add("headerName: '$(& $jsString $columnDefinition.header)'")
            $parts.Add("colId: '$(& $jsString $columnDefinition.property)'")
            if ($columnDefinition.filter -eq 'number') {
                $parts.Add("valueGetter: spiColumnNumberValueGetter(spiRowData, $valueIndex)")
                $parts.Add("cellRenderer: spiColumnTextRenderer(spiRowData, $valueIndex, spiHighlighter)")
                $parts.Add("valueFormatter: spiColumnTextFormatter(spiRowData, $valueIndex)")
                $parts.Add("cellDataType: 'number'")
                $parts.Add("filter: 'agNumberColumnFilter'")
            }
            else {
                $parts.Add("valueGetter: spiColumnValueGetter(spiRowData, $valueIndex)")
            }
            if ($columnDefinition.portal) {
                $auxProperty = $(if ($columnDefinition.appIdProperty) { $columnDefinition.appIdProperty } else { $columnDefinition.typeProperty })
                $auxIndex = $(if ($auxProperty) { $gridColumns.IndexOf($auxProperty) } else { -1 })
                if ($auxProperty -and $auxIndex -lt 0) {
                    throw "buildSpiGrid $($HtmlTableId): '$($auxProperty)' is not a column"
                }
                $parts.Add("cellRenderer: spiColumnPortalLinkRenderer(spiRowData, $valueIndex, $auxIndex, '$(& $jsString $columnDefinition.portal)', spiHighlighter)")
            }
            if ($columnDefinition.htmlProperty) {
                $parts.Add("cellRenderer: spiColumnHtmlRenderer(spiRowData, $($gridColumns.IndexOf($columnDefinition.htmlProperty)), spiHighlighter)")
                #list cells: the automatic width measures the rows of the first page only (often empty cells), keep them readable
                $parts.Add('minWidth: 220')
            }
            if ($columnDefinition.filter -eq 'select') {
                $parts.Add('floatingFilterComponent: spiSelectFloatingFilter')
                $parts.Add("floatingFilterComponentParams: { values: spiRowData.dictionaries[$valueIndex] }")
                #review F-50: the options are case-sensitive ('A', 'a'); 'equals' was not - selecting one showed the rows of both
                $parts.Add('filterParams: { caseSensitive: true }')
                $parts.Add('suppressFloatingFilterButton: true')
            }
            if ($columnDefinition.filter -eq 'selectMulti') {
                $parts.Add('floatingFilterComponent: spiSelectFloatingFilter')
                $parts.Add("floatingFilterComponentParams: { values: spiRowData.dictionaries[$valueIndex], separator: '$(& $jsString $CsvDelimiterOpposite)' }")
                $parts.Add('suppressFloatingFilterButton: true')
            }
            if ($columnDefinition.filter -eq 'date') {
                $parts.Add("filter: 'agDateColumnFilter'")
                $parts.Add('filterParams: spiDateFilterParams')
                $parts.Add('comparator: spiDateSortComparator')
            }
            if ($columnDefinition.tooltip) {
                $parts.Add("headerTooltip: '$(& $jsString $columnDefinition.tooltip)'")
            }
            if ($columnDefinition.hide) {
                $parts.Add('hide: true')
            }
            "        { $($parts -join ', ') }"
        }) -join ",`n"

    #per column dictionary of distinct values; a row holds only the indexes into those dictionaries.
    #Review P-5: no .NET method call per cell (hashtable indexer, one -join per row) - a method call costs microseconds in PowerShell
    [string[]]$columnNames = $gridColumns.ToArray()
    #review F-32: also no method call per new value or per row (List.Add) - pre-sized arrays, the dictionaries built from the maps
    $maps = New-Object 'object[]' $gridColumnCount
    for ($column = 0; $column -lt $gridColumnCount; $column++) {
        $maps[$column] = [System.Collections.Hashtable]::new([System.StringComparer]::Ordinal)
    }
    $rowTexts = New-Object 'string[]' @($Rows).Count
    $rowIndex = 0
    $cellIndexes = New-Object 'int[]' $gridColumnCount
    foreach ($entry in $Rows) {
        for ($column = 0; $column -lt $gridColumnCount; $column++) {
            #as the former table cells: PowerShell's string of the value (booleans 'True'/'False'; every column is text, cellDataType 'text')
            $value = [string]$entry.($columnNames[$column])
            $map = $maps[$column]
            $index = $map[$value]
            if ($null -eq $index) {
                $index = $map.Count
                $map[$value] = $index
            }
            $cellIndexes[$column] = $index
        }
        $rowTexts[$rowIndex] = '[' + ($cellIndexes -join ',') + ']'
        $rowIndex++
    }
    $rowsBuilder = [System.Text.StringBuilder]::new(($rowTexts -join ','))
    $dictionariesArray = New-Object 'object[]' $gridColumnCount
    for ($column = 0; $column -lt $gridColumnCount; $column++) {
        $dictionary = New-Object 'string[]' $maps[$column].Count
        foreach ($pair in $maps[$column].GetEnumerator()) { $dictionary[$pair.Value] = $pair.Key }
        $dictionariesArray[$column] = $dictionary
    }
    $indices = New-Object 'object[]' $gridColumnCount
    for ($column = 0; $column -lt $gridColumnCount; $column++) {
        $indices[$column] = addSpiGridStrings -Values @($dictionariesArray[$column])
    }
    $dictionariesJson = if ($gridColumnCount -eq 0) { '[]' } else { ConvertTo-Json -InputObject $indices -Compress -Depth 3 }
    $columnsJson = ConvertTo-Json -InputObject $gridColumns.ToArray() -Compress
    $dataBase64 = compressSpiGridText -Text ('{"rows":[' + $rowsBuilder.ToString() + '],"dictionaries":' + $dictionariesJson + ',"columns":' + $columnsJson + '}')

    #one definition per column signature
    $keyBytes = [System.Security.Cryptography.SHA1]::HashData([System.Text.Encoding]::UTF8.GetBytes($columnDefsJs))
    $key = ([System.BitConverter]::ToString($keyBytes, 0, 8)).Replace('-', '').ToLowerInvariant()
    if (-not $script:htSpiGrid.definitions.Contains($key)) {
        $script:htSpiGrid.definitions[$key] = @"
//options factory of the grids with this column signature; the pop out window builds the very same grid in its own document
spiGridDefs['$key'] = function (spiHighlighter, spiRowData, spiGridElement) {
    //the column index passed to the getters/renderers must match the column order of the emitted rowData
    return {
    theme: spiGridTheme,
    rowData: spiRowData.rows,
    columnDefs: [
$columnDefsJs
    ],
    defaultColDef: {
        minWidth: 90,
        maxWidth: 420,
        sortable: true,
        resizable: true,
        //every value is a string, without this AG Grid would infer the type from the data and e.g. render 'true'/'false' as a checkbox
        cellDataType: 'text',
        filter: 'agTextColumnFilter',
        floatingFilter: true,
        //wrapText/autoHeight/floatingFilter/pinned are applied per grid by spiPrepareOptions (display state of the grid)
        wrapHeaderText: true,
        autoHeaderHeight: true,
        //renders the plain cell value html encoded and wraps the text filter matches in <mark>
        cellRenderer: function (params) { return spiHighlighter.text(params.value, params.column.getColId()); },
        //review F-47: sort without case (the default compares code units: every lower-case value after all upper-case ones); the
        //date columns keep their comparator
        comparator: spiTextComparator
    },
    autoSizeStrategy: { type: 'fitCellContents' },
    pagination: true,
    paginationPageSize: 100,
    paginationPageSizeSelector: [10, 30, 50, 100, 250, 500, 1000],
    enableCellTextSelection: true,
    ensureDomOrder: true,
    onFirstDataRendered: function (event) {
        spiOnFirstDataRendered(event.api, spiGridElement);
    },
    onFilterChanged: function (event) {
        var changed = spiHighlighter.update(event.api.getFilterModel());
        if (changed.length) {
            //refresh all rows of the changed columns so that the marks are also updated off screen
            event.api.refreshCells({ columns: changed, force: true });
        }
    }
    };
};
"@
    }
    $title = & $jsString $PopoutTitle
    return @"
<div class="spiToolbarHost" id="spiToolbar4$($HtmlTableId)"></div>
<div id="$HtmlTableId" class="spiGrid" style="width:100%;"></div>
<script type="application/octet-stream" id="spiData4$($HtmlTableId)">$dataBase64</script>
<script>spiRegisterReportGrid('$HtmlTableId', '$key', '$title');</script>
"@
}

#the plain text of an html cell (sort, filter, CSV of the grid): tags become a space, entities decoded - as a browser shows it
function ConvertTo-SpiPlainText([AllowNull()][string]$Html) {
    if ([string]::IsNullOrEmpty($Html)) { return '' }
    return [System.Net.WebUtility]::HtmlDecode((($Html -replace '<[^>]+>', ' ') -replace '\s+', ' ').Trim())
}
#the title attribute of an html fragment (e.g. an <abbr> in a column header), decoded
function getSpiHtmlTitle([AllowNull()][string]$Html) {
    if ($Html -match 'title="([^"]*)"') { return [System.Net.WebUtility]::HtmlDecode($Matches[1]) }
    return $null
}

#bullet list for a grid cell (htmlProperty); a single item is returned as encoded text without list markup
function getSpiGridListHtml([AllowNull()][AllowEmptyCollection()][object[]]$Items) {
    $encodedItems = @(foreach ($item in $Items) {
            if (-not [string]::IsNullOrEmpty([string]$item)) { [HtmlSafe]::Encode([string]$item) }
        })
    if ($encodedItems.Count -le 1) {
        return [string]($encodedItems -join '')
    }
    return '<ul class="spiList">' + (@(foreach ($encodedItem in $encodedItems) { "<li>$encodedItem</li>" }) -join '') + '</ul>'
}

#a readable cell: one entry per line. Item (hashtable): Main = text | MainHtml = html; Dim = text or texts (details, dimmed,
#joined by a middle dot); Details = [ordered]@{ label = value } (key/value lines); State = 'muted' | 'warn'.
#-Label: a labelled group ('direct (2)'). One item without label: no bullet.
function getSpiItemsHtml {
    param(
        [AllowNull()][AllowEmptyCollection()][object[]]$Items,
        [string]$Label
    )
    #review P-6: called per row and cell - foreach statements instead of pipelines (an empty ForEach-Object costs ~24 us)
    if ($null -eq $Items -or $Items.Count -eq 0) { return '' }
    $entries = [System.Collections.Generic.List[object]]::new()
    foreach ($item in $Items) {
        if ($null -eq $item) { continue }
        $main = $(if ($item.MainHtml) { $item.MainHtml } else { [HtmlSafe]::Encode([string]$item.Main) })
        $dimParts = @(foreach ($dimValue in @($item.Dim)) { if (-not [string]::IsNullOrWhiteSpace([string]$dimValue)) { [HtmlSafe]::Encode([string]$dimValue) } })
        $dim = $(if ($dimParts.Count) { " <span class=`"spiDim`">$($dimParts -join ' &middot; ')</span>" } else { '' })
        $details = ''
        if ($item.Details) {
            $lines = @(foreach ($key in $item.Details.Keys) {
                    if (-not [string]::IsNullOrWhiteSpace([string]$item.Details[$key])) { "<span class=`"spiKvLine`"><span class=`"spiKvKey`">$([HtmlSafe]::Encode([string]$key))</span> $([HtmlSafe]::Encode([string]$item.Details[$key]))</span>" }
                })
            if ($lines.Count) { $details = "<span class=`"spiKv`">$($lines -join '')</span>" }
        }
        $entries.Add(@{ html = "$main$dim$details"; state = [string]$item.State })
    }
    if ($entries.Count -eq 0) { return '' }
    if ($entries.Count -eq 1 -and -not $Label) {
        return "<span class=`"spiItem$(if ($entries[0].state) { " spiItem-$($entries[0].state)" })`">$($entries[0].html)</span>"
    }
    $head = $(if ($Label) { "<b class=`"spiItemsLabel`">$([HtmlSafe]::Encode($Label)) ($($entries.Count))</b>" } else { '' })
    $listItems = foreach ($entry in $entries) {
        $classAttribute = $(if ($entry.state) { ' class="spiItem-' + $entry.state + '"' } else { '' })
        '<li' + $classAttribute + '>' + $entry.html + '</li>'
    }
    return "$head<ul class=`"spiList spiItems`">" + (@($listItems) -join '') + '</ul>'
}
#links in grid cells: an Azure resource type (namespace/type) to its AzAdvertizer page (a namespace alone has no page), an ARM
#resource ID (subscription or management group scope) to the Azure portal; anything else is shown as encoded text
function getSpiResourceTypeLinkHtml([string]$ResourceType) {
    if ([string]::IsNullOrEmpty($ResourceType)) { return '' }   #review F-34
    if ($ResourceType -match '^[A-Za-z0-9.]+(/[A-Za-z0-9.]+)+$') {
        $page = $ResourceType.ToLowerInvariant().Replace('/', '_')
        return "<a class=`"externallink`" href=`"https://www.azadvertizer.net/azresourcetypes/$($page).html`" target=`"_blank`">$([HtmlSafe]::Encode($ResourceType))</a>"
    }
    return [HtmlSafe]::Encode($ResourceType)
}
function getSpiPortalResourceLinkHtml([string]$ResourceId) {
    if ([string]::IsNullOrEmpty($ResourceId)) { return '' }   #review F-34
    if ($ResourceId -match '^/(subscriptions/|providers/Microsoft\.Management/managementGroups/)') {
        $href = "https://portal.azure.com/#@$($azAPICallConf['checkContext'].Tenant.Id)/resource$($ResourceId.Replace(' ', '%20'))"
        return "<a class=`"externallink`" href=`"$([HtmlSafe]::Encode($href))`" target=`"_blank`">$([HtmlSafe]::Encode($ResourceId))</a>"
    }
    return [HtmlSafe]::Encode($ResourceId)
}
#a list cell ('a, b (2)') with each resource type linked; $Delimiter: the separator of the plain text
function getSpiResourceTypeListHtml([string]$Text, [string]$Delimiter) {
    if ([string]::IsNullOrEmpty($Text)) { return '' }
    return (@(foreach ($part in ($Text -split [regex]::Escape($Delimiter))) {
                $countMatch = [regex]::Match($part, '^(.+?) (\(\d+\))$')
                if ($countMatch.Success) { "$(getSpiResourceTypeLinkHtml $countMatch.Groups[1].Value) $($countMatch.Groups[2].Value)" } else { getSpiResourceTypeLinkHtml $part }
            }) -join [HtmlSafe]::Encode($Delimiter))
}
#labelled groups: ordered dictionary label -> items
function getSpiGroupedItemsHtml($Groups) {
    return (@(foreach ($key in $Groups.Keys) { getSpiItemsHtml -Items $Groups[$key] -Label $key }) -join '')
}
#markers: critical (Entra/Azure roles), permission classification (colour from permissionClassification.json)
function getSpiCriticalIconHtml([bool]$IsCritical) {
    if ($IsCritical) { return '<i class="fa fa-triangle-exclamation spiCritical" aria-hidden="true"></i>' }
    return ''
}
#an Azure role that can write role assignments, limited by a condition to non-critical roles
function getSpiConditionLimitedIconHtml([bool]$IsLimited) {
    if ($IsLimited) { return '<i class="fa fa-filter spiConditionLimited" aria-hidden="true"></i>' }
    return ''
}
function getSpiClassificationDotHtml([string]$Sensitivity) {
    if ($Sensitivity -and $Sensitivity -ne 'unclassified' -and $script:getClassifications.permissionColors[$Sensitivity]) {
        return "<span class=`"spiClassDot`" style=`"background: $([HtmlSafe]::Encode([string]$script:getClassifications.permissionColors[$Sensitivity]))`"></span>"
    }
    return ''
}
#the name of an object linked to its Microsoft Entra page in the Azure portal (only for GUID ids; otherwise text)
#$Kind: 'user', 'group', 'serviceprincipal', 'application' or a principal/object type ('User (Member)', 'Group', 'Serviceprincipal',
#'Application', ...); Service Principals and applications need the application id: $AppId, else looked up by object id in
#$script:htSpiAppIdByObjectId (the objects of this run)
function getSpiEntraObjectLinkHtml([string]$Kind, [string]$Id, [string]$Text, [string]$AppId) {
    $encodedText = [HtmlSafe]::Encode($Text)
    $guidPattern = '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
    if ($Id -notmatch $guidPattern) { return $encodedText }
    if (-not $AppId -and $script:htSpiAppIdByObjectId) { $AppId = [string]$script:htSpiAppIdByObjectId[$Id] }
    #R-32: also the type labels of owner SPs ('SP APP INT', 'SP MI System assigned', ...)
    if ($Kind -match '^(serviceprincipal|SP )' -and $AppId -match $guidPattern) {
        return "<a class=`"externallink`" href=`"https://portal.azure.com/#view/Microsoft_AAD_IAM/ManagedAppMenuBlade/~/Overview/objectId/$($Id)/appId/$($AppId)`" target=`"_blank`">$($encodedText)</a>"
    }
    if ($Kind -match '^application' -and $AppId -match $guidPattern) {
        return "<a class=`"externallink`" href=`"https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/~/Overview/appId/$($AppId)`" target=`"_blank`">$($encodedText)</a>"
    }
    if ($Kind -match '^user') {
        return "<a class=`"externallink`" href=`"https://portal.azure.com/#view/Microsoft_AAD_UsersAndTenants/UserProfileMenuBlade/~/overview/userId/$($Id)`" target=`"_blank`">$($encodedText)</a>"
    }
    if ($Kind -match '^group') {
        return "<a class=`"externallink`" href=`"https://portal.azure.com/#view/Microsoft_AAD_IAM/GroupDetailsMenuBlade/~/Overview/groupId/$($Id)/menuId/`" target=`"_blank`">$($encodedText)</a>"
    }
    return $encodedText
}
#an API permission linked to its AzAdvertizer page https://www.azadvertizer.net/azentraidapipermissions/<API appId>_<permission id>.html;
#the suffix _application/_delegated only when the API uses the id for both (AzAdvertizer: then the page without suffix lists both).
#$ResourceServicePrincipalId: object id of the API's Service Principal; $Type: 'application' (app role) or 'delegated' (scope);
#the API appId comes from the lookups of the run ($htServicePrincipalsAppRoleIdsToAppId, $htServicePrincipalsPublishedPermissionScopesIdsToAppId)
#or $ResourceAppId; without a GUID appId and permission id: text
function getSpiApiPermissionLinkHtml([string]$ResourceServicePrincipalId, [string]$PermissionId, [string]$Type, [string]$Text, [string]$ResourceAppId) {
    $encodedText = [HtmlSafe]::Encode($Text)
    $guidPattern = '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$'
    $key = "$($ResourceServicePrincipalId)_$($PermissionId)"
    $appIdAsAppRole = $(if ($htServicePrincipalsAppRoleIdsToAppId) { [string]$htServicePrincipalsAppRoleIdsToAppId[$key] })
    $appIdAsScope = $(if ($htServicePrincipalsPublishedPermissionScopesIdsToAppId) { [string]$htServicePrincipalsPublishedPermissionScopesIdsToAppId[$key] })
    $appId = $(if ($Type -eq 'application') { $appIdAsAppRole } else { $appIdAsScope })
    if (-not $appId) { $appId = $ResourceAppId }
    if ($appId -notmatch $guidPattern -or $PermissionId -notmatch $guidPattern) { return $encodedText }
    $suffix = $(if ($appIdAsAppRole -and $appIdAsScope) { "_$($Type)" } else { '' })
    return "<a class=`"externallink`" href=`"https://www.azadvertizer.net/azentraidapipermissions/$($appId.ToLowerInvariant())_$($PermissionId.ToLowerInvariant())$($suffix).html`" target=`"_blank`">$($encodedText)</a>"
}
#an Azure role name: built-in role -> AzAdvertizer (azrolesadvertizer/<id>.html); custom role -> the role definition in the Azure portal
#(https://portal.azure.com/#@<tenant>/resource<roleDefinition resource id>/overview). The resource id: the roleDefinitionId of the
#assignment when it is subscription based, else from the assignable scopes of the definition ($htCacheDefinitionsRole; a subscription
#scope first, else the first scope); without a usable id: text
#a built-in Entra role linked to its AzAdvertizer page (review F-6: the page needs '.html', the link was a 404); only GUID ids
function getSpiEntraRoleLinkHtml([string]$RoleDefinitionId, [string]$RoleName) {
    $encodedName = [HtmlSafe]::Encode($RoleName)
    if ($RoleDefinitionId -notmatch '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$') { return $encodedName }
    return "<a class=`"externallink`" href=`"https://www.azadvertizer.net/azentraidroles/$($RoleDefinitionId).html`" target=`"_blank`">$($encodedName)</a>"
}
function getSpiAzureRoleLinkHtml([string]$RoleId, [string]$RoleName, [string]$RoleType, [string]$RoleDefinitionId) {
    $encodedName = [HtmlSafe]::Encode($RoleName)
    if ($RoleId -notmatch '^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$') { return $encodedName }
    if ($RoleType -eq 'BuiltInRole') {
        return "<a class=`"externallink`" href=`"https://www.azadvertizer.net/azrolesadvertizer/$($RoleId).html`" target=`"_blank`">$($encodedName)</a>"
    }
    #review R-6: an orphaned assignment (role definition deleted, type 'unknown') has no page to link to
    if ($RoleType -ne 'CustomRole') { return $encodedName }
    $definitionResourceId = $null
    if ($RoleDefinitionId -match '^/subscriptions/[^/]+/providers/Microsoft\.Authorization/roleDefinitions/') {
        $definitionResourceId = $RoleDefinitionId
    }
    else {
        $assignableScopes = @($(if ($htCacheDefinitionsRole -and $htCacheDefinitionsRole[$RoleId]) { $htCacheDefinitionsRole[$RoleId].definition.properties.assignableScopes }) | Where-Object { $_ -match '^/(subscriptions/[^/]+|providers/Microsoft\.Management/managementGroups/[^/]+)$' })
        $scope = @(@($assignableScopes | Where-Object { $_ -like '/subscriptions/*' }) + $assignableScopes)[0]
        if ($scope) {
            $definitionResourceId = "$($scope)/providers/Microsoft.Authorization/roleDefinitions/$($RoleId)"
        }
    }
    if (-not $definitionResourceId) { return $encodedName }
    $href = "https://portal.azure.com/#@$($azAPICallConf['checkContext'].Tenant.Id)/resource$($definitionResourceId)/overview"
    return "<a class=`"externallink`" href=`"$([HtmlSafe]::Encode($href))`" target=`"_blank`">$($encodedName)</a>"
}
#owners grouped by applicability (direct / indirect)
function getSpiOwnerItemsHtml($Owners) {
    #review F-34: called twice per row of the SP section, most SPs have no owners
    if (-not $Owners) { return '' }
    $groups = [ordered]@{}
    if ($Owners) {
        foreach ($applicability in 'direct', 'indirect') {
            $items = @(foreach ($owner in @($Owners)) { if ($owner -and $owner.applicability -eq $applicability) { @{ MainHtml = (getSpiEntraObjectLinkHtml -Kind $owner.principalType -Id $owner.id -Text $owner.displayName); Dim = @($owner.principalType, $owner.id) } } })
            if ($items.Count) { $groups[$applicability] = $items }
        }
    }
    return (getSpiGroupedItemsHtml -Groups $groups)
}
#tags of the SP (without the tags the app propagates to it) or of the app; whitespace collapsed (tags can hold JSON)
function getSpiTagsHtml($Entry, [ValidateSet('SP', 'App')][string]$Of) {
    #review F-34: no tags - nothing to build; the html of a tag list is built once (tag lists repeat across SPs)
    if ($Of -eq 'App') { if (-not $Entry.APP.APPTags) { return '' } } elseif (-not $Entry.SP.SPTags) { return '' }
    $tags = $(if ($Of -eq 'App') { @($Entry.APP.APPTags) } elseif ($Entry.APP -and $Entry.SP.SPTags -and $Entry.APP.APPTags) { @($Entry.SP.SPTags).where({ $_ -notin $Entry.APP.APPTags }) } else { @($Entry.SP.SPTags) })
    if ($null -eq $script:spiTagsHtmlCache) { $script:spiTagsHtmlCache = [System.Collections.Hashtable]::new([System.StringComparer]::Ordinal) }
    $cacheKey = @($tags) -join "`n"
    $html = $script:spiTagsHtmlCache[$cacheKey]
    if ($null -eq $html) {
        $html = [string](getSpiItemsHtml -Items @(foreach ($tag in @($tags)) { if ($tag) { @{ Main = ([string]$tag -replace '\s+', ' ').Trim() } } }))
        $script:spiTagsHtmlCache[$cacheKey] = $html
    }
    return $html
}
#credentials (secrets, certificates): expired dimmed, expiring soon highlighted
function getSpiCredentialItemsHtml($Credentials, [switch]$Certificate) {
    $items = foreach ($credential in @($Credentials).where({ $_ })) {
        @{
            Main = $(if ($credential.displayName) { $credential.displayName } else { '(no name)' })
            Dim = @($credential.expiryInfo, $credential.endDateTimeFormated, $(if ($Certificate -and $credential.customKeyIdentifier) { "thumbprint $($credential.customKeyIdentifier)" }), "key id $($credential.keyId)")
            State = $(if ($credential.expiryInfo -eq 'expired') { 'muted' } elseif ($credential.expiryInfo -like 'expires soon*') { 'warn' } else { '' })
        }
    }
    return (getSpiItemsHtml -Items @($items))
}
#the app role of an appRoleAssignedTo entry (review F-42): display name, value, 'default access' or the id
function getSpiAppRoleAssignedToRoleText($Assignment) {
    if ($Assignment.roleDisplayName) { return [string]$Assignment.roleDisplayName }
    if ($Assignment.roleValue) { return [string]$Assignment.roleValue }
    if ($Assignment.roleId -eq '00000000-0000-0000-0000-000000000000') { return 'default access' }
    if ($Assignment.roleId) { return "app role $($Assignment.roleId)" }
    return ''
}
#federated identity credentials: name, then issuer / subject / audiences as lines
function getSpiFicItemsHtml($Credentials, [switch]$ManagedIdentity) {
    $items = foreach ($fic in @($Credentials).where({ $_ })) {
        $p = $(if ($ManagedIdentity) { $fic.properties } else { $fic })
        @{
            Main = $fic.name
            Details = [ordered]@{
                issuer = $p.issuer
                subject = $p.subject
                #review F-44: a flexible credential has no subject but a claims expression
                'claims expression' = $(if (-not $ManagedIdentity) { $fic.claimsMatchingExpression.value })
                audiences = (@($p.audiences | Sort-Object) -join ', ')
                description = $(if (-not $ManagedIdentity -and $fic.description) { $fic.description })
            }
        }
    }
    return (getSpiItemsHtml -Items @($items))
}
#endregion AG Grid emitter

#region portal shell
#The report in the Azure portal style of the Azure Governance Visualizer (portal theme 001_001): the report writes the pages,
#cards and notes in their final markup and a section registry; the hosted shell script adds navigation, routing, search
#and theme switching (azadspi-portal_<version>.js/.css, hosted on azadvertizer.net under a versioned name - not part of this repository).
$script:spiPortalAssetBase = 'https://www.azadvertizer.net/azadserviceprincipalinsights'
$script:spiPortalAssetVersion = '001_001'
#pages in navigation order; icon: Azure service icon (Microsoft Azure icons, only for pages about that product), fa: Font Awesome icon
$script:spiPages = [ordered]@{}
foreach ($spiPage in @(
        @{ id = 'Overview'; title = 'Overview'; fa = 'fa-gauge-high'; description = 'Findings and key figures of the tenant' }
        @{ id = 'Changes'; title = 'Changes'; fa = 'fa-clock-rotate-left'; description = 'Changes since the previous runs: objects, permissions, roles, credentials, owners and configuration of Service Principals and applications' }
        @{ id = 'ServicePrincipals'; title = 'Service Principals'; icon = '10225-icon-service-Enterprise-Applications.svg'; fa = 'fa-cubes'; description = 'Service Principals with their applications and managed identities' }
        @{ id = 'Ownership'; title = 'Ownership'; fa = 'fa-user-tie'; description = 'Owners of Service Principals and applications, objects owned by Service Principals' }
        @{ id = 'EntraRoles'; title = 'Entra roles'; icon = '10340-icon-service-Entra-Identity-Roles-and-Administrators.svg'; fa = 'fa-user-shield'; description = 'Microsoft Entra role assignments of Service Principals and roles assigned on Service Principals and applications' }
        @{ id = 'ApiPermissions'; title = 'API permissions'; fa = 'fa-plug'; description = 'Application permissions (app role assignments), delegated permissions (OAuth2 permission grants), users and groups assigned to applications' }
        @{ id = 'AzureRbac'; title = 'Azure RBAC'; fa = 'fa-shield-halved'; description = 'Azure role assignments of Service Principals (management groups, subscriptions, resource groups, resources)' }
        @{ id = 'Groups'; title = 'Groups'; icon = '10223-icon-service-Groups.svg'; fa = 'fa-users'; description = 'Group memberships of Service Principals' }
        @{ id = 'Credentials'; title = 'Credentials'; fa = 'fa-key'; description = 'Secrets, certificates and federated identity credentials of applications and Service Principals' }
        @{ id = 'ManagedIdentities'; title = 'Managed identities'; icon = '10227-icon-service-Managed-Identities.svg'; fa = 'fa-id-badge'; description = 'User assigned managed identities: associated Azure resources, assignment restrictions and isolation scope, federated identity credentials' }
        @{ id = 'Risk'; title = 'Risk'; fa = 'fa-triangle-exclamation'; description = 'Effective privilege, credential risk, third-party applications, configuration findings, cleanup candidates and consent settings' }
        @{ id = 'HighPrivilegeUsers'; title = 'High-privilege users'; icon = '10230-icon-service-Users.svg'; fa = 'fa-user'; description = 'Users that own Service Principals with classified permissions (HiPo users)' }
        @{ id = 'OptionalData'; title = 'Optional data'; fa = 'fa-puzzle-piece'; description = 'Data that needs an optional permission or feature: Entra recommendations, custom security attributes, agent objects' }
    )) {
    $spiPage.builder = [System.Text.StringBuilder]::new()
    $script:spiPages[$spiPage.id] = $spiPage
}
#sections: page, title (navigation and search), warn: a section with rows is a finding (orange marker, Overview)
$script:spiSectionCatalog = @{}
foreach ($spiSection in @(
        @{ id = 'FindingsOverview'; page = 'Overview'; title = 'Findings overview' }
        @{ id = 'ChangeHistory'; page = 'Changes'; title = 'Change history' }
        @{ id = 'ServicePrincipals'; page = 'ServicePrincipals'; title = 'Service Principals' }
        @{ id = 'ServicePrincipalOwners'; page = 'Ownership'; title = 'Service Principal owners' }
        @{ id = 'ApplicationOwners'; page = 'Ownership'; title = 'Application owners' }
        @{ id = 'ServicePrincipalOwnedObjects'; page = 'Ownership'; title = 'Objects owned by Service Principals' }
        @{ id = 'ManagedIdentityUserAssignedAssociatedAzureResources'; page = 'ManagedIdentities'; title = 'User-assigned managed identities: associated Azure resources' }
        @{ id = 'ManagedIdentityUserAssignedAssignmentRestrictions'; page = 'ManagedIdentities'; title = 'User-assigned managed identities: assignment restrictions and isolation scope' }
        @{ id = 'ServicePrincipalsAADRoleAssignments'; page = 'EntraRoles'; title = 'Entra role assignments of Service Principals' }
        @{ id = 'ServicePrincipalsAADRoleAssignedOn'; page = 'EntraRoles'; title = 'Entra roles assigned on Service Principals' }
        @{ id = 'ApplicationsAADRoleAssignedOn'; page = 'EntraRoles'; title = 'Entra roles assigned on applications' }
        @{ id = 'ServicePrincipalsAppRoleAssignments'; page = 'ApiPermissions'; title = 'Application permissions (app role assignments)' }
        @{ id = 'ServicePrincipalsAppRoleAssignedTo'; page = 'ApiPermissions'; title = 'Users and groups assigned to applications' }
        @{ id = 'ServicePrincipalsOauth2PermissionGrants'; page = 'ApiPermissions'; title = 'Delegated permissions (OAuth2 permission grants)' }
        @{ id = 'ServicePrincipalsAzureRoleAssignments'; page = 'AzureRbac'; title = 'Azure role assignments of Service Principals' }
        @{ id = 'ServicePrincipalsGroupMemberships'; page = 'Groups'; title = 'Group memberships of Service Principals' }
        @{ id = 'ServicePrincipalsEntraRecommendations'; page = 'OptionalData'; title = 'Service Principal Entra recommendations'; warn = $true }
        @{ id = 'ServicePrincipalsCustomSecurityAttributes'; page = 'OptionalData'; title = 'Service Principal custom security attributes' }
        @{ id = 'ApplicationSecrets'; page = 'Credentials'; title = 'Application secrets' }
        @{ id = 'ApplicationCertificates'; page = 'Credentials'; title = 'Application certificates' }
        @{ id = 'ApplicationFederatedIdentityCredentials'; page = 'Credentials'; title = 'Application federated identity credentials' }
        @{ id = 'ManagedIdentityFederatedIdentityCredentials'; page = 'ManagedIdentities'; title = 'User-assigned managed identities: federated identity credentials' }
        @{ id = 'Hipos'; page = 'HighPrivilegeUsers'; title = 'High-privilege (HiPo) users'; warn = $true }
        @{ id = 'ServicePrincipalCredentials'; page = 'Credentials'; title = 'Service Principal credentials (on the service principal, not on the app registration)'; warn = $true }
        @{ id = 'ApplicationConfigurationFindings'; page = 'Risk'; title = 'Application configuration findings'; warn = $true }
        @{ id = 'FederatedIdentityCredentialTrust'; page = 'Credentials'; title = 'Federated identity credentials: who can sign in' }
        @{ id = 'ServicePrincipalEffectivePrivilege'; page = 'Risk'; title = 'Service Principal effective privilege (incl. control through ownership)'; warn = $true }
        @{ id = 'ServicePrincipalCredentialRisk'; page = 'Risk'; title = 'Credential risk of privileged Service Principals'; warn = $true }
        @{ id = 'ThirdPartyAppExposure'; page = 'Risk'; title = 'Multi-tenant apps of other organizations with sensitive permissions'; warn = $true }
        @{ id = 'CleanupCandidates'; page = 'Risk'; title = 'Cleanup candidates'; warn = $true }
        @{ id = 'AgentObjects'; page = 'OptionalData'; title = 'Agent objects (Microsoft Entra Agent ID)' }
        @{ id = 'TenantConsentSettings'; page = 'Risk'; title = 'Tenant consent settings' }
    )) {
    $script:spiSectionCatalog[$spiSection.id] = $spiSection
}
$script:spiSections = [ordered]@{}
$script:spiCurrentSection = $null
$script:spiOpenSectionBuilder = $null

function useSpiSection {
    <#
    .SYNOPSIS
    Starts a report section on its page: closes the previous section, writes the opening tag of the card (Kind card: the
    caller writes the head and the body) or of the note (Kind note: the caller writes its text) and returns the builder of
    the page, which the caller uses for the markup of the section. A grid written while the section is open counts its rows.
    #>
    param(
        [Parameter(Mandatory)][string]$Id,
        [ValidateSet('card', 'note')][string]$Kind = 'card',
        #note: 'empty' (nothing found) or 'disabled' (not collected: parameter, missing permission)
        [ValidateSet('empty', 'disabled')][string]$NoteState = 'empty',
        #card: open from the start (the head must be written with -Open too)
        [switch]$Open
    )
    closeSpiSection
    $catalogEntry = $script:spiSectionCatalog[$Id]
    if (-not $catalogEntry) {
        throw "useSpiSection: section '$Id' is not in the section catalog (spiSectionCatalog)"
    }
    $builder = $script:spiPages[$catalogEntry.page].builder
    $section = [ordered]@{
        id = $Id
        page = $catalogEntry.page
        title = $catalogEntry.title
        kind = $Kind
        count = $(if ($Kind -eq 'note' -and $NoteState -eq 'empty') { 0 } else { $null })
        state = $(if ($Kind -eq 'note') { $NoteState } else { 'ok' })
        warn = [bool]$catalogEntry.warn
        grids = [System.Collections.Generic.List[string]]::new()
    }
    $script:spiSections[$Id] = $section
    $script:spiCurrentSection = $section
    if ($Kind -eq 'card') {
        [void]$builder.AppendLine("<section class=`"az-card$(if ($Open) { ' open' })`" id=`"card-$Id`" data-section=`"$Id`">")
    }
    else {
        [void]$builder.AppendLine("<section class=`"az-note $(if ($NoteState -eq 'disabled') { 'az-note-muted' } else { 'az-note-neutral' })`" id=`"card-$Id`" data-section=`"$Id`">")
    }
    $script:spiOpenSectionBuilder = $builder
    return $builder
}

function closeSpiSection {
    if ($script:spiOpenSectionBuilder) {
        [void]$script:spiOpenSectionBuilder.AppendLine('</section>')
        $script:spiOpenSectionBuilder = $null
    }
    $script:spiCurrentSection = $null
}

#the builder of a page outside of a section (Overview: key figures)
function useSpiPage([string]$Id) {
    closeSpiSection
    return $script:spiPages[$Id].builder
}

function getSpiPageIconHtml($Page, [string]$Class) {
    if ($Page.icon) {
        return "<img class=`"$Class`" src=`"$($script:spiPortalAssetBase)/icon/$($Page.icon)`" alt=`"`" data-fa=`"$($Page.fa)`">"
    }
    return "<i class=`"fa $($Page.fa) $Class`" aria-hidden=`"true`"></i>"
}

#final state of the sections: a card without rows is empty, a card with rows of a finding section is a warning
function getSpiSectionStates {
    foreach ($section in $script:spiSections.Values) {
        if ($section.kind -eq 'card') {
            if ($section.count -eq 0) { $section.state = 'empty' }
            elseif ($section.warn -and $section.count -gt 0) { $section.state = 'warning' }
            else { $section.state = 'ok' }
        }
    }
}

#the section registry: inert JSON for the shell (navigation, search, counts, deep links)
function getSpiRegistryHtml {
    param(
        [hashtable]$Report
    )
    getSpiSectionStates
    $registry = [ordered]@{
        report = $Report
        pages = @(foreach ($page in $script:spiPages.Values) {
                [ordered]@{
                    id = $page.id
                    title = $page.title
                    description = $page.description
                    icon = $(if ($page.icon) { "$($script:spiPortalAssetBase)/icon/$($page.icon)" } else { $null })
                    fa = $page.fa
                }
            })
        sections = @(foreach ($section in $script:spiSections.Values) {
                [ordered]@{
                    id = $section.id
                    page = $section.page
                    title = $section.title
                    count = $section.count
                    state = $section.state
                    grids = @($section.grids)
                }
            })
    }
    #EscapeHtml: no '<', '>' or '&' in the element content (no '</script>' from data)
    $json = $registry | ConvertTo-Json -Depth 6 -Compress -EscapeHandling EscapeHtml
    return "<script type=`"application/json`" id=`"azadspiRegistry`">$json</script>"
}

#the pages with their head, toolbar and the sections written by function summary
function getSpiPagesHtml {
    closeSpiSection
    $sb = [System.Text.StringBuilder]::new()
    foreach ($page in $script:spiPages.Values) {
        $cardCount = @($script:spiSections.Values.where({ $_.page -eq $page.id -and $_.kind -eq 'card' })).Count
        $encodedTitle = [HtmlSafe]::Encode($page.title)
        [void]$sb.AppendLine("<div class=`"az-page`" id=`"page-$($page.id)`" data-page=`"$($page.id)`">")
        [void]$sb.AppendLine("<div class=`"az-page-head`"><div class=`"az-breadcrumb`"><a href=`"#Overview`">$([HtmlSafe]::Encode($Product))</a> <span class=`"az-crumb-sep`">&rsaquo;</span> <span>$encodedTitle</span></div><h1><span class=`"az-page-icon`">$(getSpiPageIconHtml -Page $page -Class 'az-page-icon-img')</span><span class=`"az-page-title`">$encodedTitle</span></h1><div class=`"az-subtitle`">$([HtmlSafe]::Encode($page.description))</div></div>")
        if ($cardCount -gt 1) {
            [void]$sb.AppendLine('<div class="az-page-toolbar"><button type="button" class="az-btn az-expand-all"><i class="fa fa-angles-down" aria-hidden="true"></i><span>Expand all</span></button><button type="button" class="az-btn az-collapse-all"><i class="fa fa-angles-up" aria-hidden="true"></i><span>Collapse all</span></button></div>')
        }
        [void]$sb.AppendLine('<div class="az-page-body">')
        [void]$sb.Append($page.builder.ToString())
        [void]$sb.AppendLine('</div>')
        [void]$sb.AppendLine("</div><!--page-$($page.id)-->")
    }
    return $sb.ToString()
}

#head: theme before the first paint, watchdog, the hosted stylesheet and shell script
function getSpiPortalHeadHtml {
    return @"
    <script>
        //the stored theme is applied before the first paint (no flash of the other theme); the AG Grid theme follows (data-ag-theme-mode)
        (function () { var root = document.documentElement, t = null; try { t = localStorage.getItem('azadspi.theme'); } catch (e) { } if (t === 'dark' || t === 'light') { root.setAttribute('data-theme', t); } else { t = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches ? 'dark' : 'light'; } root.setAttribute('data-ag-theme-mode', t); })();
        //fallback when the portal shell did not start (its script blocked or not reachable): the deferred shell builds before
        //'load', so az-ready missing a second after 'load' means it never ran - show the plain report instead of the spinner
        window.addEventListener('load', function () {
            setTimeout(function () {
                var root = document.documentElement;
                if (!root.classList.contains('az-ready')) {
                    root.classList.add('az-ready', 'az-noshell');
                    if (typeof window.spiCreateVisibleGrids === 'function') { window.spiCreateVisibleGrids(); }
                    if (window.console) { console.warn('$($Product): the portal shell did not start - showing the plain report'); }
                }
            }, 1000);
        });
    </script>
    <link rel="stylesheet" type="text/css" href="$($script:spiPortalAssetBase)/css/azadspi-portal_$($script:spiPortalAssetVersion).css">
    <script src="$($script:spiPortalAssetBase)/js/azadspi-portal_$($script:spiPortalAssetVersion).js" defer></script>
"@
}

#top bar, navigation container and the start of the main container
function getSpiShellStartHtml {
    param(
        [string]$TenantId,
        [string]$ReportDate,
        [string]$UpdateHtml
    )
    $encodedProduct = [HtmlSafe]::Encode($Product)
    return @"
    <header class="az-topbar">
        <button type="button" class="az-iconbtn" id="azNavToggle" aria-label="Toggle navigation" title="Toggle navigation"></button>
        <div class="az-brand"><i class="fa fa-id-badge az-brand-icon" aria-hidden="true"></i><span class="az-brand-name">$encodedProduct</span><span class="az-brand-short" aria-hidden="true">AzADSPI</span> <small id="azVersion">$([HtmlSafe]::Encode($ProductVersion))</small></div>
        <div class="az-search"><input type="search" id="azSearch" placeholder="Search sections (/)" aria-label="Search sections" autocomplete="off" spellcheck="false"></div>
        $UpdateHtml
        <div class="az-meta" id="azMeta">$([HtmlSafe]::Encode($TenantId)) <span class="az-dim">&middot; $([HtmlSafe]::Encode($ReportDate))</span></div>
        <a class="az-iconbtn az-topbar-link" href="https://github.com/JulianHayward/AzADServicePrincipalInsights" target="_blank" rel="noopener" aria-label="$encodedProduct on GitHub" title="$encodedProduct on GitHub"><i class="fa fa-github" aria-hidden="true"></i></a>
        <a class="az-iconbtn az-topbar-link" href="https://www.linkedin.com/in/julianhayward" target="_blank" rel="noopener" aria-label="Julian Hayward on LinkedIn" title="Julian Hayward on LinkedIn"><i class="fa fa-linkedin-square" aria-hidden="true"></i></a>
        <button type="button" class="az-iconbtn" id="azThemeToggle" aria-label="Switch theme" title="Switch theme"></button>
    </header>
    <div class="az-shell">
    <nav class="az-nav" id="azNav" aria-label="Report navigation"></nav>
    <main class="az-main" id="azMain">
"@
}

#the card head of a section (the shell adds the page icon and the count); the title is html
function getSpiCardHeadHtml([string]$TitleHtml, [switch]$Open) {
    return "<div class=`"az-card-head`" role=`"button`" tabindex=`"0`" aria-expanded=`"$(if ($Open) { 'true' } else { 'false' })`"><span class=`"az-card-title`">$TitleHtml</span><span class=`"az-chevron`" aria-hidden=`"true`"></span></div>"
}
#endregion portal shell

function summary() {
    Write-Host ' Building Summary'

    #application id by object id (Service Principals and applications of this run): links of names in list cells
    $script:htSpiAppIdByObjectId = @{}
    foreach ($entry in $cu) {
        $entrySP = @($entry.SP)[0]
        if ($entrySP.SPObjectId -and $entrySP.SPAppId) { $script:htSpiAppIdByObjectId[[string]$entrySP.SPObjectId] = [string]$entrySP.SPAppId }
        $entryApp = @($entry.APP)[0]
        if ($entryApp.APPObjectId -and $entryApp.APPAppClientId) { $script:htSpiAppIdByObjectId[[string]$entryApp.APPObjectId] = [string]$entryApp.APPAppClientId }
    }

    #every section starts with useSpiSection, which returns the builder of its page ($htmlTenantSummary)
    $htmlTenantSummary = $null

    #region SUMMARYFindingsOverview
    $insightRows = getInsightRows -entries $cu -tenantId $azAPICallConf['checkContext'].Tenant.Id -htAgentObjects $htAgentObjectsById
    $keyFigures = @(
        [ordered]@{ name = 'Tenant'; value = $azAPICallConf['checkContext'].Tenant.Id }
        [ordered]@{ name = 'Report date'; value = $spiReportDate }
        [ordered]@{ name = 'Service Principals'; value = $cu.where( { $_.ObjectType -like 'SP*' } ).Count }
        [ordered]@{ name = 'Managed identities'; value = $cu.where( { $_.ObjectType -like 'SP MI*' } ).Count }
        [ordered]@{ name = 'Applications (app registrations)'; value = $cu.where( { $_.APP } ).Count }
        [ordered]@{ name = 'Run parameters'; html = "<a href=`"#`" class=`"az-params-link`">$(@($paramsUsed).Count) parameters</a>" }
    )
    $changeHistoryRows = $null
    $changeHistoryReadIssue = ''
    $changeHistoryNotRead = $false
    if (-not $NoChangeHistory) {
        #an error in the history never fails the run (review R-10)
        try {
            $changeHistoryRows = @(getChangeHistoryRows -LogPath $script:changeHistoryLogPath -Days $ChangeHistoryDays -NowUtc (Get-Date).ToUniversalTime())
            if ($script:changeHistoryUnreadLines -gt 0) {
                $changeHistoryReadIssue = "$($script:changeHistoryUnreadLines) line(s) of $(Split-Path -Leaf $script:changeHistoryLogPath) could not be read and were skipped."
            }
        }
        catch {
            $changeHistoryRows = @()
            $changeHistoryNotRead = $true
            $changeHistoryReadIssue = "$(Split-Path -Leaf $script:changeHistoryLogPath) could not be read: $($_.Exception.Message)"
        }
        #review F-35: a failed update of this run is shown as well
        if ($changeHistoryUpdateIssue) {
            $changeHistoryReadIssue = (@($changeHistoryReadIssue, "$($changeHistoryUpdateIssue)." ).where({ $_ })) -join ' '
        }
        if ($changeHistoryReadIssue) {
            Write-Host "  Change history: $($changeHistoryReadIssue)" -ForegroundColor Yellow
        }
    }
    addFindingsOverview -overviewRows (getFindingsOverviewRows -insightRows $insightRows -consentSettings $tenantConsentSettings -htEntraRecommendations $htEntraRecommendationsBySPId -htAgentObjects $htAgentObjectsById -changeHistoryRows $changeHistoryRows -changeHistoryDays $ChangeHistoryDays -changeHistoryRunRecord $changeHistoryRunRecord -changeHistoryIssue $changeHistoryReadIssue -changeHistoryNotRead $changeHistoryNotRead) -keyFigures $keyFigures -collectionFailures @($htParallelFailures.Values) -outOfScopeSubscriptions @($outOfScopeSubscriptions)
    #review F-18 (added file): the subscriptions that were not collected
    if (-not $NoCsvExport -and @($outOfScopeSubscriptions).Count -gt 0) {
        @($outOfScopeSubscriptions) | Sort-Object -Property subscriptionName, subscriptionId | Select-Object -Property subscriptionId, subscriptionName, outOfScopeReason, ManagementGroupId, ManagementGroupName, Level | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_SubscriptionsOutOfScope.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
    }
    #endregion SUMMARYFindingsOverview

    #region SUMMARYChangeHistory
    if ($NoChangeHistory) {
        addReportSection -Region 'ChangeHistory' -Title 'Change history' -NotCollectedReason 'parameter NoChangeHistory'
    }
    else {
        $changeHistoryNote = "Changes between the runs of $($Product) in this output folder for the same tenant and scope, newest first; the full history is kept in $(Split-Path -Leaf $script:changeHistoryLogPath) (one JSON line per change, no time limit). " +
        'Compared by ID: objects added or removed (not Microsoft first-party apps, Microsoft adds them), effective privilege, Entra roles, critical Azure roles, application and delegated permissions, credentials (not of managed identities, Azure rotates them), federated credentials, direct owners, group memberships, names, enabled/disabled, sign-in audience, redirect and identifier URIs. ' +
        "Objects with collection errors in a run keep their previous state (the next complete run reports their changes); a run with another scope starts a new baseline. Period: parameter ChangeHistoryDays ($ChangeHistoryDays)."
        addReportSection -Region 'ChangeHistory' -Title "Changes in the last $ChangeHistoryDays days" -Description $changeHistoryNote -Warning $changeHistoryReadIssue -Rows $changeHistoryRows -Columns @(
            @{ Header = 'Time'; Property = 'Time' }
            @{ Header = 'Category'; Property = 'Category'; Select = $true }
            @{ Header = 'Change'; Property = 'Change'; Select = $true }
            @{ Header = 'Object type'; Property = 'ObjectType'; Select = $true }
            @{ Header = 'Display name'; Property = 'DisplayName' }
            @{ Header = 'Object Id'; Property = 'ObjectId' }
            @{ Header = 'Detail'; Property = 'Detail' }
            @{ Header = 'Note'; Property = 'Note' }
            @{ Header = 'Script version'; Property = 'ScriptVersion'; Select = $true }
        )
    }
    #endregion SUMMARYChangeHistory

    #region SUMMARYServicePrincipals
    $htmlTenantSummary = useSpiSection -Id 'ServicePrincipals'
    [void]$htmlTenantSummary.AppendLine(@'
    <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">Service Principals</span><span class="az-chevron" aria-hidden="true"></span></div>
    <div class="az-card-body">
'@)

    #set before the 'if': the duration below is written in any case
    $startCustPolLoop = Get-Date
    if ($cu.Count -gt 0) {
        Write-Host '  processing Summary ServicePrincipals'

        $htmlTableId = 'TenantSummary_ServicePrincipals'
        $tf = "tf$($htmlTableId)"

        $categoryColorsMax = @('rgb(1,0,103)', 'rgb(213,255,0)', 'rgb(255,0,86)', 'rgb(158,0,142)', 'rgb(14,76,161)', 'rgb(255,229,2)', 'rgb(0,95,57)', 'rgb(0,255,0)', 'rgb(149,0,58)', 'rgb(255,147,126)', 'rgb(164,36,0)', 'rgb(0,21,68)', 'rgb(145,208,203)', 'rgb(98,14,0)', 'rgb(107,104,130)', 'rgb(0,0,255)', 'rgb(0,125,181)', 'rgb(106,130,108)', 'rgb(0,0,0)', 'rgb(0,174,126)', 'rgb(194,140,159)', 'rgb(190,153,112)', 'rgb(0,143,156)', 'rgb(95,173,78)', 'rgb(255,0,0)', 'rgb(255,0,246)', 'rgb(255,2,157)', 'rgb(104,61,59)', 'rgb(255,116,163)', 'rgb(150,138,232)', 'rgb(152,255,82)', 'rgb(167,87,64)', 'rgb(1,255,254)', 'rgb(255,238,232)', 'rgb(254,137,0)', 'rgb(189,198,255)', 'rgb(1,208,255)', 'rgb(187,136,0)', 'rgb(117,68,177)', 'rgb(165,255,210)', 'rgb(255,166,254)', 'rgb(119,77,0)', 'rgb(122,71,130)', 'rgb(38,52,0)', 'rgb(0,71,84)', 'rgb(67,0,44)', 'rgb(181,0,255)', 'rgb(255,177,103)', 'rgb(255,219,102)', 'rgb(144,251,146)', 'rgb(126,45,210)', 'rgb(189,211,147)', 'rgb(229,111,254)', 'rgb(222,255,116)', 'rgb(0,255,120)', 'rgb(0,155,255)', 'rgb(0,100,1)', 'rgb(0,118,255)', 'rgb(133,169,0)', 'rgb(0,185,23)', 'rgb(120,130,49)', 'rgb(0,255,198)', 'rgb(255,110,65)', 'rgb(232,94,190)')
        #returns $count colors starting at $offset, wrapping around the palette (64 entries); 0 colors for $count 0
        function getChartColors([int]$offset, [int]$count) {
            if ($count -le 0) {
                return @()
            }
            return @(for ($i = 0; $i -lt $count; $i++) { $categoryColorsMax[($offset + $i) % $categoryColorsMax.Count] })
        }

        $groupedByOrg = $cu.SP.where( { $_.SPAppOwnerOrganizationId } ) | Group-Object -Property SPAppOwnerOrganizationId

        $arrOrgCounts = [System.Collections.ArrayList]@()
        $arrOrgIds = [System.Collections.ArrayList]@()
        foreach ($grp in $groupedByOrg | Sort-Object -Property count -Descending) {
            $null = $arrOrgCounts.Add($grp.Count)
            $null = $arrOrgIds.Add($grp.Name)
        }
        $OrgCounts = "'{0}'" -f ($arrOrgCounts -join "','")
        #review F-10: labels as JSON (EscapeHtml: no </script>, no quote breaks the script)
        $OrgIds = ConvertTo-Json -InputObject @($arrOrgIds) -Compress -EscapeHandling EscapeHtml

        $categoryColorsOrg = getChartColors 0 $arrOrgIds.Count
        $categoryColorsSeperatedOrg = "'{0}'" -f ($categoryColorsOrg -join "','")

        $groupedBySPType = $cu.ObjectType | Group-Object

        $arrSPTypeCounts = [System.Collections.ArrayList]@()
        $arrSPTypes = [System.Collections.ArrayList]@()
        foreach ($grp in $groupedBySPType | Sort-Object -Property count -Descending) {
            $null = $arrSPTypeCounts.Add($grp.Count)
            $null = $arrSPTypes.Add($grp.Name)
        }
        $SPTypeCounts = "'{0}'" -f ($arrSPTypeCounts -join "','")
        $SPTypes = ConvertTo-Json -InputObject @($arrSPTypes) -Compress -EscapeHandling EscapeHtml

        $categoryColorsSPType = getChartColors $arrOrgIds.Count $arrSPTypes.Count
        $categoryColorsSeperatedSPType = "'{0}'" -f ($categoryColorsSPType -join "','")

        $groupedByMIResourceType = $cu.where( { $_.ObjectType -like 'SP MI*' } ).ManagedIdentity.resourceType | Group-Object

        $arrMIResTypeCounts = [System.Collections.ArrayList]@()
        $arrMIResTypes = [System.Collections.ArrayList]@()
        $arrMIResTypeTerms = [System.Collections.ArrayList]@()
        foreach ($grp in $groupedByMIResourceType | Sort-Object -Property count -Descending) {
            $null = $arrMIResTypeCounts.Add($grp.Count)
            $null = $arrMIResTypes.Add($grp.Name -replace '^Microsoft\.')
            #review F-33: the click filters the full type (equals); the shortened label matched longer types (virtualMachineScaleSets)
            $null = $arrMIResTypeTerms.Add([string]$grp.Name)
        }
        $MIResTypeCounts = "'{0}'" -f ($arrMIResTypeCounts -join "','")
        $MIResTypes = ConvertTo-Json -InputObject @($arrMIResTypes) -Compress -EscapeHandling EscapeHtml
        $MIResTypeTerms = ConvertTo-Json -InputObject @($arrMIResTypeTerms) -Compress -EscapeHandling EscapeHtml

        #offset after the Org and SP type colors (was OrgIds + MIResTypes, which reused or overran colors)
        $categoryColorsMIResType = getChartColors ($arrOrgIds.Count + $arrSPTypes.Count) $arrMIResTypes.Count
        $categoryColorsSeperatedMIResType = "'{0}'" -f ($categoryColorsMIResType -join "','")

        $SPAppINT = $cu.where( { $_.ObjectType -eq 'SP APP INT' } )

        #notes
        #review F-38: also 'SP INT' (a service principal of another type of this tenant)
        $SPAppINTSPAppEXTSPEXT = $cu.where( { $_.ObjectType -eq 'SP APP INT' -or $_.ObjectType -eq 'SP APP EXT' -or $_.ObjectType -eq 'SP EXT' -or $_.ObjectType -eq 'SP INT' } )
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
        <div class="az-charts">
            <div class="az-chart-grid">
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
                            labels: $($OrgIds),
                            borderWidth:0.5,
                        }
                    ]
                },
                options: {
                    responsive: false,
                    plugins: {
                        legend: {
                            display: false,
                        },
                        tooltip: {
                            bodyFont: { size: 10 },
                            callbacks: {
                                label: function (tooltipItem) {
                                    var dataset = tooltipItem.dataset;
                                    var index = tooltipItem.dataIndex;
                                    window. datasetitem = tooltipItem.datasetIndex;
                                    window.target = dataset.labels[index];
                                    return dataset.labels[index] + ': ' + dataset.data[index];
                                }
                            }
                        }
                    },

                    //Chart.js 4: filter by the clicked slice (was: the slice of the last tooltip, so a tap on touch devices used a stale or no value)
                    onClick: (e, elements, chart) => {
                        if (elements.length == 0) {
                            return;
                        }
                        chart.options.plugins.tooltip.callbacks.label({ dataset: chart.data.datasets[elements[0].datasetIndex], datasetIndex: elements[0].datasetIndex, dataIndex: elements[0].index });
                        if (window. datasetitem == 0){
                            window. targetcolumn = '5'
                        }
                        spiChartFilter('$($tf -replace '^tf')', [[window. targetcolumn, window.target]]);

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
                            labels: $($SPTypes),
                            borderWidth:0.5,
                        }
                    ]
                },
                options: {
                    responsive: false,
                    plugins: {
                        legend: {
                            display: false,
                        },
                        tooltip: {
                            bodyFont: { size: 10 },
                            callbacks: {
                                label: function (tooltipItem) {
                                    var dataset = tooltipItem.dataset;
                                    var index = tooltipItem.dataIndex;
                                    window. datasetitem = tooltipItem.datasetIndex;
                                    window.target = dataset.labels[index];
                                    return dataset.labels[index] + ': ' + dataset.data[index];
                                }
                            }
                        }
                    },

                    //Chart.js 4: filter by the clicked slice (was: the slice of the last tooltip, so a tap on touch devices used a stale or no value)
                    onClick: (e, elements, chart) => {
                        if (elements.length == 0) {
                            return;
                        }
                        chart.options.plugins.tooltip.callbacks.label({ dataset: chart.data.datasets[elements[0].datasetIndex], datasetIndex: elements[0].datasetIndex, dataIndex: elements[0].index });
                        if (window. datasetitem == 0){
                            window. targetcolumn = '6'
                        }
                        spiChartFilter('$($tf -replace '^tf')', [[window. targetcolumn, window.target]]);

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
                            labels: $($MIResTypes),
                            terms: $($MIResTypeTerms),
                            borderWidth:0.5,
                        }
                    ]
                },
                options: {
                    responsive: false,
                    plugins: {
                        legend: {
                            display: false,
                        },
                        tooltip: {
                            bodyFont: { size: 10 },
                            callbacks: {
                                label: function (tooltipItem) {
                                    var dataset = tooltipItem.dataset;
                                    var index = tooltipItem.dataIndex;
                                    window. datasetitem = tooltipItem.datasetIndex;
                                    window.target = dataset.labels[index];
                                    return dataset.labels[index] + ': ' + dataset.data[index];
                                }
                            }
                        }
                    },

                    //Chart.js 4: filter by the clicked slice (was: the slice of the last tooltip, so a tap on touch devices used a stale or no value)
                    onClick: (e, elements, chart) => {
                        if (elements.length == 0) {
                            return;
                        }
                        chart.options.plugins.tooltip.callbacks.label({ dataset: chart.data.datasets[elements[0].datasetIndex], datasetIndex: elements[0].datasetIndex, dataIndex: elements[0].index });
                        var clickedDataset = chart.data.datasets[elements[0].datasetIndex];
                        spiChartFilter('$($tf -replace '^tf')', [['16', clickedDataset.terms ? clickedDataset.terms[elements[0].index] : window.target]]);

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
                    plugins: {
                        legend: {
                            display: false,
                        },
                        tooltip: {
                            bodyFont: { size: 10 },
                            callbacks: {
                                label: function (tooltipItem) {
                                    var dataset = tooltipItem.dataset;
                                    var index = tooltipItem.dataIndex;
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
                        }
                    },

                    //Chart.js 4: filter by the clicked slice (was: the slice of the last tooltip, so a tap on touch devices used a stale or no value)
                    onClick: (e, elements, chart) => {
                        if (elements.length == 0) {
                            return;
                        }
                        chart.options.plugins.tooltip.callbacks.label({ dataset: chart.data.datasets[elements[0].datasetIndex], datasetIndex: elements[0].datasetIndex, dataIndex: elements[0].index });
                        window. extratargetcolumn = '6'
                        if (window. datasetitem == 0){
                            window. targetcolumn = '4'
                        }
                        if (window. datasetitem == 1){
                            window. targetcolumn = '11'
                        }
                        spiChartFilter('$($tf -replace '^tf')', [[window. extratargetcolumn, window.extratarget], [window. targetcolumn, window.target]]);

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
                    plugins: {
                        legend: {
                            display: false,
                        },
                        tooltip: {
                            bodyFont: { size: 10 },
                            callbacks: {
                                label: function (tooltipItem) {
                                    var dataset = tooltipItem.dataset;
                                    var index = tooltipItem.dataIndex;
                                    window. datasetitem = tooltipItem.datasetIndex;
                                    window.target = dataset.labels[index];
                                    return dataset.labels[index] + ': ' + dataset.data[index];
                                }
                            }
                        }
                    },

                    //Chart.js 4: filter by the clicked slice (was: the slice of the last tooltip, so a tap on touch devices used a stale or no value)
                    onClick: (e, elements, chart) => {
                        if (elements.length == 0) {
                            return;
                        }
                        chart.options.plugins.tooltip.callbacks.label({ dataset: chart.data.datasets[elements[0].datasetIndex], datasetIndex: elements[0].datasetIndex, dataIndex: elements[0].index });
                        if (window. datasetitem == 0){
                            window. targetcolumn = '3'
                        }
                        spiChartFilter('$($tf -replace '^tf')', [[window. targetcolumn, window.target]]);

                    }
                }
});

</script>
"@)

        $abbrMIRelict = "<abbr title=`"relict: the associated Azure Resource does not exist (anymore)&#13;`"><i class=`"fa fa-question-circle`" aria-hidden=`"true`"></i></abbr>"
        $spiGridRows = [System.Collections.ArrayList]@()

        $servicePrincipals4CSVExport = [System.Collections.ArrayList]@()
        foreach ($sp in ($cu)) {

            $spType = $sp.ObjectType

            if ($spType -eq 'SP APP INT' -or $spType -eq 'SP APP EXT' -or $spType -eq 'SP EXT' -or $spType -eq 'SP INT') {
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

            if ($spType -eq 'SP APP INT' -or $spType -eq 'SP APP EXT' -or $spType -eq 'AppOnly') {
                if ([string]::IsNullOrWhiteSpace($sp.APP.APPNotes)) {
                    $appNotes = 'notesNotSet'
                }
                else {
                    $appNotes = $sp.APP.APPNotes
                }
            }
            else {
                $appNotes = 'notesN/A'
            }

            $appObjectId = ''
            $appId = ''
            $appDisplayName = ''
            $appTags = ''
            $spTags = $sp.SP.SPTags -join "$CsvDelimiterOpposite "
            if ($sp.APP) {
                $appObjectId = $sp.APP.APPObjectId
                $appId = $sp.APP.APPAppClientId
                $appDisplayName = $sp.APP.APPDisplayName
                $appTags = $sp.APP.APPTags -join "$CsvDelimiterOpposite "
                if ($sp.SP.SPTags -and $sp.APP.APPTags) {
                    #App tags are automatically propagated to the service principal, removing redundant information (SP tags that are not App tags; Compare-Object -PassThru also returned the App-only tags).
                    $spTags = @($sp.SP.SPTags).where( { $_ -notin $sp.APP.APPTags } ) -join "$CsvDelimiterOpposite "
                }
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
                    $array = [System.Collections.ArrayList]@()
                    foreach ($owner in $sp.SPOwners) {
                        $null = $array.Add("$($owner.applicability) - $($owner.displayName) $($owner.principalType) $($owner.id)")
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
                    $array = [System.Collections.ArrayList]@()
                    foreach ($owner in $sp.APPAppOwners) {
                        $null = $array.Add("$($owner.applicability) - $($owner.displayName) $($owner.principalType) $($owner.id)")
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

            $miAssignedToResourcesCount = ''
            $miAssignedToResources = ''
            if ($spType -eq 'SP MI System assigned') {
                $miAssignedToResourcesCount = 1
                $miAssignedToResources = $SP.ManagedIdentity.alternativeName
            }
            if ($spType -eq 'SP MI User assigned' -and $NoAzureResourceSideRelations) {
                #review F-36: the attachments were not collected - not 0
                $miAssignedToResources = 'not collected (parameter NoAzureResourceSideRelations)'
            }
            elseif ($spType -eq 'SP MI User assigned') {
                $miAssignedToResourcesCount = @($SP.ManagedIdentityAssociatedAzureResources).Count
                if ($miAssignedToResourcesCount -gt 0) {
                    #CSV: plain text; the HTML version ($miAssignedToResourcesHtml below) adds the <i>
                    $miAssignedToResources = 'check Managed Identity User Assigned - associated Azure Resources'
                }
                else {
                    $miAssignedToResources = ''
                }
            }
            #HTML: the UAMI hint is static markup; the system assigned MI value is a resource id (data) and gets encoded
            if ($spType -eq 'SP MI User assigned' -and $NoAzureResourceSideRelations) {
                $miAssignedToResourcesHtml = [HtmlSafe]::Encode($miAssignedToResources)
            }
            elseif ($spType -eq 'SP MI User assigned') {
                $miAssignedToResourcesHtml = $(if ($miAssignedToResources) { 'see <a href="#ManagedIdentityUserAssignedAssociatedAzureResources">User-assigned managed identities: associated Azure resources</a>' } else { '' })
            }
            else {
                $miAssignedToResourcesHtml = getSpiPortalResourceLinkHtml $miAssignedToResources
            }

            $spiGridRow = [ordered]@{}
            $spiGridRow.c0 = $sp.SP.SPObjectId
            $spiGridRow.c1 = $sp.SP.SPAppId
            $spiGridRow.c2 = $sp.SP.SPDisplayName
            $spiGridRow.c3 = $spNotes
            $spiGridRow.c4 = $spOwners
            $spiGridRow.c4Html = getSpiOwnerItemsHtml -Owners $sp.SPOwners
            $spiGridRow.c5 = $sp.SP.SPAppOwnerOrganizationId
            $spiGridRow.c6 = $spType
            $spiGridRow.c7 = $spTags
            $spiGridRow.c7Html = getSpiTagsHtml -Entry $sp -Of SP
            $spiGridRow.c8 = $appObjectId
            $spiGridRow.c9 = $appId
            $spiGridRow.c10 = $appDisplayName
            $spiGridRow.c11 = $appOwners
            $spiGridRow.c11Html = getSpiOwnerItemsHtml -Owners $sp.APPAppOwners
            $spiGridRow.c12 = $hasApp
            $spiGridRow.c13 = $appSignInAudience
            $spiGridRow.c14 = $appTags
            $spiGridRow.c14Html = getSpiTagsHtml -Entry $sp -Of App
            $spiGridRow.c15 = $appNotes
            $spiGridRow.c16 = $miResourceType
            $spiGridRow.c16Html = getSpiResourceTypeLinkHtml $miResourceType
            $spiGridRow.c17 = $miResourceScope
            $spiGridRow.c18 = $miRelict
            $spiGridRow.c19 = $miAssignedToResourcesCount
            $spiGridRow.c20Html = @"
$($miAssignedToResourcesHtml)
"@
            $spiGridRow.c20 = $(if ($spiGridRow.c20Html) { ConvertTo-SpiPlainText $spiGridRow.c20Html } else { '' })   #review F-34
            $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)
            if (-not $NoCsvExport) {
                $null = $servicePrincipals4CSVExport.Add([PSCustomObject]@{
                        SPObjectId = $sp.SP.SPObjectId
                        SPAppId = $sp.SP.SPAppId
                        SPDisplayName = $sp.SP.SPDisplayName
                        SPNotes = $spNotes
                        SPOwners = $spOwners
                        SPAppOwnerOrganizationId = $sp.SP.SPAppOwnerOrganizationId
                        SPType = $spType
                        SPTags = $spTags
                        AppObjectId = $appObjectId
                        AppClientId = $appId
                        AppDisplayName = $appDisplayName
                        AppOwners = $appOwners
                        HasAppRegistration = $hasApp
                        AppSignInAudience = $appSignInAudience
                        AppTags = $appTags
                        AppNotes = $appNotes
                        MIResourceType = $miResourceType
                        MIResourceScope = $miResourceScope
                        MIRelict = $miRelict
                        MIAssociatedResourceCount = $miAssignedToResourcesCount
                        MIAssociatedResources = $miAssignedToResources
                    })
            }
        }

        if (-not $NoCsvExport) {
            $servicePrincipals4CSVExport | Sort-Object -Property SPDisplayName, SPObjectId, AppDisplayName, AppObjectId | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_ServicePrincipals.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
        }
        $servicePrincipals4CSVExport = $null

        [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle 'Service Principals' -ColumnDefinitions @(
                    @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                    @{ header = 'SP application Id'; property = 'c1' }
                    @{ header = 'SP displayName'; property = 'c2' }
                    @{ header = 'SP notes'; property = 'c3' }
                    @{ header = 'SP Owners'; property = 'c4'; htmlProperty = 'c4Html' }
                    @{ header = 'SP App Owner Organization Id'; property = 'c5'; filter = 'select' }
                    @{ header = 'Type'; property = 'c6'; filter = 'select' }
                    @{ header = 'SP Tags'; property = 'c7'; htmlProperty = 'c7Html' }
                    @{ header = 'App object Id'; property = 'c8'; portal = 'application'; appIdProperty = 'c9' }
                    @{ header = 'App application (client) Id'; property = 'c9' }
                    @{ header = 'App displayName'; property = 'c10' }
                    @{ header = 'App Owners'; property = 'c11'; htmlProperty = 'c11Html' }
                    @{ header = 'AppReg'; property = 'c12'; filter = 'select' }
                    @{ header = 'App SignIn Audience'; property = 'c13'; filter = 'select' }
                    @{ header = 'App Tags'; property = 'c14'; htmlProperty = 'c14Html' }
                    @{ header = 'App Notes'; property = 'c15' }
                    @{ header = 'MI Resource type'; property = 'c16'; htmlProperty = 'c16Html'; filter = 'select' }
                    @{ header = 'MI Resource scope'; property = 'c17' }
                    @{ header = (ConvertTo-SpiPlainText @"
MI Relict $abbrMIRelict
"@); property = 'c18'; filter = 'select'; tooltip = (getSpiHtmlTitle @"
MI Relict $abbrMIRelict
"@)
                    }
                    @{ header = 'MI associated Resource(s) count'; property = 'c19'; filter = 'number' }
                    @{ header = 'MI associated Resource(s)'; property = 'c20'; htmlProperty = 'c20Html' }
                )))
        $spiGridRows = $null
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
        $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalOwners'
        [void]$htmlTenantSummary.AppendLine(@'
        <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">Service Principal owners</span><span class="az-chevron" aria-hidden="true"></span></div>
        <div class="az-card-body">
'@)

        $htmlTableId = 'TenantSummary_ServicePrincipalOwners'
        $spiGridRows = [System.Collections.ArrayList]@()

        $servicePrincipalOwners4CSVExport = [System.Collections.ArrayList]@()
        foreach ($sp in ($cu.where( { $_.SPOwners.Count -gt 0 } ))) {

            $spType = $sp.ObjectType
            foreach ($ownerinfo in $sp.SPOwners) {
                $ownerOwnedBy = ''
                $ownerOwnedByHtml = ''
                $ownerDisplayName = "$($ownerinfo.displayName)"
                $ownerPrincipalType = "$($ownerinfo.principalType)"
                $ownerId = "$($ownerinfo.id)"
                $ownerApplicability = $($ownerinfo.applicability)

                if ($ownerPrincipalType -like 'SP*') {
                    $ownerOwnedByCell = getOwnerOwnedByCell -OwnerEntries $htSPOwnersFinal[$ownerinfo.id] -SelfId $(if ($sp.SP) { @($sp.SP)[0].SPObjectId })
                    $ownerOwnedBy = $ownerOwnedByCell.Csv
                    $ownerOwnedByHtml = $ownerOwnedByCell.Html
                }

                $spiGridRow = [ordered]@{}
                $spiGridRow.c0 = $sp.SP.SPObjectId
                $spiGridRow.c1 = $sp.SP.SPAppId
                $spiGridRow.c2 = $sp.SP.SPDisplayName
                $spiGridRow.c3 = $sp.SP.SPAppOwnerOrganizationId
                $spiGridRow.c4 = $spType
                $spiGridRow.c5 = $ownerDisplayName
                $spiGridRow.c6 = $ownerPrincipalType
                $spiGridRow.c7 = $ownerId
                $spiGridRow.c8 = $ownerApplicability
                $spiGridRow.c9Html = @"
$($ownerOwnedByHtml)
"@
                $spiGridRow.c9 = ConvertTo-SpiPlainText $spiGridRow.c9Html
                #how an indirect owner controls this SP (report only)
                $spiGridRow.c10 = $(if ($ownerApplicability -eq 'indirect') { [string](@($htSPOwnersFinal[$sp.SP.SPObjectId]).where({ $_.id -eq $ownerId -and $_.applicability -eq 'indirect' }) | Select-Object -First 1).via } else { '' })
                $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)
                #one CSV row per owner (was: one row per SP holding the last owner's values)
                if (-not $NoCsvExport) {
                    $null = $servicePrincipalOwners4CSVExport.Add([PSCustomObject]@{
                            SPObjectId = $sp.SP.SPObjectId
                            SPAppId = $sp.SP.SPAppId
                            SPDisplayName = $sp.SP.SPDisplayName
                            SPAppOwnerOrganizationId = $sp.SP.SPAppOwnerOrganizationId
                            SPType = $spType
                            OwnerDisplayName = $ownerDisplayName
                            OwnerPrincipalType = $ownerPrincipalType
                            OwnerId = $ownerId
                            OwnerApplicability = $ownerApplicability
                            OwnerOwnedBy = $ownerOwnedBy
                        })
                }
            }
        }
        if (-not $NoCsvExport) {
            $servicePrincipalOwners4CSVExport | Sort-Object -Property SPDisplayName, SPObjectId, OwnerApplicability, OwnerDisplayName, OwnerId | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_ServicePrincipalOwners.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
        }
        $servicePrincipalOwners4CSVExport = $null

        [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle 'Service Principal owners' -ColumnDefinitions @(
                    @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                    @{ header = 'SP application Id'; property = 'c1' }
                    @{ header = 'SP displayName'; property = 'c2' }
                    @{ header = 'SP App Owner Organization Id'; property = 'c3'; filter = 'select' }
                    @{ header = 'Type'; property = 'c4'; filter = 'select' }
                    @{ header = 'Owner DisplayName'; property = 'c5' }
                    @{ header = 'Owner PrincipalType'; property = 'c6'; filter = 'select' }
                    @{ header = 'Owner Id'; property = 'c7'; portal = 'principal'; typeProperty = 'c6' }
                    @{ header = 'Owner Applicability'; property = 'c8'; filter = 'select' }
                    @{ header = 'Indirect via'; property = 'c10'; tooltip = 'how an indirect owner controls this Service Principal: owner (or app registration owner) of a Service Principal that controls the direct owner of this Service Principal' }
                    @{ header = 'Owner OwnedBy'; property = 'c9'; htmlProperty = 'c9Html'; tooltip = 'who controls this owner Service Principal: direct = its owners, indirect = principals that control those (through ownership of the Service Principal or of its app registration), with the path' }
                )))
        $spiGridRows = $null

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalOwners' -Kind note
        [void]$htmlTenantSummary.AppendLine(@'
    <i class="fa fa-ban" aria-hidden="true"></i> <span>Service Principal owners</span>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalOwners

    #region SUMMARYApplicationOwners
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary Application Owners'

    if ($cu.APPAppOwners.Count -gt 0) {
        $htmlTenantSummary = useSpiSection -Id 'ApplicationOwners'
        [void]$htmlTenantSummary.AppendLine(@'
        <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">Application owners</span><span class="az-chevron" aria-hidden="true"></span></div>
        <div class="az-card-body">
'@)

        $htmlTableId = 'TenantSummary_ApplicationOwners'
        $spiGridRows = [System.Collections.ArrayList]@()
        $applicationOwners4CSVExport = [System.Collections.ArrayList]@()
        foreach ($sp in ($cu.where( { $_.APPAppOwners.Count -gt 0 } ))) {

            $spType = $sp.ObjectType

            foreach ($ownerinfo in $sp.APPAppOwners) {
                $ownerOwnedBy = ''
                $ownerOwnedByHtml = ''
                $ownerDisplayName = "$($ownerinfo.displayName)"
                $ownerPrincipalType = "$($ownerinfo.principalType)"
                $ownerId = "$($ownerinfo.id)"
                $ownerApplicability = $($ownerinfo.applicability)

                if ($ownerPrincipalType -like 'SP*') {
                    $ownerOwnedByCell = getOwnerOwnedByCell -OwnerEntries $htSPOwnersFinal[$ownerinfo.id] -SelfId $(if ($sp.SP) { @($sp.SP)[0].SPObjectId })
                    $ownerOwnedBy = $ownerOwnedByCell.Csv
                    $ownerOwnedByHtml = $ownerOwnedByCell.Html
                }

                $spiGridRow = [ordered]@{}
                $spiGridRow.c0 = $sp.APP.APPObjectId
                $spiGridRow.c1 = $sp.APP.APPAppClientId
                $spiGridRow.c2 = $sp.APP.APPDisplayName
                $spiGridRow.c3 = $sp.SP.SPAppOwnerOrganizationId
                $spiGridRow.c4 = $spType
                $spiGridRow.c5 = $ownerDisplayName
                $spiGridRow.c6 = $ownerPrincipalType
                $spiGridRow.c7 = $ownerId
                $spiGridRow.c8 = $ownerApplicability
                $spiGridRow.c9Html = @"
$($ownerOwnedByHtml)
"@
                $spiGridRow.c9 = ConvertTo-SpiPlainText $spiGridRow.c9Html
                $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)
                #one CSV row per owner (was: one row per App holding the last owner's values)
                if (-not $NoCsvExport) {
                    $null = $applicationOwners4CSVExport.Add([PSCustomObject]@{
                            AppObjectId = $sp.APP.APPObjectId
                            AppClientId = $sp.APP.APPAppClientId
                            AppDisplayName = $sp.APP.APPDisplayName
                            SPAppOwnerOrganizationId = $sp.SP.SPAppOwnerOrganizationId
                            SPType = $spType
                            OwnerDisplayName = $ownerDisplayName
                            OwnerPrincipalType = $ownerPrincipalType
                            OwnerId = $ownerId
                            OwnerApplicability = $ownerApplicability
                            OwnerOwnedBy = $ownerOwnedBy
                        })
                }
            }
        }

        if (-not $NoCsvExport) {
            $applicationOwners4CSVExport | Sort-Object -Property AppDisplayName, AppObjectId, OwnerApplicability, OwnerDisplayName, OwnerId | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_ApplicationOwners.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
        }
        $applicationOwners4CSVExport = $null

        [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle 'Application owners' -ColumnDefinitions @(
                    @{ header = 'App object Id'; property = 'c0'; portal = 'application'; appIdProperty = 'c1' }
                    @{ header = 'App application Id'; property = 'c1' }
                    @{ header = 'App displayName'; property = 'c2' }
                    @{ header = 'SP App Owner Organization Id'; property = 'c3'; filter = 'select' }
                    @{ header = 'Type'; property = 'c4'; filter = 'select' }
                    @{ header = 'Owner DisplayName'; property = 'c5' }
                    @{ header = 'Owner PrincipalType'; property = 'c6'; filter = 'select' }
                    @{ header = 'Owner Id'; property = 'c7'; portal = 'principal'; typeProperty = 'c6' }
                    @{ header = 'Owner Applicability'; property = 'c8'; filter = 'select' }
                    @{ header = 'Owner OwnedBy'; property = 'c9'; htmlProperty = 'c9Html'; tooltip = 'who controls this owner Service Principal: direct = its owners, indirect = principals that control those (through ownership of the Service Principal or of its app registration), with the path' }
                )))
        $spiGridRows = $null

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        $htmlTenantSummary = useSpiSection -Id 'ApplicationOwners' -Kind note
        [void]$htmlTenantSummary.AppendLine(@'
        <i class="fa fa-ban" aria-hidden="true"></i> <span>Application owners</span>
'@)
    }


    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYApplicationOwners

    #region SUMMARYServicePrincipalOwnedObjects
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary ServicePrincipal Owned Objects'

    if ($cu.SPOwnedObjects.Count -gt 0) {
        $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalOwnedObjects'
        [void]$htmlTenantSummary.AppendLine(@'
        <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">Objects owned by Service Principals</span><span class="az-chevron" aria-hidden="true"></span></div>
        <div class="az-card-body">
'@)

        #one row per SP with owned objects (was the number of owned objects, so paging/row counter were wrong)
        $htmlTableId = 'TenantSummary_ServicePrincipalOwnedObjects'
        $spiGridRows = [System.Collections.ArrayList]@()
        $servicePrincipalOwnedObjects4CSVExport = [System.Collections.ArrayList]@()
        foreach ($sp in ($cu.where( { $_.SPOwnedObjects.Count -gt 0 } ))) {

            $spType = $sp.ObjectType
            $arrayOwnedObjects = [System.Collections.ArrayList]@()
            $ownedObjectItemsByType = [ordered]@{}
            #scriptblocks: these entries are [ordered] dictionaries, Sort-Object -Property does not sort them
            foreach ($ownedObject in $sp.SPOwnedObjects | Sort-Object { $_.type }, { $_.typeDetailed }, { $_.displayName }) {
                $null = $arrayOwnedObjects.Add("$([HtmlSafe]::Encode($ownedObject.displayName)) <b>$([HtmlSafe]::Encode($ownedObject.type))</b> $([HtmlSafe]::Encode($ownedObject.objectId))")
                if (-not $ownedObjectItemsByType.Contains([string]$ownedObject.type)) { $ownedObjectItemsByType[[string]$ownedObject.type] = [System.Collections.ArrayList]@() }
                $null = $ownedObjectItemsByType[[string]$ownedObject.type].Add(@{ MainHtml = (getSpiEntraObjectLinkHtml -Kind $ownedObject.type -Id $ownedObject.objectId -Text $ownedObject.displayName); Dim = @($ownedObject.objectId) })

                if (-not $NoCsvExport) {
                    $null = $servicePrincipalOwnedObjects4CSVExport.Add([PSCustomObject]@{
                            SPObjectId = $sp.SP.SPObjectId
                            SPAppId = $sp.SP.SPAppId
                            SPDisplayName = $sp.SP.SPDisplayName
                            SPAppOwnerOrganizationId = $sp.SP.SPAppOwnerOrganizationId
                            SPType = $spType
                            OwnedObjectDisplayName = $ownedObject.displayName
                            OwnedObjectType = $ownedObject.type
                            OwnedObjectId = $ownedObject.objectId
                        })
                }
            }

            $spiGridRow = [ordered]@{}
            $spiGridRow.c0 = $sp.SP.SPObjectId
            $spiGridRow.c1 = $sp.SP.SPAppId
            $spiGridRow.c2 = $sp.SP.SPDisplayName
            $spiGridRow.c3 = $sp.SP.SPAppOwnerOrganizationId
            $spiGridRow.c4 = $spType
            $spiGridRow.c5Html = getSpiGroupedItemsHtml -Groups $ownedObjectItemsByType
            $spiGridRow.c5 = ConvertTo-SpiPlainText "$($arrayOwnedObjects.Count) ($($arrayOwnedObjects -join ', '))"
            $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)

        }
        if (-not $NoCsvExport) {
            $servicePrincipalOwnedObjects4CSVExport | Sort-Object -Property SPDisplayName, SPObjectId, OwnedObjectDisplayName, OwnedObjectId | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_ServicePrincipalOwnedObjects.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
        }
        $servicePrincipalOwnedObjects4CSVExport = $null

        [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle 'Objects owned by Service Principals' -ColumnDefinitions @(
                    @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                    @{ header = 'SP application Id'; property = 'c1' }
                    @{ header = 'SP displayName'; property = 'c2' }
                    @{ header = 'SP App Owner Organization Id'; property = 'c3'; filter = 'select' }
                    @{ header = 'Type'; property = 'c4'; filter = 'select' }
                    @{ header = 'Owned Objects'; property = 'c5'; htmlProperty = 'c5Html' }
                )))
        $spiGridRows = $null

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalOwnedObjects' -Kind note
        [void]$htmlTenantSummary.AppendLine(@'
            <i class="fa fa-ban" aria-hidden="true"></i> <span>Objects owned by Service Principals</span>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalOwnedObjects

    if (-not $NoAzureResourceSideRelations) {
        #region SUMMARYManagedIdentityUserAssignedAssociatedAzureResources

        Write-Host '  processing TenantSummary Managed Identity User Assigned associated Azure Resources'
        $arrayUserAssignedIdentities4ResourcesCount = $arrayUserAssignedIdentities4Resources.Count
        $startUserAssignedIdentities4Resources = Get-Date

        if ($arrayUserAssignedIdentities4ResourcesCount -gt 0) {

            $htUserAssignedIdentitiesAssignedResources = @{}
            $htResourcesAssignedUserAssignedIdentities = @{}
            foreach ($entry in $arrayUserAssignedIdentities4Resources) {
                #UserAssignedIdentities
                if (-not $htUserAssignedIdentitiesAssignedResources[$entry.miPrincipalId]) {
                    $htUserAssignedIdentitiesAssignedResources[$entry.miPrincipalId] = @{}
                    $htUserAssignedIdentitiesAssignedResources[$entry.miPrincipalId].ResourcesCount = 1
                }
                else {
                    $htUserAssignedIdentitiesAssignedResources[$entry.miPrincipalId].ResourcesCount++
                }
                #Resources
                if (-not $htResourcesAssignedUserAssignedIdentities[($entry.resourceId).tolower()]) {
                    $htResourcesAssignedUserAssignedIdentities[($entry.resourceId).tolower()] = @{}
                    $htResourcesAssignedUserAssignedIdentities[($entry.resourceId).tolower()].UserAssignedIdentitiesCount = 1
                }
                else {
                    $htResourcesAssignedUserAssignedIdentities[($entry.resourceId).tolower()].UserAssignedIdentitiesCount++
                }
            }

            $htmlTenantSummary = useSpiSection -Id 'ManagedIdentityUserAssignedAssociatedAzureResources'
            [void]$htmlTenantSummary.AppendLine(@'
        <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">User-assigned managed identities: associated Azure resources</span><span class="az-chevron" aria-hidden="true"></span></div>
        <div class="az-card-body">
'@)

            $htmlTableId = 'TenantSummary_ManagedIdentityUserAssignedAssociatedAzureResources'

            $spiGridRows = [System.Collections.ArrayList]@()

            $userAssignedIdentities4Resources4CSVExport = [System.Collections.ArrayList]@()
            foreach ($miResEntry in $arrayUserAssignedIdentities4Resources | Sort-Object -Property miResourceId, resourceId) {
                $spiGridRow = [ordered]@{}
                $spiGridRow.c0 = $miResEntry.miResourceName
                $spiGridRow.c1 = $miResEntry.miMgPath
                $spiGridRow.c2 = $miResEntry.miSubscriptionName
                $spiGridRow.c3 = $miResEntry.miSubscriptionId
                $spiGridRow.c4 = $miResEntry.miResourceGroupName
                $spiGridRow.c5 = $miResEntry.miResourceId
                $spiGridRow.c5Html = getSpiPortalResourceLinkHtml $miResEntry.miResourceId
                $spiGridRow.c6 = $miResEntry.miPrincipalId
                $spiGridRow.c7 = $miResEntry.miClientId
                $spiGridRow.c8 = $htUserAssignedIdentitiesAssignedResources[$miResEntry.miPrincipalId].ResourcesCount
                $spiGridRow.c9 = $miResEntry.resourceName
                $spiGridRow.c10 = $miResEntry.resourceType
                $spiGridRow.c10Html = getSpiResourceTypeLinkHtml $miResEntry.resourceType
                $spiGridRow.c11 = $miResEntry.resourceMgPath
                $spiGridRow.c12 = $miResEntry.resourceSubscriptionName
                $spiGridRow.c13 = $miResEntry.resourceSubscriptionId
                $spiGridRow.c14 = $miResEntry.resourceResourceGroupName
                $spiGridRow.c15 = $miResEntry.resourceId
                $spiGridRow.c15Html = getSpiPortalResourceLinkHtml $miResEntry.resourceId
                $spiGridRow.c16 = $htResourcesAssignedUserAssignedIdentities[($miResEntry.resourceId).tolower()].UserAssignedIdentitiesCount
                $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)

                if (-not $NoCsvExport) {
                    $null = $userAssignedIdentities4Resources4CSVExport.Add([PSCustomObject]@{
                            MIName = $miResEntry.miResourceName
                            MIMgPath = $miResEntry.miMgPath
                            MISubscriptionName = $miResEntry.miSubscriptionName
                            MISubscriptionId = $miResEntry.miSubscriptionId
                            MIResourceGroup = $miResEntry.miResourceGroupName
                            MIResourceId = $miResEntry.miResourceId
                            MIAADSPObjectId = $miResEntry.miPrincipalId
                            MIAADSPApplicationId = $miResEntry.miClientId
                            MICountResAssignments = $htUserAssignedIdentitiesAssignedResources[$miResEntry.miPrincipalId].ResourcesCount
                            ResName = $miResEntry.resourceName
                            ResType = $miResEntry.resourceType
                            ResMgPath = $miResEntry.resourceMgPath
                            ResSubscriptionName = $miResEntry.resourceSubscriptionName
                            ResSubscriptionId = $miResEntry.resourceSubscriptionId
                            ResResourceGroup = $miResEntry.resourceResourceGroupName
                            ResId = $miResEntry.resourceId
                            ResCountAssignedMIs = $htResourcesAssignedUserAssignedIdentities[($miResEntry.resourceId).tolower()].UserAssignedIdentitiesCount
                        })
                }

            }

            if (-not $NoCsvExport) {
                Write-Host "   Exporting ManagedIdentityUserAssignedAssociatedAzureResources CSV '$($outputPath)$($DirectorySeparatorChar)$($fileName)_ManagedIdentityUserAssignedAssociatedAzureResources.csv'"
                $userAssignedIdentities4Resources4CSVExport | Sort-Object -Property MIResourceId, ResId | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_ManagedIdentityUserAssignedAssociatedAzureResources.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
            }

            [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle 'User-assigned managed identities: associated Azure resources' -ColumnDefinitions @(
                        @{ header = 'MI Name'; property = 'c0' }
                        @{ header = 'MI MgPath'; property = 'c1' }
                        @{ header = 'MI Subscription Name'; property = 'c2' }
                        @{ header = 'MI Subscription Id'; property = 'c3' }
                        @{ header = 'MI ResourceGroup'; property = 'c4' }
                        @{ header = 'MI ResourceId'; property = 'c5'; htmlProperty = 'c5Html' }
                        @{ header = 'MI SP object Id'; property = 'c6'; portal = 'servicePrincipal'; appIdProperty = 'c7' }
                        @{ header = 'MI SP application Id'; property = 'c7' }
                        @{ header = 'MI count Res assignments'; property = 'c8'; filter = 'number' }
                        @{ header = 'Res Name'; property = 'c9' }
                        @{ header = 'Res Type'; property = 'c10'; htmlProperty = 'c10Html'; filter = 'select' }
                        @{ header = 'Res MgPath'; property = 'c11' }
                        @{ header = 'Res Subscription Name'; property = 'c12' }
                        @{ header = 'Res Subscription Id'; property = 'c13' }
                        @{ header = 'Res ResourceGroup'; property = 'c14' }
                        @{ header = 'Res Id'; property = 'c15'; htmlProperty = 'c15Html' }
                        @{ header = 'Res count assigned MIs'; property = 'c16'; filter = 'number' }
                    )))
            $spiGridRows = $null
            [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
        }
        else {
            $htmlTenantSummary = useSpiSection -Id 'ManagedIdentityUserAssignedAssociatedAzureResources' -Kind note
            [void]$htmlTenantSummary.AppendLine(@'
        <i class="fa fa-ban" aria-hidden="true"></i> <span>User-assigned managed identities: associated Azure resources</span>
'@)
        }
        $endUserAssignedIdentities4Resources = Get-Date
        Write-Host "   Managed Identity User Assigned associated Azure Resources processing duration: $((New-TimeSpan -Start $startUserAssignedIdentities4Resources -End $endUserAssignedIdentities4Resources).TotalMinutes) minutes ($((New-TimeSpan -Start $startUserAssignedIdentities4Resources -End $endUserAssignedIdentities4Resources).TotalSeconds) seconds)"
        #endregion SUMMARYManagedIdentityUserAssignedAssociatedAzureResources

        $uamiSettingsRows = @(getUserAssignedIdentitySettingsRows -Settings $htUserAssignedIdentitiesSettings.Values -Attachments $arrayUserAssignedIdentities4Resources -FicByMI $htMIUserAssignedFederatedCreds -PolicyAssignments @($htCacheAssignmentsPolicy.Values.Assignment) -Delimiter "$CsvDelimiterOpposite ")
        #report only: resource types linked to AzAdvertizer, the resource ID to the Azure portal (the CSV keeps the text)
        foreach ($uamiSettingsRow in $uamiSettingsRows) {
            $uamiSettingsRow | Add-Member -NotePropertyName MIResourceIdHtml -NotePropertyValue (getSpiPortalResourceLinkHtml $uamiSettingsRow.MIResourceId)
            $uamiSettingsRow | Add-Member -NotePropertyName AssignmentRestrictionsHtml -NotePropertyValue (getSpiResourceTypeListHtml -Text $uamiSettingsRow.AssignmentRestrictions -Delimiter "$CsvDelimiterOpposite ")
            $uamiSettingsRow | Add-Member -NotePropertyName AttachedResourceTypesHtml -NotePropertyValue (getSpiResourceTypeListHtml -Text $uamiSettingsRow.AttachedResourceTypes -Delimiter "$CsvDelimiterOpposite ")
        }
        addReportSection -Region 'ManagedIdentityUserAssignedAssignmentRestrictions' -Title 'User-assigned managed identities: assignment restrictions and isolation scope' -Description $script:spiUamiSettingsNote -Rows $uamiSettingsRows -Columns @(
            @{ Header = 'MI Name'; Property = 'MIName' }, @{ Header = 'MI Subscription Name'; Property = 'MISubscriptionName' }, @{ Header = 'MI Subscription Id'; Property = 'MISubscriptionId' }, @{ Header = 'MI ResourceGroup'; Property = 'MIResourceGroup' },
            @{ Header = 'MI Location'; Property = 'MILocation'; Select = $true }, @{ Header = 'MI ResourceId'; Property = 'MIResourceId'; HtmlProperty = 'MIResourceIdHtml' }, @{ Header = 'MI SP object Id'; Property = 'MIAADSPObjectId' },
            @{ Header = 'Isolation scope'; Property = 'IsolationScope'; Select = $true }, @{ Header = 'Assignment restrictions'; Property = 'AssignmentRestrictions'; HtmlProperty = 'AssignmentRestrictionsHtml'; Select = $true },
            @{ Header = 'Usage'; Property = 'Usage'; Select = $true }, @{ Header = 'Attached resources'; Property = 'AttachedResourcesCount'; Number = $true }, @{ Header = 'Attached resource types'; Property = 'AttachedResourceTypes'; HtmlProperty = 'AttachedResourceTypesHtml' }, @{ Header = 'Attached regions'; Property = 'AttachedRegions' },
            @{ Header = 'Federated identity credentials'; Property = 'FederatedIdentityCredentialsCount'; Number = $true }, @{ Header = 'Suggestion'; Property = 'Suggestion' })
    }
    else {
        $htmlTenantSummary = useSpiSection -Id 'ManagedIdentityUserAssignedAssociatedAzureResources' -Kind note -NoteState disabled
        [void]$htmlTenantSummary.AppendLine(@"
            <i class="fa fa-ban" aria-hidden="true"></i> <span>User-assigned managed identities: associated Azure resources (parameter NoAzureResourceSideRelations = $NoAzureResourceSideRelations)</span>
"@)
        addReportSection -Region 'ManagedIdentityUserAssignedAssignmentRestrictions' -Title 'User-assigned managed identities: assignment restrictions and isolation scope' -NotCollectedReason "parameter NoAzureResourceSideRelations = $NoAzureResourceSideRelations"
    }

    #region SUMMARYServicePrincipalsAADRoleAssignments
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary ServicePrincipalsAADRoleAssignments'
    $servicePrincipalsAADRoleAssignments = $cu.where( { $_.SPAADRoleAssignments.Count -ne 0 } )
    $servicePrincipalsAADRoleAssignmentsCount = $servicePrincipalsAADRoleAssignments.Count
    if ($servicePrincipalsAADRoleAssignmentsCount -gt 0) {
        $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalsAADRoleAssignments'
        [void]$htmlTenantSummary.AppendLine(@'
        <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">Entra role assignments of Service Principals</span><span class="az-chevron" aria-hidden="true"></span></div>
        <div class="az-card-body">
'@)
        [void]$htmlTenantSummary.AppendLine((getThomasCreditHtml -What 'Critical Entra roles (as Microsoft marks them privileged) and Entra roles through role-assignable groups'))

        $htmlTableId = 'TenantSummary_ServicePrincipalsAADRoleAssignments'
        $spiGridRows = [System.Collections.ArrayList]@()

        $cntRow = 0
        $arrayServicePrincipalsAADRoleAssignments4CSV = [System.Collections.ArrayList]@()
        foreach ($sp in ($servicePrincipalsAADRoleAssignments)) {
            $cntRow++
            $cnt = 0

            $spType = $sp.ObjectType
            $appTags = ''
            $spTags = $sp.SP.SPTags -join "$CsvDelimiterOpposite "
            if ($sp.APP) {
                $appTags = $sp.APP.APPTags -join "$CsvDelimiterOpposite "
                if ($sp.SP.SPTags -and $sp.APP.APPTags) {
                    #App tags are automatically propagated to the service principal, removing redundant information (SP tags that are not App tags; Compare-Object -PassThru also returned the App-only tags).
                    $spTags = @($sp.SP.SPTags).where( { $_ -notin $sp.APP.APPTags } ) -join "$CsvDelimiterOpposite "
                }
            }
            $spAADRoleAssignments = $null
            $raItems = [System.Collections.ArrayList]@()
            if (($sp.SPAADRoleAssignments)) {
                if (($sp.SPAADRoleAssignments.count -gt 0)) {
                    $array = [System.Collections.ArrayList]@()
                    $arrayPlain = [System.Collections.ArrayList]@()
                    $cnt = 0
                    $roleClassification = ''
                    foreach ($ra in $sp.SPAADRoleAssignments) {
                        $cnt++
                        #review F-7: the scope also when it is not an SP/app of this run (administrative unit, group, user)
                        $raScopeText = $(if ($ra.scopeDetail) { $ra.scopeDetail } elseif ($ra.directoryScopeId -and $ra.directoryScopeId -ne '/') { $ra.directoryScopeId } elseif ($ra.resourceScope -and $ra.resourceScope -ne '/') { $ra.resourceScope })

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

                        #HTML fragment: data parts encoded, link/markup kept
                        $raRoleDefinitionName = [HtmlSafe]::Encode($ra.roleDefinitionName)

                        if ($ra.roleType -eq 'BuiltIn') {
                            #$raRoleDefinitionName = "<a class=`"externallink`" href=`"https://github.com/MicrosoftDocs/azure-docs/blob/main/articles/active-directory/roles/permissions-reference.md#$($ra.roleDefinitionName -replace ' ', '-')`" target=`"_blank`">$($ra.roleDefinitionName)</a>"
                            $raRoleDefinitionName = (getSpiEntraRoleLinkHtml -RoleDefinitionId $ra.roleDefinitionId -RoleName $ra.roleDefinitionName)
                        }

                        $faIcon = ''
                        if ($ra.roleIsCritical -eq $true) {
                            $roleClassification = 'critical'
                            $faIcon = '<i class="fa fa-exclamation-triangle" aria-hidden="true" style="color: #ff5e00; font-size: 9px;"></i> '
                        }

                        #through a group: same notation as the Azure RBAC table
                        $indirectRef = ''
                        if ($ra.roleAssignmentApplicability -eq 'indirect') {
                            $indirectRef = " [$([HtmlSafe]::Encode($ra.roleAssignmentApplicability)) - $([HtmlSafe]::Encode($ra.roleAssignmentAppliesThrough))]"
                        }
                        if ($raScopeText) {
                            $null = $array.Add("$faIcon<span $class><b>$([HtmlSafe]::Encode($ra.roleType))</b> '$($raRoleDefinitionName)' $([HtmlSafe]::Encode($ra.roleDefinitionId)) (scope: $([HtmlSafe]::Encode($raScopeText)))$indirectRef</span>")
                        }
                        else {
                            $null = $array.Add("$faIcon<span $class><b>$([HtmlSafe]::Encode($ra.roleType))</b> '$($raRoleDefinitionName)' $([HtmlSafe]::Encode($ra.roleDefinitionId))$indirectRef</span>")
                        }
                        #review F-45: the plain text (sort, filter, grid CSV export) from the values, not from the markup above
                        $null = $arrayPlain.Add("$($ra.roleType) '$($ra.roleDefinitionName)' $($ra.roleDefinitionId)$(if ($raScopeText) { " (scope: $($raScopeText))" })$(if ($ra.roleAssignmentApplicability -eq 'indirect') { " [$($ra.roleAssignmentApplicability) - $($ra.roleAssignmentAppliesThrough)]" })")

                        $null = $raItems.Add(@{ MainHtml = "$(getSpiCriticalIconHtml ($ra.roleIsCritical -eq $true))$raRoleDefinitionName"; Dim = @($ra.roleType, $(if ($raScopeText) { "scope: $($raScopeText)" }), $(if ($ra.roleAssignmentApplicability -eq 'indirect') { "through group $($ra.roleAssignmentAppliesThrough)" }), $ra.roleDefinitionId) })
                        $null = $arrayServicePrincipalsAADRoleAssignments4CSV.Add([PSCustomObject]@{
                                SPObjectType = $sp.ObjectType
                                SPObjectId = $sp.ObjectId
                                SPDisplayName = $sp.SP.SPDisplayName
                                SPTags = $spTags
                                AppTags = $appTags
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
                                #per row; $roleClassification is the per-SP aggregate for the HTML and depended on row order here
                                Classification = $(if ($ra.roleIsCritical -eq $true) { 'critical' } else { '' })
                                RoleAssignmentApplicability = $ra.roleAssignmentApplicability
                                RoleAssignmentAppliesThrough = $ra.roleAssignmentAppliesThrough
                            })
                    }
                    $spAADRoleAssignments = "$($array -join '<br>')"

                }
                else {
                    $spAADRoleAssignments = $null
                }
            }

            $spiGridRow = [ordered]@{}
            $spiGridRow.c0 = $sp.SP.SPObjectId
            $spiGridRow.c1 = $sp.SP.SPappId
            $spiGridRow.c2 = $sp.SP.SPdisplayName
            $spiGridRow.c3 = $spType
            $spiGridRow.c4 = $spTags
            $spiGridRow.c4Html = getSpiTagsHtml -Entry $sp -Of SP
            $spiGridRow.c5 = $appTags
            $spiGridRow.c5Html = getSpiTagsHtml -Entry $sp -Of App
            $spiGridRow.c6 = $sp.SP.SPappOwnerOrganizationId
            $spiGridRow.c7 = $roleClassification
            $spiGridRow.c8 = ($sp.SPAADRoleAssignments).Count
            $spiGridRow.c9Html = getSpiItemsHtml -Items $raItems
            $spiGridRow.c9 = $(if ($spAADRoleAssignments) { $arrayPlain -join "$CsvDelimiterOpposite " } else { '' })
            $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)
        }

        if (-not $NoCsvExport) {
            $arrayServicePrincipalsAADRoleAssignments4CSV | Sort-Object -Property SPDisplayName, SPObjectId, RoleDefinitionName, ScopeDetail | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_AADRoleAssignments_.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
        }
        $arrayServicePrincipalsAADRoleAssignments4CSV = $null
        [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle 'Entra role assignments of Service Principals' -ColumnDefinitions @(
                    @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                    @{ header = 'SP application Id'; property = 'c1' }
                    @{ header = 'SP displayName'; property = 'c2' }
                    @{ header = 'SP type'; property = 'c3'; filter = 'select' }
                    @{ header = 'SP Tags'; property = 'c4'; htmlProperty = 'c4Html' }
                    @{ header = 'App Tags'; property = 'c5'; htmlProperty = 'c5Html' }
                    @{ header = 'SP App Owner Organization Id'; property = 'c6'; filter = 'select' }
                    @{ header = 'Classification'; property = 'c7'; filter = 'select' }
                    @{ header = '#'; property = 'c8'; filter = 'number' }
                    @{ header = 'Entra role assignments'; property = 'c9'; htmlProperty = 'c9Html' }
                )))
        $spiGridRows = $null

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalsAADRoleAssignments' -Kind note
        [void]$htmlTenantSummary.AppendLine(@'
            <i class="fa fa-ban" aria-hidden="true"></i> <span>Entra role assignments of Service Principals</span>
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
        $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalsAADRoleAssignedOn'
        [void]$htmlTenantSummary.AppendLine(@'
        <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">Entra roles assigned on Service Principals</span><span class="az-chevron" aria-hidden="true"></span></div>
        <div class="az-card-body">
'@)

        $htmlTableId = 'TenantSummary_ServicePrincipalsAADRoleAssignedOn'
        $spiGridRows = [System.Collections.ArrayList]@()
        $SPAAADRoleAssignedOn4CSVExport = [System.Collections.ArrayList]@()
        foreach ($sp in ($servicePrincipalsAADRoleAssignedOn)) {

            $spType = $sp.ObjectType
            #SP Tags / App Tags cells were missing (header had 8 columns, rows 6); same logic as the other SP tables
            $appTags = ''
            $spTags = $sp.SP.SPTags -join "$CsvDelimiterOpposite "
            if ($sp.APP) {
                $appTags = $sp.APP.APPTags -join "$CsvDelimiterOpposite "
                if ($sp.SP.SPTags -and $sp.APP.APPTags) {
                    #App tags are automatically propagated to the service principal, removing redundant information (SP tags that are not App tags; Compare-Object -PassThru also returned the App-only tags).
                    $spTags = @($sp.SP.SPTags).where( { $_ -notin $sp.APP.APPTags } ) -join "$CsvDelimiterOpposite "
                }
            }

            $SPAAADRoleAssignedOn = $null
            $raoItems = [System.Collections.ArrayList]@()
            if (($sp.SPAAADRoleAssignedOn)) {
                if (($sp.SPAAADRoleAssignedOn.count -gt 0)) {
                    $array = [System.Collections.ArrayList]@()
                    foreach ($rao in $sp.SPAAADRoleAssignedOn) {

                        #HTML fragment: data parts encoded, link/markup kept
                        $raRoleDefinitionName = [HtmlSafe]::Encode($rao.roleName)
                        if ($htAadRoleDefinitions[$rao.roleId]) {
                            if ($htAadRoleDefinitions[$rao.roleId].isBuiltIn -eq $true) {
                                #$raRoleDefinitionName = "<a class=`"externallink`" href=`"https://github.com/MicrosoftDocs/azure-docs/blob/main/articles/active-directory/roles/permissions-reference.md#$($rao.roleName -replace ' ', '-')`" target=`"_blank`">$($rao.roleName)</a>"
                                $raRoleDefinitionName = (getSpiEntraRoleLinkHtml -RoleDefinitionId $rao.roleId -RoleName $rao.roleName)
                            }
                        }

                        $null = $array.Add("$raRoleDefinitionName ($([HtmlSafe]::Encode($rao.roleId))) on $([HtmlSafe]::Encode($rao.principalDisplayName)) - $([HtmlSafe]::Encode($rao.principalType)) ($([HtmlSafe]::Encode($rao.principalId)))")
                        $null = $raoItems.Add(@{ MainHtml = $raRoleDefinitionName; Dim = @("assigned to $($rao.principalDisplayName)", $rao.principalType, $rao.principalId) })
                        if (-not $NoCsvExport) {
                            $null = $SPAAADRoleAssignedOn4CSVExport.Add([PSCustomObject]@{
                                    SPObjectId = $sp.SP.SPObjectId
                                    SPAppId = $sp.SP.SPAppId
                                    SPDisplayName = $sp.SP.SPdisplayName
                                    SPType = $spType
                                    SPAppOwnerOrganizationId = $sp.SP.SPappOwnerOrganizationId
                                    RoleName = $rao.roleName
                                    RoleId = $rao.roleId
                                    PrincipalDisplayName = $rao.principalDisplayName
                                    PrincipalType = $rao.principalType
                                    PrincipalId = $rao.principalId
                                })
                        }
                    }
                    $SPAAADRoleAssignedOn = "$(($sp.SPAAADRoleAssignedOn).Count) ($($array -join "$CsvDelimiterOpposite "))"

                }
                else {
                    $SPAAADRoleAssignedOn = $null
                }
            }

            $spiGridRow = [ordered]@{}
            $spiGridRow.c0 = $sp.SP.SPObjectId
            $spiGridRow.c1 = $sp.SP.SPappId
            $spiGridRow.c2 = $sp.SP.SPdisplayName
            $spiGridRow.c3 = $spType
            $spiGridRow.c4 = $spTags
            $spiGridRow.c4Html = getSpiTagsHtml -Entry $sp -Of SP
            $spiGridRow.c5 = $appTags
            $spiGridRow.c5Html = getSpiTagsHtml -Entry $sp -Of App
            $spiGridRow.c6 = $sp.SP.SPappOwnerOrganizationId
            $spiGridRow.c7Html = getSpiItemsHtml -Items $raoItems
            $spiGridRow.c7 = ConvertTo-SpiPlainText $SPAAADRoleAssignedOn
            $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)
        }
        if (-not $NoCsvExport) {
            $SPAAADRoleAssignedOn4CSVExport | Sort-Object -Property SPDisplayName, SPObjectId | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_ServicePrincipalsAADRoleAssignedOn.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
        }
        $SPAAADRoleAssignedOn4CSVExport = $null

        [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle 'Entra roles assigned on Service Principals' -ColumnDefinitions @(
                    @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                    @{ header = 'SP application Id'; property = 'c1' }
                    @{ header = 'SP displayName'; property = 'c2' }
                    @{ header = 'SP type'; property = 'c3'; filter = 'select' }
                    @{ header = 'SP Tags'; property = 'c4'; htmlProperty = 'c4Html' }
                    @{ header = 'App Tags'; property = 'c5'; htmlProperty = 'c5Html' }
                    @{ header = 'SP App Owner Organization Id'; property = 'c6'; filter = 'select' }
                    @{ header = 'Entra roles assigned on the SP'; property = 'c7'; htmlProperty = 'c7Html' }
                )))
        $spiGridRows = $null

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalsAADRoleAssignedOn' -Kind note
        [void]$htmlTenantSummary.AppendLine(@'
            <i class="fa fa-ban" aria-hidden="true"></i> <span>Entra roles assigned on Service Principals</span>
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
        $htmlTenantSummary = useSpiSection -Id 'ApplicationsAADRoleAssignedOn'
        [void]$htmlTenantSummary.AppendLine(@'
        <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">Entra roles assigned on applications</span><span class="az-chevron" aria-hidden="true"></span></div>
        <div class="az-card-body">
'@)

        $htmlTableId = 'TenantSummary_ApplicationsAADRoleAssignedOn'
        $spiGridRows = [System.Collections.ArrayList]@()

        $applicationsAADRoleAssignedOn4CSVExport = [System.Collections.ArrayList]@()
        foreach ($sp in ($applicationsAADRoleAssignedOn)) {

            $spType = $sp.ObjectType

            $APPAAADRoleAssignedOn = $null
            $raoItems = [System.Collections.ArrayList]@()
            if (($sp.APPAAADRoleAssignedOn)) {
                if (($sp.APPAAADRoleAssignedOn.count -gt 0)) {
                    $array = [System.Collections.ArrayList]@()
                    foreach ($rao in $sp.APPAAADRoleAssignedOn) {

                        #HTML fragment: data parts encoded, link/markup kept
                        $raRoleDefinitionName = [HtmlSafe]::Encode($rao.roleName)
                        if ($htAadRoleDefinitions[$rao.roleId]) {
                            if ($htAadRoleDefinitions[$rao.roleId].isBuiltIn -eq $true) {
                                #$raRoleDefinitionName = "<a class=`"externallink`" href=`"https://github.com/MicrosoftDocs/azure-docs/blob/main/articles/active-directory/roles/permissions-reference.md#$($rao.roleName -replace ' ', '-')`" target=`"_blank`">$($rao.roleName)</a>"
                                $raRoleDefinitionName = (getSpiEntraRoleLinkHtml -RoleDefinitionId $rao.roleId -RoleName $rao.roleName)
                            }
                        }

                        $null = $array.Add("$raRoleDefinitionName ($([HtmlSafe]::Encode($rao.roleId))) on $([HtmlSafe]::Encode($rao.principalDisplayName)) - $([HtmlSafe]::Encode($rao.principalType)) ($([HtmlSafe]::Encode($rao.principalId)))")
                        $null = $raoItems.Add(@{ MainHtml = $raRoleDefinitionName; Dim = @("assigned to $($rao.principalDisplayName)", $rao.principalType, $rao.principalId) })
                        if (-not $NoCsvExport) {
                            $null = $applicationsAADRoleAssignedOn4CSVExport.Add([PSCustomObject]@{
                                    AppObjectId = $sp.APP.APPObjectId
                                    AppClientId = $sp.APP.APPAppClientId
                                    AppDisplayName = $sp.APP.APPDisplayName
                                    SPType = $spType
                                    SPAppOwnerOrganizationId = $sp.SP.SPappOwnerOrganizationId
                                    RoleName = $rao.roleName
                                    RoleId = $rao.roleId
                                    PrincipalDisplayName = $rao.principalDisplayName
                                    PrincipalType = $rao.principalType
                                    PrincipalId = $rao.principalId
                                })
                        }
                    }
                    $APPAAADRoleAssignedOn = "$(($sp.APPAAADRoleAssignedOn).Count) ($($array -join "$CsvDelimiterOpposite "))"

                }
                else {
                    $APPAAADRoleAssignedOn = $null
                }
            }

            $spiGridRow = [ordered]@{}
            $spiGridRow.c0 = $sp.APP.APPObjectId
            $spiGridRow.c1 = $sp.APP.APPAppClientId
            $spiGridRow.c2 = $sp.APP.APPDisplayName
            $spiGridRow.c3 = $spType
            $spiGridRow.c4 = $sp.SP.SPappOwnerOrganizationId
            $spiGridRow.c5Html = getSpiItemsHtml -Items $raoItems
            $spiGridRow.c5 = ConvertTo-SpiPlainText $APPAAADRoleAssignedOn
            $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)
        }
        if (-not $NoCsvExport) {
            $applicationsAADRoleAssignedOn4CSVExport | Sort-Object -Property AppDisplayName, AppObjectId | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_ApplicationsAADRoleAssignedOn.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
        }
        $applicationsAADRoleAssignedOn4CSVExport = $null

        [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle 'Entra roles assigned on applications' -ColumnDefinitions @(
                    @{ header = 'App object Id'; property = 'c0'; portal = 'application'; appIdProperty = 'c1' }
                    @{ header = 'App application Id'; property = 'c1' }
                    @{ header = 'App displayName'; property = 'c2' }
                    @{ header = 'type'; property = 'c3'; filter = 'select' }
                    @{ header = 'SP App Owner Organization Id'; property = 'c4'; filter = 'select' }
                    @{ header = 'Entra roles assigned on the app'; property = 'c5'; htmlProperty = 'c5Html' }
                )))
        $spiGridRows = $null

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        $htmlTenantSummary = useSpiSection -Id 'ApplicationsAADRoleAssignedOn' -Kind note
        [void]$htmlTenantSummary.AppendLine(@'
            <i class="fa fa-ban" aria-hidden="true"></i> <span>Entra roles assigned on applications</span>
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
            $buttonDataContent = "Application permissions (app role assignments) [$summaryClassifications permissions: $classifiedCriticalCount]"
        }
        else {
            $buttonDataContent = 'Application permissions (app role assignments)'
        }

        $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalsAppRoleAssignments'
        [void]$htmlTenantSummary.AppendLine(@"
        <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">$buttonDataContent</span><span class="az-chevron" aria-hidden="true"></span></div>
        <div class="az-card-body">
"@)

        $htmlTableId = 'TenantSummary_ServicePrincipalsAppRoleAssignments'
        $spiGridRows = [System.Collections.ArrayList]@()

        $arrayServicePrincipalsAppRoleAssignments4CSV = [System.Collections.ArrayList]@()
        foreach ($sp in ($servicePrincipalsAppRoleAssignments)) {

            $spType = $sp.ObjectType
            $appTags = ''
            $spTags = $sp.SP.SPTags -join "$CsvDelimiterOpposite "
            if ($sp.APP) {
                $appTags = $sp.APP.APPTags -join "$CsvDelimiterOpposite "
                if ($sp.SP.SPTags -and $sp.APP.APPTags) {
                    #App tags are automatically propagated to the service principal, removing redundant information (SP tags that are not App tags; Compare-Object -PassThru also returned the App-only tags).
                    $spTags = @($sp.SP.SPTags).where( { $_ -notin $sp.APP.APPTags } ) -join "$CsvDelimiterOpposite "
                }
            }
            $SPAppRoleAssignments = $null
            $appRoleItemsByResource = [ordered]@{}
            if (($sp.SPAppRoleAssignments)) {
                if (($sp.SPAppRoleAssignments.count -gt 0)) {
                    $array = [System.Collections.ArrayList]@()
                    $arrayPlain = [System.Collections.ArrayList]@()
                    $classificationCollection = [System.Collections.ArrayList]@()
                    foreach ($approleAss in $sp.SPAppRoleAssignments) {

                        $classification4CSV = 'unclassified'

                        if ($htServicePrincipalsAppRoleIdsToAppId["$($approleAss.AppRoleAssignmentResourceId)_$($approleAss.AppRoleId)"]) {
                            $appIdRefAppRoleAssignment = $htServicePrincipalsAppRoleIdsToAppId["$($approleAss.AppRoleAssignmentResourceId)_$($approleAss.AppRoleId)"]
                        }
                        else {
                            $appIdRefAppRoleAssignment = $null
                        }

                        if ($approleAss.AppRolePermissionSensitivity -ne 'unclassified') {
                            $null = $classificationCollection.Add($approleAss.AppRolePermissionSensitivity)
                            $classification4CSV = $approleAss.AppRolePermissionSensitivity
                            if ($appIdRefAppRoleAssignment) {
                                $null = $array.Add("$([HtmlSafe]::Encode($approleAss.AppRoleAssignmentResourceDisplayName)) (<span style=`"color: $($getClassifications.permissionColors[$approleAss.AppRolePermissionSensitivity])`">!</span><a class=`"externallink`" href=`"https://www.azadvertizer.net/azEntraIdAPIpermissionsAdvertizer.html?targetAppId=$([HtmlSafe]::Encode($appIdRefAppRoleAssignment))&targetPermissionId=$([HtmlSafe]::Encode($approleAss.AppRoleId))`" target=`"_blank`">$([HtmlSafe]::Encode($approleAss.AppRolePermission))</a>)")
                            }
                            else {
                                $null = $array.Add("$([HtmlSafe]::Encode($approleAss.AppRoleAssignmentResourceDisplayName)) (<span style=`"color: $($getClassifications.permissionColors[$approleAss.AppRolePermissionSensitivity])`">!</span>$([HtmlSafe]::Encode($approleAss.AppRolePermission)))")
                            }

                        }
                        else {
                            if ($appIdRefAppRoleAssignment) {
                                $null = $array.Add("$([HtmlSafe]::Encode($approleAss.AppRoleAssignmentResourceDisplayName)) (<a class=`"externallink`" href=`"https://www.azadvertizer.net/azEntraIdAPIpermissionsAdvertizer.html?targetAppId=$([HtmlSafe]::Encode($appIdRefAppRoleAssignment))&targetPermissionId=$([HtmlSafe]::Encode($approleAss.AppRoleId))`" target=`"_blank`">$([HtmlSafe]::Encode($approleAss.AppRolePermission))</a>)")
                            }
                            else {
                                $null = $array.Add("$([HtmlSafe]::Encode($approleAss.AppRoleAssignmentResourceDisplayName)) ($([HtmlSafe]::Encode($approleAss.AppRolePermission)))")
                            }
                        }

                        #review F-40: a role that is not found (the default access role, a deleted role) had an empty item
                        if (-not $approleAss.AppRolePermission) {
                            $appRolePermissionText = $(if ($approleAss.AppRoleId -eq '00000000-0000-0000-0000-000000000000') { '(default access)' } else { "(app role $($approleAss.AppRoleId) not found)" })
                            $null = $array.RemoveAt($array.Count - 1)
                            $null = $array.Add("$([HtmlSafe]::Encode($approleAss.AppRoleAssignmentResourceDisplayName)) ($([HtmlSafe]::Encode($appRolePermissionText)))")
                        }
                        else {
                            $appRolePermissionText = $approleAss.AppRolePermission
                        }
                        $null = $arrayPlain.Add("$($approleAss.AppRoleAssignmentResourceDisplayName) ($($appRolePermissionText))")
                        $appRolePermissionHtml = $(if (-not $approleAss.AppRolePermission) { [HtmlSafe]::Encode($appRolePermissionText) } elseif ($appIdRefAppRoleAssignment) { "<a class=`"externallink`" href=`"https://www.azadvertizer.net/azEntraIdAPIpermissionsAdvertizer.html?targetAppId=$([HtmlSafe]::Encode($appIdRefAppRoleAssignment))&targetPermissionId=$([HtmlSafe]::Encode($approleAss.AppRoleId))`" target=`"_blank`">$([HtmlSafe]::Encode($approleAss.AppRolePermission))</a>" } else { [HtmlSafe]::Encode($approleAss.AppRolePermission) })
                        if (-not $appRoleItemsByResource.Contains([string]$approleAss.AppRoleAssignmentResourceDisplayName)) { $appRoleItemsByResource[[string]$approleAss.AppRoleAssignmentResourceDisplayName] = [System.Collections.ArrayList]@() }
                        $null = $appRoleItemsByResource[[string]$approleAss.AppRoleAssignmentResourceDisplayName].Add(@{ MainHtml = "$(getSpiClassificationDotHtml $approleAss.AppRolePermissionSensitivity)$appRolePermissionHtml"; Dim = @($(if ($approleAss.AppRolePermissionSensitivity -ne 'unclassified') { $approleAss.AppRolePermissionSensitivity })) })
                        $null = $arrayServicePrincipalsAppRoleAssignments4CSV.Add([PSCustomObject]@{
                                SPObjectType = $sp.ObjectType
                                SPObjectId = $sp.ObjectId
                                SPDisplayName = $sp.SP.SPDisplayName
                                SPTags = $spTags
                                AppTags = $appTags
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

            $spiGridRow = [ordered]@{}
            $spiGridRow.c0 = $sp.SP.SPObjectId
            $spiGridRow.c1 = $sp.SP.SPappId
            $spiGridRow.c2 = $sp.SP.SPdisplayName
            $spiGridRow.c3 = $spType
            $spiGridRow.c4 = $spTags
            $spiGridRow.c4Html = getSpiTagsHtml -Entry $sp -Of SP
            $spiGridRow.c5 = $appTags
            $spiGridRow.c5Html = getSpiTagsHtml -Entry $sp -Of App
            $spiGridRow.c6 = $sp.SP.SPappOwnerOrganizationId
            $spiGridRow.c7 = ($classificationCollection | Sort-Object -Unique) -join ', '
            $spiGridRow.c8Html = getSpiGroupedItemsHtml -Groups $appRoleItemsByResource
            #review F-45: the plain text from the values
            $spiGridRow.c8 = $(if ($SPAppRoleAssignments) { "$(($sp.SPAppRoleAssignments).Count) ($($arrayPlain -join "$CsvDelimiterOpposite "))" } else { '' })
            $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)
        }

        if (-not $NoCsvExport) {
            $arrayServicePrincipalsAppRoleAssignments4CSV | Sort-Object -Property SPDisplayName, SPObjectId, AppRoleAssignmentResourceDisplayName, AppRolePermission | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_AppRoleAssignments_.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
        }
        $arrayServicePrincipalsAppRoleAssignments4CSV = $null

        [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle (ConvertTo-SpiPlainText ($buttonDataContent -replace '&nbsp;', ' ')) -ColumnDefinitions @(
                    @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                    @{ header = 'SP application Id'; property = 'c1' }
                    @{ header = 'SP displayName'; property = 'c2' }
                    @{ header = 'SP type'; property = 'c3'; filter = 'select' }
                    @{ header = 'SP Tags'; property = 'c4'; htmlProperty = 'c4Html' }
                    @{ header = 'App Tags'; property = 'c5'; htmlProperty = 'c5Html' }
                    @{ header = 'SP App Owner Organization Id'; property = 'c6'; filter = 'select' }
                    @{ header = 'Classification'; property = 'c7' }
                    @{ header = 'App role assignments (application permissions)'; property = 'c8'; htmlProperty = 'c8Html' }
                )))
        $spiGridRows = $null

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalsAppRoleAssignments' -Kind note
        [void]$htmlTenantSummary.AppendLine(@'
            <i class="fa fa-ban" aria-hidden="true"></i> <span>Application permissions (app role assignments)</span>
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
        $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalsAppRoleAssignedTo'
        [void]$htmlTenantSummary.AppendLine(@'
        <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">Users and groups assigned to applications</span><span class="az-chevron" aria-hidden="true"></span></div>
        <div class="az-card-body">
'@)

        $htmlTableId = 'TenantSummary_ServicePrincipalsAppRoleAssignedTo'
        $spiGridRows = [System.Collections.ArrayList]@()

        $servicePrincipalsAppRoleAssignedTo4CSVExport = [System.Collections.ArrayList]@()
        foreach ($sp in ($servicePrincipalsAppRoleAssignedTo)) {

            $spType = $sp.ObjectType
            $appTags = ''
            $spTags = $sp.SP.SPTags -join "$CsvDelimiterOpposite "
            if ($sp.APP) {
                $appTags = $sp.APP.APPTags -join "$CsvDelimiterOpposite "
                if ($sp.SP.SPTags -and $sp.APP.APPTags) {
                    #App tags are automatically propagated to the service principal, removing redundant information (SP tags that are not App tags; Compare-Object -PassThru also returned the App-only tags).
                    $spTags = @($sp.SP.SPTags).where( { $_ -notin $sp.APP.APPTags } ) -join "$CsvDelimiterOpposite "
                }
            }
            $SPAppRoleAssignedTo = $null
            #users and groups only (the table's scope); assignments to service principals are in 'SP App RoleAssignments' of the assignee
            $appRoleAssignedToUsersAndGroups = @(@($sp.SPAppRoleAssignedTo).where( { $_.principalType -like 'User*' -or $_.principalType -eq 'Group' } ))
            if ($appRoleAssignedToUsersAndGroups.Count -gt 0) {
                $array = [System.Collections.ArrayList]@()
                foreach ($approleAssTo in $appRoleAssignedToUsersAndGroups) {
                    #review F-42: the role (a principal with two roles was listed twice the same)
                    $approleAssToRole = getSpiAppRoleAssignedToRoleText $approleAssTo
                    $null = $array.Add("$($approleAssTo.principalDisplayName) - $($approleAssTo.principalType)$(if ($approleAssToRole) { " ($($approleAssToRole))" })")
                    if (-not $NoCsvExport) {
                        $null = $servicePrincipalsAppRoleAssignedTo4CSVExport.Add([PSCustomObject]@{
                                SPObjectId = $sp.SP.SPObjectId
                                SPAppId = $sp.SP.SPAppId
                                SPDisplayName = $sp.SP.SPdisplayName
                                SPTags = $spTags
                                AppTags = $appTags
                                SPAppOwnerOrganizationId = $sp.SP.SPappOwnerOrganizationId
                                AppRoleAssignedToPrincipalDisplayName = $approleAssTo.principalDisplayName
                                AppRoleAssignedToPrincipalType = $approleAssTo.principalType
                                AppRoleAssignedToPrincipalId = $approleAssTo.principalId
                                AppRoleAssignedToRoleId = $approleAssTo.roleId
                            })
                    }
                }
                $SPAppRoleAssignedTo = "$($appRoleAssignedToUsersAndGroups.Count) ($($array -join "$CsvDelimiterOpposite "))"
            }

            $spiGridRow = [ordered]@{}
            $spiGridRow.c0 = $sp.SP.SPObjectId
            $spiGridRow.c1 = $sp.SP.SPappId
            $spiGridRow.c2 = $sp.SP.SPdisplayName
            $spiGridRow.c3 = $spType
            $spiGridRow.c4 = $spTags
            $spiGridRow.c4Html = getSpiTagsHtml -Entry $sp -Of SP
            $spiGridRow.c5 = $appTags
            $spiGridRow.c5Html = getSpiTagsHtml -Entry $sp -Of App
            $spiGridRow.c6 = $sp.SP.SPappOwnerOrganizationId
            $spiGridRow.c7 = $SPAppRoleAssignedTo
            $spiGridRow.c7Html = getSpiItemsHtml -Items @($appRoleAssignedToUsersAndGroups | ForEach-Object { @{ MainHtml = (getSpiEntraObjectLinkHtml -Kind $_.principalType -Id $_.principalId -Text $_.principalDisplayName); Dim = @($_.principalType, (getSpiAppRoleAssignedToRoleText $_)) } })
            $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)
        }
        if (-not $NoCsvExport) {
            $servicePrincipalsAppRoleAssignedTo4CSVExport | Sort-Object -Property SPDisplayName, SPObjectId, AppRoleAssignedToPrincipalId, AppRoleAssignedToRoleId | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_ServicePrincipalsAppRoleAssignedTo.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
        }
        $servicePrincipalsAppRoleAssignedTo4CSVExport = $null

        [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle 'Users and groups assigned to applications' -ColumnDefinitions @(
                    @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                    @{ header = 'SP application Id'; property = 'c1' }
                    @{ header = 'SP displayName'; property = 'c2' }
                    @{ header = 'SP type'; property = 'c3'; filter = 'select' }
                    @{ header = 'SP Tags'; property = 'c4'; htmlProperty = 'c4Html' }
                    @{ header = 'App Tags'; property = 'c5'; htmlProperty = 'c5Html' }
                    @{ header = 'SP App Owner Organization Id'; property = 'c6'; filter = 'select' }
                    @{ header = 'Assigned users and groups'; property = 'c7'; htmlProperty = 'c7Html' }
                )))
        $spiGridRows = $null

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalsAppRoleAssignedTo' -Kind note
        [void]$htmlTenantSummary.AppendLine(@'
            <i class="fa fa-ban" aria-hidden="true"></i> <span>Users and groups assigned to applications</span>
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
        $buttonDataContent = "Delegated permissions (OAuth2 permission grants) [$summaryClassifications permissions: $classifiedCriticalCount]"
    }
    else {
        $buttonDataContent = 'Delegated permissions (OAuth2 permission grants)'
    }

    if ($servicePrincipalsOauth2PermissionGrantsCount -gt 0) {
        $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalsOauth2PermissionGrants'
        [void]$htmlTenantSummary.AppendLine(@"
            <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">$buttonDataContent</span><span class="az-chevron" aria-hidden="true"></span></div>
            <div class="az-card-body">
"@)

        $htmlTableId = 'TenantSummary_ServicePrincipalsOauth2PermissionGrants'
        $spiGridRows = [System.Collections.ArrayList]@()

        $arrayServicePrincipalsOauth2PermissionGrants4CSV = [System.Collections.ArrayList]@()
        foreach ($sp in ($servicePrincipalsOauth2PermissionGrants)) {

            $spType = $sp.ObjectType
            $appTags = ''
            $spTags = $sp.SP.SPTags -join "$CsvDelimiterOpposite "
            if ($sp.APP) {
                $appTags = $sp.APP.APPTags -join "$CsvDelimiterOpposite "
                if ($sp.SP.SPTags -and $sp.APP.APPTags) {
                    #App tags are automatically propagated to the service principal, removing redundant information (SP tags that are not App tags; Compare-Object -PassThru also returned the App-only tags).
                    $spTags = @($sp.SP.SPTags).where( { $_ -notin $sp.APP.APPTags } ) -join "$CsvDelimiterOpposite "
                }
            }
            $SPOauth2PermissionGrants = $null
            $grantItemsByResource = [ordered]@{}
            if (($sp.SPOauth2PermissionGrants)) {
                if (($sp.SPOauth2PermissionGrants.count -gt 0)) {
                    $array = [System.Collections.ArrayList]@()
                    $classificationCollection = [System.Collections.ArrayList]@()
                    #review R-1: one entry per resource, permission and consent (all users / n single users); was one per grant row,
                    #labelled with the type of the published scope ('Admin'/'User': who may consent), not how it was granted
                    #review F-39: a scope the API does not publish (any more) has no permission - grouped and shown by its scope
                    $grantGroups = @($sp.SPOauth2PermissionGrants | Group-Object -Property { "$($_.SPDisplayName)|$(if ($_.permission) { $_.permission } else { "scope:$($_.scope)" })|$(if ($_.consentType -eq 'AllPrincipals') { 'all' } else { 'user' })" } | Sort-Object -Property Name)
                    foreach ($grantGroup in $grantGroups) {
                        $oauthGrant = $grantGroup.Group[0]
                        $grantPermissionText = $(if ($oauthGrant.permission) { $oauthGrant.permission } else { "$($oauthGrant.scope) (not published by the API)" })
                        $consentLabel = $(if ($oauthGrant.consentType -eq 'AllPrincipals') { 'all users' } else {
                                $consentUserCount = @($grantGroup.Group.principalId | Where-Object { $_ } | Sort-Object -Unique).Count
                                "$consentUserCount $(if ($consentUserCount -eq 1) { 'user' } else { 'users' })"
                            })

                        if ($htServicePrincipalsPublishedPermissionScopesIdsToAppId["$($oauthGrant.SPId)_$($oauthGrant.id)"]) {
                            $appIdRefOauth2PermissionGrant = $htServicePrincipalsPublishedPermissionScopesIdsToAppId["$($oauthGrant.SPId)_$($oauthGrant.id)"]
                        }
                        else {
                            $appIdRefOauth2PermissionGrant = $null
                        }

                        if ($oauthGrant.permissionSensitivity -ne 'unclassified') {
                            $null = $classificationCollection.Add($oauthGrant.permissionSensitivity)
                        }
                        $null = $array.Add("$($oauthGrant.SPDisplayName) ($($grantPermissionText) - $($consentLabel))")

                        $grantPermissionHtml = $(if ($appIdRefOauth2PermissionGrant) { "<a class=`"externallink`" href=`"https://www.azadvertizer.net/azEntraIdAPIpermissionsAdvertizer.html?targetAppId=$([HtmlSafe]::Encode($appIdRefOauth2PermissionGrant))&targetPermissionId=$([HtmlSafe]::Encode($oauthGrant.id))`" target=`"_blank`">$([HtmlSafe]::Encode($oauthGrant.permission))</a>" } else { [HtmlSafe]::Encode($grantPermissionText) })
                        if (-not $grantItemsByResource.Contains([string]$oauthGrant.SPDisplayName)) { $grantItemsByResource[[string]$oauthGrant.SPDisplayName] = [System.Collections.ArrayList]@() }
                        $null = $grantItemsByResource[[string]$oauthGrant.SPDisplayName].Add(@{ MainHtml = "$(getSpiClassificationDotHtml $oauthGrant.permissionSensitivity)$grantPermissionHtml"; Dim = @($consentLabel, $(if ($oauthGrant.permissionSensitivity -ne 'unclassified') { $oauthGrant.permissionSensitivity })) })
                    }
                    foreach ($oauthGrant in $sp.SPOauth2PermissionGrants) {
                        $null = $arrayServicePrincipalsOauth2PermissionGrants4CSV.Add([PSCustomObject]@{
                                SPObjectType = $sp.ObjectType
                                SPObjectId = $sp.ObjectId
                                SPDisplayName = $sp.SP.SPDisplayName
                                SPTags = $spTags
                                AppTags = $appTags
                                APPObjectId = $sp.APP.APPObjectId
                                APPAppClientId = $sp.APP.APPAppClientId
                                APPDisplayName = $sp.APP.APPDisplayName
                                Oauth2PermissionGrantResourceDisplayName = $oauthGrant.SPDisplayName
                                Oauth2PermissionGrantResourceId = $oauthGrant.SPId
                                Oauth2PermissionGrantPermission = $oauthGrant.permission
                                Oauth2PermissionGrantDisplayNameUser = $oauthGrant.userConsentDisplayName
                                Oauth2PermissionGrantDisplayNameAdmin = $oauthGrant.adminConsentDisplayName
                                Oauth2PermissionGrantPermissionId = $oauthGrant.id
                                Oauth2PermissionGrantPermissionClassification = $(if ($oauthGrant.permissionSensitivity -ne 'unclassified') { $oauthGrant.permissionSensitivity } else { 'unclassified' })
                                #review R-1 (added): how the grant was given; the user for a single-user grant; the grant id
                                Oauth2PermissionGrantConsentType = $oauthGrant.consentType
                                Oauth2PermissionGrantPrincipalId = $oauthGrant.principalId
                                Oauth2PermissionGrantId = $oauthGrant.grantId
                                #review F-39 (added): the granted scope (the permission is empty when the API does not publish it)
                                Oauth2PermissionGrantScope = $oauthGrant.scope
                            })
                    }
                    $SPOauth2PermissionGrants = "$($grantGroups.Count) ($($array -join "$CsvDelimiterOpposite "))"
                }
                else {
                    $SPOauth2PermissionGrants = $null
                }
            }

            $spiGridRow = [ordered]@{}
            $spiGridRow.c0 = $sp.SP.SPObjectId
            $spiGridRow.c1 = $sp.SP.SPappId
            $spiGridRow.c2 = $sp.SP.SPdisplayName
            $spiGridRow.c3 = $spType
            $spiGridRow.c4 = $spTags
            $spiGridRow.c4Html = getSpiTagsHtml -Entry $sp -Of SP
            $spiGridRow.c5 = $appTags
            $spiGridRow.c5Html = getSpiTagsHtml -Entry $sp -Of App
            $spiGridRow.c6 = $sp.SP.SPappOwnerOrganizationId
            $spiGridRow.c7 = ($classificationCollection | Sort-Object -Unique) -join ', '
            $spiGridRow.c8Html = getSpiGroupedItemsHtml -Groups $grantItemsByResource
            $spiGridRow.c8 = [string]$SPOauth2PermissionGrants   #plain text (R-1: built from the values, no html)
            $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)
        }

        if (-not $NoCsvExport) {
            $arrayServicePrincipalsOauth2PermissionGrants4CSV | Sort-Object -Property SPDisplayName, SPObjectId, Oauth2PermissionGrantResourceDisplayName, Oauth2PermissionGrantPermission, Oauth2PermissionGrantConsentType, Oauth2PermissionGrantPrincipalId | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_Oauth2PermissionGrants_.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
        }
        $arrayServicePrincipalsOauth2PermissionGrants4CSV = $null

        [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle (ConvertTo-SpiPlainText ($buttonDataContent -replace '&nbsp;', ' ')) -ColumnDefinitions @(
                    @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                    @{ header = 'SP application Id'; property = 'c1' }
                    @{ header = 'SP displayName'; property = 'c2' }
                    @{ header = 'SP type'; property = 'c3'; filter = 'select' }
                    @{ header = 'SP Tags'; property = 'c4'; htmlProperty = 'c4Html' }
                    @{ header = 'App Tags'; property = 'c5'; htmlProperty = 'c5Html' }
                    @{ header = 'SP App Owner Organization Id'; property = 'c6'; filter = 'select' }
                    @{ header = 'Classification'; property = 'c7' }
                    @{ header = 'OAuth2 permission grants (delegated permissions)'; property = 'c8'; htmlProperty = 'c8Html' }
                )))
        $spiGridRows = $null
        #closes the 'content' div opened above; it used to be appended outside this 'if', so with 0 grants it closed #summary early
        [void]$htmlTenantSummary.AppendLine(@'
    </div>
'@)
    }
    else {
        $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalsOauth2PermissionGrants' -Kind note
        [void]$htmlTenantSummary.AppendLine(@'
            <i class="fa fa-ban" aria-hidden="true"></i> <span>Delegated permissions (OAuth2 permission grants)</span>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsOauth2PermissionGrants

    if (-not $NoAzureResourceSideRelations) {
        #region SUMMARYServicePrincipalsAzureRoleAssignments
        $azureRoleLinkCache = [System.Collections.Hashtable]::new([System.StringComparer]::Ordinal)   #review F-46
        $startCustPolLoop = Get-Date
        Write-Host '  processing Summary ServicePrincipalsAzureRoleAssignments'

        $servicePrincipalsAzureRoleAssignments = $cu.where( { $_.SPAzureRoleAssignments.Count -ne 0 } )
        $servicePrincipalsAzureRoleAssignmentsCount = $servicePrincipalsAzureRoleAssignments.Count

        if ($servicePrincipalsAzureRoleAssignmentsCount -gt 0) {
            $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalsAzureRoleAssignments'
            [void]$htmlTenantSummary.AppendLine(@'
            <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">Azure role assignments of Service Principals</span><span class="az-chevron" aria-hidden="true"></span></div>
            <div class="az-card-body">
'@)

            $htmlTableId = 'TenantSummary_ServicePrincipalsAzureRoleAssignments'
            $spiGridRows = [System.Collections.ArrayList]@()

            $servicePrincipalsAzureRoleAssignments4CSVExport = [System.Collections.ArrayList]@()
            foreach ($sp in ($servicePrincipalsAzureRoleAssignments)) {

                $spType = $sp.ObjectType
                $appTags = ''
                $spTags = $sp.SP.SPTags -join "$CsvDelimiterOpposite "
                if ($sp.APP) {
                    $appTags = $sp.APP.APPTags -join "$CsvDelimiterOpposite "
                    if ($sp.SP.SPTags -and $sp.APP.APPTags) {
                        #App tags are automatically propagated to the service principal, removing redundant information (SP tags that are not App tags; Compare-Object -PassThru also returned the App-only tags).
                        $spTags = @($sp.SP.SPTags).where( { $_ -notin $sp.APP.APPTags } ) -join "$CsvDelimiterOpposite "
                    }
                }
                $SPAzureRoleAssignments = $null
                $azureItemsByScope = [ordered]@{}
                if (($sp.SPAzureRoleAssignments)) {
                    $RBACClassification = ''
                    $faIcon = ''
                    if (($sp.SPAzureRoleAssignments.count -gt 0)) {
                        $array = [System.Collections.ArrayList]@()
                        $arrayPlain = [System.Collections.ArrayList]@()
                        $importance = 'ManagementGroup', 'Subscription', 'ResourceGroup', 'Resource'

                        #review F-46: the sort only for more than one assignment; the link html once per role
                        $azureRoleAssignmentsSorted = $(if (@($sp.SPAzureRoleAssignments).Count -gt 1) { $sp.SPAzureRoleAssignments | Sort-Object @{Expression = { $importance.IndexOf($_.roleAssignmentAssignmentResourceType) } }, @{Expression = { $_.roleAssignmentAssignmentScopeName } }, @{Expression = { $_.roleName } } } else { $sp.SPAzureRoleAssignments })
                        foreach ($azureroleAss in $azureRoleAssignmentsSorted) {
                            $azureRoleLinkKey = "$($azureroleAss.roleId)|$($azureroleAss.roleName)|$($azureroleAss.roleType)|$($azureroleAss.roleAssignment.roleDefinitionId)"
                            $roleName = $azureRoleLinkCache[$azureRoleLinkKey]
                            if ($null -eq $roleName) {
                                $roleName = getSpiAzureRoleLinkHtml -RoleId $azureroleAss.roleId -RoleName $azureroleAss.roleName -RoleType $azureroleAss.roleType -RoleDefinitionId $azureroleAss.roleAssignment.roleDefinitionId
                                $azureRoleLinkCache[$azureRoleLinkKey] = $roleName
                            }

                            if ($azureroleAss.priviledgedIdentityManagementBased -eq $true) {
                                $pimRef = ' [PIM]'
                            }
                            else {
                                $pimRef = ''
                            }

                            if ($azureroleAss.roleAssignmentApplicability -eq 'indirect') {
                                $indirectRef = " [$([HtmlSafe]::Encode($azureroleAss.roleAssignmentApplicability)) - $([HtmlSafe]::Encode($azureroleAss.roleAssignmentAppliesThrough))]"
                            }
                            else {
                                $indirectRef = ''
                            }

                            $azureScopeLabel = @{ ManagementGroup = 'management group'; Subscription = 'subscription'; ResourceGroup = 'resource group'; Resource = 'resource' }[[string]$azureroleAss.roleAssignmentAssignmentResourceType]
                            if (-not $azureScopeLabel) { $azureScopeLabel = [string]$azureroleAss.roleAssignmentAssignmentResourceType }
                            if (-not $azureItemsByScope.Contains($azureScopeLabel)) { $azureItemsByScope[$azureScopeLabel] = [System.Collections.ArrayList]@() }
                            #Azure role conditions: 'limited by role definition condition: can assign only ...' and its marker
                            $azureRoleConditionText = getAzureRoleConditionText $azureroleAss
                            $null = $azureItemsByScope[$azureScopeLabel].Add(@{ MainHtml = "$(getSpiCriticalIconHtml ($azureroleAss.roleIsCritical -eq $true))$(getSpiConditionLimitedIconHtml ($azureroleAss.roleConditionEffect -eq 'limited'))$roleName"; Dim = @($azureRoleConditionText, $azureroleAss.roleAssignmentAssignmentScopeName, $(if ($azureroleAss.roleAssignmentAssignmentResourceType -in 'ManagementGroup', 'Subscription' -and $azureroleAss.roleAssignmentAssignmentScopeId -ne $azureroleAss.roleAssignmentAssignmentScopeName) { $azureroleAss.roleAssignmentAssignmentScopeId }), $(if ($azureroleAss.priviledgedIdentityManagementBased -eq $true) { 'PIM' }), $(if ($azureroleAss.roleAssignmentApplicability -eq 'indirect') { "through group $($azureroleAss.roleAssignmentAppliesThrough)" })) })
                            #review F-45: the plain text from the values
                            $null = $arrayPlain.Add("$($azureroleAss.roleName) ($($azureroleAss.roleAssignmentAssignmentResourceType) $($azureroleAss.roleAssignmentAssignmentScopeName))$(if ($azureroleAss.priviledgedIdentityManagementBased -eq $true) { ' [PIM]' })$(if ($azureroleAss.roleAssignmentApplicability -eq 'indirect') { " [$($azureroleAss.roleAssignmentApplicability) - $($azureroleAss.roleAssignmentAppliesThrough)]" })$(if ($azureRoleConditionText) { " [$($azureRoleConditionText)]" })")
                            if ($azureroleAss.roleIsCritical -eq $true) {
                                $RBACClassification = 'critical'
                                $faIcon = '<i class="fa fa-exclamation-triangle" aria-hidden="true" style="color: #ff5e00; font-size: 9px;"></i> '
                                $null = $array.Add("$($faIcon)$($roleName) (<b>$([HtmlSafe]::Encode($azureroleAss.roleAssignmentAssignmentResourceType))</b> $([HtmlSafe]::Encode($azureroleAss.roleAssignmentAssignmentScopeName)))$($pimRef)$($indirectRef)")
                            }
                            else {
                                $null = $array.Add("$($roleName) (<b>$([HtmlSafe]::Encode($azureroleAss.roleAssignmentAssignmentResourceType))</b> $([HtmlSafe]::Encode($azureroleAss.roleAssignmentAssignmentScopeName)))$($pimRef)$($indirectRef)")
                            }

                            if (-not $NoCsvExport) {
                                $null = $servicePrincipalsAzureRoleAssignments4CSVExport.Add([PSCustomObject]@{
                                        SPObjectId = $sp.SP.SPObjectId
                                        SPAppId = $sp.SP.SPAppId
                                        SPDisplayName = $sp.SP.SPdisplayName
                                        SPTags = $spTags
                                        AppTags = $appTags
                                        SPAppOwnerOrganizationId = $sp.SP.SPappOwnerOrganizationId
                                        #per row; $RBACClassification is the per-SP aggregate for the HTML and depended on row order here
                                        RBACClassification = $(if ($azureroleAss.roleIsCritical -eq $true) { 'critical' } else { '' })
                                        RoleName = $azureroleAss.roleName
                                        RoleType = $azureroleAss.roleType
                                        RoleAssignmentResourceType = $azureroleAss.roleAssignmentAssignmentResourceType
                                        RoleAssignmentScopeName = $azureroleAss.roleAssignmentAssignmentScopeName
                                        PrivilegedIdentityManagementBased = $azureroleAss.priviledgedIdentityManagementBased
                                        RoleAssignmentApplicability = $azureroleAss.roleAssignmentApplicability
                                        RoleAssignmentAppliesThrough = $azureroleAss.roleAssignmentAppliesThrough
                                        #review F-43 (added): subscriptions/management groups with equal names are distinguishable
                                        RoleAssignmentScopeId = $azureroleAss.roleAssignmentAssignmentScopeId
                                        RoleAssignmentId = $azureroleAss.roleAssignmentId
                                        #Azure role conditions (added)
                                        ConditionEffect = $azureroleAss.roleConditionEffect
                                        ConditionAssignableRoles = $azureroleAss.roleConditionAssignableRoles
                                        ConditionCriticalRoles = $azureroleAss.roleConditionCriticalRoles
                                        RoleDefinitionCondition = $azureroleAss.roleDefinitionCondition
                                        RoleAssignmentCondition = $azureroleAss.roleAssignmentCondition
                                    })
                            }
                        }
                        $SPAzureRoleAssignments = "$($array -join '<br>')"
                    }
                    else {
                        $SPAzureRoleAssignments = $null
                    }
                }

                $spiGridRow = [ordered]@{}
                $spiGridRow.c0 = $sp.SP.SPObjectId
                $spiGridRow.c1 = $sp.SP.SPappId
                $spiGridRow.c2 = $sp.SP.SPdisplayName
                $spiGridRow.c3 = $spType
                $spiGridRow.c4 = $spTags
                $spiGridRow.c4Html = getSpiTagsHtml -Entry $sp -Of SP
                $spiGridRow.c5 = $appTags
                $spiGridRow.c5Html = getSpiTagsHtml -Entry $sp -Of App
                $spiGridRow.c6 = $sp.SP.SPappOwnerOrganizationId
                $spiGridRow.c7 = $RBACClassification
                $spiGridRow.c8 = ($sp.SPAzureRoleAssignments).Count
                $spiGridRow.c9Html = getSpiGroupedItemsHtml -Groups $azureItemsByScope
                $spiGridRow.c9 = $(if ($SPAzureRoleAssignments) { $arrayPlain -join "$CsvDelimiterOpposite " } else { '' })
                $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)
            }
            if (-not $NoCsvExport) {
                $servicePrincipalsAzureRoleAssignments4CSVExport | Sort-Object -Property SPDisplayName, SPObjectId, RoleAssignmentResourceType, RoleName, RoleAssignmentScopeName, RoleAssignmentApplicability, RoleAssignmentAppliesThrough | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_ServicePrincipalsAzureRoleAssignments.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
            }
            $servicePrincipalsAzureRoleAssignments4CSVExport = $null

            [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle 'Azure role assignments of Service Principals' -ColumnDefinitions @(
                        @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                        @{ header = 'SP application Id'; property = 'c1' }
                        @{ header = 'SP displayName'; property = 'c2' }
                        @{ header = 'SP type'; property = 'c3'; filter = 'select' }
                        @{ header = 'SP Tags'; property = 'c4'; htmlProperty = 'c4Html' }
                        @{ header = 'App Tags'; property = 'c5'; htmlProperty = 'c5Html' }
                        @{ header = 'SP App Owner Organization Id'; property = 'c6'; filter = 'select' }
                        @{ header = 'Classification'; property = 'c7'; filter = 'select' }
                        @{ header = '#'; property = 'c8'; filter = 'number' }
                        @{ header = 'Azure role assignments'; property = 'c9'; htmlProperty = 'c9Html' }
                    )))
            $spiGridRows = $null

            [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
        }
        else {
            $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalsAzureRoleAssignments' -Kind note
            [void]$htmlTenantSummary.AppendLine(@'
                <i class="fa fa-ban" aria-hidden="true"></i> <span>Azure role assignments of Service Principals</span>
'@)
        }

        $endCustPolLoop = Get-Date
        Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
        #endregion SUMMARYServicePrincipalsAzureRoleAssignments
    }
    else {
        $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalsAzureRoleAssignments' -Kind note -NoteState disabled
        [void]$htmlTenantSummary.AppendLine(@"
            <i class="fa fa-ban" aria-hidden="true"></i> <span>Azure role assignments of Service Principals (parameter NoAzureResourceSideRelations = $NoAzureResourceSideRelations)</span>
"@)
    }

    #region SUMMARYServicePrincipalsGroupMemberships
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary ServicePrincipalsGroupMemberships'

    $servicePrincipalsGroupMemberships = $cu.where( { $_.SPGroupMemberships.Count -ne 0 } )
    $servicePrincipalsGroupMembershipsCount = $servicePrincipalsGroupMemberships.Count

    if ($servicePrincipalsGroupMembershipsCount -gt 0) {
        $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalsGroupMemberships'
        [void]$htmlTenantSummary.AppendLine(@'
        <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">Group memberships of Service Principals</span><span class="az-chevron" aria-hidden="true"></span></div>
        <div class="az-card-body">
'@)

        $htmlTableId = 'TenantSummary_ServicePrincipalsGroupMemberships'
        $spiGridRows = [System.Collections.ArrayList]@()
        $servicePrincipalsGroupMemberships4CSVExport = [System.Collections.ArrayList]@()
        foreach ($sp in ($servicePrincipalsGroupMemberships)) {

            $spType = $sp.ObjectType
            $appTags = ''
            $spTags = $sp.SP.SPTags -join "$CsvDelimiterOpposite "
            if ($sp.APP) {
                $appTags = $sp.APP.APPTags -join "$CsvDelimiterOpposite "
                if ($sp.SP.SPTags -and $sp.APP.APPTags) {
                    #App tags are automatically propagated to the service principal, removing redundant information (SP tags that are not App tags; Compare-Object -PassThru also returned the App-only tags).
                    $spTags = @($sp.SP.SPTags).where( { $_ -notin $sp.APP.APPTags } ) -join "$CsvDelimiterOpposite "
                }
            }
            $SPGroupMemberships = $null
            if (($sp.SPGroupMemberships)) {
                if (($sp.SPGroupMemberships.count -gt 0)) {
                    $array = [System.Collections.ArrayList]@()
                    foreach ($groupMembership in $sp.SPGroupMemberships) {
                        $null = $array.Add("$($groupMembership.DisplayName) ($($groupMembership.ObjectId))")

                        if (-not $NoCsvExport) {
                            $null = $servicePrincipalsGroupMemberships4CSVExport.Add([PSCustomObject]@{
                                    SPObjectId = $sp.SP.SPObjectId
                                    SPAppId = $sp.SP.SPAppId
                                    SPDisplayName = $sp.SP.SPdisplayName
                                    SPTags = $spTags
                                    AppTags = $appTags
                                    SPAppOwnerOrganizationId = $sp.SP.SPappOwnerOrganizationId
                                    GroupMembershipDisplayName = $groupMembership.DisplayName
                                    GroupmembershipObjectId = $groupMembership.ObjectId

                                })
                        }
                    }
                    $SPGroupMemberships = "$(($sp.SPGroupMemberships).Count) ($($array -join "$CsvDelimiterOpposite "))"
                }
                else {
                    $SPGroupMemberships = $null
                }
            }

            $spiGridRow = [ordered]@{}
            $spiGridRow.c0 = $sp.SP.SPObjectId
            $spiGridRow.c1 = $sp.SP.SPappId
            $spiGridRow.c2 = $sp.SP.SPdisplayName
            $spiGridRow.c3 = $spType
            $spiGridRow.c4 = $spTags
            $spiGridRow.c4Html = getSpiTagsHtml -Entry $sp -Of SP
            $spiGridRow.c5 = $appTags
            $spiGridRow.c5Html = getSpiTagsHtml -Entry $sp -Of App
            $spiGridRow.c6 = $sp.SP.SPappOwnerOrganizationId
            $spiGridRow.c7 = $SPGroupMemberships
            $spiGridRow.c7Html = getSpiItemsHtml -Items @(@($sp.SPGroupMemberships).where({ $_ }) | ForEach-Object { @{ MainHtml = (getSpiEntraObjectLinkHtml -Kind 'group' -Id $_.ObjectId -Text $_.DisplayName); Dim = @($_.ObjectId) } })
            $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)
        }
        if (-not $NoCsvExport) {
            $servicePrincipalsGroupMemberships4CSVExport | Sort-Object -Property SPDisplayName, SPObjectId, GroupMembershipDisplayName, GroupmembershipObjectId | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_ServicePrincipalsGroupMemberships.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
        }
        $servicePrincipalsGroupMemberships4CSVExport = $null

        [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle 'Group memberships of Service Principals' -ColumnDefinitions @(
                    @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                    @{ header = 'SP application Id'; property = 'c1' }
                    @{ header = 'SP displayName'; property = 'c2' }
                    @{ header = 'SP type'; property = 'c3'; filter = 'select' }
                    @{ header = 'SP Tags'; property = 'c4'; htmlProperty = 'c4Html' }
                    @{ header = 'App Tags'; property = 'c5'; htmlProperty = 'c5Html' }
                    @{ header = 'SP App Owner Organization Id'; property = 'c6'; filter = 'select' }
                    @{ header = 'SP Group memberships'; property = 'c7'; htmlProperty = 'c7Html' }
                )))
        $spiGridRows = $null

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        $htmlTenantSummary = useSpiSection -Id 'ServicePrincipalsGroupMemberships' -Kind note
        [void]$htmlTenantSummary.AppendLine(@'
            <i class="fa fa-ban" aria-hidden="true"></i> <span>Group memberships of Service Principals</span>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYServicePrincipalsGroupMemberships

    #region SUMMARYOptionalData
    $rows = foreach ($sp in $cu.where( { $_.SPEntraRecommendations.Count -gt 0 } )) {
        foreach ($item in $sp.SPEntraRecommendations) {
            $row = getSPRowBase $sp
            $row.Recommendation = $item.displayName; $row.RecommendationType = $item.recommendationType; $row.Priority = $item.priority; $row.Status = $item.status
            [PSCustomObject]$row
        }
    }
    addReportSection -Region 'ServicePrincipalsEntraRecommendations' -Title 'Service Principal Entra recommendations' -Rows @($rows | Sort-Object -Property Recommendation, SPDisplayName, SPObjectId) -NotCollectedReason $(if ($null -eq $htEntraRecommendationsBySPId) { 'optional permission DirectoryRecommendations.Read.All missing' }) -Columns (@(getSPColumns) + @(
            @{ Header = 'Recommendation'; Property = 'Recommendation'; Select = $true }, @{ Header = 'Recommendation type'; Property = 'RecommendationType'; Select = $true }, @{ Header = 'Priority'; Property = 'Priority'; Select = $true }, @{ Header = 'Status'; Property = 'Status'; Select = $true }))
    $rows = foreach ($sp in $cu.where( { $_.SPCustomSecurityAttributes.Count -gt 0 } )) {
        foreach ($item in $sp.SPCustomSecurityAttributes) {
            $row = getSPRowBase $sp
            $row.AttributeSet = $item.attributeSet; $row.Attribute = $item.attribute; $row.Value = (@($item.value) -join "$CsvDelimiterOpposite ")
            [PSCustomObject]$row
        }
    }
    addReportSection -Region 'ServicePrincipalsCustomSecurityAttributes' -Title 'Service Principal custom security attributes' -Rows @($rows | Sort-Object -Property SPDisplayName, SPObjectId, AttributeSet, Attribute) -NotCollectedReason $(if ($null -eq $htCustomSecurityAttributesBySPId) { 'optional permission CustomSecAttributeAssignment.Read.All missing' }) -Columns (@(getSPColumns) + @(
            @{ Header = 'Attribute set'; Property = 'AttributeSet'; Select = $true }, @{ Header = 'Attribute'; Property = 'Attribute'; Select = $true }, @{ Header = 'Value'; Property = 'Value' }))
    #endregion SUMMARYOptionalData

    #region SUMMARYApplicationSecrets
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary ApplicationSecrets'

    $applicationSecrets = $cu.where( { $_.APPPasswordCredentials.Count -gt 0 } )
    $applicationSecretsCount = $applicationSecrets.Count

    if ($applicationSecretsCount -gt 0) {

        $htmlTableId = 'TenantSummary_ApplicationSecrets'
        $tf = "tf$($htmlTableId)"

        $applicationSecretsExpireSoon = $applicationSecrets.APPPasswordCredentials.expiryInfo.where( { $_ -like 'expires soon*' } )
        $applicationSecretsExpireSoonCount = $applicationSecretsExpireSoon.Count

        if ($applicationSecretsExpireSoonCount -gt 0) {
            $htmlTenantSummary = useSpiSection -Id 'ApplicationSecrets'
            [void]$htmlTenantSummary.AppendLine(@"
        <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">Application secrets ($applicationSecretsExpireSoonCount expire soon)</span><span class="az-chevron" aria-hidden="true"></span></div>
        <div class="az-card-body">
"@)
        }
        else {
            $htmlTenantSummary = useSpiSection -Id 'ApplicationSecrets'
            [void]$htmlTenantSummary.AppendLine(@'
        <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">Application secrets</span><span class="az-chevron" aria-hidden="true"></span></div>
        <div class="az-card-body">
'@)
        }

        $groupedExpiryNoteWorthy = $applicationSecrets.APPPasswordCredentials.expiryInfo.where( { $_ -like 'expires soon*' -or $_ -eq 'expired' -or $_ -eq "expires in more than $ApplicationSecretExpiryMax days" } ) | Group-Object
        if (($groupedExpiryNoteWorthy | Measure-Object).Count -gt 0) {
            $arrExpiryNoteWorthyCounts = [System.Collections.ArrayList]@()
            $arrExpiryNoteWorthyStates = [System.Collections.ArrayList]@()
            foreach ($grp in $groupedExpiryNoteWorthy | Sort-Object -Property count -Descending) {
                $null = $arrExpiryNoteWorthyCounts.Add($grp.Count)
                $null = $arrExpiryNoteWorthyStates.Add($grp.Name)
            }
            $ExpiryNoteWorthyCounts = "'{0}'" -f ($arrExpiryNoteWorthyCounts -join "','")
            $ExpiryNoteWorthyStates = "'{0}'" -f ($arrExpiryNoteWorthyStates -join "','")

            #one color per state (was always 2, with up to 3 states)
            $categoryColoreExpiryNoteWorthy = getChartColors 0 $arrExpiryNoteWorthyStates.Count
            $categoryColorsSeperatedExpiryNoteWorthy = "'{0}'" -f ($categoryColoreExpiryNoteWorthy -join "','")

            [void]$htmlTenantSummary.AppendLine(@"
        <div class="az-charts">
            <div class="az-chart-grid">
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
                    plugins: {
                        legend: {
                            display: false,
                        },
                        tooltip: {
                            bodyFont: { size: 10 },
                            callbacks: {
                                label: function (tooltipItem) {
                                    var dataset = tooltipItem.dataset;
                                    var index = tooltipItem.dataIndex;
                                    window. datasetitem = tooltipItem.datasetIndex;
                                    window.target = dataset.labels[index];
                                    return dataset.labels[index] + ': ' + dataset.data[index];
                                }
                            }
                        }
                    },

                    //Chart.js 4: filter by the clicked slice (was: the slice of the last tooltip, so a tap on touch devices used a stale or no value)
                    onClick: (e, elements, chart) => {
                        if (elements.length == 0) {
                            return;
                        }
                        chart.options.plugins.tooltip.callbacks.label({ dataset: chart.data.datasets[elements[0].datasetIndex], datasetIndex: elements[0].datasetIndex, dataIndex: elements[0].index });
                        if (window. datasetitem == 0){
                            window. targetcolumn = '7'
                        }
                        spiChartFilter('$($tf -replace '^tf')', [[window. targetcolumn, window.target]]);

                    }
                }
});

</script>
"@)

        }

        $htmlTableId = 'TenantSummary_ApplicationSecrets'
        $spiGridRows = [System.Collections.ArrayList]@()

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
                        $array = [System.Collections.ArrayList]@()
                        foreach ($secret in $sp.APPPasswordCredentials) {
                            $null = $array.Add("$($secret.keyId)/$($secret.displayName) ($($secret.expiryInfo); $($secret.endDateTimeFormated))")
                            $null = $arrayApplicationSecrets4CSV.Add([PSCustomObject]@{
                                    SPObjectId = $sp.SP.SPObjectId
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

                $spiGridRow = [ordered]@{}
                $spiGridRow.c0 = $sp.SP.SPObjectId
                $spiGridRow.c1 = $sp.SP.SPappId
                $spiGridRow.c2 = $sp.SP.SPdisplayName
                $spiGridRow.c3 = $sp.SP.SPappOwnerOrganizationId
                $spiGridRow.c4 = $appObjectId
                $spiGridRow.c5 = $appId
                $spiGridRow.c6 = $appDisplayName
                $spiGridRow.c7 = $APPPasswordCredentials
                $spiGridRow.c7Html = getSpiCredentialItemsHtml -Credentials $sp.APPPasswordCredentials
                $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)
            }
        }

        if (-not $NoCsvExport) {
            $arrayApplicationSecrets4CSV | Sort-Object -Property SPDisplayName, SPObjectId, APPSecretDisplayName, APPSecretKeyId | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_AppSecrets_.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
        }
        $arrayApplicationSecrets4CSV = $null

        [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle 'Application secrets' -ColumnDefinitions @(
                    @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                    @{ header = 'SP application Id'; property = 'c1' }
                    @{ header = 'SP displayName'; property = 'c2' }
                    @{ header = 'SP App Owner Organization Id'; property = 'c3' }
                    @{ header = 'Application ObjectId'; property = 'c4'; portal = 'application'; appIdProperty = 'c5' }
                    @{ header = 'Application (client) Id'; property = 'c5' }
                    @{ header = 'Application DisplayName'; property = 'c6' }
                    @{ header = 'Application Secrets'; property = 'c7'; htmlProperty = 'c7Html' }
                )))
        $spiGridRows = $null

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        $htmlTenantSummary = useSpiSection -Id 'ApplicationSecrets' -Kind note
        [void]$htmlTenantSummary.AppendLine(@'
            <i class="fa fa-ban" aria-hidden="true"></i> <span>Application secrets</span>
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

        $htmlTableId = 'TenantSummary_ApplicationCertificates'
        $tf = "tf$($htmlTableId)"

        $applicationCertificatesExpireSoon = $applicationCertificates.APPKeyCredentials.expiryInfo.where( { $_ -like 'expires soon*' } )
        $applicationCertificatesExpireSoonCount = $applicationCertificatesExpireSoon.Count

        if ($applicationCertificatesExpireSoonCount -gt 0) {
            $htmlTenantSummary = useSpiSection -Id 'ApplicationCertificates'
            [void]$htmlTenantSummary.AppendLine(@"
        <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">Application certificates ($applicationCertificatesExpireSoonCount expire soon)</span><span class="az-chevron" aria-hidden="true"></span></div>
        <div class="az-card-body">
"@)
        }
        else {
            $htmlTenantSummary = useSpiSection -Id 'ApplicationCertificates'
            [void]$htmlTenantSummary.AppendLine(@'
                <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">Application certificates</span><span class="az-chevron" aria-hidden="true"></span></div>
                <div class="az-card-body">
'@)
        }

        $groupedExpiryNoteWorthy = $applicationCertificates.APPKeyCredentials.expiryInfo.where( { $_ -like 'expires soon*' -or $_ -eq 'expired' } ) | Group-Object
        if (($groupedExpiryNoteWorthy | Measure-Object).Count -gt 0) {
            $arrExpiryNoteWorthyCounts = [System.Collections.ArrayList]@()
            $arrExpiryNoteWorthyStates = [System.Collections.ArrayList]@()
            foreach ($grp in $groupedExpiryNoteWorthy | Sort-Object -Property count -Descending) {
                $null = $arrExpiryNoteWorthyCounts.Add($grp.Count)
                $null = $arrExpiryNoteWorthyStates.Add($grp.Name)
            }
            $ExpiryNoteWorthyCounts = "'{0}'" -f ($arrExpiryNoteWorthyCounts -join "','")
            $ExpiryNoteWorthyStates = "'{0}'" -f ($arrExpiryNoteWorthyStates -join "','")

            #one color per state (was always 2, with up to 3 states)
            $categoryColoreExpiryNoteWorthy = getChartColors 0 $arrExpiryNoteWorthyStates.Count
            $categoryColorsSeperatedExpiryNoteWorthy = "'{0}'" -f ($categoryColoreExpiryNoteWorthy -join "','")

            [void]$htmlTenantSummary.AppendLine(@"
    <div class="az-charts">
        <div class="az-chart-grid">
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
                plugins: {
                    legend: {
                        display: false,
                    },
                    tooltip: {
                        bodyFont: { size: 10 },
                        callbacks: {
                            label: function (tooltipItem) {
                                var dataset = tooltipItem.dataset;
                                var index = tooltipItem.dataIndex;
                                window. datasetitem = tooltipItem.datasetIndex;
                                window.target = dataset.labels[index];
                                return dataset.labels[index] + ': ' + dataset.data[index];
                            }
                        }
                    }
                },

                //Chart.js 4: filter by the clicked slice (was: the slice of the last tooltip, so a tap on touch devices used a stale or no value)
                onClick: (e, elements, chart) => {
                    if (elements.length == 0) {
                        return;
                    }
                    chart.options.plugins.tooltip.callbacks.label({ dataset: chart.data.datasets[elements[0].datasetIndex], datasetIndex: elements[0].datasetIndex, dataIndex: elements[0].index });
                    if (window. datasetitem == 0){
                        window. targetcolumn = '7'
                    }
                    spiChartFilter('$($tf -replace '^tf')', [[window. targetcolumn, window.target]]);

                }
            }
});

</script>
"@)

        }

        $spiGridRows = [System.Collections.ArrayList]@()

        $arrayApplicationCertificates4CSV = [System.Collections.ArrayList]@()
        foreach ($sp in ($applicationCertificates)) {
            if ($sp.APP) {

                $spType = $sp.SP.servicePrincipalType
                $appObjectId = $sp.APP.APPObjectId
                $appId = $sp.APP.APPAppClientId
                $appDisplayName = $sp.APP.APPDisplayName
                $APPKeyCredentials = $null
                if (($sp.APPKeyCredentials)) {
                    if (($sp.APPKeyCredentials.count -gt 0)) {
                        $array = [System.Collections.ArrayList]@()
                        foreach ($key in $sp.APPKeyCredentials) {
                            $null = $array.Add("$($key.keyId)($($key.customKeyIdentifier))/$($key.displayName) ($($key.expiryInfo); $($key.endDateTimeFormated))")
                            $null = $arrayApplicationCertificates4CSV.Add([PSCustomObject]@{
                                    SPObjectId = $sp.SP.SPObjectId
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

                $spiGridRow = [ordered]@{}
                $spiGridRow.c0 = $sp.SP.SPObjectId
                $spiGridRow.c1 = $sp.SP.SPappId
                $spiGridRow.c2 = $sp.SP.SPdisplayName
                $spiGridRow.c3 = $sp.SP.SPappOwnerOrganizationId
                $spiGridRow.c4 = $appObjectId
                $spiGridRow.c5 = $appId
                $spiGridRow.c6 = $appDisplayName
                $spiGridRow.c7 = $APPKeyCredentials
                $spiGridRow.c7Html = getSpiCredentialItemsHtml -Credentials $sp.APPKeyCredentials -Certificate
                $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)
            }
        }

        if (-not $NoCsvExport) {
            $arrayApplicationCertificates4CSV | Sort-Object -Property SPDisplayName, SPObjectId, APPCertificateDisplayName, APPCertificateKeyId | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_AppCertificates_.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
        }
        $arrayApplicationCertificates4CSV = $null

        [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle 'Application certificates' -ColumnDefinitions @(
                    @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                    @{ header = 'SP application Id'; property = 'c1' }
                    @{ header = 'SP displayName'; property = 'c2' }
                    @{ header = 'SP App Owner Organization Id'; property = 'c3' }
                    @{ header = 'Application ObjectId'; property = 'c4'; portal = 'application'; appIdProperty = 'c5' }
                    @{ header = 'Application (client) Id'; property = 'c5' }
                    @{ header = 'Application DisplayName'; property = 'c6' }
                    @{ header = 'Application Certificates'; property = 'c7'; htmlProperty = 'c7Html' }
                )))
        $spiGridRows = $null

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        $htmlTenantSummary = useSpiSection -Id 'ApplicationCertificates' -Kind note
        [void]$htmlTenantSummary.AppendLine(@'
            <i class="fa fa-ban" aria-hidden="true"></i> <span>Application certificates</span>
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

        $htmlTableId = 'TenantSummary_ApplicationFederatedIdentityCredentials'
        $tf = "tf$($htmlTableId)"

        $htmlTenantSummary = useSpiSection -Id 'ApplicationFederatedIdentityCredentials'
        [void]$htmlTenantSummary.AppendLine(@'
        <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">Application federated identity credentials</span><span class="az-chevron" aria-hidden="true"></span></div>
        <div class="az-card-body">
'@)

        $spiGridRows = [System.Collections.ArrayList]@()
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
                        $array = [System.Collections.ArrayList]@()
                        foreach ($fic in $sp.APPFederatedIdentityCredentials) {
                            if ([string]::IsNullOrWhiteSpace($fic.description)) {
                                $descriptionFederatedIdentityCredential = 'not given'
                            }
                            else {
                                $descriptionFederatedIdentityCredential = $fic.description
                            }
                            $null = $array.Add("$($fic.name)(id: $($fic.id)) / description: '$($descriptionFederatedIdentityCredential)' (issuer: $($fic.issuer); subject: $($fic.subject)$(if ($fic.claimsMatchingExpression.value) { "; claims expression: $($fic.claimsMatchingExpression.value)" }); audiences: $((($fic.audiences | Sort-Object) -join "$CsvDelimiterOpposite ")))")
                            $null = $arrayApplicationFederatedIdentityCredentials4CSV.Add([PSCustomObject]@{
                                    SPObjectId = $sp.SP.SPObjectId
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
                                    #review F-44 (added)
                                    APPFederatedIdentityCredentialClaimsMatchingExpression = $fic.claimsMatchingExpression.value
                                })
                        }
                        $APPFederatedIdentityCredentials = "$(($sp.APPFederatedIdentityCredentials).Count) ($($array -join "$CsvDelimiterOpposite "))"
                    }
                    else {
                        $APPFederatedIdentityCredentials = $null
                    }
                }

                $spiGridRow = [ordered]@{}
                $spiGridRow.c0 = $sp.SP.SPObjectId
                $spiGridRow.c1 = $sp.SP.SPappId
                $spiGridRow.c2 = $sp.SP.SPdisplayName
                $spiGridRow.c3 = $sp.SP.SPappOwnerOrganizationId
                $spiGridRow.c4 = $appObjectId
                $spiGridRow.c5 = $appId
                $spiGridRow.c6 = $appDisplayName
                $spiGridRow.c7 = $APPFederatedIdentityCredentials
                $spiGridRow.c7Html = getSpiFicItemsHtml -Credentials $sp.APPFederatedIdentityCredentials
                $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)
            }
        }

        if (-not $NoCsvExport) {
            $arrayApplicationFederatedIdentityCredentials4CSV | Sort-Object -Property SPDisplayName, SPObjectId, APPFederatedIdentityCredentialName, APPFederatedIdentityCredentialId, APPFederatedIdentityCredentialIssuer, APPFederatedIdentityCredentialSubject | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_FederatedIdentityCredentials_.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
        }
        $arrayApplicationFederatedIdentityCredentials4CSV = $null

        [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle 'Application federated identity credentials' -ColumnDefinitions @(
                    @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                    @{ header = 'SP application Id'; property = 'c1' }
                    @{ header = 'SP displayName'; property = 'c2' }
                    @{ header = 'SP App Owner Organization Id'; property = 'c3' }
                    @{ header = 'Application ObjectId'; property = 'c4'; portal = 'application'; appIdProperty = 'c5' }
                    @{ header = 'Application (client) Id'; property = 'c5' }
                    @{ header = 'Application DisplayName'; property = 'c6' }
                    @{ header = 'Application Federated Identity Credentials'; property = 'c7'; htmlProperty = 'c7Html' }
                )))
        $spiGridRows = $null

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        $htmlTenantSummary = useSpiSection -Id 'ApplicationFederatedIdentityCredentials' -Kind note
        [void]$htmlTenantSummary.AppendLine(@'
            <i class="fa fa-ban" aria-hidden="true"></i> <span>Application federated identity credentials</span>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYApplicationFederatedIdentityCredentials

    if (-not $NoAzureResourceSideRelations) {
        #region SUMMARYManagedIdentityFederatedIdentityCredentials
        $startCustPolLoop = Get-Date
        Write-Host '  processing Summary ManagedIdentityFederatedIdentityCredentials'

        $managedIdentityFederatedIdentityCredentials = $cu.where( { $_.ManagedIdentityFederatedIdentityCredentials.Count -gt 0 } )
        $managedIdentityFederatedIdentityCredentialsCount = $managedIdentityFederatedIdentityCredentials.Count

        if ($managedIdentityFederatedIdentityCredentialsCount -gt 0) {

            $htmlTableId = 'TenantSummary_ManagedIdentityFederatedIdentityCredentials'
            $tf = "tf$($htmlTableId)"

            $spiGridRows = [System.Collections.ArrayList]@()
            $htmlTenantSummary = useSpiSection -Id 'ManagedIdentityFederatedIdentityCredentials'
            [void]$htmlTenantSummary.AppendLine(@'
<div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">User-assigned managed identities: federated identity credentials</span><span class="az-chevron" aria-hidden="true"></span></div>
<div class="az-card-body">
'@)
            $arrayManagedIdentityFederatedIdentityCredentials4CSV = [System.Collections.ArrayList]@()
            foreach ($sp in ($managedIdentityFederatedIdentityCredentials)) {

                $MIFederatedIdentityCredentials = $null
                if (($sp.ManagedIdentityFederatedIdentityCredentials)) {
                    if (($sp.ManagedIdentityFederatedIdentityCredentials.count -gt 0)) {
                        $array = [System.Collections.ArrayList]@()
                        foreach ($fic in $sp.ManagedIdentityFederatedIdentityCredentials) {
                            $null = $array.Add("$($fic.name) (id: $($fic.id)) (issuer: $($fic.properties.issuer); subject: $($fic.properties.subject); audiences: $((($fic.properties.audiences | Sort-Object) -join "$CsvDelimiterOpposite ")))")
                            $null = $arrayManagedIdentityFederatedIdentityCredentials4CSV.Add([PSCustomObject]@{
                                    ObjectId = $sp.ObjectId
                                    AppId = $sp.SP.SPappId
                                    DisplayName = $sp.SP.SPDisplayName
                                    ARMResourceId = $sp.ManagedIdentity.alternativeName
                                    SPObjectType = $sp.ObjectType
                                    FederatedIdentityCredentialName = $fic.name
                                    FederatedIdentityCredentialId = $fic.id
                                    FederatedIdentityCredentialIssuer = $fic.properties.issuer
                                    FederatedIdentityCredentialSubject = $fic.properties.subject
                                    FederatedIdentityCredentialAudiences = (($fic.properties.audiences | Sort-Object) -join "$CsvDelimiterOpposite ")
                                })
                        }
                        $MIFederatedIdentityCredentials = "$(($sp.ManagedIdentityFederatedIdentityCredentials).Count) ($($array -join "$CsvDelimiterOpposite "))"
                    }
                    else {
                        $MIFederatedIdentityCredentials = $null
                    }
                }

                $spiGridRow = [ordered]@{}
                $spiGridRow.c0 = $sp.SP.SPObjectId
                $spiGridRow.c1 = $sp.SP.SPappId
                $spiGridRow.c2 = $sp.SP.SPdisplayName
                $spiGridRow.c3 = $sp.ManagedIdentity.alternativeName
                $spiGridRow.c3Html = getSpiPortalResourceLinkHtml $sp.ManagedIdentity.alternativeName
                $spiGridRow.c4 = $MIFederatedIdentityCredentials
                $spiGridRow.c4Html = getSpiFicItemsHtml -Credentials $sp.ManagedIdentityFederatedIdentityCredentials -ManagedIdentity
                $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)

            }

            if (-not $NoCsvExport) {
                $arrayManagedIdentityFederatedIdentityCredentials4CSV | Sort-Object -Property DisplayName, ObjectId, FederatedIdentityCredentialName, FederatedIdentityCredentialId, FederatedIdentityCredentialIssuer, FederatedIdentityCredentialSubject | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_FederatedIdentityCredentialsManagedIdentity_.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
            }
            $arrayManagedIdentityFederatedIdentityCredentials4CSV = $null

            [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle 'User-assigned managed identities: federated identity credentials' -ColumnDefinitions @(
                        @{ header = 'SP (MI) ObjectId'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                        @{ header = 'SP (MI) Application Id'; property = 'c1' }
                        @{ header = 'SP (MI) DisplayName'; property = 'c2' }
                        @{ header = 'Resource Id (ARM)'; property = 'c3'; htmlProperty = 'c3Html' }
                        @{ header = 'Federated Identity Credentials'; property = 'c4'; htmlProperty = 'c4Html' }
                    )))
            $spiGridRows = $null

            [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
        }
        else {
            $htmlTenantSummary = useSpiSection -Id 'ManagedIdentityFederatedIdentityCredentials' -Kind note
            [void]$htmlTenantSummary.AppendLine(@'
            <i class="fa fa-ban" aria-hidden="true"></i> <span>User-assigned managed identities: federated identity credentials</span>
'@)
        }
        $endCustPolLoop = Get-Date
        Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
        #endregion SUMMARYManagedIdentityFederatedIdentityCredentials
    }
    else {
        $htmlTenantSummary = useSpiSection -Id 'ManagedIdentityFederatedIdentityCredentials' -Kind note -NoteState disabled
        [void]$htmlTenantSummary.AppendLine(@"
            <i class="fa fa-ban" aria-hidden="true"></i> <span>User-assigned managed identities: federated identity credentials (parameter NoAzureResourceSideRelations = $NoAzureResourceSideRelations)</span>
"@)
    }

    #region SUMMARYHipos
    $startCustPolLoop = Get-Date
    Write-Host '  processing Summary HiPo Users'

    $arrayHipos = [System.Collections.ArrayList]@()
    foreach ($object in $cu) {
        #review R-14: every user owner ('User (Member)', 'User (Guest)', 'User (userType not set)', ...); was Member/Guest only
        if (@(@($object.SPOwners).where({ $_.principalType -like 'User*' })).Count -gt 0) {

            foreach ($user in $object.SPOwners.where({ $_.principalType -like 'User*' })) {

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
                                    permission4HTML = "$([HtmlSafe]::Encode($SPAppRoleAssignment.AppRoleAssignmentResourceDisplayName)) ($(getSpiApiPermissionLinkHtml -ResourceServicePrincipalId $SPAppRoleAssignment.AppRoleAssignmentResourceId -PermissionId $SPAppRoleAssignment.AppRoleId -Type 'application' -Text $SPAppRoleAssignment.AppRolePermission))"
                                })
                        }
                    }
                }

                if ($object.SPOauth2PermissionGrants.count -gt 0) {
                    #review F-8: the entries are per grant and scope - one per consenting user for single-user grants (consentType
                    #Principal); one row per resource, permission and consent kind, single-user grants with their user count
                    $hipoGrantGroups = [ordered]@{}
                    foreach ($SPOauth2PermissionGrant in $object.SPOauth2PermissionGrants) {
                        if ($SPOauth2PermissionGrant.permissionSensitivity -eq 'unclassified') { continue }
                        $hipoGrantKey = "$($SPOauth2PermissionGrant.SPId)|$($SPOauth2PermissionGrant.permission)|$(if ($SPOauth2PermissionGrant.consentType -eq 'AllPrincipals') { 'all' } else { 'user' })"
                        if (-not $hipoGrantGroups.Contains($hipoGrantKey)) { $hipoGrantGroups[$hipoGrantKey] = @{ grant = $SPOauth2PermissionGrant; principals = [System.Collections.Generic.HashSet[string]]::new() } }
                        if ($SPOauth2PermissionGrant.principalId) { $null = $hipoGrantGroups[$hipoGrantKey].principals.Add([string]$SPOauth2PermissionGrant.principalId) }
                    }
                    foreach ($hipoGrantGroup in $hipoGrantGroups.Values) {
                        $SPOauth2PermissionGrant = $hipoGrantGroup.grant
                        $hipoConsentSuffix = $(if ($SPOauth2PermissionGrant.consentType -ne 'AllPrincipals') { " - $($hipoGrantGroup.principals.Count) $(if ($hipoGrantGroup.principals.Count -eq 1) { 'user' } else { 'users' })" } else { '' })
                        if ($SPOauth2PermissionGrant) {
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
                                    permission = "$($SPOauth2PermissionGrant.SPDisplayName) ($($SPOauth2PermissionGrant.permission))$($hipoConsentSuffix)"
                                    permission4HTML = "$([HtmlSafe]::Encode($SPOauth2PermissionGrant.SPDisplayName)) ($(getSpiApiPermissionLinkHtml -ResourceServicePrincipalId $SPOauth2PermissionGrant.SPId -PermissionId $SPOauth2PermissionGrant.id -Type 'delegated' -Text $SPOauth2PermissionGrant.permission -ResourceAppId $SPOauth2PermissionGrant.SPAppId))$([HtmlSafe]::Encode($hipoConsentSuffix))"
                                })
                        }
                    }
                }

                if ($object.SPAzureRoleAssignments.count -gt 0) {
                    foreach ($SPAzureRoleAssignment in $object.SPAzureRoleAssignments) {
                        if ($SPAzureRoleAssignment.roleIsCritical -eq $true) {
                            $roleName = getSpiAzureRoleLinkHtml -RoleId $SPAzureRoleAssignment.roleId -RoleName $SPAzureRoleAssignment.roleName -RoleType $(if ($htCacheDefinitionsRole[$SPAzureRoleAssignment.roleId]) { $htCacheDefinitionsRole[$SPAzureRoleAssignment.roleId].definition.properties.type } else { $SPAzureRoleAssignment.roleType }) -RoleDefinitionId $SPAzureRoleAssignment.roleAssignment.roleDefinitionId
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
                                    permission = "$($SPAzureRoleAssignment.roleName) ($($SPAzureRoleAssignment.roleAssignmentAssignmentResourceType): $($SPAzureRoleAssignment.roleAssignmentAssignmentScopeName))$(if ($SPAzureRoleAssignment.roleConditionEffect) { " - $(getAzureRoleConditionText $SPAzureRoleAssignment)" })"
                                    permission4HTML = "$($roleName) ($([HtmlSafe]::Encode($SPAzureRoleAssignment.roleAssignmentAssignmentResourceType)): $([HtmlSafe]::Encode($SPAzureRoleAssignment.roleAssignmentAssignmentScopeName)))$(if ($SPAzureRoleAssignment.roleConditionEffect) { " - $([HtmlSafe]::Encode((getAzureRoleConditionText $SPAzureRoleAssignment)))" })"
                                })
                        }
                    }
                }

                if ($object.SPAADRoleAssignments.count -gt 0) {
                    foreach ($SPAADRoleAssignment in $object.SPAADRoleAssignments) {
                        if ($SPAADRoleAssignment.roleIsCritical -eq $true) {
                            if ($SPAADRoleAssignment.roleType -eq 'BuiltIn') {
                                #$roleName = "<a class=`"externallink`" href=`"https://github.com/MicrosoftDocs/azure-docs/blob/main/articles/active-directory/roles/permissions-reference.md#$($SPAADRoleAssignment.roleDefinitionName -replace ' ', '-')`" target=`"_blank`">$($SPAADRoleAssignment.roleDefinitionName)</a>"
                                $roleName = (getSpiEntraRoleLinkHtml -RoleDefinitionId $SPAADRoleAssignment.roleDefinitionId -RoleName $SPAADRoleAssignment.roleDefinitionName)
                            }
                            else {
                                $roleName = [HtmlSafe]::Encode($SPAADRoleAssignment.roleDefinitionName)
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
                                    permission4HTML = "$($roleName) ($([HtmlSafe]::Encode($SPAADRoleAssignment.roleDefinitionId)))"
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
        $htmlTableId = 'TenantSummary_Hipos'
        $tf = "tf$($htmlTableId)"

        $htmlTenantSummary = useSpiSection -Id 'Hipos'
        [void]$htmlTenantSummary.AppendLine(@"
        <div class="az-card-head" role="button" tabindex="0" aria-expanded="false"><span class="az-card-title">High-privilege (HiPo) users</span><span class="az-chevron" aria-hidden="true"></span></div>
        <div class="az-card-body">
        <div class="az-note az-note-info"><i class="fa fa-lightbulb-o" aria-hidden="true"></i> A HiPo user has direct or indirect ownership of Service Principals with classified permissions. $arrayHiposGroupedCount users; one row per user, Service Principal and permission.</div>
"@)
        [void]$htmlTenantSummary.AppendLine((getThomasCreditHtml -What 'The critical Entra roles counted here (as Microsoft marks them privileged, also through role-assignable groups)'))


        $spiGridRows = [System.Collections.ArrayList]@()

        $arrayHiposSorted = $arrayHipos | Sort-Object -Property userId, SPId, capability, permission
        foreach ($hipo in ($arrayHiposSorted)) {

            $spiGridRow = [ordered]@{}
            $spiGridRow.c0 = $hipo.user
            $spiGridRow.c1 = $hipo.userId
            $spiGridRow.c2 = $hipo.userType
            $spiGridRow.c3 = $hipo.ownership
            $spiGridRow.c4 = $hipo.SPDisplayName
            $spiGridRow.c5 = $hipo.SPType
            $spiGridRow.c6 = $hipo.SPId
            $spiGridRow.c7 = $hipo.SPappId
            $spiGridRow.c8 = $hipo.capability
            $spiGridRow.c9 = $hipo.classification
            $spiGridRow.c10Html = @"
$($hipo.permission4HTML)
"@
            #review F-45: the plain text from the value (as in the CSV), not from the markup
            $spiGridRow.c10 = $hipo.permission
            $null = $spiGridRows.Add([PSCustomObject]$spiGridRow)

        }

        if (-not $NoCsvExport) {
            $arrayHiposSorted | Select-Object -ExcludeProperty permission4HTML | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_HiPoUsers_.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
        }

        [void]$htmlTenantSummary.AppendLine((buildSpiGrid -HtmlTableId $htmlTableId -Rows $spiGridRows -PopoutTitle 'High-privilege (HiPo) users' -ColumnDefinitions @(
                    @{ header = 'User'; property = 'c0' }
                    @{ header = 'UserId'; property = 'c1'; portal = 'user' }
                    @{ header = 'UserType'; property = 'c2'; filter = 'select' }
                    @{ header = 'ownership'; property = 'c3'; filter = 'select' }
                    @{ header = 'SP displayName'; property = 'c4' }
                    @{ header = 'SP type'; property = 'c5'; filter = 'select' }
                    @{ header = 'SP objectId'; property = 'c6'; portal = 'servicePrincipal'; appIdProperty = 'c7' }
                    @{ header = 'SP appId'; property = 'c7' }
                    @{ header = 'Capability'; property = 'c8'; filter = 'select' }
                    @{ header = 'Classification'; property = 'c9'; filter = 'select' }
                    @{ header = 'Permissions'; property = 'c10'; htmlProperty = 'c10Html' }
                )))
        $spiGridRows = $null

        [void]$htmlTenantSummary.AppendLine(@'
</div>
'@)
    }
    else {
        $htmlTenantSummary = useSpiSection -Id 'Hipos' -Kind note
        [void]$htmlTenantSummary.AppendLine(@'
            <i class="fa fa-ban" aria-hidden="true"></i> <span>High-privilege (HiPo) users</span>
'@)
    }

    $endCustPolLoop = Get-Date
    Write-Host "   processing duration: $((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalMinutes) minutes ($((New-TimeSpan -Start $startCustPolLoop -End $endCustPolLoop).TotalSeconds) seconds)"
    #endregion SUMMARYHipos

    #region SUMMARYInsights
    addReportSection -Region 'ServicePrincipalCredentials' -Title 'Service Principal credentials (on the service principal, not on the app registration)' -Rows $insightRows.SPCredentials -Columns (@(getSPColumns) + @(
            @{ Header = 'Credential type'; Property = 'CredentialType'; Select = $true }, @{ Header = 'Key Id'; Property = 'KeyId' }, @{ Header = 'Display name'; Property = 'DisplayName' }, @{ Header = 'Expiry'; Property = 'ExpiryInfo'; Select = $true }, @{ Header = 'End (UTC)'; Property = 'EndDateTime' }, @{ Header = 'Note'; Property = 'Note'; Select = $true }))

    addReportSection -Region 'ApplicationConfigurationFindings' -Title 'Application configuration findings' -Rows $insightRows.AppConfiguration -Columns @(
        @{ Header = 'App object Id'; Property = 'APPObjectId' }, @{ Header = 'App displayName'; Property = 'APPDisplayName' }, @{ Header = 'SP object Id'; Property = 'SPObjectId' }, @{ Header = 'SP type'; Property = 'SPType'; Select = $true },
        @{ Header = 'Severity'; Property = 'Severity'; Select = $true }, @{ Header = 'Finding'; Property = 'Finding'; Select = $true }, @{ Header = 'Why it is risky'; Property = 'Why'; Csv = $false }, @{ Header = 'Detail'; Property = 'Detail' })

    addReportSection -Region 'FederatedIdentityCredentialTrust' -Title 'Federated identity credentials: who can sign in' -Rows $insightRows.FederatedCredentialTrust -Columns (@(getSPColumns) + @(
            @{ Header = 'Credential on'; Property = 'CredentialOn'; Select = $true }, @{ Header = 'Name'; Property = 'Name' }, @{ Header = 'Issuer type'; Property = 'IssuerType'; Select = $true }, @{ Header = 'Trust'; Property = 'Trust' }, @{ Header = 'Issuer'; Property = 'Issuer' }, @{ Header = 'Subject'; Property = 'Subject' }, @{ Header = 'Finding'; Property = 'Finding'; Select = $true }))

    addReportSection -Region 'ServicePrincipalEffectivePrivilege' -Title 'Service Principal effective privilege (incl. control through ownership)' -CreditHtml (getThomasCreditHtml -What 'The classification of privileged Entra roles (as Microsoft marks them) and Entra roles through role-assignable groups') -Rows $insightRows.EffectivePrivilege -Columns (@(getSPColumns) + @(
            @{ Header = 'Effective level'; Property = 'EffectiveLevel'; Select = $true }, @{ Header = 'Level'; Property = 'Level'; Select = $true }, @{ Header = 'Category'; Property = 'Category'; Select = $true }, @{ Header = 'Why it is risky'; Property = 'Why'; Csv = $false }, @{ Header = 'Detail'; Property = 'Detail' }, @{ Header = 'Via'; Property = 'Via' }))

    addReportSection -Region 'ServicePrincipalCredentialRisk' -Title 'Credential risk of privileged Service Principals' -Rows $insightRows.CredentialRisk -Columns (@(getSPColumns) + @(
            @{ Header = 'Level'; Property = 'Level'; Select = $true }, @{ Header = 'Credentials'; Property = 'Credentials' }, @{ Header = 'Findings'; Property = 'Findings' }, @{ Header = 'Why it is risky'; Property = 'Why'; Csv = $false }))

    addReportSection -Region 'ThirdPartyAppExposure' -Title 'Multi-tenant apps of other organizations with sensitive permissions' -Description $script:spiRiskSectionNotes.ThirdPartyAppExposure -Rows $insightRows.ThirdParty -Columns (@(getSPColumns) + @(
            @{ Header = 'Publisher'; Property = 'Publisher'; Select = $true }, @{ Header = 'Microsoft first-party'; Property = 'MicrosoftFirstParty'; Select = $true }, @{ Header = 'App roles'; Property = 'AppRoles' }, @{ Header = 'Delegated (consent for all users)'; Property = 'DelegatedAdminConsent' }, @{ Header = 'Delegated (consent of single users, critical)'; Property = 'DelegatedUserConsent' }, @{ Header = 'Effective level'; Property = 'Level'; Select = $true }))

    addReportSection -Region 'CleanupCandidates' -Title 'Cleanup candidates' -Rows $insightRows.Cleanup -Columns (@(getSPColumns) + @(
            @{ Header = 'Reason'; Property = 'Reason'; Select = $true }, @{ Header = 'Why it is risky'; Property = 'Why'; Csv = $false }, @{ Header = 'Detail'; Property = 'Detail' }))

    addReportSection -Region 'AgentObjects' -Title 'Agent objects (Microsoft Entra Agent ID)' -Rows $insightRows.AgentObjects -NotCollectedReason $(if ($null -eq $htAgentObjectsById) { 'agent object types not available' }) -Columns @(
        @{ Header = 'Agent type'; Property = 'AgentType'; Select = $true }, @{ Header = 'Object Id'; Property = 'ObjectId' }, @{ Header = 'Display name'; Property = 'DisplayName' }, @{ Header = 'In this report'; Property = 'InThisReport'; Select = $true }, @{ Header = 'Effective level'; Property = 'EffectiveLevel'; Select = $true })

    addReportSection -Region 'TenantConsentSettings' -Title 'Tenant consent settings' -Rows @($tenantConsentSettings | Select-Object -Property *, @{ Name = 'Why'; Expression = { getRiskExplanation -Table 'ConsentSettings' -Key $_.Setting } }) -NotCollectedReason $(if ($null -eq $tenantConsentSettings) { 'optional permission Policy.Read.All missing' }) -Columns @(
        @{ Header = 'Setting'; Property = 'Setting' }, @{ Header = 'Value'; Property = 'Value' }, @{ Header = 'Note'; Property = 'Note' }, @{ Header = 'Why it matters'; Property = 'Why'; Csv = $false })
    #endregion SUMMARYInsights

    #the pages are written after function summary (getSpiPagesHtml)
    closeSpiSection

}
#endregion TenantSummary

#endregion Function

#region verifyClassifications
Write-Host 'Verify Classifications (permissionClassification.json)'
#review R-36: relative to the working directory (as before: started from the repository root) or next to the script; a file that
#is not valid JSON is reported as such (was 'not found' for every error)
$classificationFile = @(".$($DirectorySeparatorChar)$($ScriptPath)$($DirectorySeparatorChar)permissionClassification.json", (Join-Path -Path $PSScriptRoot -ChildPath 'permissionClassification.json')).where({ Test-Path -LiteralPath $_ -PathType Leaf }, 'First')
if (-not $classificationFile) {
    Write-Host "file permissionClassification.json not found (looked in '.$($DirectorySeparatorChar)$($ScriptPath)' and '$($PSScriptRoot)')"
    throw 'permissionClassification.json not found'
}
try {
    $getClassifications = Get-Content -Raw -LiteralPath $classificationFile[0] | ConvertFrom-Json -AsHashtable -ErrorAction Stop
}
catch {
    Write-Host "file '$($classificationFile[0])' could not be read: $($_.Exception.Message)"
    throw
}

#validate permission assigned to ONE classification
foreach ($permissionType in $getClassifications.permissions.Keys) {
    $classifications = ($getClassifications.permissions[$permissionType].'classifications'.Keys)
    $arrayPermissions4Classification = [System.Collections.ArrayList]@()
    foreach ($classification in $classifications) {
        foreach ($permission in $getClassifications.permissions[$permissionType].'classifications'[$classification].'includes') {
            $null = $arrayPermissions4Classification.Add($permission)
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
$htCacheDefinitionsRole = [System.Collections.Hashtable]::Synchronized(@{})
$htCacheAssignmentsRole = [System.Collections.Hashtable]::Synchronized(@{})
$htCacheAssignmentsPolicy = [System.Collections.Hashtable]::Synchronized(@{})
$htRoleAssignmentsFromAPIInheritancePrevention = [System.Collections.Hashtable]::Synchronized(@{})
$outOfScopeSubscriptions = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))
$htAllSubscriptionsFromAPI = @{}
$customDataCollectionDuration = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))
$arrayDataCollectionProgressMg = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))
$arrayDataCollectionProgressSub = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))
$arrayUserAssignedIdentities4Resources = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))
#user-assigned managed identities keyed by resource id (lower case): isolation scope and assignment restrictions
$htUserAssignedIdentitiesSettings = [System.Collections.Hashtable]::Synchronized(@{})

$htAssignmentsByPrincipalIdServicePrincipals = @{}
$htAssignmentsByPrincipalIdGroups = @{}
$htDoARMRoleAssignmentScheduleInstances = [System.Collections.Hashtable]::Synchronized(@{})
$htDoARMRoleAssignmentScheduleInstances.Do = $true
$arrayMIUserAssignedFederatedCreds = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))
$arrayUserAssignedIdentityIds4FederatedCreds = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))
$htMIUserAssignedFederatedCreds = @{}
#endregion helper ht / collect results /save some time

$permissionCheckResults = @()
#region validation / check 'Microsoft Graph API' Access
#review R-35: also managed identities (ManagedService) and workload identity federation (ClientAssertion): every non-user account
if ($azAPICallConf['htParameters'].onAzureDevOps -eq $true -or $azAPICallConf['checkContext'].Account.Type -in 'ServicePrincipal', 'ManagedService', 'ClientAssertion') {
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

if (-not $NoAzureResourceSideRelations) {
    Write-Host "Running $($Product) for ManagementGroupId: '$($ManagementGroupId -join ', ')'" -ForegroundColor Yellow

    #region permissionsCheck
    foreach ($managementGroupIdEntry in $ManagementGroupId) {
        $currentTask = "Checking permissions for ManagementGroup '$managementGroupIdEntry'"
        Write-Host $currentTask
        $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/providers/Microsoft.Management/managementGroups/$($managementGroupIdEntry)?api-version=2023-04-01"
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
        Write-Host 'Please consult the documentation: https://github.com/JulianHayward/AzADServicePrincipalInsights/blob/main/setup/README.md#permissions'
        if ($azAPICallConf['htParameters'].onAzureDevOps -eq $true) {
            Write-Error 'Error'
        }
        else {
            throw 'Error - check the last console output for details'
        }
    }
    #endregion permissionsCheck

    #region Azure phase (review P-3 a)
    #The collection of management groups, subscriptions, role assignments, resources and user-assigned identities. Default mode: it
    #runs as one task of the tenant-wide Graph block (beside the Graph lists, see there); its variables are set in this scope
    #afterwards, as if it had run here. -OnlyProcessSPs...: the SP selection needs its role assignments - it runs here, first.
    $azurePhase = {
        #region GettingEntities
        $startEntities = Get-Date
        $currentTask = 'Getting Entities'
        Write-Host "$currentTask"
        $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/providers/Microsoft.Management/getEntities?api-version=2023-04-01"
        $method = 'POST'
        $arrayEntitiesFromAPI = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

        $htSubscriptionsMgPath = @{}
        $htManagementGroupsMgPath = @{}
        $htEntitiesPlain = @{}

        foreach ($entity in $arrayEntitiesFromAPI) {
            $htEntitiesPlain[$entity.Name] = @{}
            $htEntitiesPlain[$entity.Name] = $entity
        }

        foreach ($entity in $arrayEntitiesFromAPI) {
            if ($entity.Type -eq '/subscriptions') {
                $array = $entity.properties.parentNameChain
                $array += $entity.name
                $htSubscriptionsMgPath[$entity.name] = @{
                    ParentNameChain = $entity.properties.parentNameChain
                    ParentNameChainDelimited = $entity.properties.parentNameChain -join '/'
                    Parent = $entity.properties.parent.Id -replace '.*/'
                    ParentName = $htEntitiesPlain[$entity.properties.parent.Id -replace '.*/'].properties.displayName
                    DisplayName = $entity.properties.displayName
                    path = $array
                    pathDelimited = $array -join '/'
                    level = (($entity.properties.parentNameChain).Count - 1)
                }
            }
            if ($entity.Type -eq 'Microsoft.Management/managementGroups') {
                if ([string]::IsNullOrEmpty($entity.properties.parent.Id)) {
                    $parent = '_TenantRoot_'
                }
                else {
                    $parent = $entity.properties.parent.Id -replace '.*/'
                }

                $array = $entity.properties.parentNameChain
                $array += $entity.name
                $htManagementGroupsMgPath[$entity.name] = @{
                    ParentNameChain = $entity.properties.parentNameChain
                    ParentNameChainDelimited = $entity.properties.parentNameChain -join '/'
                    ParentNameChainCount = ($entity.properties.parentNameChain | Measure-Object).Count
                    Parent = $parent
                    DisplayName = $entity.properties.displayName
                    path = $array
                    pathDelimited = $array -join '/'
                }
            }

        }

        $endEntities = Get-Date
        Write-Host "Getting Entities duration: $((New-TimeSpan -Start $startEntities -End $endEntities).TotalSeconds) seconds"
        #endregion GettingEntities

        #region subscriptions
        $startGetSubscriptions = Get-Date
        $currentTask = 'Getting all Subscriptions'
        Write-Host "$currentTask"
        $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/subscriptions?api-version=2022-12-01"
        $method = 'GET'
        $requestAllSubscriptionsAPI = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask

        foreach ($subscription in $requestAllSubscriptionsAPI) {
            $htAllSubscriptionsFromAPI[$subscription.subscriptionId] = @{
                subDetails = $subscription
            }
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
            if ($htAllSubscriptionsFromAPI[$childrenSubscription.name]) {
                $sub = $htAllSubscriptionsFromAPI[$childrenSubscription.name]
                if ($sub.subDetails.subscriptionPolicies.quotaId.startswith('AAD_', 'CurrentCultureIgnoreCase') -or $sub.subDetails.state -ne 'Enabled') {
                    if (($sub.subDetails.subscriptionPolicies.quotaId).startswith('AAD_', 'CurrentCultureIgnoreCase')) {
                        $null = $script:outOfScopeSubscriptions.Add([PSCustomObject]@{
                                subscriptionId = $childrenSubscription.name
                                subscriptionName = $childrenSubscription.properties.displayName
                                outOfScopeReason = "QuotaId: AAD_ (State: $($sub.subDetails.state))"
                                ManagementGroupId = $htSubscriptionsMgPath[$childrenSubscription.name].Parent
                                ManagementGroupName = $htSubscriptionsMgPath[$childrenSubscription.name].ParentName
                                Level = $htSubscriptionsMgPath[$childrenSubscription.name].level
                            })
                    }
                    #review F-18: one entry per subscription (AAD_ and not enabled were two; the AAD_ reason names the state)
                    elseif ($sub.subDetails.state -ne 'Enabled') {
                        $null = $script:outOfScopeSubscriptions.Add([PSCustomObject]@{
                                subscriptionId = $childrenSubscription.name
                                subscriptionName = $childrenSubscription.properties.displayName
                                outOfScopeReason = "State: $($sub.subDetails.state)"
                                ManagementGroupId = $htSubscriptionsMgPath[$childrenSubscription.name].Parent
                                ManagementGroupName = $htSubscriptionsMgPath[$childrenSubscription.name].ParentName
                                Level = $htSubscriptionsMgPath[$childrenSubscription.name].level
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
                                    ManagementGroupId = $htSubscriptionsMgPath[$childrenSubscription.name].Parent
                                    ManagementGroupName = $htSubscriptionsMgPath[$childrenSubscription.name].ParentName
                                    Level = $htSubscriptionsMgPath[$childrenSubscription.name].level
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
            else {
                Write-Host "The subscription '$($childrenSubscription.name)' doesn't exists in the response of the subscriptions API call, but in the response of the entity API call. Skipping this subscription, because it doesn't exist in the tenant!"
            }
        }
        $subsToProcessInCustomDataCollectionCount = ($subsToProcessInCustomDataCollection | Measure-Object).Count
        #endregion subscriptionFilter

        #region dataprocessingDefinitionCaching
        $startDefinitionsCaching = Get-Date

        $currentTask = 'Caching built-in Role definitions'
        Write-Host " $currentTask"
        #Azure role conditions: the role definitions with their conditions (permissions.condition) - only the preview api-versions
        #return them (2022-04-01, the newest stable one, does not); AzureUSGovernment / AzureChinaCloud: 2022-05-01-preview (as AzGovViz).
        #If the preview api-version is not accepted: 2022-04-01 (conditions of role definitions then not evaluated)
        $roleDefinitionsApiVersion = $(if ($azAPICallConf['checkContext'].Environment.Name -in 'AzureUSGovernment', 'AzureChinaCloud') { '2022-05-01-preview' } else { '2023-07-01-preview' })
        $requestRoleDefinitionAPI = $null
        foreach ($roleDefinitionsApiVersionTry in @($roleDefinitionsApiVersion, '2022-04-01')) {
            #from the provider (R-34: also without a subscription in the context). Azure role conditions: the provider returns every
            #built-in role (1,004 on 2026-11-10); a subscription only those assignable there (988 - without Reservations, Savings plan,
            #Service Group roles), and a role a condition names but the listing lacks would count as unknown (critical)
            $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/providers/Microsoft.Authorization/roleDefinitions?api-version=$($roleDefinitionsApiVersionTry)&`$filter=type eq 'BuiltInRole'"
            $method = 'GET'
            try {
                $requestRoleDefinitionAPI = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask
            }
            catch {
                $requestRoleDefinitionAPI = "failed: $($_.Exception.Message)"
            }
            if ($requestRoleDefinitionAPI -isnot [string] -and @($requestRoleDefinitionAPI).Count -gt 0) {
                $roleDefinitionsApiVersion = $roleDefinitionsApiVersionTry
                break
            }
            if ($roleDefinitionsApiVersionTry -ne '2022-04-01') {
                Write-Host "  Role definitions api-version $($roleDefinitionsApiVersionTry) not usable ($($requestRoleDefinitionAPI)) - using 2022-04-01 (conditions of role definitions are not evaluated)" -ForegroundColor Yellow
            }
        }
        if ($requestRoleDefinitionAPI -is [string]) {
            throw "Built-in role definitions could not be read: $($requestRoleDefinitionAPI)"
        }
        Write-Host "  role definitions api-version: $($roleDefinitionsApiVersion)"

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

            ($htCacheDefinitionsRole)[$roleDefinition.name] = @{
                definition = ($roleDefinition)
                linkToAzAdvertizer = "<a class=`"externallink`" href=`"https://www.azadvertizer.net/azrolesadvertizer/$($roleDefinition.name).html`" target=`"_blank`">$($roleDefinition.properties.roleName)</a>"
                roleIsCritical = $roleCapable4RoleAssignmentsWrite
            }

        }

        $endDefinitionsCaching = Get-Date
        Write-Host "Caching built-in definitions duration: $((New-TimeSpan -Start $startDefinitionsCaching -End $endDefinitionsCaching).TotalSeconds) seconds"
        #endregion dataprocessingDefinitionCaching

        Write-Host 'Collecting custom data'
        $startDataCollection = Get-Date
        #AzAPICall tracks every request in $azAPICallConf['arrayAPICallTracking']; remember where this phase starts
        $apiCallTrackingIndexStartDataCollection = $azAPICallConf['arrayAPICallTracking'].Count

        dataCollection -mgId $ManagementGroupId
        resolveMissingRoleDefinitions
        applyAzureRoleConditions

        #review P-14: federated identity credentials of the user-assigned managed identities, in parallel (was per subscription item)
        if ($arrayUserAssignedIdentityIds4FederatedCreds.Count -gt 0) {
            $startUamiFederatedCreds = Get-Date
            @($arrayUserAssignedIdentityIds4FederatedCreds) | ForEach-Object -Parallel {
                #R-7: the AzAPICall version of the main runspace (an autoload would take the newest installed version)
                if (-not (Get-Module -Name $using:AzAPICallModuleName)) { Import-Module -Name $using:azAPICallModuleImportPath -ErrorAction Stop }
                $uami = $_
                $azAPICallConf = $using:azAPICallConf
                $arrayMIUserAssignedFederatedCreds = $using:arrayMIUserAssignedFederatedCreds
                $htParallelFailures = $using:htParallelFailures
                try {
                    $currentTask = "Getting MI USER federatedCreds: 'MI: $($uami.name -replace '##', '# #' -replace '[\r\n]+', ' ')' '$($uami.subscriptionName -replace '##', '# #' -replace '[\r\n]+', ' ')' ('$($uami.subscriptionId)')"
                    #$uami.id starts with '/'
                    $uri = "$($azAPICallConf['azAPIEndpointUrls'].ARM)$($uami.id)/federatedIdentityCredentials?api-version=2024-11-30"
                    $miUserAssignedFederatedCreds = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method 'GET' -currentTask $currentTask -caller 'CustomDataCollection'
                    if ($miUserAssignedFederatedCreds -eq 'SupportForFederatedIdentityCredentialsNotEnabled' -or $miUserAssignedFederatedCreds -eq 'NotFound') {
                        #skipping
                    }
                    elseif ($miUserAssignedFederatedCreds.Count -gt 0) {
                        foreach ($miUserAssignedFederatedCred in $miUserAssignedFederatedCreds) {
                            $miUserAssignedFederatedCred | Add-Member -MemberType NoteProperty -Name miResourceId -Value $uami.id
                            $null = $arrayMIUserAssignedFederatedCreds.Add($miUserAssignedFederatedCred)
                        }
                    }
                }
                catch {
                    $failedItem = "$($uami.name -replace '##', '# #' -replace '[\r\n]+', ' ') ($($uami.id))"
                    $htParallelFailures["UAMI federated credentials|$failedItem"] = [PSCustomObject]@{ Phase = 'UAMI federated credentials'; Item = $failedItem; Error = $_.Exception.Message }
                    Write-Host "  UAMI federated credentials: '$failedItem' failed - skipped: $($_.Exception.Message)" -ForegroundColor Yellow
                }
            } -ThrottleLimit $ThrottleLimitARM
            checkParallelFailures -phase 'UAMI federated credentials' -itemCount $arrayUserAssignedIdentityIds4FederatedCreds.Count
            Write-Host " UAMI federated credentials ($($arrayUserAssignedIdentityIds4FederatedCreds.Count) UAMIs) duration: $([math]::Round((New-TimeSpan -Start $startUamiFederatedCreds -End (Get-Date)).TotalSeconds, 1)) seconds"
        }

        Write-Host '**** arrayUserAssignedIdentities4ResourcesCount:' $arrayUserAssignedIdentities4Resources.Count
        Write-Host '**** arrayMIUserAssignedFederatedCredsCount:' $arrayMIUserAssignedFederatedCreds.Count

        if ($arrayMIUserAssignedFederatedCreds.Count -gt 0) {
            $arrayMIUserAssignedFederatedCredsGroupedbyMiResourceId = $arrayMIUserAssignedFederatedCreds | Group-Object -Property miResourceId
            foreach ($entry in $arrayMIUserAssignedFederatedCredsGroupedbyMiResourceId) {
                $htMIUserAssignedFederatedCreds[$entry.Name] = @{
                    federatedCredentials = $entry.Group | Sort-Object -Property name
                }
            }
        }
        Write-Host "**** htMIUserAssignedFederatedCredsCount: $($htMIUserAssignedFederatedCreds.Keys.count)"

        #region dataColletionAz summary
        $endDataCollection = Get-Date
        Write-Host "Collecting custom data duration: $((New-TimeSpan -Start $startDataCollection -End $endDataCollection).TotalMinutes) minutes ($((New-TimeSpan -Start $startDataCollection -End $endDataCollection).TotalSeconds) seconds)"

        $durationDataMG = ($customDataCollectionDuration | Where-Object { $_.Type -eq 'MG' })
        $durationDataSUB = ($customDataCollectionDuration | Where-Object { $_.Type -eq 'SUB' })
        $durationMGAverageMaxMin = ($durationDataMG.DurationSec | Measure-Object -Average -Maximum -Minimum)
        $durationSUBAverageMaxMin = ($durationDataSUB.DurationSec | Measure-Object -Average -Maximum -Minimum)
        Write-Host "Collecting custom data for $($allManagementGroupsFromEntitiesChildOfRequestedMgCount) ManagementGroups Avg/Max/Min duration in seconds: Average: $([math]::Round($durationMGAverageMaxMin.Average,4)); Maximum: $([math]::Round($durationMGAverageMaxMin.Maximum,4)); Minimum: $([math]::Round($durationMGAverageMaxMin.Minimum,4))"
        Write-Host "Collecting custom data for $($subsToProcessInCustomDataCollection.Count) Subscriptions Avg/Max/Min duration in seconds: Average: $([math]::Round($durationSUBAverageMaxMin.Average,4)); Maximum: $([math]::Round($durationSUBAverageMaxMin.Maximum,4)); Minimum: $([math]::Round($durationSUBAverageMaxMin.Minimum,4))"


        #review F-1: a copy of the list (ToArray takes the list's lock); GetRange returned a live view that throws as soon as the Graph
        #requests running at the same time (P-3 a) add an entry. Console statistics only - they never fail the run.
        try {
            $apiCallTrackingAll = $azAPICallConf['arrayAPICallTracking'].ToArray()
            #(review P-3 a: the Graph requests of the same time are not counted here)
            $apiCallTrackingDataCollection = @(for ($apiCallTrackingIndex = $apiCallTrackingIndexStartDataCollection; $apiCallTrackingIndex -lt $apiCallTrackingAll.Count; $apiCallTrackingIndex++) { if ($apiCallTrackingAll[$apiCallTrackingIndex].TargetEndpoint -eq 'ARM') { $apiCallTrackingAll[$apiCallTrackingIndex] } })
            $apiCallTrackingAll = $null
            $APICallTrackingCount = $apiCallTrackingDataCollection.Count
            #TryCounter is 1 for the first attempt (nextLink pages do not increment it)
            $APICallTrackingRetriesCount = $apiCallTrackingDataCollection.where( { $_.TryCounter -gt 1 } ).Count
            $APICallTrackingRestartDueToDuplicateNextlinkCounterCount = $apiCallTrackingDataCollection.where( { $_.RestartDueToDuplicateNextlinkCounter -gt 0 } ).Count
            Write-Host "Collecting custom data APICalls (Management) total count: $APICallTrackingCount ($APICallTrackingRetriesCount retries; $APICallTrackingRestartDueToDuplicateNextlinkCounterCount nextLinkReset)"
        }
        catch {
            Write-Host "Collecting custom data APICalls (Management): statistics not available ($($_.Exception.Message))" -ForegroundColor Yellow
        }
        #endregion dataColletionAz summary
    }
    $azurePhaseDeferred = -not $OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes
    if (-not $azurePhaseDeferred) {
        . $azurePhase
    }
    #endregion Azure phase

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
        Write-Host 'Please consult the documentation: https://github.com/JulianHayward/AzADServicePrincipalInsights/blob/main/setup/README.md#permissions'
        if ($azAPICallConf['htParameters'].onAzureDevOps -eq $true) {
            Write-Error 'Error'
        }
        else {
            throw 'Error - check the last console output for details'
        }
    }
    Write-Host "Paramter -NoAzureResourceSideRelations = '$NoAzureResourceSideRelations'" -ForegroundColor Yellow
    Write-Host "Running $($Product) without resolving 'Role assignments' on the Azure Resource side" -ForegroundColor Yellow
    Write-Host "Running $($Product) without feature 'Managed Identity User Assigned - associated Azure Resources' in Azure" -ForegroundColor Yellow
}
#region AADSP

#region dataColletionAADSP
$startSP = Get-Date
Write-Host 'Getting Service Principal count'
$currentTask = 'getSPCount'
$uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/servicePrincipals/`$count"
$method = 'GET'
$spCount = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask -listenOn 'Content' -consistencyLevel 'eventual'

Write-Host "API `$Count returned $spCount Service Principals"

#region tenant-wide Graph requests in parallel (review P-3 b)
#The tenant-wide lists were requested one after another, page by page. They are independent: one ForEach-Object -Parallel over a
#task list (access test first where there was one: -validateAccess returns 'failed' on 403/400 - e.g. all oauth2PermissionGrants need
#DelegatedPermissionGrant.Read.All or Directory.Read.All, optional data its optional permission - then the request);
#the results are processed below as before. The relationship passes (P-1) are part of it except with -OnlyProcessSPs... (there they
#are only used if enough SPs are processed, decided after the SP selection).
$graphUrl = $azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph
$relationshipSignalPasses = @(
    @{ name = 'spOwners'; property = 'owners'; uri = "$($graphUrl)/v1.0/servicePrincipals?`$select=id&`$expand=owners(`$select=id)" }
    @{ name = 'spOwnedObjects'; property = 'ownedObjects'; uri = "$($graphUrl)/v1.0/servicePrincipals?`$select=id&`$expand=ownedObjects(`$select=id)" }
    @{ name = 'spAppRoleAssignments'; property = 'appRoleAssignments'; uri = "$($graphUrl)/v1.0/servicePrincipals?`$select=id&`$expand=appRoleAssignments" }
    @{ name = 'spAppRoleAssignedTo'; property = 'appRoleAssignedTo'; uri = "$($graphUrl)/v1.0/servicePrincipals?`$select=id&`$expand=appRoleAssignedTo" }
    #direct memberships (also directory roles); without one there is no transitive group membership (getMemberGroups)
    @{ name = 'spMemberOf'; property = 'memberOf'; uri = "$($graphUrl)/v1.0/servicePrincipals?`$select=id&`$expand=memberOf(`$select=id)" }
    @{ name = 'appOwners'; property = 'owners'; uri = "$($graphUrl)/v1.0/applications?`$select=id&`$expand=owners(`$select=id)&`$top=999" }
    @{ name = 'appFederatedIdentityCredentials'; property = 'federatedIdentityCredentials'; uri = "$($graphUrl)/beta/applications?`$select=id&`$expand=federatedIdentityCredentials&`$top=999" }
)
$graphPrefetchTasks = [System.Collections.Generic.List[hashtable]]::new()
$graphPrefetchTasks.Add(@{ name = 'servicePrincipals'; currentTask = 'Get all Service Principals'; uri = "$($graphUrl)/v1.0/servicePrincipals"; consistencyLevel = 'eventual' })
#beta: only beta returns isPrivileged (Microsoft's flag for roles that can lead to privilege escalation)
$graphPrefetchTasks.Add(@{ name = 'aadRoleDefinitions'; currentTask = 'get AAD RoleDefinitions'; uri = "$($graphUrl)/beta/roleManagement/directory/roleDefinitions" })
$graphPrefetchTasks.Add(@{ name = 'applications'; currentTask = 'Get all Applications'; uri = "$($graphUrl)/v1.0/applications?`$top=999" })
#v1 does not return principalOrganizationId, resourceScope
$graphPrefetchTasks.Add(@{ name = 'aadRoleAssignments'; currentTask = 'Get all directory role assignments'; uri = "$($graphUrl)/beta/roleManagement/directory/roleAssignments"; testKind = 'tenantWide'; testUri = "$($graphUrl)/beta/roleManagement/directory/roleAssignments?`$top=1"; testName = 'directory role assignments' })
$graphPrefetchTasks.Add(@{ name = 'aadRoleAssignmentsExpanded'; currentTask = 'Get all directory role assignments (expand principal)'; uri = "$($graphUrl)/beta/roleManagement/directory/roleAssignments?`$expand=principal"; testKind = 'tenantWide'; testUri = "$($graphUrl)/beta/roleManagement/directory/roleAssignments?`$expand=principal&`$top=1"; testName = 'directory role assignments (expand principal)' })
#v1 does not return startTime, expiryTime
$graphPrefetchTasks.Add(@{ name = 'oauth2PermissionGrants'; currentTask = 'Get all oauth2PermissionGrants'; uri = "$($graphUrl)/beta/oauth2PermissionGrants"; testKind = 'tenantWide'; testUri = "$($graphUrl)/beta/oauth2PermissionGrants?`$top=1"; testName = 'oauth2PermissionGrants' })
$graphPrefetchTasks.Add(@{ name = 'entraRecommendations'; currentTask = 'Get Entra recommendations'; uri = "$($graphUrl)/beta/directory/recommendations"; testKind = 'optional'; testUri = "$($graphUrl)/beta/directory/recommendations?`$top=1"; testName = 'Entra recommendations'; testPermission = 'DirectoryRecommendations.Read.All' })
$graphPrefetchTasks.Add(@{ name = 'customSecurityAttributes'; currentTask = 'Get custom security attributes of Service Principals'; uri = "$($graphUrl)/beta/servicePrincipals?`$select=id,customSecurityAttributes&`$top=999"; testKind = 'optional'; testUri = "$($graphUrl)/beta/servicePrincipals?`$select=id,customSecurityAttributes&`$top=1"; testName = 'custom security attributes'; testPermission = 'CustomSecAttributeAssignment.Read.All' })
foreach ($agentType in ([ordered]@{ AgentIdentity = 'servicePrincipals/microsoft.graph.agentIdentity'; AgentIdentityBlueprintPrincipal = 'servicePrincipals/microsoft.graph.agentIdentityBlueprintPrincipal'; AgentIdentityBlueprint = 'applications/microsoft.graph.agentIdentityBlueprint'; AgentUser = 'users/microsoft.graph.agentUser' }).GetEnumerator()) {
    $graphPrefetchTasks.Add(@{ name = "agent:$($agentType.Key)"; currentTask = "Get agent objects ($($agentType.Key))"; uri = "$($graphUrl)/beta/$($agentType.Value)?`$select=id,displayName"; testKind = 'optional'; testUri = "$($graphUrl)/beta/$($agentType.Value)?`$select=id,displayName&`$top=1"; testName = "agent objects ($($agentType.Key))"; testPermission = 'Application.Read.All / User.Read.All' })
}
$graphPrefetchTasks.Add(@{ name = 'authorizationPolicy'; currentTask = 'Get authorization policy'; uri = "$($graphUrl)/v1.0/policies/authorizationPolicy"; listenOn = 'Content'; testKind = 'optional'; testUri = "$($graphUrl)/v1.0/policies/authorizationPolicy"; testName = 'tenant consent settings'; testPermission = 'Policy.Read.All' })
$graphPrefetchTasks.Add(@{ name = 'adminConsentRequestPolicy'; currentTask = 'Get admin consent request policy'; uri = "$($graphUrl)/beta/policies/adminConsentRequestPolicy"; listenOn = 'Content'; testKind = 'optional'; testUri = "$($graphUrl)/beta/policies/adminConsentRequestPolicy"; testName = 'admin consent workflow'; testPermission = 'Policy.Read.All' })
$graphPrefetchTasks.Add(@{ name = 'permissionGrantPolicies'; currentTask = 'Get permission grant policies'; uri = "$($graphUrl)/v1.0/policies/permissionGrantPolicies"; testKind = 'optional'; testUri = "$($graphUrl)/v1.0/policies/permissionGrantPolicies"; testName = 'permission grant policies'; testPermission = 'Policy.Read.PermissionGrant' })
if (-not $OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes) {
    foreach ($pass in $relationshipSignalPasses) {
        $graphPrefetchTasks.Add(@{ name = "signal:$($pass.name)"; currentTask = "Relationship signal $($pass.name)"; uri = $pass.uri; signalProperty = $pass.property })
    }
}
#review P-3 a: the Azure phase as one more task - its runspace gets the functions of this script and its script variables
#(references), runs the phase and returns what it created or changed
$htAzurePhaseResult = [System.Collections.Hashtable]::Synchronized(@{})
$azurePhaseText = $null
$azurePhaseInputs = $null
$scriptFunctionsText = $null
if ($azurePhaseDeferred) {
    $azurePhaseText = $azurePhase.ToString()
    $azurePhaseInputs = @{}
    foreach ($scriptVariable in @(Get-Variable -Scope Script)) {
        $azurePhaseInputs[$scriptVariable.Name] = $scriptVariable.Value
    }
    $scriptFunctionsText = @(Get-ChildItem -Path function: | Where-Object { $_.CommandType -eq 'Function' -and $_.ScriptBlock.File -eq $PSCommandPath } | ForEach-Object { "function $($_.Name) {`n$($_.Definition)`n}" }) -join "`n"
    $graphPrefetchTasks.Insert(0, @{ name = 'azurePhase' })
}
$startGraphPrefetch = Get-Date
$htGraphPrefetch = [System.Collections.Hashtable]::Synchronized(@{})
$graphPrefetchTasks | ForEach-Object -Parallel {
    #R-7: the AzAPICall version of the main runspace (an autoload would take the newest installed version)
    if (-not (Get-Module -Name $using:AzAPICallModuleName)) { Import-Module -Name $using:azAPICallModuleImportPath -ErrorAction Stop }
    $task = $_
    if ($task.name -eq 'azurePhase') {
        #variables of this runspace before (automatic ones): not taken over, not returned
        $runspaceVariables = [System.Collections.Generic.HashSet[string]]::new([string[]]@(Get-Variable | ForEach-Object Name), [System.StringComparer]::OrdinalIgnoreCase)
        $phaseInputs = $using:azurePhaseInputs
        foreach ($phaseInput in $phaseInputs.GetEnumerator()) {
            if (-not $runspaceVariables.Contains($phaseInput.Key)) { Set-Variable -Name $phaseInput.Key -Value $phaseInput.Value }
        }
        #the error behaviour of the main runspace (the phase stops the run on errors as before)
        $ErrorActionPreference = $using:ErrorActionPreference
        . ([scriptblock]::Create($using:scriptFunctionsText))
        . ([scriptblock]::Create($using:azurePhaseText))
        #created or changed by the phase (a filled collection is the same object and needs nothing)
        $phaseResult = $using:htAzurePhaseResult
        foreach ($phaseVariable in @(Get-Variable)) {
            if ($runspaceVariables.Contains($phaseVariable.Name) -or $phaseVariable.Name -in 'runspaceVariables', 'phaseInputs', 'phaseInput', 'phaseResult', 'phaseVariable', 'task') { continue }
            if ($phaseInputs.ContainsKey($phaseVariable.Name)) {
                $before = $phaseInputs[$phaseVariable.Name]
                if ([object]::ReferenceEquals($before, $phaseVariable.Value)) { continue }
                if ($null -ne $before -and $before.GetType().IsValueType -and $before -eq $phaseVariable.Value) { continue }
                if ($before -is [string] -and $before -ceq $phaseVariable.Value) { continue }
            }
            $phaseResult[$phaseVariable.Name] = $phaseVariable.Value
        }
        return
    }
    $azAPICallConf = $using:azAPICallConf
    $htGraphPrefetch = $using:htGraphPrefetch
    $permitted = $true
    if ($task.testKind) {
        $testResult = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $task.testUri -method 'GET' -currentTask "Test $(if ($task.testKind -eq 'tenantWide') { 'tenant-wide' } else { 'optional' }) $($task.testName)" -validateAccess -noPaging
        if ($testResult -eq 'failed') {
            $permitted = $false
            if ($task.testKind -eq 'tenantWide') {
                Write-Host "  Tenant-wide $($task.testName) not permitted - using per-SP requests" -ForegroundColor Yellow
            }
            else {
                Write-Host "  $($task.testName) not collected (optional permission $($task.testPermission) missing)" -ForegroundColor Yellow
            }
        }
    }
    $result = $null
    if ($permitted) {
        $parameters = @{ AzAPICallConfiguration = $azAPICallConf; uri = $task.uri; method = 'GET'; currentTask = $task.currentTask }
        if ($task.consistencyLevel) { $parameters.consistencyLevel = $task.consistencyLevel }
        if ($task.listenOn) { $parameters.listenOn = $task.listenOn }
        if ($task.signalProperty -or $task.testKind) {
            #review F-2: a relationship pass, a list with a per-SP fallback or optional data: a request that fails for good (AzAPICall
            #throws: e.g. a page that keeps failing, a 403 on beta for a guest) or returns an error string must not stop the run -
            #signals and tenant-wide lists fall back to the per-object requests, optional data is 'not collected'
            try {
                $result = AzAPICall @parameters
            }
            catch {
                $result = "failed: $($_.Exception.Message)"
            }
            if ($result -is [string]) {
                $permitted = $false
                if ($task.signalProperty) {
                    Write-Host "  relationship signal $($task.name -replace '^signal:') not available ($($result)) - per-object requests for all objects" -ForegroundColor Yellow
                }
                elseif ($task.testKind -eq 'tenantWide') {
                    Write-Host "  Tenant-wide $($task.testName) failed ($($result)) - using per-SP requests" -ForegroundColor Yellow
                }
                else {
                    Write-Host "  $($task.testName) not collected (request failed: $($result))" -ForegroundColor Yellow
                }
                $result = $null
            }
            elseif ($task.signalProperty) {
                #a relationship pass (P-1): the ids of the objects with entries, and (review F-20) the ids the pass returned at all - an
                #object created while the lists were read is in the SP list but not in the pass: it gets the per-object request
                $signal = @{
                    withEntries = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
                    seen = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
                }
                foreach ($item in $result) {
                    $null = $signal.seen.Add([string]$item.id)
                    if (@($item.($task.signalProperty)).Count -gt 0) { $null = $signal.withEntries.Add([string]$item.id) }
                }
                $result = $signal
            }
        }
        else {
            $result = AzAPICall @parameters
        }
    }
    $htGraphPrefetch[$task.name] = @{ permitted = $permitted; result = $result }
} -ThrottleLimit ([math]::Max(1, [math]::Min($ThrottleLimitGraph, $graphPrefetchTasks.Count)))
Write-Host "Tenant-wide Graph requests ($($graphPrefetchTasks.Count) in parallel$(if ($azurePhaseDeferred) { ', with the Azure phase' })) duration: $([math]::Round((New-TimeSpan -Start $startGraphPrefetch -End (Get-Date)).TotalSeconds, 1)) seconds"
if ($azurePhaseDeferred) {
    #the variables of the Azure phase, as if it had run in this scope
    foreach ($phaseVariableName in @($htAzurePhaseResult.Keys)) {
        try { Set-Variable -Name $phaseVariableName -Value $htAzurePhaseResult[$phaseVariableName] -Scope Script -Force -ErrorAction Stop } catch { Write-Host "  Azure phase variable '$($phaseVariableName)' not set: $($_.Exception.Message)" -ForegroundColor Yellow }
    }
    $azurePhaseInputs = $null
    $htAzurePhaseResult = $null
}
#endregion tenant-wide Graph requests in parallel

$getServicePrincipalsFromAPI = $htGraphPrefetch['servicePrincipals'].result

Write-Host "API returned count: $($getServicePrincipalsFromAPI.Count)"
$getServicePrincipals = $getServicePrincipalsFromAPI | Sort-Object -Property id -Unique
Write-Host "Sorting unique by Id count: $(@($getServicePrincipals).Count)"
#all Service Principals by id, also those that are not processed (-OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes):
#their type and name for owner and createdBy labels (review R-8)
$htAllServicePrincipalsById = @{}
foreach ($servicePrincipal in $getServicePrincipals) {
    $htAllServicePrincipalsById[[string]$servicePrincipal.id] = $servicePrincipal
}
$endSP = Get-Date
$duration = New-TimeSpan -Start $startSP -End $endSP
Write-Host "Getting $($getServicePrincipals.Count) Service Principals duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"

if ($getServicePrincipals.Count -eq 0) {
    throw 'No SPs found'
}
else {
    $htServicePrincipalsAndAppsOnlyEnriched = [System.Collections.Hashtable]::Synchronized(@{})
    $htServicePrincipalsAppRoles = @{}
    $htServicePrincipalsPublishedPermissionScopes = @{}
    $htAppRoles = @{}
    $htPublishedPermissionScopes = @{}
    $htServicePrincipalsAppRoleIdsToAppId = @{}
    $htServicePrincipalsPublishedPermissionScopesIdsToAppId = @{}
    $htAadGroupsToResolve = [System.Collections.Hashtable]::Synchronized(@{})
    $htAppRoleAssignments = [System.Collections.Hashtable]::Synchronized(@{})
    $htSPOauth2PermissionGrantedTo = [System.Collections.Hashtable]::Synchronized(@{})
    $htUsersAndGroupsToCheck4AppRoleAssignmentsUser = [System.Collections.Hashtable]::Synchronized(@{})
    $htApplications = [System.Collections.Hashtable]::Synchronized(@{})
    $htSPOwners = [System.Collections.Hashtable]::Synchronized(@{})
    $htAppOwners = [System.Collections.Hashtable]::Synchronized(@{})
    $htFederatedIdentityCredentials = [System.Collections.Hashtable]::Synchronized(@{})
    $htOwnedBy = [System.Collections.Hashtable]::Synchronized(@{})
    $htProcessedTracker = [System.Collections.Hashtable]::Synchronized(@{})
    $htMeanwhileDeleted = [System.Collections.Hashtable]::Synchronized(@{})
    #objects whose data could not be collected completely (e.g. Directory_ResultSizeLimitExceeded)
    $htDataIncomplete = [System.Collections.Hashtable]::Synchronized(@{})
    $htSpLookup = [System.Collections.Hashtable]::Synchronized(@{})
    $htPrincipalsResolved = [System.Collections.Hashtable]::Synchronized(@{})
    Write-Host 'Creating mapping AppRoles & PublishedPermissionScopes'
    foreach ($sp in $getServicePrincipals) {
        #appRoles
        if (($sp.appRoles).Count -gt 0) {
            $htServicePrincipalsAppRoles[$sp.id] = @{
                spDetails = $sp
                appRoles = @{}
            }

            foreach ($spAppRole in $sp.appRoles) {
                $htServicePrincipalsAppRoles[$sp.id].appRoles[$spAppRole.id] = $spAppRole
                if (-not $htAppRoles[$spAppRole.id]) {
                    $htAppRoles[$spAppRole.id] = $spAppRole
                }
                $htServicePrincipalsAppRoleIdsToAppId["$($sp.id)_$($spAppRole.id)"] = $sp.appId
            }
        }
        #publishedPermissionScopes
        if (($sp.oauth2PermissionScopes).Count -gt 0) {
            $htServicePrincipalsPublishedPermissionScopes[$sp.id] = @{
                spDetails = $sp
                publishedPermissionScopes = @{}
            }

            foreach ($spPublishedPermissionScope in $sp.oauth2PermissionScopes) {
                $htServicePrincipalsPublishedPermissionScopes[$sp.id].publishedPermissionScopes[$spPublishedPermissionScope.id] = $spPublishedPermissionScope
                if (-not $htPublishedPermissionScopes[$sp.id]) {
                    $htPublishedPermissionScopes[$sp.id] = @{}
                }
                if (-not $htPublishedPermissionScopes[$sp.id][$spPublishedPermissionScope.value]) {
                    $htPublishedPermissionScopes[$sp.id][$spPublishedPermissionScope.value] = $spPublishedPermissionScope
                }
                $htServicePrincipalsPublishedPermissionScopesIdsToAppId["$($sp.id)_$($spPublishedPermissionScope.id)"] = $sp.appId
            }
        }
    }

    Write-Host 'Getting all AAD Role definitions'
    $currentTask = 'get AAD RoleDefinitions'
    $htAadRoleDefinitions = @{}
    #beta: only beta returns isPrivileged (Microsoft's flag for roles that can lead to privilege escalation)
    $aadRoleDefinitions = $htGraphPrefetch['aadRoleDefinitions'].result   #P-3 b: requested in the parallel block

    foreach ($aadRoleDefinition in $aadRoleDefinitions) {
        $htAadRoleDefinitions[$aadRoleDefinition.id] = $aadRoleDefinition
    }

    $arraySPsAndAppsWithoutSP = [System.Collections.ArrayList]@()
    if ($OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes) {
        #HashSet: O(1) lookup per SP instead of scanning the array for every SP
        $spsWithAzureResourceRoleAssignmentUnique = [System.Collections.Generic.HashSet[string]]::new([string[]]@(($htCacheAssignmentsRole).values.assignment.properties.principalId | Where-Object { $_ }), [StringComparer]::OrdinalIgnoreCase)
        #review R-8 (maintainer decision 2026-11-07): also the Service Principals that get Azure RBAC through a group - the
        #transitive Service Principal members of the groups that hold role assignments in scope (one request per group)
        $groupsWithRoleAssignments = @(($htCacheAssignmentsRole).values.assignment.properties | Where-Object { $_.principalType -eq 'Group' -and $_.principalId } | ForEach-Object { [string]$_.principalId } | Sort-Object -Unique)
        $spsThroughGroupsCount = 0
        #review P-13: in parallel (one request per group; 3,000 groups one after another took ~10 min)
        $groupMemberIds = [System.Collections.Concurrent.ConcurrentBag[string]]::new()
        $groupsWithRoleAssignments | ForEach-Object -Parallel {
            #R-7: the AzAPICall version of the main runspace (an autoload would take the newest installed version)
            if (-not (Get-Module -Name $using:AzAPICallModuleName)) { Import-Module -Name $using:azAPICallModuleImportPath -ErrorAction Stop }
            $groupWithRoleAssignments = $_
            $azAPICallConf = $using:azAPICallConf
            $currentTask = "Service Principals in group '$($groupWithRoleAssignments)' (Azure role assignments)"
            $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/groups/$($groupWithRoleAssignments)/transitiveMembers/microsoft.graph.servicePrincipal?`$select=id"
            $groupServicePrincipals = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method 'GET' -currentTask $currentTask
            if ($groupServicePrincipals -is [string]) {
                Write-Host "  group '$($groupWithRoleAssignments)': members not read ($($groupServicePrincipals))" -ForegroundColor Yellow
                return
            }
            $bag = $using:groupMemberIds
            foreach ($groupServicePrincipal in @($groupServicePrincipals)) {
                if ($groupServicePrincipal.id) { $bag.Add([string]$groupServicePrincipal.id) }
            }
        } -ThrottleLimit $ThrottleLimitGraph
        foreach ($groupMemberId in $groupMemberIds) {
            if ($spsWithAzureResourceRoleAssignmentUnique.Add($groupMemberId)) {
                $spsThroughGroupsCount++
            }
        }
        Write-Host "  $($groupsWithRoleAssignments.Count) groups with Azure role assignments in scope: $($spsThroughGroupsCount) more Service Principals (members)"
    }
    foreach ($sp in $getServicePrincipals) {
        if ($OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes) {
            if ($spsWithAzureResourceRoleAssignmentUnique.Contains($sp.id)) {
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

    #region tenant-wide prefetch
    #One paged request per data set instead of one request per SP (review P1-20). The results are indexed here and
    #looked up in the parallel collection below. If a request fails (AzAPICall returns an error string instead of
    #objects), the per-SP requests are used for that data set, as before.

    #all applications: index by appId (replaces 'applications?$filter=appId eq ...' per SP) and detect Applications without SP
    #(this used to run only if '$count' of applications was greater than the number of internal SPs, which can lag)
    Write-Host 'Getting all Applications'
    $currentTask = 'Get all Applications'
    $getApplicationsFromAPI = $htGraphPrefetch['applications'].result   #P-3 b: requested in the parallel block
    $htApplicationsByAppId = $null
    #review R-22: 'skipApplications' (AzAPICall: e.g. a guest caller without access to applications) - the per-SP requests to the
    #same endpoint fail the same way; they are skipped and the SPs are marked as incomplete
    $skipApplicationRequests = $false
    if ($getApplicationsFromAPI -is [string]) {
        if ($getApplicationsFromAPI -eq 'skipApplications') {
            $skipApplicationRequests = $true
            Write-Host "  Getting all Applications failed ($getApplicationsFromAPI) - application data is not collected" -ForegroundColor Yellow
        }
        else {
            Write-Host "  Getting all Applications failed ($getApplicationsFromAPI) - falling back to per-SP requests" -ForegroundColor Yellow
        }
        $getApplicationsFromAPI = @()
    }
    else {
        $htApplicationsByAppId = @{}
        foreach ($application in $getApplicationsFromAPI) {
            $htApplicationsByAppId[[string]$application.appId] = $application
        }
        Write-Host "  $($htApplicationsByAppId.Keys.Count) Applications"
    }

    if ($OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes) {
        Write-Host "Skipping AppsOnly due to parameter -OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes $OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes"
    }
    else {
        $cnt = 0
        #HashSet of SP appIds built once ('$getServicePrincipals.appid -notcontains' rebuilt and scanned the whole array per application)
        $servicePrincipalAppIds = [System.Collections.Generic.HashSet[string]]::new([string[]]@($getServicePrincipals.appid | Where-Object { $_ }), [StringComparer]::OrdinalIgnoreCase)
        foreach ($application in $getApplicationsFromAPI) {
            if (-not $servicePrincipalAppIds.Contains([string]$application.appid)) {
                $cnt++
                $null = $arraySPsAndAppsWithoutSP.Add([PSCustomObject]@{
                        SPOrAppWithoutSP = 'AppWithoutSP'
                        Details = $application
                    })
            }
        }
        Write-Host "$cnt Applications without ServicePrincipal/Enterprise Application collected"
    }

    #helper: group a list of objects into a hashtable of arrays by one property (keys case-insensitive)
    function groupByProperty($items, [string]$property) {
        $ht = @{}
        foreach ($item in $items) {
            $key = [string]$item.$property
            if (-not $ht.ContainsKey($key)) {
                $ht[$key] = [System.Collections.Generic.List[object]]::new()
            }
            $ht[$key].Add($item)
        }
        return $ht
    }

    #directory role assignments by principalId (replaces 'roleAssignments?$filter=principalId eq ...' per SP)
    #v1 does not return principalOrganizationId, resourceScope
    Write-Host 'Getting all directory role assignments'
    $currentTask = 'Get all directory role assignments'
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/roleManagement/directory/roleAssignments"
    $method = 'GET'
    $prefetchAADRoleAssignments = 'notPermitted'
    if ($htGraphPrefetch['aadRoleAssignments'].permitted) {
        $prefetchAADRoleAssignments = $htGraphPrefetch['aadRoleAssignments'].result
    }
    $htAADRoleAssignmentsByPrincipalId = $null
    if ($prefetchAADRoleAssignments -is [string]) {
        Write-Host "  Getting all directory role assignments not used ($prefetchAADRoleAssignments) - per-SP requests" -ForegroundColor Yellow
    }
    else {
        $htAADRoleAssignmentsByPrincipalId = groupByProperty $prefetchAADRoleAssignments 'principalId'
        Write-Host "  $(@($prefetchAADRoleAssignments).Count) directory role assignments"
    }

    #directory role assignments scoped to an SP or App, by resourceScope (replaces 'roleAssignments?$filter=resourceScope eq ...&$expand=principal' per SP and per App)
    Write-Host 'Getting all directory role assignments (expand principal)'
    $currentTask = 'Get all directory role assignments (expand principal)'
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/roleManagement/directory/roleAssignments?`$expand=principal"
    $method = 'GET'
    $prefetchAADRoleAssignmentsExpanded = 'notPermitted'
    if ($htGraphPrefetch['aadRoleAssignmentsExpanded'].permitted) {
        $prefetchAADRoleAssignmentsExpanded = $htGraphPrefetch['aadRoleAssignmentsExpanded'].result
    }
    $htAADRoleAssignmentsByResourceScope = $null
    if ($prefetchAADRoleAssignmentsExpanded -is [string]) {
        Write-Host "  Getting all directory role assignments (expand principal) not used ($prefetchAADRoleAssignmentsExpanded) - per-SP requests" -ForegroundColor Yellow
    }
    else {
        $htAADRoleAssignmentsByResourceScope = groupByProperty @($prefetchAADRoleAssignmentsExpanded).where({ $_.resourceScope -and $_.resourceScope -ne '/' }) 'resourceScope'
        Write-Host "  $($htAADRoleAssignmentsByResourceScope.Keys.Count) resource scopes with directory role assignments"
    }

    #delegated permission grants by clientId (replaces 'servicePrincipals/{id}/oauth2PermissionGrants' per SP)
    #v1 does not return startTime, expiryTime
    Write-Host 'Getting all oauth2PermissionGrants'
    $currentTask = 'Get all oauth2PermissionGrants'
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/oauth2PermissionGrants"
    $method = 'GET'
    $prefetchOauth2PermissionGrants = 'notPermitted'
    if ($htGraphPrefetch['oauth2PermissionGrants'].permitted) {
        $prefetchOauth2PermissionGrants = $htGraphPrefetch['oauth2PermissionGrants'].result
    }
    $htOauth2PermissionGrantsByClientId = $null
    if ($prefetchOauth2PermissionGrants -is [string]) {
        Write-Host "  Getting all oauth2PermissionGrants not used ($prefetchOauth2PermissionGrants) - per-SP requests" -ForegroundColor Yellow
    }
    else {
        $htOauth2PermissionGrantsByClientId = groupByProperty $prefetchOauth2PermissionGrants 'clientId'
        Write-Host "  $(@($prefetchOauth2PermissionGrants).Count) oauth2PermissionGrants"
    }
    #endregion tenant-wide prefetch

    #region optional data (only with the optional permission; skipped otherwise)
    #Entra recommendations for applications (e.g. 'Convert your application(s) to Managed Identity'): per SP, the active/postponed ones.
    #impactedResources.subjectId is the SP object id
    $htEntraRecommendationsBySPId = $null
    Write-Host 'Getting Entra recommendations (optional)'
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/directory/recommendations"
    if ($htGraphPrefetch['entraRecommendations'].permitted) {
        $htEntraRecommendationsBySPId = @{}
        $entraRecommendations = $htGraphPrefetch['entraRecommendations'].result
        $entraRecommendations4Apps = @(@($entraRecommendations).where({ $_.impactType -eq 'apps' -and $_.status -in 'active', 'postponed' }))
        foreach ($entraRecommendation in $entraRecommendations4Apps) {
            $impactedResources = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri "$($uri)/$($entraRecommendation.id)/impactedResources" -method 'GET' -currentTask "Get Entra recommendation impacted resources $($entraRecommendation.recommendationType)"
            foreach ($impactedResource in @(@($impactedResources).where({ $_.subjectId -and $_.status -in 'active', 'postponed' }))) {
                if (-not $htEntraRecommendationsBySPId[$impactedResource.subjectId]) {
                    $htEntraRecommendationsBySPId[$impactedResource.subjectId] = [System.Collections.ArrayList]@()
                }
                $null = $htEntraRecommendationsBySPId[$impactedResource.subjectId].Add([ordered]@{
                        recommendationId = $entraRecommendation.id
                        recommendationType = $entraRecommendation.recommendationType
                        displayName = $entraRecommendation.displayName
                        priority = $entraRecommendation.priority
                        status = $impactedResource.status
                    })
            }
        }
        Write-Host "  $($entraRecommendations4Apps.Count) active Entra recommendations for applications; $($htEntraRecommendationsBySPId.Count) Service Principals impacted"
    }

    #custom security attributes assigned to SPs (e.g. a tiering attribute); only with an explicit $select
    $htCustomSecurityAttributesBySPId = $null
    Write-Host 'Getting custom security attributes of Service Principals (optional)'
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/servicePrincipals?`$select=id,customSecurityAttributes"
    if ($htGraphPrefetch['customSecurityAttributes'].permitted) {
        $htCustomSecurityAttributesBySPId = @{}
        $spsCustomSecurityAttributes = $htGraphPrefetch['customSecurityAttributes'].result
        foreach ($spCustomSecurityAttributes in @($spsCustomSecurityAttributes).where({ $_.customSecurityAttributes })) {
            $attributes = [System.Collections.ArrayList]@()
            foreach ($attributeSet in $spCustomSecurityAttributes.customSecurityAttributes.PSObject.Properties) {
                foreach ($attribute in @($attributeSet.Value.PSObject.Properties).where({ $_.Name -notlike '*@odata*' })) {
                    $null = $attributes.Add([ordered]@{
                            attributeSet = $attributeSet.Name
                            attribute = $attribute.Name
                            value = @($attribute.Value)
                        })
                }
            }
            if ($attributes.Count -gt 0) {
                $htCustomSecurityAttributesBySPId[$spCustomSecurityAttributes.id] = $attributes
            }
        }
        Write-Host "  $($htCustomSecurityAttributesBySPId.Count) Service Principals with custom security attributes"
    }

    #agent objects (Microsoft Entra Agent ID; beta): identities, blueprints (applications), blueprint principals, agent users
    $htAgentObjectsById = $null
    Write-Host 'Getting agent objects (Microsoft Entra Agent ID)'
    foreach ($agentType in ([ordered]@{ AgentIdentity = 'servicePrincipals/microsoft.graph.agentIdentity'; AgentIdentityBlueprintPrincipal = 'servicePrincipals/microsoft.graph.agentIdentityBlueprintPrincipal'; AgentIdentityBlueprint = 'applications/microsoft.graph.agentIdentityBlueprint'; AgentUser = 'users/microsoft.graph.agentUser' }).GetEnumerator()) {
        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/$($agentType.Value)?`$select=id,displayName"
        if ($htGraphPrefetch["agent:$($agentType.Key)"].permitted) {
            if ($null -eq $htAgentObjectsById) {
                $htAgentObjectsById = @{}
            }
            foreach ($agentObject in @($htGraphPrefetch["agent:$($agentType.Key)"].result)) {
                if ($agentObject.id) {
                    $htAgentObjectsById[$agentObject.id] = [ordered]@{ agentType = $agentType.Key; displayName = $agentObject.displayName }
                }
            }
        }
    }
    if ($null -ne $htAgentObjectsById) {
        Write-Host "  $($htAgentObjectsById.Count) agent objects"
    }

    #tenant consent settings: who can consent to apps, admin consent workflow, custom permission grant policies
    $tenantConsentSettings = $null
    Write-Host 'Getting tenant consent settings (optional)'
    $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/policies/authorizationPolicy"
    if ($htGraphPrefetch['authorizationPolicy'].permitted) {
        $tenantConsentSettings = [System.Collections.ArrayList]@()
        $authorizationPolicy = $htGraphPrefetch['authorizationPolicy'].result
        $permissionGrantPoliciesAssigned = @($authorizationPolicy.defaultUserRolePermissions.permissionGrantPoliciesAssigned)
        $userConsentPolicies = @($permissionGrantPoliciesAssigned.where({ $_ -like 'ManagePermissionGrantsForSelf.*' }) | ForEach-Object { $_ -replace '^ManagePermissionGrantsForSelf\.' })
        $userConsent = switch ($true) {
            ($userConsentPolicies.Count -eq 0) { @('no', 'users cannot consent; admins consent (see admin consent workflow)'); break }
            ($userConsentPolicies -contains 'microsoft-user-default-legacy') { @('yes, to all apps', 'for permissions that do not require admin consent; Microsoft recommends: verified publishers / this organization, low-impact permissions'); break }
            ($userConsentPolicies -contains 'microsoft-user-default-low') { @('yes, limited', 'apps from verified publishers or this organization, low-impact permissions'); break }
            #review F-9: the Microsoft-managed policy (Microsoft moves tenants from the legacy default to it); other microsoft-* ids are
            #built-in policies, only other ids are custom
            ($userConsentPolicies -contains 'microsoft-user-default-recommended') { @('yes, limited (Microsoft-managed)', 'Microsoft-managed: the current Microsoft recommendation for user consent'); break }
            (@($userConsentPolicies.where({ $_ -notlike 'microsoft-*' })).Count -eq 0) { @("built-in policy: $($userConsentPolicies -join ', ')", 'built-in permission grant policy') ; break }
            default { @("custom: $($userConsentPolicies -join ', ')", 'custom permission grant policy') }
        }
        $null = $tenantConsentSettings.Add([PSCustomObject][ordered]@{ Setting = 'Users can consent to apps'; Value = $userConsent[0]; Note = $userConsent[1] })
        #review 2026-11-10 (first real run with the permission): the Teams/chat resource-specific consent states, not the bare policy ids
        $ownedResourceConsent = getOwnedResourceConsentSetting -PermissionGrantPoliciesAssigned $permissionGrantPoliciesAssigned
        $null = $tenantConsentSettings.Add([PSCustomObject][ordered]@{ Setting = 'Group/team owners can consent to apps accessing their data'; Value = $ownedResourceConsent.Value; Note = $ownedResourceConsent.Note })
        $null = $tenantConsentSettings.Add([PSCustomObject][ordered]@{ Setting = 'Users can register applications'; Value = $(if ($authorizationPolicy.defaultUserRolePermissions.allowedToCreateApps) { 'yes' } else { 'no' }); Note = $(if ($authorizationPolicy.defaultUserRolePermissions.allowedToCreateApps) { 'any user can create app registrations and becomes their owner' } else { '' }) })
        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/policies/adminConsentRequestPolicy"
        if ($htGraphPrefetch['adminConsentRequestPolicy'].permitted) {
            $adminConsentRequestPolicy = $htGraphPrefetch['adminConsentRequestPolicy'].result
            $null = $tenantConsentSettings.Add([PSCustomObject][ordered]@{ Setting = 'Admin consent workflow'; Value = $(if ($adminConsentRequestPolicy.isEnabled) { "enabled ($(@($adminConsentRequestPolicy.reviewers).Count) reviewers, requests expire after $($adminConsentRequestPolicy.requestDurationInDays) days)" } else { 'disabled' }); Note = $(if (-not $adminConsentRequestPolicy.isEnabled -and $userConsent[0] -ne 'yes, to all apps') { 'users cannot request admin consent for apps they cannot consent to' } else { '' }) })
        }
        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/policies/permissionGrantPolicies"
        if ($htGraphPrefetch['permissionGrantPolicies'].permitted) {
            $customPermissionGrantPolicies = @(@($htGraphPrefetch['permissionGrantPolicies'].result).where({ $_.id -notlike 'microsoft-*' }))
            $null = $tenantConsentSettings.Add([PSCustomObject][ordered]@{ Setting = 'Custom permission grant policies'; Value = $(if ($customPermissionGrantPolicies) { ($customPermissionGrantPolicies | ForEach-Object { "$($_.displayName) ($($_.id))" }) -join ', ' } else { 'none' }); Note = '' })
        }
        Write-Host "  users can consent to apps: $($userConsent[0])"
    }
    #endregion optional data

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

    #region relationship signals (review P-1)
    #Most per-object requests return nothing (test tenant: 246 of ~6,350 non-empty). One list pass per relationship with $expand
    #(one expand per request) tells which objects have entries; only those get the per-object request, which still delivers the
    #data (expanded lists are capped: ownedObjects returns at most 20 without nextLink). A pass that fails: every object gets
    #the request, as before. -OnlyProcessSPs...: the passes list all SPs - used only if at least 1 of 20 SPs is processed.
    $htRelationshipSignals = $null
    $relationshipSignalsWorthIt = (-not $OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes) -or ($arraySPsAndAppsWithoutSP.Count * 20 -ge @($getServicePrincipals).Count)
    if (-not $OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes) {
        #P-3 b: the passes ran in the tenant-wide parallel block
        $htRelationshipSignals = @{}
        foreach ($pass in $relationshipSignalPasses) {
            if ($htGraphPrefetch["signal:$($pass.name)"].permitted) { $htRelationshipSignals[$pass.name] = $htGraphPrefetch["signal:$($pass.name)"].result }
        }
        Write-Host " Relationship signals: $(@($htRelationshipSignals.Keys | Sort-Object | ForEach-Object { "$($_) $($htRelationshipSignals[$_].withEntries.Count)" }) -join ', ')"
    }
    elseif ($relationshipSignalsWorthIt -and $arraySPsAndAppsWithoutSP.Count -gt 0) {
        $startRelationshipSignals = Get-Date
        $htRelationshipSignalsCollected = [System.Collections.Hashtable]::Synchronized(@{})
        $relationshipSignalPasses | ForEach-Object -Parallel {
            #R-7: the AzAPICall version of the main runspace (an autoload would take the newest installed version)
            if (-not (Get-Module -Name $using:AzAPICallModuleName)) { Import-Module -Name $using:azAPICallModuleImportPath -ErrorAction Stop }
            $pass = $_
            #review F-2: a pass that fails for good (AzAPICall throws) falls back to the per-object requests like an error string
            try {
                $result = AzAPICall -AzAPICallConfiguration $using:azAPICallConf -uri $pass.uri -method 'GET' -currentTask "Relationship signal $($pass.name)"
            }
            catch {
                $result = "failed: $($_.Exception.Message)"
            }
            if ($result -is [string]) {
                Write-Host "  relationship signal $($pass.name) not available ($($result)) - per-object requests for all objects" -ForegroundColor Yellow
                return
            }
            #review F-20: also the ids the pass returned at all (see the tenant-wide block)
            $signal = @{
                withEntries = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
                seen = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
            }
            foreach ($item in $result) {
                $null = $signal.seen.Add([string]$item.id)
                if (@($item.($pass.property)).Count -gt 0) { $null = $signal.withEntries.Add([string]$item.id) }
            }
            ($using:htRelationshipSignalsCollected)[$pass.name] = $signal
        } -ThrottleLimit $relationshipSignalPasses.Count
        $htRelationshipSignals = @{}
        foreach ($key in $htRelationshipSignalsCollected.Keys) { $htRelationshipSignals[$key] = $htRelationshipSignalsCollected[$key] }
        Write-Host " Relationship signals: $(@($htRelationshipSignals.Keys | Sort-Object | ForEach-Object { "$($_) $($htRelationshipSignals[$_].withEntries.Count)" }) -join ', ') ($([math]::Round((New-TimeSpan -Start $startRelationshipSignals -End (Get-Date)).TotalSeconds, 1)) seconds)"
    }
    #endregion relationship signals

    $batchSize = getParallelBatchSize -itemCount ($arraySPsAndAppsWithoutSP.Count) -throttleLimit $ThrottleLimitGraph
    Write-Host "Optimal batch size: $($batchSize)"
    $SPsAndAppsWithoutSPBatch = getParallelBatches -Items $arraySPsAndAppsWithoutSP -BatchSize $batchSize
    Write-Host "Processing data in $($SPsAndAppsWithoutSPBatch.Count) batches"

    $SPsAndAppsWithoutSPBatch | ForEach-Object -Parallel {
        #R-7: the AzAPICall version of the main runspace (an autoload would take the newest installed version)
        if (-not (Get-Module -Name $using:AzAPICallModuleName)) { Import-Module -Name $using:azAPICallModuleImportPath -ErrorAction Stop }
        $htParallelFailures = $using:htParallelFailures

        #AzAPICall
        $azAPICallConf = $using:azAPICallConf
        #array&ht
        $htServicePrincipalsAndAppsOnlyEnriched = $using:htServicePrincipalsAndAppsOnlyEnriched
        $htAadGroupsToResolve = $using:htAadGroupsToResolve
        $htAppRoleAssignments = $using:htAppRoleAssignments
        $htUsersAndGroupsToCheck4AppRoleAssignmentsUser = $using:htUsersAndGroupsToCheck4AppRoleAssignmentsUser
        $htApplications = $using:htApplications
        $indicator = $using:indicator
        $htSPOwners = $using:htSPOwners
        $htAppOwners = $using:htAppOwners
        $htFederatedIdentityCredentials = $using:htFederatedIdentityCredentials
        $htOwnedBy = $using:htOwnedBy
        $htProcessedTracker = $using:htProcessedTracker
        $htMeanwhileDeleted = $using:htMeanwhileDeleted
        $htDataIncomplete = $using:htDataIncomplete
        #tenant-wide prefetch indexes ($null = not available, use the per-SP request)
        $htApplicationsByAppId = $using:htApplicationsByAppId
        $htAADRoleAssignmentsByPrincipalId = $using:htAADRoleAssignmentsByPrincipalId
        $htAADRoleAssignmentsByResourceScope = $using:htAADRoleAssignmentsByResourceScope
        $htOauth2PermissionGrantsByClientId = $using:htOauth2PermissionGrantsByClientId
        #review P-1: true = the relationship signal says this object has no entries (the request is skipped, the result is empty)
        $htRelationshipSignals = $using:htRelationshipSignals
        #review F-20: only an object the pass returned without entries; an object the pass did not return gets the request
        $hasNoEntries = { param([string]$Signal, [string]$Id) $null -ne $htRelationshipSignals -and $null -ne $htRelationshipSignals[$Signal] -and $htRelationshipSignals[$Signal].seen.Contains($Id) -and -not $htRelationshipSignals[$Signal].withEntries.Contains($Id) }
        $htSpLookup = $using:htSpLookup
        $htPrincipalsResolved = $using:htPrincipalsResolved

        foreach ($spOrAppWithoutSP in $_.Group) {
            #P1-21: one failing item must not abort the whole run (a throw inside -Parallel stops the script under ErrorActionPreference Stop)
            try {
                #region inner
                #$spOrAppWithoutSP = $_

                $meanwhileDeleted = $false
                $spType = ''
                #write-host $spOrAppWithoutSP.SPOrAppWithoutSP
                if ($spOrAppWithoutSP.SPOrAppWithoutSP -eq 'SP') {
                    $hlperType = 'SP'
                    $object = $spOrAppWithoutSP.Details

                    $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id] = [ordered] @{
                        ServicePrincipalDetails = $object
                    }
                    $script:htSpLookup[$object.id] = @{}
                }
                else {
                    $hlperType = 'AppOnly'
                    $object = $spOrAppWithoutSP.Details
                    $script:htServicePrincipalsAndAppsOnlyEnriched["AppWithoutSP_$($object.id)"] = [ordered] @{}
                    $script:htSpLookup[$object.id] = @{}
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
                    $getSPOwnedObjects = $(if (& $hasNoEntries 'spOwnedObjects' $object.id) { @() } else { AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask })

                    if ($getSPOwnedObjects -eq 'Request_ResourceNotFound') {
                        if (-not $htMeanwhileDeleted[$object.id]) {
                            Write-Host "  $($object.displayName -replace '##', '# #' -replace '[\r\n]+', ' ') ($($object.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                            $script:htMeanwhileDeleted[$object.id] = @{}
                            $meanwhileDeleted = $true
                        }
                    }
                    else {
                        if ($getSPOwnedObjects.Count -gt 0) {
                            $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].ServicePrincipalOwnedObjects = $getSPOwnedObjects | Select-Object '@odata.type', displayName, id
                        }
                    }
                    #endregion spownedObjects

                    #region spAADRoleAssignments
                    if (-not $meanwhileDeleted) {
                        if ($htAADRoleAssignmentsByPrincipalId) {
                            #tenant-wide prefetch (verified identical to the per-SP request)
                            $getSPAADRoleAssignments = $htAADRoleAssignmentsByPrincipalId[[string]$object.id]
                            if ($null -eq $getSPAADRoleAssignments) {
                                $getSPAADRoleAssignments = @()
                            }
                        }
                        else {
                            $currentTask = "getSP AADRoleAssignments $($object.id)"
                            #v1 does not return principalOrganizationId, resourceScope
                            $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/roleManagement/directory/roleAssignments?`$filter=principalId eq '$($object.id)'"
                            $method = 'GET'
                            $getSPAADRoleAssignments = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask
                        }

                        if ($getSPAADRoleAssignments -eq 'Request_ResourceNotFound') {
                            if (-not $htMeanwhileDeleted[$object.id]) {
                                Write-Host "  $($object.displayName -replace '##', '# #' -replace '[\r\n]+', ' ') ($($object.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                                $script:htMeanwhileDeleted[$object.id] = @{}
                                $meanwhileDeleted = $true
                            }
                        }
                        else {
                            if ($getSPAADRoleAssignments.Count -gt 0) {
                                $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].ServicePrincipalAADRoleAssignments = $getSPAADRoleAssignments
                            }
                        }
                    }
                    #endregion spAADRoleAssignments

                    #test later

                    #region spAppRoleAssignments
                    if (-not $meanwhileDeleted) {
                        $currentTask = "getSP AppRoleAssignments $($object.id)"
                        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/servicePrincipals/$($object.id)/appRoleAssignments"
                        $method = 'GET'
                        $getSPAppRoleAssignments = $(if (& $hasNoEntries 'spAppRoleAssignments' $object.id) { @() } else { AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask })

                        if ($getSPAppRoleAssignments -eq 'Request_ResourceNotFound') {
                            if (-not $htMeanwhileDeleted[$object.id]) {
                                Write-Host "  $($object.displayName -replace '##', '# #' -replace '[\r\n]+', ' ') ($($object.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                                $script:htMeanwhileDeleted[$object.id] = @{}
                                $meanwhileDeleted = $true
                            }
                        }
                        else {
                            if ($getSPAppRoleAssignments.Count -gt 0) {
                                $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].ServicePrincipalAppRoleAssignments = $getSPAppRoleAssignments
                                foreach ($SPAppRoleAssignment in $getSPAppRoleAssignments) {
                                    if (-not $htAppRoleAssignments[$SPAppRoleAssignment.id]) {
                                        $script:htAppRoleAssignments[$SPAppRoleAssignment.id] = $SPAppRoleAssignment
                                    }
                                }
                            }
                        }
                    }
                    #endregion spAppRoleAssignments

                    #region SPAADRoleAssignedOn
                    if (-not $meanwhileDeleted) {
                        if ($htAADRoleAssignmentsByResourceScope) {
                            #tenant-wide prefetch (verified identical to the per-SP request)
                            $getSPAADRoleAssignedOn = $htAADRoleAssignmentsByResourceScope["/$($object.id)"]
                            if ($null -eq $getSPAADRoleAssignedOn) {
                                $getSPAADRoleAssignedOn = @()
                            }
                        }
                        else {
                            $currentTask = "getSP AADRoleAssignedOn $($object.id)"
                            $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/roleManagement/directory/roleAssignments?`$filter=resourceScope eq '/$($object.id)'&`$expand=principal"
                            $method = 'GET'
                            $getSPAADRoleAssignedOn = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask
                        }
                        if ($getSPAADRoleAssignedOn.Count -gt 0) {
                            $tmpArray = [System.Collections.ArrayList]@()
                            foreach ($SPAADRoleAssignedOn in $getSPAADRoleAssignedOn) {
                                if ($SPAADRoleAssignedOn.principal.'@odata.type' -eq '#microsoft.graph.user') {

                                    if ([string]::IsNullOrEmpty($SPAADRoleAssignedOn.principal.userType)) {
                                        $principalUserType = 'userType not set'
                                    }
                                    else {
                                        $principalUserType = $SPAADRoleAssignedOn.principal.userType
                                    }
                                    if (-not $htPrincipalsResolved[$SPAADRoleAssignedOn.principal.id]) {
                                        $type = 'User'
                                        $script:htPrincipalsResolved[$SPAADRoleAssignedOn.principal.id] = @{
                                            full = "$($type) ($($principalUserType)), DisplayName: $($SPAADRoleAssignedOn.principal.displayName), Id: $(($SPAADRoleAssignedOn.principal.id))"
                                            typeOnly = "$($type) ($($principalUserType))"
                                        }

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
                            $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].ServicePrincipalAADRoleAssignedOn = $tmpArray
                        }
                    }
                    #endregion SPAADRoleAssignedOn

                    #region spAppRoleAssignedTo
                    if (-not $meanwhileDeleted) {
                        $currentTask = "getSP appRoleAssignedTo $($object.id)"
                        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/servicePrincipals/$($object.id)/appRoleAssignedTo"
                        $method = 'GET'
                        $getSPAppRoleAssignedTo = $(if (& $hasNoEntries 'spAppRoleAssignedTo' $object.id) { @() } else { AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask })

                        if ($getSPAppRoleAssignedTo -eq 'Request_ResourceNotFound') {
                            if (-not $htMeanwhileDeleted[$object.id]) {
                                Write-Host "  $($object.displayName -replace '##', '# #' -replace '[\r\n]+', ' ') ($($object.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                                $script:htMeanwhileDeleted[$object.id] = @{}
                                $meanwhileDeleted = $true
                            }
                        }
                        else {
                            if ($getSPAppRoleAssignedTo.Count -gt 0) {
                                $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].ServicePrincipalAppRoleAssignedTo = $getSPAppRoleAssignedTo
                                foreach ($SPAppRoleAssignedTo in $getSPAppRoleAssignedTo) {
                                    if ($SPAppRoleAssignedTo.principalType -eq 'User' -or $SPAppRoleAssignedTo.principalType -eq 'Group') {
                                        if ($SPAppRoleAssignedTo.principalType -eq 'User') {
                                            if (-not $htUsersAndGroupsToCheck4AppRoleAssignmentsUser[$SPAppRoleAssignedTo.principalId]) {
                                                $script:htUsersAndGroupsToCheck4AppRoleAssignmentsUser[$SPAppRoleAssignedTo.principalId] = @{}
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
                        $getSPGroupMemberships = $(if (& $hasNoEntries 'spMemberOf' $object.id) { @() } else { AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -body $body -currentTask $currentTask })

                        if ($getSPGroupMemberships -eq 'Request_ResourceNotFound') {
                            if (-not $htMeanwhileDeleted[$object.id]) {
                                Write-Host "  $($object.displayName -replace '##', '# #' -replace '[\r\n]+', ' ') ($($object.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                                $script:htMeanwhileDeleted[$object.id] = @{}
                                $meanwhileDeleted = $true
                            }
                        }
                        elseif ($getSPGroupMemberships -eq 'Directory_ResultSizeLimitExceeded') {
                            #group memberships are missing for this SP, and with them its group based Azure RBAC
                            Write-Host "  $($object.displayName -replace '##', '# #' -replace '[\r\n]+', ' ') ($($object.id)) - Directory_ResultSizeLimitExceeded: group memberships NOT collected (group based Azure RBAC of this SP will be missing)" -ForegroundColor Yellow
                            $htDataIncomplete[$object.id] = "$($object.displayName): group memberships (Directory_ResultSizeLimitExceeded)"
                        }
                        else {
                            if ($getSPGroupMemberships.Count -gt 0) {
                                $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].ServicePrincipalGroupMemberships = $getSPGroupMemberships
                                foreach ($aadGroupId in $getSPGroupMemberships) {
                                    if (-not $script:htAadGroupsToResolve[$aadGroupId]) {
                                        $script:htAadGroupsToResolve[$aadGroupId] = @{}
                                    }
                                }
                            }
                        }
                    }
                    #endregion spGetMemberGroups

                    #region spDelegatedPermissions
                    if (-not $meanwhileDeleted) {
                        if ($htOauth2PermissionGrantsByClientId) {
                            #tenant-wide prefetch (only if permitted, see the tenant-wide parallel block)
                            $getSPOauth2PermissionGrants = $htOauth2PermissionGrantsByClientId[[string]$object.id]
                            if ($null -eq $getSPOauth2PermissionGrants) {
                                $getSPOauth2PermissionGrants = @()
                            }
                        }
                        else {
                            $currentTask = "getSP oauth2PermissionGrants $($object.id)"
                            #v1 does not return startTime, expiryTime
                            $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/servicePrincipals/$($object.id)/oauth2PermissionGrants"
                            $method = 'GET'
                            $getSPOauth2PermissionGrants = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask
                        }

                        if ($getSPOauth2PermissionGrants -eq 'Request_ResourceNotFound') {
                            if (-not $htMeanwhileDeleted[$object.id]) {
                                Write-Host "  $($object.displayName -replace '##', '# #' -replace '[\r\n]+', ' ') ($($object.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                                $script:htMeanwhileDeleted[$object.id] = @{}
                                $meanwhileDeleted = $true
                            }
                        }
                        else {
                            if ($getSPOauth2PermissionGrants.Count -gt 0) {
                                #htSPOauth2PermissionGrantedTo is built from this after the parallel collection (review P0-4)
                                $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].ServicePrincipalOauth2PermissionGrants = $getSPOauth2PermissionGrants
                            }
                        }
                    }
                    #endregion spDelegatedPermissions

                    #region spOwner
                    if (-not $meanwhileDeleted) {
                        $currentTask = "getSPOwner $($object.id)"
                        $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/servicePrincipals/$($object.id)/owners"
                        $method = 'GET'
                        $getSPOwner = $(if (& $hasNoEntries 'spOwners' $object.id) { @() } else { AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask })

                        if ($getSPOwner -eq 'Request_ResourceNotFound') {
                            if (-not $htMeanwhileDeleted[$object.id]) {
                                Write-Host "  $($object.displayName -replace '##', '# #' -replace '[\r\n]+', ' ') ($($object.id)) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                                $script:htMeanwhileDeleted[$object.id] = @{}
                                $meanwhileDeleted = $true
                            }
                        }
                        else {
                            if ($getSPOwner.Count -gt 0) {
                                foreach ($spOwner in $getSPOwner) {

                                    if (-not $htOwnedBy[$object.id]) {
                                        $script:htOwnedBy[$object.id] = @{
                                            ownedBy = [array]$($spOwner | Select-Object id, displayName, '@odata.type')
                                        }
                                    }
                                    else {
                                        $array = [array]($htOwnedBy[$object.id].ownedBy)
                                        $array += $spOwner | Select-Object id, displayName, '@odata.type'
                                        $script:htOwnedBy[$object.id].ownedBy = $array
                                    }
                                }
                                if (-not $htSPOwners[$object.id]) {
                                    #by name and id (as the app owners): the walk over indirect owners and its 'via' path follow this order
                                    $script:htSPOwners[$object.id] = @($getSPOwner | Select-Object id, displayName, '@odata.type' | Sort-Object { $_.displayName }, { $_.id })
                                }
                            }
                            else {
                                $script:htOwnedBy[$object.id] = @{
                                    ownedBy = 'noOwner'
                                }
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

                        if ($htApplicationsByAppId) {
                            #all applications were listed once (verified identical to 'applications?$filter=appId eq ...');
                            #an SP whose app is registered in another tenant has none here, as before
                            #the object itself, like AzAPICall returns a single result: a one-element array here made
                            #'.ApplicationDetails.tags' enumerate through it and empty arrays (tags, requiredResourceAccess) became $null
                            $getApplication = $htApplicationsByAppId[[string]$object.appId]
                            if ($null -eq $getApplication) {
                                $getApplication = @()
                            }
                        }
                        elseif ($using:skipApplicationRequests) {
                            $getApplication = 'skipApplications'
                        }
                        else {
                            $currentTask = "getApp $($object.appId)"
                            $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/applications?`$filter=appId eq '$($object.appId)'"
                            $method = 'GET'
                            $getApplication = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask
                        }
                        #review R-22: another error string (e.g. 'skipApplications') was stored as the application - null key, run stopped
                        if ($getApplication -is [string] -and $getApplication -ne 'Request_ResourceNotFound') {
                            $htDataIncomplete[$object.id] = "$($object.displayName): application not read ($($getApplication))"
                            $getApplication = @()
                        }

                        if ($getApplication -eq 'Request_ResourceNotFound') {
                            if (-not $htMeanwhileDeleted[$object.id]) {
                                Write-Host "  $($object.displayName -replace '##', '# #' -replace '[\r\n]+', ' ') ($($object.id)) AppId $($object.appId) - Request_ResourceNotFound, marking as 'meanwhile deleted'"
                                $script:htMeanwhileDeleted[$object.id] = @{}
                                $meanwhileDeleted = $true
                            }
                        }
                        else {
                            if ($getApplication.Count -gt 0) {
                                if ($object.servicePrincipalType -eq 'Application') {
                                    $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].Application = @{
                                        ApplicationDetails = $getApplication
                                    }
                                }
                                if ($hlperType -eq 'AppOnly') {
                                    $script:htServicePrincipalsAndAppsOnlyEnriched["AppWithoutSP_$($object.id)"].Application = @{
                                        ApplicationDetails = $getApplication
                                    }
                                }

                                $script:htApplications[$getApplication.id] = $getApplication

                                #region AppAADRoleAssignedOn
                                if ($htAADRoleAssignmentsByResourceScope) {
                                    #tenant-wide prefetch (verified identical to the per-App request)
                                    $getAppAADRoleAssignedOn = $htAADRoleAssignmentsByResourceScope["/$($getApplication.id)"]
                                    if ($null -eq $getAppAADRoleAssignedOn) {
                                        $getAppAADRoleAssignedOn = @()
                                    }
                                }
                                else {
                                    $currentTask = "getApp AADRoleAssignedOn $($getApplication.id)"
                                    $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/roleManagement/directory/roleAssignments?`$filter=resourceScope eq '/$($getApplication.id)'&`$expand=principal"
                                    $method = 'GET'
                                    $getAppAADRoleAssignedOn = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask
                                }
                                if ($getAppAADRoleAssignedOn.Count -gt 0) {
                                    $tmpArray = [System.Collections.ArrayList]@()
                                    foreach ($AppAADRoleAssignedOn in $getAppAADRoleAssignedOn) {
                                        if ($AppAADRoleAssignedOn.principal.'@odata.type' -eq '#microsoft.graph.user') {

                                            if ([string]::IsNullOrEmpty($AppAADRoleAssignedOn.principal.userType)) {
                                                $principalUserType = 'userType not set'
                                            }
                                            else {
                                                $principalUserType = $AppAADRoleAssignedOn.principal.userType
                                            }
                                            if (-not $htPrincipalsResolved[$AppAADRoleAssignedOn.principal.id]) {
                                                $type = 'User'
                                                $script:htPrincipalsResolved[$AppAADRoleAssignedOn.principal.id] = @{
                                                    full = "$($type) ($($principalUserType)), DisplayName: $($AppAADRoleAssignedOn.principal.displayName), Id: $(($AppAADRoleAssignedOn.principal.id))"
                                                    typeOnly = "$($type) ($($principalUserType))"
                                                }

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
                                        $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].Application.ApplicationAADRoleAssignedOn = $tmpArray
                                    }
                                    if ($hlperType -eq 'AppOnly') {
                                        $script:htServicePrincipalsAndAppsOnlyEnriched["AppWithoutSP_$($object.id)"].Application.ApplicationAADRoleAssignedOn = $tmpArray
                                    }
                                }
                                #endregion AppAADRoleAssignedOn

                                #region getAppOwner
                                $currentTask = "getAppOwner $($getApplication.id)"
                                $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/applications/$($getApplication.id)/owners"
                                $method = 'GET'
                                $getAppOwner = $(if (& $hasNoEntries 'appOwners' $getApplication.id) { @() } else { AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask })

                                if ($getAppOwner -eq 'Request_ResourceNotFound') {
                                    #without this check the error string was stored as an owner with empty id/displayName
                                    if (-not $htMeanwhileDeleted[$object.id]) {
                                        Write-Host "  $($object.displayName -replace '##', '# #' -replace '[\r\n]+', ' ') ($($object.id)) AppId $($object.appId) - getAppOwner Request_ResourceNotFound, marking as 'meanwhile deleted'"
                                        $htMeanwhileDeleted[$object.id] = @{}
                                        $meanwhileDeleted = $true
                                    }
                                }
                                elseif ($getAppOwner.Count -gt 0) {
                                    if (-not $htAppOwners[$getApplication.id]) {
                                        #deterministic order (Graph returns owners in varying order)
                                        $script:htAppOwners[$getApplication.id] = @($getAppOwner | Select-Object id, displayName, '@odata.type' | Sort-Object { $_.displayName }, { $_.id })
                                    }
                                }
                                #endregion getAppOwner

                                #region getFederatedIdentityCredentials
                                $currentTask = "getFederatedIdentityCredentials $($getApplication.id)"
                                $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/applications/$($getApplication.id)/federatedIdentityCredentials"
                                $method = 'GET'
                                $getFederatedIdentityCredentials = $(if (& $hasNoEntries 'appFederatedIdentityCredentials' $getApplication.id) { @() } else { AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method $method -currentTask $currentTask })

                                if ($getFederatedIdentityCredentials -eq 'Request_ResourceNotFound') {
                                    #without this check the error string was stored as the app's federated credentials
                                    if (-not $htMeanwhileDeleted[$object.id]) {
                                        Write-Host "  $($object.displayName -replace '##', '# #' -replace '[\r\n]+', ' ') ($($object.id)) AppId $($object.appId) - getFederatedIdentityCredentials Request_ResourceNotFound, marking as 'meanwhile deleted'"
                                        $htMeanwhileDeleted[$object.id] = @{}
                                        $meanwhileDeleted = $true
                                    }
                                }
                                elseif ($getFederatedIdentityCredentials.Count -gt 0) {
                                    if (-not $htFederatedIdentityCredentials[$getApplication.id]) {
                                        $script:htFederatedIdentityCredentials[$getApplication.id] = $getFederatedIdentityCredentials
                                    }
                                }
                                #endregion getFederatedIdentityCredentials

                                #region spAppKeyCredentials
                                if (($getApplication.keyCredentials).Count -gt 0) {
                                    if ($object.servicePrincipalType -eq 'Application') {
                                        $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].Application.ApplicationKeyCredentials = @{}
                                        foreach ($keyCredential in $getApplication.keyCredentials) {
                                            $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].Application.ApplicationKeyCredentials[$keyCredential.keyId] = $keyCredential
                                        }
                                    }
                                    if ($hlperType -eq 'AppOnly') {
                                        $script:htServicePrincipalsAndAppsOnlyEnriched["AppWithoutSP_$($object.id)"].Application.ApplicationKeyCredentials = @{}
                                        foreach ($keyCredential in $getApplication.keyCredentials) {
                                            $script:htServicePrincipalsAndAppsOnlyEnriched["AppWithoutSP_$($object.id)"].Application.ApplicationKeyCredentials[$keyCredential.keyId] = $keyCredential
                                        }
                                    }
                                }
                                #endregion spAppKeyCredentials

                                #region spAppPasswordCredentials
                                if (($getApplication.passwordCredentials).Count -gt 0) {
                                    if ($object.servicePrincipalType -eq 'Application') {
                                        $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].Application.ApplicationPasswordCredentials = @{}
                                        foreach ($passwordCredential in $getApplication.passwordCredentials) {
                                            $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].Application.ApplicationPasswordCredentials[$passwordCredential.keyId] = $passwordCredential
                                        }
                                    }
                                    if ($hlperType -eq 'AppOnly') {
                                        $script:htServicePrincipalsAndAppsOnlyEnriched["AppWithoutSP_$($object.id)"].Application.ApplicationPasswordCredentials = @{}
                                        foreach ($passwordCredential in $getApplication.passwordCredentials) {
                                            $script:htServicePrincipalsAndAppsOnlyEnriched["AppWithoutSP_$($object.id)"].Application.ApplicationPasswordCredentials[$passwordCredential.keyId] = $passwordCredential
                                        }
                                    }
                                }
                                #endregion spAppPasswordCredentials
                            }
                            elseif ($hlperType -eq 'AppOnly') {
                                #the application was listed earlier, but the appId filter now returns nothing: it was deleted in the meantime
                                if (-not $htMeanwhileDeleted[$object.id]) {
                                    Write-Host "  $($object.displayName -replace '##', '# #' -replace '[\r\n]+', ' ') ($($object.id)) AppId $($object.appId) - application not found anymore, marking as 'meanwhile deleted'"
                                    $htMeanwhileDeleted[$object.id] = @{}
                                    $meanwhileDeleted = $true
                                }
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

                            $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].ManagedIdentity = @{
                                ManagedIdentityDetails = $object
                            }

                            if (($object.alternativeNames).Count -gt 0) {
                                $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].ManagedIdentity.ManagedIdentityAlternativeNames = $object.alternativeNames
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
                        }
                    }
                    #endregion spManagedIdentity
                }

                if (-not $meanwhileDeleted) {
                    if ($hlperType -eq 'SP') {
                        $script:htSpLookup[$object.id].spDisplayName = $object.displayName
                        $script:htSpLookup[$object.id].spId = $object.id
                        $script:htSpLookup[$object.id].spAppId = $object.appId
                        $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].SPOrAppOnly = 'SP'
                        if ($spType -eq 'APP') {
                            $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].objectTypeConcatinated = "SP $($spType) $($spTypeINTEXT)"
                            $script:htSpLookup[$object.id].objectTypeConcatinated = "SP $($spType) $($spTypeINTEXT)"
                            $script:htSpLookup[$object.id].appDisplayName = $getApplication.displayName
                            $script:htSpLookup[$object.id].appId = $getApplication.id
                            $script:htSpLookup[$object.id].appAppId = $getApplication.appId
                            $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].type = $spType
                            $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].subtype = $spTypeINTEXT
                        }
                        elseif ($spType -eq 'MI') {
                            $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].objectTypeConcatinated = "SP $($spType) $($miType)"
                            $script:htSpLookup[$object.id].objectTypeConcatinated = "SP $($spType) $($miType)"
                            $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].type = $spType
                            $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].subtype = $miType
                        }
                        else {
                            $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].objectTypeConcatinated = "SP $($spTypeINTEXT)"
                            $script:htSpLookup[$object.id].objectTypeConcatinated = "SP $($spTypeINTEXT)"
                            $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].type = 'SP'
                            $script:htServicePrincipalsAndAppsOnlyEnriched[$object.id].subtype = $spTypeINTEXT
                        }
                    }
                    if ($hlperType -eq 'AppOnly') {
                        $script:htServicePrincipalsAndAppsOnlyEnriched["AppWithoutSP_$($object.id)"].SPOrAppOnly = 'AppOnly'
                        $script:htSpLookup[$object.id].appDisplayName = $object.displayName
                        $script:htServicePrincipalsAndAppsOnlyEnriched["AppWithoutSP_$($object.id)"].objectTypeConcatinated = 'AppOnly'
                        $script:htSpLookup[$object.id].objectTypeConcatinated = 'AppOnly'
                        $script:htServicePrincipalsAndAppsOnlyEnriched["AppWithoutSP_$($object.id)"].type = 'APP'
                        $script:htServicePrincipalsAndAppsOnlyEnriched["AppWithoutSP_$($object.id)"].subtype = 'AppOnly'
                    }
                }
                else {
                    #AppOnly entries are keyed 'AppWithoutSP_<applicationObjectId>'
                    if ($hlperType -eq 'AppOnly') {
                        $htServicePrincipalsAndAppsOnlyEnriched["AppWithoutSP_$($object.id)"]['MeanWhileDeleted'] = $true
                    }
                    else {
                        $htServicePrincipalsAndAppsOnlyEnriched[$object.id]['MeanWhileDeleted'] = $true
                    }
                }

                $processedServicePrincipalsCount = ($script:htServicePrincipalsAndAppsOnlyEnriched.Keys).Count
                if ($processedServicePrincipalsCount) {
                    if ($processedServicePrincipalsCount % $indicator -eq 0) {
                        if (-not $script:htProcessedTracker[$processedServicePrincipalsCount]) {
                            $script:htProcessedTracker[$processedServicePrincipalsCount] = @{}
                            Write-Host " $processedServicePrincipalsCount Service Principals processed"
                        }
                    }
                }
                #endregion inner
            }
            catch {
                $failedItem = "$($spOrAppWithoutSP.Details.displayName -replace '##', '# #' -replace '[\r\n]+', ' ') ($($spOrAppWithoutSP.Details.id))"
                $htParallelFailures["ServicePrincipal collection|$failedItem"] = [PSCustomObject]@{ Phase = 'ServicePrincipal collection'; Item = $failedItem; Error = $_.Exception.Message; ObjectId = [string]$spOrAppWithoutSP.Details.id }
                Write-Host "  ServicePrincipal collection: '$failedItem' failed - skipped: $($_.Exception.Message)" -ForegroundColor Yellow
            }
        }

    } -ThrottleLimit $ThrottleLimitGraph
    checkParallelFailures -phase 'ServicePrincipal collection' -itemCount ($arraySPsAndAppsWithoutSP.Count)

    $endForeachSP = Get-Date
    $duration = New-TimeSpan -Start $startForeachSP -End $endForeachSP
    Write-Host " Collecting data for all Service Principals ($($arraySPsAndAppsWithoutSP.Count)) duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"
    Write-Host " Service Principals that have been meanwhile deleted: $($htMeanwhileDeleted.Keys.Count)"
    if ($htDataIncomplete.Keys.Count -gt 0) {
        Write-Host " Service Principals with incomplete data: $($htDataIncomplete.Keys.Count)" -ForegroundColor Yellow
        foreach ($incomplete in $htDataIncomplete.Values) {
            Write-Host "  $($incomplete)" -ForegroundColor Yellow
        }
    }
    else {
        Write-Host ' Service Principals with incomplete data: 0'
    }

    #SPOauth2PermissionGrantedTo index (grants by resource SP), built once and sequentially from the grants collected per SP.
    #It used to be built inside the parallel per-SP collection, where threads raced on the shared per-resource list and
    #grants got lost (review P0-4). Same entry shape as before (one-element list incl. '@odata.id'), so the JSON stays the same.
    #review F-22: -OnlyProcessSPs...: the grants of all clients (the tenant-wide list), not only of the processed ones - a processed
    #resource SP showed only the processed clients
    if ($OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes -and $prefetchOauth2PermissionGrants -isnot [string] -and $null -ne $prefetchOauth2PermissionGrants) {
        $grantedToSources = @(@{ ServicePrincipalOauth2PermissionGrants = @($prefetchOauth2PermissionGrants) })
    }
    else {
        $grantedToSources = @($htServicePrincipalsAndAppsOnlyEnriched.Values)
    }
    foreach ($enrichedEntry in $grantedToSources) {
        if ($enrichedEntry['MeanWhileDeleted'] -or -not $enrichedEntry['ServicePrincipalOauth2PermissionGrants']) {
            continue
        }
        foreach ($permissionGrant in $enrichedEntry['ServicePrincipalOauth2PermissionGrants']) {
            if ([string]::IsNullOrEmpty($permissionGrant.scope)) {
                continue
            }
            foreach ($permissionscope in ($permissionGrant.scope).split(' ')) {
                if ([string]::IsNullOrWhiteSpace($permissionscope)) {
                    continue
                }
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
                if (-not $htSPOauth2PermissionGrantedTo[$permissionGrant.resourceId]) {
                    $htSPOauth2PermissionGrantedTo[$permissionGrant.resourceId] = [System.Collections.ArrayList]@()
                }
                $null = $htSPOauth2PermissionGrantedTo[$permissionGrant.resourceId].Add($permissionGrantArray)
            }
        }
    }

    #Entra role assignments through groups: an SP that is a member of a role-assignable group holds the group's roles
    #(before, only role assignments with the SP itself as principal were collected). The SPs' group memberships are transitive
    #(getMemberGroups); the role assignments come from the tenant-wide prefetch (no extra request) or, without it, one request per group.
    $htAADRoleAssignmentsOfGroups = @{}
    $spGroupIds = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    foreach ($enrichedEntry in @($htServicePrincipalsAndAppsOnlyEnriched.Values)) {
        if ($enrichedEntry['MeanWhileDeleted']) {
            continue
        }
        foreach ($groupId in @($enrichedEntry['ServicePrincipalGroupMemberships'])) {
            if ($groupId) {
                $null = $spGroupIds.Add([string]$groupId)
            }
        }
    }
    foreach ($groupId in $spGroupIds) {
        if ($htAADRoleAssignmentsByPrincipalId) {
            $groupAADRoleAssignments = $htAADRoleAssignmentsByPrincipalId[$groupId]
        }
        else {
            $currentTask = "getGroup AADRoleAssignments $($groupId)"
            $uri = "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/beta/roleManagement/directory/roleAssignments?`$filter=principalId eq '$($groupId)'"
            $groupAADRoleAssignments = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uri -method 'GET' -currentTask $currentTask
            if ($groupAADRoleAssignments -is [string]) {
                #e.g. 'Request_ResourceNotFound' (group deleted meanwhile)
                $groupAADRoleAssignments = $null
            }
        }
        if ($groupAADRoleAssignments) {
            $htAADRoleAssignmentsOfGroups[$groupId] = @($groupAADRoleAssignments)
        }
    }
    $aadRoleAssignmentsThroughGroupsCount = 0
    foreach ($enrichedEntry in @($htServicePrincipalsAndAppsOnlyEnriched.Values)) {
        if ($enrichedEntry['MeanWhileDeleted'] -or -not $enrichedEntry['ServicePrincipalGroupMemberships']) {
            continue
        }
        $throughGroups = [System.Collections.ArrayList]@()
        foreach ($groupId in @($enrichedEntry['ServicePrincipalGroupMemberships'])) {
            foreach ($groupAADRoleAssignment in @($htAADRoleAssignmentsOfGroups[[string]$groupId])) {
                if ($groupAADRoleAssignment) {
                    $null = $throughGroups.Add([PSCustomObject]@{ groupId = [string]$groupId; assignment = $groupAADRoleAssignment })
                }
            }
        }
        if ($throughGroups.Count -gt 0) {
            $enrichedEntry['ServicePrincipalAADRoleAssignmentsThroughGroups'] = $throughGroups
            $aadRoleAssignmentsThroughGroupsCount += $throughGroups.Count
        }
    }
    Write-Host " Entra role assignments through groups: $($aadRoleAssignmentsThroughGroupsCount) (groups with Entra role assignments: $($htAADRoleAssignmentsOfGroups.Count) of $($spGroupIds.Count) groups the Service Principals are member of)"
}
$end = Get-Date
$duration = New-TimeSpan -Start $startSP -End $end
Write-Host "SP Collection duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"
#endregion dataColletionAADSP

$htUsersToResolveGuestMember = @{}

#region UsersToResolveGuestMember
#Users that have an app role assignment on an SP: their userType (Guest/Member) is resolved later.
#(The former region AppRoleAssignments4UsersAndGroups also called GET /users/{id}/appRoleAssignments and
# /groups/{id}/appRoleAssignments per principal, only to read .appRoleId of an assignment that
# /servicePrincipals/{id}/appRoleAssignedTo already returns; a principal deleted in between crashed the run, GitHub issue #43.)
foreach ($user in $htUsersAndGroupsToCheck4AppRoleAssignmentsUser.Keys) {
    if (-not $htUsersToResolveGuestMember[$user]) {
        $htUsersToResolveGuestMember[$user] = @{}
    }
}
#endregion UsersToResolveGuestMember

#region AADGroupsResolve

$htAadGroups = [System.Collections.Hashtable]::Synchronized(@{})

#region groupsFromSPs
$startgroupsFromSPs = Get-Date
Write-Host 'Resolving AAD Groups where any SP is memberOf'
if (($htAadGroupsToResolve.Keys).Count -gt 0) {
    Write-Host " Resolving $(($htAadGroupsToResolve.Keys).Count) AAD Groups where any SP is memberOf"
    $startgroupsFromSPs = Get-Date

    #review R-21: batched getByIds (1,000 groups per request, as the groups of the Azure role assignments below) instead of one
    #GET per group; a group that is not returned (deleted meanwhile) stays unresolved and shows as '<n/a>', as before
    $groupBatches = @(getParallelBatches -Items @($htAadGroupsToResolve.Keys) -BatchSize 1000)
    $groupBatchNumber = 0
    foreach ($groupBatch in $groupBatches) {
        $groupBatchNumber++
        $body = '{ "ids":[' + ('"{0}"' -f ($groupBatch.Group -join '","')) + '], "types":["group"] }'
        $currentTask = "Resolving AAD Groups where any SP is memberOf - Batch #$groupBatchNumber/$($groupBatches.Count) ($($groupBatch.Group.Count) groups)"
        $resolvedGroups = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri "$($azAPICallConf['azAPIEndpointUrls'].MicrosoftGraph)/v1.0/directoryObjects/getByIds" -method 'POST' -body $body -currentTask $currentTask
        if ($resolvedGroups -is [string]) {
            $failedItem = "batch $groupBatchNumber ($($groupBatch.Group.Count) groups)"
            $htParallelFailures["AAD Groups resolve|$failedItem"] = [PSCustomObject]@{ Phase = 'AAD Groups resolve'; Item = $failedItem; Error = $resolvedGroups }
            Write-Host "  AAD Groups resolve: '$failedItem' failed - skipped: $($resolvedGroups)" -ForegroundColor Yellow
            continue
        }
        foreach ($resolvedGroup in @($resolvedGroups)) {
            if ($resolvedGroup.id) {
                $script:htAadGroups[$resolvedGroup.id] = @{
                    groupDetails = $resolvedGroup
                }
            }
        }
    }
    Write-Host " $(@($htAadGroupsToResolve.Keys).where({ $htAadGroups.ContainsKey($_) }).Count) of $(@($htAadGroupsToResolve.Keys).Count) groups resolved"
    checkParallelFailures -phase 'AAD Groups resolve' -itemCount $groupBatches.Count

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
$batchSize = 1000
$arrayObjectIdsToProcess = [System.Collections.ArrayList]@()
#review P-15: only principals of type Group (or without type) can be groups - users and SPs were sent to getByIds as well
$objectIdsUnique = @(($htCacheAssignmentsRole).values.assignment.properties | Where-Object { $_.principalId -and ($_.principalType -eq 'Group' -or [string]::IsNullOrEmpty($_.principalType)) } | ForEach-Object { [string]$_.principalId } | Sort-Object -Unique)
Write-Host " Unique objectIds that have Azure Role assignments (groups): $($objectIdsUnique.Count)"
foreach ($objectId in $objectIdsUnique) {
    if (-not $htAadGroups.ContainsKey($objectId)) {
        $null = $arrayObjectIdsToProcess.Add($objectId)
    }
}
$objectIdsCount = $arrayObjectIdsToProcess.Count
Write-Host " Unique objectIds that have Azure Role assignments and are not resolved, yet: $($objectIdsCount)"

$objectIdsBatch = getParallelBatches -Items $arrayObjectIdsToProcess -BatchSize $batchSize
$objectIdsBatchCount = ($objectIdsBatch | Measure-Object).Count
$batchCnt = 0

Write-Host "Processing $objectIdsCount objectIds"

#region resolve groups that have Azure role assignments
#step 1: resolve which objectIds are groups (getByIds is batched to 1000, few calls)
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
    #review F-21: an error string (handled error, non-JSON content) is a failed batch, as in the group resolve above (R-21)
    if ($resolveObjectIdsTypeGroup -is [string]) {
        $failedItem = "batch $batchCnt ($(($batch.Group).Count) objectIds)"
        $htParallelFailures["Azure role assignment groups resolve|$failedItem"] = [PSCustomObject]@{ Phase = 'Azure role assignment groups resolve'; Item = $failedItem; Error = $resolveObjectIdsTypeGroup }
        Write-Host "  Azure role assignment groups resolve: '$failedItem' failed - skipped: $($resolveObjectIdsTypeGroup)" -ForegroundColor Yellow
        continue
    }

    foreach ($group in @($resolveObjectIdsTypeGroup)) {
        if (-not $group.id) { continue }
        $script:htAadGroups[$group.id] = @{
            groupDetails = $group
        }
    }
    Write-Host "Groups resolved: $($htAadGroups.Keys.Count)"
}

#(step 2 fetched the nested groups of each group via transitivemembers/microsoft.graph.group; removed: every group that
# has an Azure role assignment is already resolved in step 1, and nested membership is covered by getMemberGroups)
#endregion resolve groups that have Azure role assignments

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
            if (-not $htUsersToResolveGuestMember[$owner.id]) {
                #Write-Host "UsersToResolveGuestMember SPowner added ($($owner.id))"
                $htUsersToResolveGuestMember[$owner.id] = @{}
            }
        }
    }
}
foreach ($appOwner in $htAppOwners.Values) {
    foreach ($owner in $appOwner) {
        if ($owner.'@odata.type' -eq '#microsoft.graph.user') {
            if (-not $htUsersToResolveGuestMember[$owner.id]) {
                #Write-Host "UsersToResolveGuestMember appOwner added ($($owner.id))"
                $htUsersToResolveGuestMember[$owner.id] = @{}
            }
        }
    }
}
resolveObjectsById -objects $htUsersToResolveGuestMember.Keys -targetHt 'htPrincipalsResolved'

$htOwnedByEnriched = @{}
#owners by name and id (Graph returns the owners of an object in varying order; the nested 'ownedBy' data of owner SPs and the
#order of the owner CSV rows followed it from run to run)
foreach ($sp in @($htOwnedBy.Keys)) {
    $ownedByList = @($htOwnedBy[$sp].ownedBy)
    if ($ownedByList.Count -gt 1) {
        $htOwnedBy[$sp].ownedBy = @($ownedByList | Sort-Object { $_.displayName }, { $_.id })
    }
}
foreach ($sp in $htOwnedBy.Keys) {
    $htOwnedByEnriched[$sp] = @{}
    foreach ($ownedBy in $htOwnedBy[$sp].ownedBy) {
        $arrayx = @()
        if ($ownedBy -ne 'noOwner') {
            foreach ($owner in $ownedBy) {
                $htTmp = [ordered] @{
                    id = $owner.id
                    displayName = $owner.displayName
                    '@odata.type' = $owner.'@odata.type'
                }

                if ($owner.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
                    $hlpType = getServicePrincipalTypeLabelById -Id $owner.id   #R-8: also owner SPs that were not processed
                    $htTmp.spType = $hlpType
                    $htTmp.principalType = $hlpType
                }
                if ($owner.'@odata.type' -eq '#microsoft.graph.user') {
                    $htTmp.principalType = $htPrincipalsResolved[$owner.id].typeOnly
                }
                $htTmp.applicability = 'direct'
                $arrayx += $htTmp
                if (-not $htOwnedByEnriched[$sp]) {
                    $htOwnedByEnriched[$sp] = @{
                        ownedBy = [array]$arrayx
                    }
                }
                else {
                    $array = [array]($htOwnedByEnriched[$sp].ownedBy)
                    $array += $arrayx
                    $htOwnedByEnriched[$sp].ownedBy = $array
                }
            }
        }
        else {
            $arrayx += $ownedBy
            if (-not $htOwnedByEnriched[$sp]) {
                $htOwnedByEnriched[$sp] = @{
                    ownedBy = [array]$arrayx
                }
            }
            else {
                $array = [array]($htOwnedByEnriched[$sp].ownedBy)
                $array += $arrayx
                $htOwnedByEnriched[$sp].ownedBy = $array
            }
        }
    }
}

function getOwner($owner) {
    return $htOwnedByEnriched[$owner].ownedBy
}
#who can take control of a Service Principal: its owners, and the owners of its app registration (in this tenant); both can
#add a credential and sign in as it. Items: @{ owner; how = 'owner' | 'app registration owner' }
function getSPControllers([string]$spId) {
    foreach ($owner in @(getowner -owner $spId)) {
        if ($null -ne $owner -and $owner -isnot [string]) {
            @{ owner = $owner; how = 'owner' }
        }
    }
    $appObjectId = $htServicePrincipalsAndAppsOnlyEnriched[$spId].Application.ApplicationDetails.id
    if ($appObjectId) {
        foreach ($owner in @($htAppOwners[$appObjectId])) {
            if ($null -ne $owner -and $owner -isnot [string]) {
                @{ owner = $owner; how = 'app registration owner' }
            }
        }
    }
}
$htSPOwnersTmp = @{}
$htSPOwnersFinal = @{}
#review R-8: owner SPs that were not processed (their owners are not known; the indirect owners stop there)
$ownerSPsNotProcessed = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)

foreach ($sp in $htServicePrincipalsAndAppsOnlyEnriched.Keys) {
    #review P-16: without direct owners there are no indirect owners and nothing is stored (most SPs: Microsoft, third party, MIs)
    if (-not $htSPOwners[$sp]) { continue }

    $htSPOwnersTmp[$sp] = @{
        direct = [System.Collections.ArrayList]@()
        indirect = [System.Collections.ArrayList]@()
    }

    foreach ($owner in $htSPOwners[$sp].where({ $_.'@odata.type' -eq '#microsoft.graph.user' })) {
        $null = ($htSPOwnersTmp[$sp].direct).Add($owner)
    }
    foreach ($owner in $htSPOwners[$sp].where({ $_.'@odata.type' -eq '#microsoft.graph.servicePrincipal' })) {
        $null = ($htSPOwnersTmp[$sp].direct).Add($owner)
    }
    #indirect owners: every principal that can take control of an owner SP (its owners and the owners of its app registration),
    #transitively, breadth-first over the ownership graph (the first path found is the shortest one, kept as 'via').
    #Not listed: the object itself (ownership cycles) and principals that are direct owners anyway.
    #(was: SP owners only, so the owners of an owner SP's app registration were missing, and in cycles the object was listed
    # as its own indirect owner and direct owners again as indirect; before that a do/until loop followed only the last branch)
    $directOwnerIds = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    foreach ($owner in $htSPOwnersTmp[$sp].direct) { $null = $directOwnerIds.Add($owner.id) }
    $indirectOwnerIds = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    $htIndirectVia = @{}
    $expandedOwnerSPs = [System.Collections.Generic.HashSet[string]]::new([StringComparer]::OrdinalIgnoreCase)
    $null = $expandedOwnerSPs.Add($sp)
    $ownerSPQueue = [System.Collections.Generic.Queue[object]]::new()
    foreach ($owner in $htSPOwnersTmp[$sp].direct.where({ $_.'@odata.type' -eq '#microsoft.graph.servicePrincipal' })) {
        #path: the owner SPs from the one that is controlled up to the direct owner of this object
        $ownerSPQueue.Enqueue(@($owner.id, @($owner.displayName)))
    }
    while ($ownerSPQueue.Count -gt 0) {
        $item = $ownerSPQueue.Dequeue()
        $ownerSPId = $item[0]
        $path = $item[1]
        if (-not $expandedOwnerSPs.Add($ownerSPId)) {
            continue
        }
        foreach ($controller in @(getSPControllers -spId $ownerSPId)) {
            $owner = $controller.owner
            if ($owner.id -eq $sp -or $directOwnerIds.Contains($owner.id)) {
                continue
            }
            if ($indirectOwnerIds.Add($owner.id)) {
                $null = $htSPOwnersTmp[$sp].indirect.Add($owner)
                #e.g. "app registration owner of 'P', which controls 'O'" (O: the direct owner of this object)
                $htIndirectVia[$owner.id] = "$($controller.how) of '$($path[0])'$(@(for ($pathIndex = 1; $pathIndex -lt $path.Count; $pathIndex++) { ", which controls '$($path[$pathIndex])'" }) -join '')"
            }
            if ($owner.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
                $ownerSPQueue.Enqueue(@($owner.id, @(@($owner.displayName) + $path)))
            }
        }
    }

    $arrayOwners = [System.Collections.ArrayList]@()
    foreach ($owner in $htSPOwnersTmp[$sp].direct) {
        $htOptInfo = [ordered] @{
            id = $($owner.id)
            displayName = $($owner.displayName)
            type = $($owner.'@odata.type')
        }

        if ($owner.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
            $htOptInfo.spType = getServicePrincipalTypeLabelById -Id $owner.id
            $htOptInfo.principalType = $htOptInfo.spType
            if (-not $htServicePrincipalsAndAppsOnlyEnriched[$owner.id]) {
                #review R-8: an owner SP that was not processed: its owners (the indirect owners of this object) are not known
                $htOptInfo.ownersNotCollected = 'not processed (OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes): its owners are not known'
                $null = $ownerSPsNotProcessed.Add([string]$owner.id)
            }
        }
        if ($owner.'@odata.type' -eq '#microsoft.graph.user') {
            $htOptInfo.principalType = $htPrincipalsResolved[$owner.id].typeOnly
        }
        $htOptInfo.applicability = 'direct'
        $owners = $null
        if ($owner.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
            $owners = getowner -owner $owner.id
        }
        $htOptInfo.ownedBy = $owners
        $null = $arrayOwners.Add($htOptInfo)
    }

    foreach ($owner in $htSPOwnersTmp[$sp].indirect) {
        if ($owner -eq 'noOwner' -or $owner.'@odata.type' -eq '#microsoft.graph.user') {
            if ($owner.'@odata.type' -eq '#microsoft.graph.user') {
                #(review R-23: no check for an existing entry - the walk adds every indirect owner once, $indirectOwnerIds)
                $htOptInfo = [ordered] @{
                    id = $($owner.id)
                    displayName = $($owner.displayName)
                    type = $($owner.'@odata.type')
                    principalType = $htPrincipalsResolved[$owner.id].typeOnly
                    applicability = 'indirect'
                    via = $htIndirectVia[$owner.id]
                }
                $null = $arrayOwners.Add($htOptInfo)
            }
        }
        else {
            $htOptInfo = [ordered] @{
                id = $($owner.id)
                displayName = $($owner.displayName)
                type = $($owner.'@odata.type')
                applicability = 'indirect'
                via = $htIndirectVia[$owner.id]
            }

            if ($owner.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
                $htOptInfo.principalType = getServicePrincipalTypeLabelById -Id $owner.id
                if (-not $htServicePrincipalsAndAppsOnlyEnriched[$owner.id]) {
                    $htOptInfo.ownersNotCollected = 'not processed (OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes): its owners are not known'
                    $null = $ownerSPsNotProcessed.Add([string]$owner.id)
                }
            }
            if ($owner.'@odata.type' -eq '#microsoft.graph.user') {
                $htOptInfo.principalType = $htPrincipalsResolved[$owner.id].typeOnly
            }

            $owners = getowner -owner $owner.id
            $htOptInfo.ownedBy = $owners
            $null = $arrayOwners.Add($htOptInfo)
        }
    }

    if ($arrayOwners.Count -gt 0) {
        $htSPOwnersFinal[$sp] = @{}
        #deterministic order (direct before indirect, then by name and id); the order from Graph / the parallel collection
        #varied between runs. Scriptblocks: Sort-Object -Property does not sort [ordered] dictionaries
        $htSPOwnersFinal[$sp] = $(if ($arrayOwners.Count -gt 1) { @($arrayOwners | Sort-Object { $_.applicability }, { $_.displayName }, { $_.id }) } else { @($arrayOwners) })
    }

}
if ($ownerSPsNotProcessed.Count -gt 0) {
    Write-Host " $($ownerSPsNotProcessed.Count) owner Service Principals were not processed (-OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes): their owners (indirect owners) are not known" -ForegroundColor Yellow
}


#endregion owners

if (-not $NoAzureResourceSideRelations) {
    #region AzureRoleAssignmentMapping
    $startAzureRoleAssignmentMapping = Get-Date

    #build O(1) lookup set of Service Principal ids (avoids O(n) '-contains' array scans in the loops below)
    $htServicePrincipalsIdsLookup = [System.Collections.Generic.HashSet[string]]::new([string[]]$getServicePrincipals.id)

    #resolving createdby/updatedby
    $htCreatedByUpdatedByObjectIdsToBeResolved = @{}
    foreach ($createdByItem in ($htCacheAssignmentsRole).values.assignment.properties.createdBy | Sort-Object -Unique) {

        if ([guid]::TryParse(($createdByItem), $([ref][guid]::Empty))) {
            if (-not $htPrincipalsResolved[$createdByItem]) {
                if ($htServicePrincipalsIdsLookup.Contains($createdByItem)) {
                    #
                }
                else {
                    if (-not $htCreatedByUpdatedByObjectIdsToBeResolved[$createdByItem]) {
                        $htCreatedByUpdatedByObjectIdsToBeResolved[$createdByItem] = @{}
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
        resolveObjectsById -objects $arrayUnresolvedIdentities -targetHt 'htPrincipalsResolved'
    }

    if (($htCacheAssignmentsRole).Keys.Count -gt 0) {
        foreach ($assignment in ($htCacheAssignmentsRole).values) {
            #hoist repeated deep member access (otherwise traversed up to ~6x per iteration)
            $assignmentProperties = $assignment.assignment.properties
            $assignmentCreatedBy = $assignmentProperties.createdBy
            $assignmentPrincipalId = $assignmentProperties.principalId

            #todo: sp created ra in azure
            if (-not [string]::IsNullOrEmpty($assignmentCreatedBy)) {
                if ($htPrincipalsResolved[$assignmentCreatedBy]) {
                    $assignmentProperties.createdBy = $htPrincipalsResolved[$assignmentCreatedBy].full
                }
                else {
                    if ($htServicePrincipalsAndAppsOnlyEnriched[$assignmentCreatedBy] -and $htServicePrincipalsAndAppsOnlyEnriched[$assignmentCreatedBy].objectTypeConcatinated) {
                        $hlper = $htServicePrincipalsAndAppsOnlyEnriched[$assignmentCreatedBy]
                        $assignmentProperties.createdBy = "$($hlper.objectTypeConcatinated), DisplayName: $($hlper.ServicePrincipalDetails.displayName), Id: $($assignmentCreatedBy)"
                    }
                    elseif ($htAllServicePrincipalsById[$assignmentCreatedBy]) {
                        #review R-8: a Service Principal of the tenant that was not processed (-OnlyProcessSPs...) or not completely
                        #(deleted during the run, failed item): was 'Unknown (deleted or not in this tenant)' / ', DisplayName: ...'
                        $assignmentProperties.createdBy = "$(getServicePrincipalTypeLabelById -Id $assignmentCreatedBy), DisplayName: $($htAllServicePrincipalsById[$assignmentCreatedBy].displayName), Id: $($assignmentCreatedBy)"
                    }
                    elseif ([guid]::TryParse($assignmentCreatedBy, $([ref][guid]::Empty))) {
                        #not returned by getByIds (also not as foreign principal): deleted (beyond the 30 days of the recycle bin) or an
                        #object of another tenant - labelled instead of a bare GUID
                        $assignmentProperties.createdBy = "Unknown (deleted or not in this tenant), Id: $($assignmentCreatedBy)"
                    }
                }
            }
            if ($htServicePrincipalsIdsLookup.Contains($assignmentPrincipalId)) {
                if (-not $htAssignmentsByPrincipalIdServicePrincipals.ContainsKey($assignmentPrincipalId)) {
                    $htAssignmentsByPrincipalIdServicePrincipals[$assignmentPrincipalId] = [System.Collections.ArrayList]@()
                }
                $null = $htAssignmentsByPrincipalIdServicePrincipals[$assignmentPrincipalId].Add($assignment)
            }
            if ($htAadGroups.ContainsKey($assignmentPrincipalId)) {
                if (-not $htAssignmentsByPrincipalIdGroups.ContainsKey($assignmentPrincipalId)) {
                    $htAssignmentsByPrincipalIdGroups[$assignmentPrincipalId] = [System.Collections.ArrayList]@()
                }
                $null = $htAssignmentsByPrincipalIdGroups[$assignmentPrincipalId].Add($assignment)

            }
        }
    }
    else {
        #'break' here was outside any loop and silently ended the whole script; continue without the Azure RBAC mapping instead
        Write-Host ' No Azure RoleAssignments found - skipping Azure RoleAssignment mapping' -ForegroundColor Yellow
    }
    $end = Get-Date
    $duration = New-TimeSpan -Start $startAzureRoleAssignmentMapping -End $end
    Write-Host "AzureRoleAssignmentMapping duration: $(($duration).TotalMinutes) minutes ($(($duration).TotalSeconds) seconds)"
    #endregion AzureRoleAssignmentMapping
}

#region enrichedAADSPData
Write-Host 'Enrichment starting prep'
$cu = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))

$htSPandAPPHelper4AADRoleAssignmentsWithScope = @{}
foreach ($aadRoleAssignment in $htServicePrincipalsAndAppsOnlyEnriched.values.ServicePrincipalAADRoleAssignments) {
    if ($aadRoleAssignment.resourceScope -ne '/') {

        if ($htApplications[$aadRoleAssignment.resourceScope -replace '/']) {
            if (-not $htSPandAPPHelper4AADRoleAssignmentsWithScope[$aadRoleAssignment.resourceScope -replace '/']) {
                $hlp = $htApplications[$aadRoleAssignment.resourceScope -replace '/']
                $htSPandAPPHelper4AADRoleAssignmentsWithScope[$aadRoleAssignment.resourceScope -replace '/'] = "Application: $($hlp.displayname) ($($hlp.id))"
            }

        }
        else {
            if ($htServicePrincipalsAndAppsOnlyEnriched[$aadRoleAssignment.resourceScope -replace '/']) {
                if (-not $htSPandAPPHelper4AADRoleAssignmentsWithScope[$aadRoleAssignment.resourceScope -replace '/']) {
                    $hlp = $htServicePrincipalsAndAppsOnlyEnriched[$aadRoleAssignment.resourceScope -replace '/'].ServicePrincipalDetails
                    $htSPandAPPHelper4AADRoleAssignmentsWithScope[$aadRoleAssignment.resourceScope -replace '/'] = "ServicePrincipal: $($hlp.displayname) ($($hlp.id))"
                }
            }
        }
    }
}
Write-Host 'Enrichment completed prep'

#hashtable keyed by MI principalId (was an array of groups searched with .where() for every UAMI)
#Group-Object -AsHashTable returns $null (not an empty hashtable) for empty input
$htUserAssignedIdentities4ResourcesByMI = $arrayUserAssignedIdentities4Resources | Group-Object -Property miPrincipalId -AsHashTable -AsString
if (-not $htUserAssignedIdentities4ResourcesByMI) {
    $htUserAssignedIdentities4ResourcesByMI = @{}
}

Write-Host 'Enrichment starting'
$enrichmentProcessCounter = [pscustomobject]@{counter = 0 }
$enrichmentProcessindicator = 100
$processedServicePrincipalsCount = 0
$startEnrichmentSP = Get-Date
$arrayPerformanceTracking = [System.Collections.ArrayList]::Synchronized((New-Object System.Collections.ArrayList))

$servicePrincipalsAndAppsOnlyEnriched = ($htServicePrincipalsAndAppsOnlyEnriched.values).where( { -not $_.MeanWhileDeleted } )

#review P-4: this block makes no API calls and catches per object: few large batches (cores x 4) and one thread per core - each
#batch costs a fixed ~35-60 ms (the block is prepared per input item), so the P1-21 small batches (3 objects in the test tenant:
#400 batches) cost more than the work (enrichment 14-16 s instead of 5-7 s)
$enrichmentThrottleLimit = [math]::Max(1, [math]::Min($ThrottleLimitLocal, [Environment]::ProcessorCount))
$batchSize = [math]::Max(1, [math]::Ceiling($servicePrincipalsAndAppsOnlyEnriched.Count / ($enrichmentThrottleLimit * 4)))
Write-Host "Optimal batch size: $($batchSize)"
$servicePrincipalsAndAppsOnlyEnrichedSPBatch = getParallelBatches -Items $servicePrincipalsAndAppsOnlyEnriched -BatchSize $batchSize
#review F-3: the management groups and subscriptions of the tenant whose policy assignments were NOT collected in this run (excluded
#by state, quota or whitelist, or failed): a policy assignment managed identity there is not checked for 'relict'. A scope that does
#not exist any more (deleted management group) is not in the list - its assignment cannot exist, the identity is relict.
$policyAssignmentScopesNotCollected = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
foreach ($tenantEntity in @($arrayEntitiesFromAPI)) {
    if ($tenantEntity.type -eq 'Microsoft.Management/managementGroups') { $null = $policyAssignmentScopesNotCollected.Add("mg:$($tenantEntity.name)") }
    elseif ($tenantEntity.type -eq '/subscriptions') { $null = $policyAssignmentScopesNotCollected.Add("sub:$($tenantEntity.name)") }
}
foreach ($collectedManagementGroup in @($arrayDataCollectionProgressMg)) { $null = $policyAssignmentScopesNotCollected.Remove("mg:$($collectedManagementGroup)") }
foreach ($collectedSubscription in @($arrayDataCollectionProgressSub)) { $null = $policyAssignmentScopesNotCollected.Remove("sub:$($collectedSubscription)") }
Write-Host "Processing data in $($servicePrincipalsAndAppsOnlyEnrichedSPBatch.Count) batches"

$servicePrincipalsAndAppsOnlyEnrichedSPBatch | ForEach-Object -Parallel {
    #no AzAPICall import here (review P-4): this block makes no API calls
    $htParallelFailures = $using:htParallelFailures

    $cu = $using:cu
    $enrichmentProcessCounter = $using:enrichmentProcessCounter
    $enrichmentProcessindicator = $using:enrichmentProcessindicator
    $arrayPerformanceTracking = $using:arrayPerformanceTracking
    $PerfTracking = $using:PerfTracking
    $htSPandAPPHelper4AADRoleAssignmentsWithScope = $using:htSPandAPPHelper4AADRoleAssignmentsWithScope
    $htSPOwnersFinal = $using:htSPOwnersFinal
    #vars
    $NoAzureResourceSideRelations = $using:NoAzureResourceSideRelations
    $ApplicationSecretExpiryWarning = $using:ApplicationSecretExpiryWarning
    $ApplicationSecretExpiryMax = $using:ApplicationSecretExpiryMax
    $ApplicationCertificateExpiryWarning = $using:ApplicationCertificateExpiryWarning
    $ApplicationCertificateExpiryMax = $using:ApplicationCertificateExpiryMax
    $runTenantRoot = $using:runTenantRoot
    $policyAssignmentScopesNotCollected = $using:policyAssignmentScopesNotCollected
    #review F-22: names of client SPs that were not processed (-OnlyProcessSPs...)
    $htAllServicePrincipalsById = $using:htAllServicePrincipalsById

    $htCacheAssignmentsPolicy = $using:htCacheAssignmentsPolicy
    $htAadRoleDefinitions = $using:htAadRoleDefinitions
    $htPublishedPermissionScopes = $using:htPublishedPermissionScopes
    $htSPOauth2PermissionGrantedTo = $using:htSPOauth2PermissionGrantedTo
    $htAppRoles = $using:htAppRoles
    #review R-3: app role ids are only unique per application (Microsoft reuses them across its APIs, e.g. Exchange Online
    #Calendars.ReadWrite.All = Microsoft Graph Calendars.ReadWrite): the app role of the resource SP first, the id-only lookup as fallback
    $htServicePrincipalsAppRoles = $using:htServicePrincipalsAppRoles
    $getResourceAppRole = {
        param([string]$ResourceId, [string]$AppRoleId)
        $resourceAppRoles = $(if ($ResourceId) { $htServicePrincipalsAppRoles[$ResourceId] })
        if ($resourceAppRoles -and $resourceAppRoles.appRoles[$AppRoleId]) { return $resourceAppRoles.appRoles[$AppRoleId] }
        return $htAppRoles[$AppRoleId]
    }
    $htPrincipalsResolved = $using:htPrincipalsResolved
    $htAppRoleAssignments = $using:htAppRoleAssignments
    $htAaDGroups = $using:htAaDGroups
    $htAssignmentsByPrincipalIdGroups = $using:htAssignmentsByPrincipalIdGroups
    $htAssignmentsByPrincipalIdServicePrincipals = $using:htAssignmentsByPrincipalIdServicePrincipals
    $htAppOwners = $using:htAppOwners
    $htServicePrincipalsPublishedPermissionScopes = $using:htServicePrincipalsPublishedPermissionScopes
    $htSpLookup = $using:htSpLookup
    $getClassifications = $using:getClassifications
    $htFederatedIdentityCredentials = $using:htFederatedIdentityCredentials
    $CriticalAADRoles = $using:CriticalAADRoles
    $htUserAssignedIdentities4ResourcesByMI = $using:htUserAssignedIdentities4ResourcesByMI
    $htMIUserAssignedFederatedCreds = $using:htMIUserAssignedFederatedCreds
    $htUserAssignedIdentitiesSettings = $using:htUserAssignedIdentitiesSettings
    #functions
    $function:getClassification = $using:funcGetClassification

    foreach ($spOrAppWithoutSP in $_.Group) {
        #P1-21: one failing item must not abort the whole run (a throw inside -Parallel stops the script under ErrorActionPreference Stop)
        try {
            #region parallel
            #$spOrAppWithoutSP = $_


            $object = $spOrAppWithoutSP
            if ($spOrAppWithoutSP.SPOrAppOnly -eq 'SP') {
                $spId = $object.ServicePrincipalDetails.id
                #Write-host "processing SP:" $object.ServicePrincipalDetails.displayName "objId: $($spId)" "appId: $($object.ServicePrincipalDetails.appId)"
            }
            elseif ($spOrAppWithoutSP.SPOrAppOnly -eq 'AppOnly') {
                #Write-host "processing AppOnly:" $object.Application.ApplicationDetails.displayName "objId: $($objId)" "appId: $($object.Application.ApplicationDetails.appId)"
            }
            else {
                #neither SP nor AppOnly, e.g. an object that was deleted while data was collected and is therefore incomplete
                Write-Host "  Skipping unexpected object (SPOrAppOnly: '$($spOrAppWithoutSP.SPOrAppOnly)'; keys: $(@($spOrAppWithoutSP.Keys) -join ', '))" -ForegroundColor Yellow
                continue
            }

            if ($spOrAppWithoutSP.SPOrAppOnly -eq 'SP') {

                #region ServicePrincipalOwnedObjects
                $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
                $arrayServicePrincipalOwnedObjectsOpt = [System.Collections.ArrayList]@()
                if (($object.ServicePrincipalOwnedObjects).Count -gt 0) {
                    foreach ($ownedObject in $object.ServicePrincipalOwnedObjects | Sort-Object -Property '@odata.type', id) {

                        $type = 'unforeseen type'
                        if ($ownedObject.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
                            $type = 'Serviceprincipal'
                        }
                        if ($ownedObject.'@odata.type' -eq '#microsoft.graph.application') {
                            $type = 'Application'
                        }
                        if ($ownedObject.'@odata.type' -eq '#microsoft.graph.group') {
                            $type = 'Group'
                        }
                        $htOptInfo = [ordered] @{
                            type = $type
                        }

                        if ($type -eq 'Serviceprincipal') {
                            $htOptInfo.typeDetailed = $htSpLookup[$ownedObject.id].objectTypeConcatinated
                        }
                        $htOptInfo.displayName = $ownedObject.displayName
                        $htOptInfo.objectId = $ownedObject.id
                        $null = $arrayServicePrincipalOwnedObjectsOpt.Add($htOptInfo)
                        #Write-Host "SP OwnedObjects             : $($type) $($ownedObject.displayName) ($($ownedObject.id))"
                    }
                    $durationPerfTrackServicePrincipalOwnedObjects = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })
                }
                #endregion ServicePrincipalOwnedObjects

                #region ServicePrincipalOwners
                $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
                $arrayServicePrincipalOwnerOpt = [System.Collections.ArrayList]@()
                if ($htSPOwnersFinal[$spId]) {
                    foreach ($servicePrincipalOwner in $htSPOwnersFinal[$spId]) {
                        $htOptInfo = [ordered] @{
                            id = $servicePrincipalOwner.id
                            displayName = $servicePrincipalOwner.displayName
                            principalType = $servicePrincipalOwner.principalType
                            applicability = $servicePrincipalOwner.applicability
                        }

                        $arrayOwnedBy = @()

                        foreach ($owner in $servicePrincipalOwner.ownedBy) {
                            if ($owner -ne 'noOwner') {
                                if ($htSPOwnersFinal[$owner.id]) {
                                    $arrayOwnedBy += ($htSPOwnersFinal[$owner.id])
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
                    $durationPerfTrackServicePrincipalOwners = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })
                }
                #endregion ServicePrincipalOwners

                #region ServicePrincipalAADRoleAssignments
                $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
                $arrayServicePrincipalAADRoleAssignmentsOpt = [System.Collections.ArrayList]@()
                #direct assignments, then the ones through groups (roleAssignmentApplicability/roleAssignmentAppliesThrough as for Azure RBAC)
                $aadRoleAssignments4Opt = @(@($object.ServicePrincipalAADRoleAssignments).where({ $_ }) | ForEach-Object { [PSCustomObject]@{ groupId = $null; assignment = $_ } }) + @(@($object.ServicePrincipalAADRoleAssignmentsThroughGroups).where({ $_ }))
                if ($aadRoleAssignments4Opt.Count -gt 0) {
                    foreach ($aadRoleAssignment4Opt in $aadRoleAssignments4Opt) {
                        $servicePrincipalAADRoleAssignment = $aadRoleAssignment4Opt.assignment
                        $hlper = $htAadRoleDefinitions[$servicePrincipalAADRoleAssignment.roleDefinitionId]
                        if ($hlper.isBuiltIn) {
                            $roleType = 'BuiltIn'
                        }
                        else {
                            $roleType = 'Custom'
                        }

                        #critical: Microsoft marks the role as privileged, or it is one of -CriticalAADRoles (before: only -CriticalAADRoles)
                        $aadRoleIsPrivileged = $hlper.isPrivileged -eq $true
                        $aadRoleIsCritical = $aadRoleIsPrivileged -or ($CriticalAADRoles -contains $servicePrincipalAADRoleAssignment.roleDefinitionId)
                        if ($aadRoleAssignment4Opt.groupId) {
                            $aadRoleAssignmentApplicability = 'indirect'
                            $aadRoleAssignmentGroupName = if ($htAaDGroups[$aadRoleAssignment4Opt.groupId]) { $htAaDGroups[$aadRoleAssignment4Opt.groupId].groupDetails.displayName } else { '<n/a>' }
                            $aadRoleAssignmentAppliesThrough = "$($aadRoleAssignmentGroupName) ($($aadRoleAssignment4Opt.groupId))"
                        }
                        else {
                            $aadRoleAssignmentApplicability = 'direct'
                            $aadRoleAssignmentAppliesThrough = ''
                        }

                        $htOptInfo = [ordered] @{
                            id = $servicePrincipalAADRoleAssignment.id
                            roleDefinitionId = $servicePrincipalAADRoleAssignment.roleDefinitionId
                            roleDefinitionName = $hlper.displayName
                            roleDefinitionDescription = $hlper.description
                            roleType = $roleType
                            roleIsCritical = $aadRoleIsCritical
                            roleIsPrivileged = $aadRoleIsPrivileged
                            directoryScopeId = $servicePrincipalAADRoleAssignment.directoryScopeId
                            resourceScope = $servicePrincipalAADRoleAssignment.resourceScope
                            roleAssignmentApplicability = $aadRoleAssignmentApplicability
                            roleAssignmentAppliesThrough = $aadRoleAssignmentAppliesThrough
                        }

                        if ($servicePrincipalAADRoleAssignment.resourceScope -ne '/') {
                            if ($htSPandAPPHelper4AADRoleAssignmentsWithScope[$servicePrincipalAADRoleAssignment.resourceScope -replace '/']) {
                                $htOptInfo.scopeDetail = $htSPandAPPHelper4AADRoleAssignmentsWithScope[$servicePrincipalAADRoleAssignment.resourceScope -replace '/']
                            }
                        }
                        $null = $arrayServicePrincipalAADRoleAssignmentsOpt.Add($htOptInfo)
                    }
                    $durationPerfTrackServicePrincipalAADRoleAssignments = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })
                }
                #endregion ServicePrincipalAADRoleAssignments

                #region ServicePrincipalAADRoleAssignedOn
                $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
                $arrayServicePrincipalAADRoleAssignedOnOpt = [System.Collections.ArrayList]@()
                if ($object.ServicePrincipalAADRoleAssignedOn) {
                    foreach ($aadRoleAssignedOn in $object.ServicePrincipalAADRoleAssignedOn | Sort-Object -Property roleDefinitionId, id) {
                        $hlperAaDRoleDefinition = $htAadRoleDefinitions[$aadRoleAssignedOn.roleDefinitionId]

                        $htOptInfo = [ordered] @{
                            id = $aadRoleAssignedOn.id
                            roleName = $hlperAaDRoleDefinition.displayName
                            roleId = $aadRoleAssignedOn.roleDefinitionId
                            roleDescription = $hlperAaDRoleDefinition.description
                            principalId = $aadRoleAssignedOn.principalId
                            principalDisplayName = $aadRoleAssignedOn.principalDisplayName
                        }

                        if ($aadRoleAssignedOn.principalType -eq 'User') {
                            $htOptInfo.principalType = $aadRoleAssignedOn.principalUserType
                        }
                        elseif ($aadRoleAssignedOn.principalType -eq 'ServicePrincipal') {
                            $hlperSP = $htSpLookup[$aadRoleAssignedOn.principalId]
                            $htOptInfo.principalType = $hlperSP.objectTypeConcatinated
                        }
                        else {
                            $htOptInfo.principalType = $aadRoleAssignedOn.principalType
                        }

                        $null = $arrayServicePrincipalAADRoleAssignedOnOpt.Add($htOptInfo)
                    }
                    $durationPerfTrackServicePrincipalAADRoleAssignedOn = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })
                }
                #endregion ServicePrincipalAADRoleAssignedOn

                #region ServicePrincipalOauth2PermissionGrants
                $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
                $arrayServicePrincipalOauth2PermissionGrantsOpt = [System.Collections.ArrayList]@()
                if ($object.ServicePrincipalOauth2PermissionGrants) {
                    foreach ($servicePrincipalOauth2PermissionGrant in $object.ServicePrincipalOauth2PermissionGrants | Sort-Object -Property resourceId) {
                        if (-not [string]::IsNullOrEmpty($servicePrincipalOauth2PermissionGrant.scope)) {
                            $multipleScopes = ($servicePrincipalOauth2PermissionGrant.scope).split(' ')
                            foreach ($scope in $multipleScopes | Sort-Object) {
                                if (-not [string]::IsNullOrEmpty($scope) -and -not [string]::IsNullOrWhiteSpace($scope)) {
                                    $hlperServicePrincipalsPublishedPermissionScope = $htServicePrincipalsPublishedPermissionScopes[$servicePrincipalOauth2PermissionGrant.resourceId].spdetails
                                    #guard: the resource SP may have no published scopes ($null[...] throws, $null.(...) did not)
                                    $hlperPublishedPermissionScope = $null
                                    if ($htPublishedPermissionScopes[$servicePrincipalOauth2PermissionGrant.resourceId]) {
                                        $hlperPublishedPermissionScope = $htPublishedPermissionScopes[$servicePrincipalOauth2PermissionGrant.resourceId][$scope]
                                    }

                                    $oauth2PermissionSensitivity = 'unclassified'
                                    $oauth2PermissionSensitivity = getClassification -permission $hlperPublishedPermissionScope.value -permissionType 'oauth2Permissions'

                                    $htOptInfo = [ordered] @{
                                        SPId = $hlperServicePrincipalsPublishedPermissionScope.id
                                        SPAppId = $hlperServicePrincipalsPublishedPermissionScope.appId
                                        SPDisplayName = $hlperServicePrincipalsPublishedPermissionScope.displayName
                                        scope = $scope
                                        permission = $hlperPublishedPermissionScope.value
                                        permissionSensitivity = $oauth2PermissionSensitivity
                                        id = $hlperPublishedPermissionScope.id
                                        type = $hlperPublishedPermissionScope.type
                                        adminConsentDisplayName = $hlperPublishedPermissionScope.adminConsentDisplayName
                                        adminConsentDescription = $hlperPublishedPermissionScope.adminConsentDescription
                                        userConsentDisplayName = $hlperPublishedPermissionScope.userConsentDisplayName
                                        userConsentDescription = $hlperPublishedPermissionScope.userConsentDescription
                                        #the grant itself (review R-1): 'type' above is the type of the published scope (who may consent: Admin/User),
                                        #consentType is how this grant was given: AllPrincipals (for all users) or Principal (for the user principalId)
                                        consentType = $servicePrincipalOauth2PermissionGrant.consentType
                                        principalId = $servicePrincipalOauth2PermissionGrant.principalId
                                        grantId = $servicePrincipalOauth2PermissionGrant.id
                                    }
                                    $null = $arrayServicePrincipalOauth2PermissionGrantsOpt.Add($htOptInfo)
                                }
                            }
                        }
                    }
                    $durationPerfTrackServicePrincipalOauth2PermissionGrants = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })
                }
                #endregion ServicePrincipalOauth2PermissionGrants

                #region SPOauth2PermissionGrantedTo
                $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
                $arraySPOauth2PermissionGrantedTo = [System.Collections.ArrayList]@()
                if ($htSPOauth2PermissionGrantedTo[$spId]) {
                    foreach ($SPOauth2PermissionGrantedTo in $htSPOauth2PermissionGrantedTo[$spId] <#| Sort-Object -Property clientId, id#>) {
                        foreach ($SPOauth2PermissionGrantedToScope in $SPOauth2PermissionGrantedTo.scope <#| Sort-Object#>) {
                            $hlper = $htSpLookup[$SPOauth2PermissionGrantedTo.clientId]
                            if (-not $hlper.spId -and $htAllServicePrincipalsById -and $htAllServicePrincipalsById[[string]$SPOauth2PermissionGrantedTo.clientId]) {
                                #review F-22: a client that was not processed (-OnlyProcessSPs...)
                                $grantClient = $htAllServicePrincipalsById[[string]$SPOauth2PermissionGrantedTo.clientId]
                                $hlper = @{ spDisplayName = $grantClient.displayName; spId = $grantClient.id; spAppId = $grantClient.appId }
                            }
                            $htOptInfo = [ordered] @{
                                servicePrincipalDisplayName = $hlper.spDisplayName
                                servicePrincipalObjectId = $hlper.spId
                                servicePrincipalAppId = $hlper.spAppId
                                applicationDisplayName = $hlper.appDisplayName
                                applicationObjectId = $hlper.appId
                                applicationAppId = $hlper.appAppId
                                clientId = $SPOauth2PermissionGrantedTo.clientId
                                id = $SPOauth2PermissionGrantedTo.id
                                #guard: the resource SP may have no published scopes ($null[...] throws, $null.(...) did not)
                                permissionId = $(if ($htPublishedPermissionScopes[$SPOauth2PermissionGrantedTo.resourceId]) { $htPublishedPermissionScopes[$SPOauth2PermissionGrantedTo.resourceId][$SPOauth2PermissionGrantedTo.scope].id })
                                scope = $SPOauth2PermissionGrantedToScope
                                consentType = $SPOauth2PermissionGrantedTo.consentType
                                startTime = $SPOauth2PermissionGrantedTo.startTime
                                expiryTime = $SPOauth2PermissionGrantedTo.expiryTime
                            }

                            $null = $arraySPOauth2PermissionGrantedTo.Add($htOptInfo)
                        }
                    }
                    $durationPerfTrackSPOauth2PermissionGrantedTo = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })
                }
                #endregion SPOauth2PermissionGrantedTo

                #region ServicePrincipalAppRoleAssignments
                $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
                $arrayServicePrincipalAppRoleAssignmentsOpt = [System.Collections.ArrayList]@()
                if ($object.ServicePrincipalAppRoleAssignments) {
                    foreach ($servicePrincipalAppRoleAssignment in $object.ServicePrincipalAppRoleAssignments) {
                        $hlper = & $getResourceAppRole $servicePrincipalAppRoleAssignment.resourceId $servicePrincipalAppRoleAssignment.appRoleId

                        #Critical permissions
                        #https://m365internals.com/2021/07/24/everything-about-service-principals-applications-and-api-permissions/ -> What applications are considered critical?
                        #https://www.youtube.com/watch?v=T-ZnAUt1IP8 - Monitoring and Incident Response in Azure AD
                        #https://docs.microsoft.com/en-us/security/compass/incident-response-playbook-app-consent#classifying-risky-permissions
                        $appRolePermissionSensitivity = 'unclassified'
                        $appRolePermissionSensitivity = getClassification -permission $hlper.value -permissionType 'appRolePermissions'

                        $htOptInfo = [ordered] @{
                            AppRoleAssignmentId = $servicePrincipalAppRoleAssignment.id
                            AppRoleAssignmentResourceId = $servicePrincipalAppRoleAssignment.resourceId
                            AppRoleAssignmentResourceDisplayName = $servicePrincipalAppRoleAssignment.resourceDisplayName
                            AppRoleAssignmentCreatedDateTime = $servicePrincipalAppRoleAssignment.createdDateTime
                            #review F-40: the assignment's own app role id when the role is not found (default access, deleted role)
                            AppRoleId = $(if ($hlper) { $hlper.id } else { $servicePrincipalAppRoleAssignment.appRoleId })
                            AppRoleAllowedMemberTypes = $hlper.allowedMemberTypes
                            AppRoleOrigin = $hlper.origin
                            AppRolePermission = $hlper.value
                            AppRolePermissionSensitivity = $appRolePermissionSensitivity
                            AppRoleDisplayName = $hlper.displayName
                            AppRoleDescription = $hlper.description
                        }

                        $null = $arrayServicePrincipalAppRoleAssignmentsOpt.Add($htOptInfo)
                    }
                    $durationPerfTrackServicePrincipalAppRoleAssignments = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })
                }
                #endregion ServicePrincipalAppRoleAssignments

                #region ServicePrincipalAppRoleAssignedTo
                $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
                $arrayServicePrincipalAppRoleAssignedToOpt = [System.Collections.ArrayList]@()
                if ($object.ServicePrincipalAppRoleAssignedTo) {

                    foreach ($servicePrincipalAppRoleAssignedTo in $object.ServicePrincipalAppRoleAssignedTo) {

                        $htOptInfo = [ordered] @{
                            principalDisplayName = $servicePrincipalAppRoleAssignedTo.principalDisplayName
                            principalId = $servicePrincipalAppRoleAssignedTo.principalId
                        }

                        if ($servicePrincipalAppRoleAssignedTo.principalType -eq 'User') {
                            if ($htPrincipalsResolved[$servicePrincipalAppRoleAssignedTo.principalId]) {
                                $htOptInfo.principalType = $htPrincipalsResolved[$servicePrincipalAppRoleAssignedTo.principalId].typeOnly
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
                        if ($htAppRoleAssignments["$($servicePrincipalAppRoleAssignedTo.id)"]) {
                            $hlper = & $getResourceAppRole $servicePrincipalAppRoleAssignedTo.resourceId $htAppRoleAssignments["$($servicePrincipalAppRoleAssignedTo.id)"].appRoleId
                            $htOptInfo.roleId = $hlper.id
                            $htOptInfo.roleOrigin = $hlper.origin
                            $htOptInfo.roleAllowedMemberTypes = $hlper.allowedMemberTypes
                            $htOptInfo.roleDisplayName = $hlper.displayName
                            $htOptInfo.roleDescription = $hlper.description
                            $htOptInfo.roleValue = $hlper.value
                        }
                        else {
                            if ($servicePrincipalAppRoleAssignedTo.principalType -eq 'User' -or $servicePrincipalAppRoleAssignedTo.principalType -eq 'Group') {
                                #appRoleAssignedTo returns the full appRoleAssignment incl. appRoleId (verified on all rows of a real run:
                                #identical to the per-user/per-group appRoleAssignments lookup that was used here before)
                                $appRoleId = $servicePrincipalAppRoleAssignedTo.appRoleId
                                if ($appRoleId) {
                                    $hlper = & $getResourceAppRole $servicePrincipalAppRoleAssignedTo.resourceId $appRoleId
                                    if ($hlper) {
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
                    if ($arrayServicePrincipalAppRoleAssignedToOpt.Count -gt 0) {
                        #@(): Sort-Object returns a single item as scalar, which changed the JSON shape and made .Count return the item's key count
                        $arrayServicePrincipalAppRoleAssignedToOpt = @($arrayServicePrincipalAppRoleAssignedToOpt | Sort-Object -Property { $_.id })
                    }
                    $durationPerfTrackServicePrincipalAppRoleAssignedTo = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })
                }
                #endregion ServicePrincipalAppRoleAssignedTo


                if (-not $NoAzureResourceSideRelations) {

                    $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
                    $htSPAzureRoleAssignments = @{}

                    #region AzureRoleAssignmentsPrep
                    $arrayServicePrincipalGroupMembershipsOpt = [System.Collections.ArrayList]@()
                    if ($object.ServicePrincipalGroupMemberships) {

                        foreach ($servicePrincipalGroupMembership in $object.ServicePrincipalGroupMemberships | Sort-Object) {
                            $htOptInfo = [ordered] @{}
                            if ($htAaDGroups[$servicePrincipalGroupMembership]) {
                                $htOptInfo.DisplayName = $htAaDGroups[$servicePrincipalGroupMembership].groupDetails.displayName
                            }
                            else {
                                $htOptInfo.DisplayName = '<n/a>'
                            }
                            $htOptInfo.ObjectId = $servicePrincipalGroupMembership
                            $null = $arrayServicePrincipalGroupMembershipsOpt.Add($htOptInfo)

                            if ($htAssignmentsByPrincipalIdGroups[$servicePrincipalGroupMembership]) {
                                foreach ($roleAssignmentSPThroughGroup in $htAssignmentsByPrincipalIdGroups[$servicePrincipalGroupMembership]) {
                                    $roleAssignmentSPThroughGroup_assignment_id = $roleAssignmentSPThroughGroup.assignment.id
                                    if (-not $htSPAzureRoleAssignments[$roleAssignmentSPThroughGroup_assignment_id]) {
                                        $htSPAzureRoleAssignments[$roleAssignmentSPThroughGroup_assignment_id] = @{
                                            results = [System.Collections.ArrayList]@()
                                        }
                                    }
                                    $htTemp = @{
                                        roleAssignment = $roleAssignmentSPThroughGroup_assignment_id
                                        roleAssignmentFull = $roleAssignmentSPThroughGroup
                                        appliesThrough = "$($htAaDGroups[$servicePrincipalGroupMembership].groupDetails.displayName) ($servicePrincipalGroupMembership)"
                                        applicability = 'indirect'
                                    }

                                    $null = ($htSPAzureRoleAssignments[$roleAssignmentSPThroughGroup_assignment_id].results).Add($htTemp)
                                }
                            }
                        }
                        $durationPerfTrackAzureRoleAssignmentsPrep = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })
                    }
                    #endregion AzureRoleAssignmentsPrep

                    #region AzureRoleAssignmentsOpt
                    $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
                    if ($htAssignmentsByPrincipalIdServicePrincipals[$spId]) {
                        foreach ($roleAssignmentSP in $htAssignmentsByPrincipalIdServicePrincipals[$spId]) {
                            $roleAssignmentSP_assignment_id = $roleAssignmentSP.assignment.id
                            if (-not $htSPAzureRoleAssignments[$roleAssignmentSP_assignment_id]) {
                                $htSPAzureRoleAssignments[$roleAssignmentSP_assignment_id] = @{
                                    results = [System.Collections.ArrayList]@()
                                }
                            }
                            $htTemp = @{
                                roleAssignment = $roleAssignmentSP_assignment_id
                                roleAssignmentFull = $roleAssignmentSP
                                appliesThrough = ''
                                applicability = 'direct'
                            }

                            $null = ($htSPAzureRoleAssignments[$roleAssignmentSP_assignment_id].results).Add($htTemp)
                        }
                        $durationPerfTrackAzureRoleAssignmentsOpt1 = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })
                    }

                    $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
                    $arrayServicePrincipalAzureRoleAssignmentsOpt = [System.Collections.ArrayList]@()
                    if ($htSPAzureRoleAssignments.Keys.Count -gt 0) {
                        foreach ($roleAssignment in $htSPAzureRoleAssignments.Values.results | Sort-Object { $_.roleAssignment }) {

                            $hlproleAssignmentFull = $roleAssignment.roleAssignmentFull


                            if ($hlproleAssignmentFull.assignmentPIMDetails) {
                                $pimBased = $true
                            }
                            else {
                                $pimBased = $false
                            }

                            $optHlproleAssignmentFull = $hlproleAssignmentFull.assignment.properties | Select-Object -ExcludeProperty principalType, condition, conditionVersion, delegatedManagedIdentityResourceId, description
                            # if (-not $optHlproleAssignmentFull.condition) {
                            #     $optHlproleAssignmentFull = $optHlproleAssignmentFull | Select-Object -ExcludeProperty condition, conditionVersion
                            # }
                            # if (-not $optHlproleAssignmentFull.delegatedManagedIdentityResourceId) {
                            #     $optHlproleAssignmentFull = $optHlproleAssignmentFull | Select-Object -ExcludeProperty delegatedManagedIdentityResourceId
                            # }
                            # if (-not $optHlproleAssignmentFull.description) {
                            #     $optHlproleAssignmentFull = $optHlproleAssignmentFull | Select-Object -ExcludeProperty description
                            # }

                            $htOptInfo = [ordered] @{
                                priviledgedIdentityManagementBased = $pimBased
                                roleAssignmentId = $roleAssignment.roleAssignment
                                roleIsCritical = $hlproleAssignmentFull.roleIsCritical
                                #Azure role conditions (added): 'limited' / 'allows critical' / 'not evaluated' / '', and the conditions
                                roleConditionEffect = [string]$hlproleAssignmentFull.roleConditionEffect
                                roleConditionSources = [string]$hlproleAssignmentFull.roleConditionSources
                                roleConditionAssignableRoles = [string]$hlproleAssignmentFull.roleConditionAssignableRoles
                                roleConditionCriticalRoles = [string]$hlproleAssignmentFull.roleConditionCriticalRoles
                                roleDefinitionCondition = [string]$hlproleAssignmentFull.roleDefinitionCondition
                                roleAssignmentCondition = [string]$hlproleAssignmentFull.roleAssignmentCondition
                                roleName = $hlproleAssignmentFull.roleName
                                roleId = $hlproleAssignmentFull.roleId
                                roleType = $hlproleAssignmentFull.type
                                roleAssignmentApplicability = $roleAssignment.applicability
                                roleAssignmentAppliesThrough = $roleAssignment.appliesThrough
                                roleAssignmentAssignmentScope = $hlproleAssignmentFull.assignmentScope
                                roleAssignmentAssignmentScopeId = $hlproleAssignmentFull.assignmentScopeId
                                roleAssignmentAssignmentScopeName = $hlproleAssignmentFull.assignmentScopeName
                                roleAssignmentAssignmentResourceName = $hlproleAssignmentFull.assignmentResourceName
                                roleAssignmentAssignmentResourceType = $hlproleAssignmentFull.assignmentResourceType
                                roleAssignment = $optHlproleAssignmentFull
                            }

                            if ($pimBased) {
                                $hlproleAssignmentFullAssignmentPIMDetails = $hlproleAssignmentFull.assignmentPIMDetails
                                $htOptInfo.priviledgedIdentityManagement = [ordered] @{
                                    assignmentType = $hlproleAssignmentFullAssignmentPIMDetails.assignmentType
                                    startDateTime = $hlproleAssignmentFullAssignmentPIMDetails.startDateTime
                                    endDateTime = $hlproleAssignmentFullAssignmentPIMDetails.endDateTime
                                    createdOn = $hlproleAssignmentFullAssignmentPIMDetails.createdOn
                                    updatedOn = $hlproleAssignmentFullAssignmentPIMDetails.updatedOn
                                }
                            }
                            $null = $arrayServicePrincipalAzureRoleAssignmentsOpt.Add($htOptInfo)
                        }
                        $durationPerfTrackAzureRoleAssignmentsOpt2 = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })
                    }
                    #endregion AzureRoleAssignmentsOpt
                }
                else {
                    $arrayServicePrincipalAzureRoleAssignmentsOpt = $null

                    $arrayServicePrincipalGroupMembershipsOpt = [System.Collections.ArrayList]@()
                    if ($object.ServicePrincipalGroupMemberships) {
                        foreach ($servicePrincipalGroupMembership in $object.ServicePrincipalGroupMemberships | Sort-Object) {

                            if ($htAaDGroups[$servicePrincipalGroupMembership]) {
                                #Write-Host "SP GroupMembership      :" $htAaDGroups.($servicePrincipalGroupMembership).groupDetails.displayName "($($servicePrincipalGroupMembership))"
                                $htOptInfo = [ordered] @{
                                    DisplayName = $htAaDGroups[$servicePrincipalGroupMembership].groupDetails.displayName
                                    ObjectId = $servicePrincipalGroupMembership
                                }
                            }
                            else {
                                #Write-Host "SP GroupMembership      :" "notResolved" "($($servicePrincipalGroupMembership))"
                                $htOptInfo = [ordered] @{
                                    DisplayName = '<n/a>'
                                    ObjectId = $servicePrincipalGroupMembership
                                }
                            }
                            $null = $arrayServicePrincipalGroupMembershipsOpt.Add($htOptInfo)
                        }
                    }
                }

                #region ManagedIdentity
                $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
                $arrayManagedIdentityOpt = [System.Collections.ArrayList]@()
                #flat arrays with 0..n items (JSON: [] / [{..}] / [{..},{..}]); they used to be nested in an extra list
                $arrayManagedIdentityFederatedIdentityCredentialOpt = @()
                $arrayManagedIdentityAssociatedResourcesOpt = @()
                if ($object.ManagedIdentity) {

                    #reset per object, otherwise an MI without a resource id inherits the values of the previous MI in this batch.
                    #Only the resource id is relevant, not 'isExplicit=<bool>' (which can be the last alternativeName)
                    $miResourceId = $null
                    $resourceType = $null
                    $miResourceScope = $null
                    $relict = $false
                    foreach ($altName in @($object.ManagedIdentity.ManagedIdentityAlternativeNames).where( { $_ -notlike 'isExplicit=*' } )) {

                        $miResourceId = $altName
                        $relict = $false
                        if ($altName -notlike 'isExplicit=*') {
                            $s1 = $altName -replace '.*/providers/'
                            #review R-30: '<namespace>/<type>/<name>[/<childType>/<childName>...]' -> namespace and the types
                            #(e.g. Microsoft.Web/sites/slots); removing the last segment gave 'Microsoft.Web/sites/<app>/slots' for a slot
                            #and removed every segment equal to the name (a web app named 'sites' gave 'Microsoft.Web')
                            $resourceTypeSegments = $s1.Split('/')
                            $resourceType = @($resourceTypeSegments[0]; for ($segmentIndex = 1; $segmentIndex -lt $resourceTypeSegments.Count; $segmentIndex += 2) { $resourceTypeSegments[$segmentIndex] }) -join '/'

                            $altNameSplit = $altName.split('/')
                            if ($altName -like '/subscriptions/*') {
                                if ($resourceType -eq 'Microsoft.Authorization/policyAssignments') {
                                    #review F-3: not when the subscription was not collected in this run (excluded, failed)
                                    if ($runTenantRoot -and -not $policyAssignmentScopesNotCollected.Contains("sub:$($altNameSplit[2])")) {
                                        if (-not $NoAzureResourceSideRelations) {
                                            if (-not $htCacheAssignmentsPolicy[$altname.ToLower()]) {
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
                                    #review F-3: not when the management group was not collected in this run
                                    if ($runTenantRoot -and -not $policyAssignmentScopesNotCollected.Contains("mg:$($altNameSplit[4])")) {
                                        if (-not $NoAzureResourceSideRelations) {
                                            if (-not $htCacheAssignmentsPolicy[$altname.ToLower()]) {
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
                                Write-Host "Relict (MI PolicyAssignment) found: Name/ObjectId:'$($object.ServicePrincipalDetails.displayName -replace '##', '# #' -replace '[\r\n]+', ' ')/$($object.ServicePrincipalDetails.id)' - $altName"
                            }
                        }
                    }

                    $htOptInfo = [ordered]@{}

                    if ($object.subtype -eq 'User assigned') {
                        $azureResourcesAssociated = $htUserAssignedIdentities4ResourcesByMI[$spId]
                        if ($azureResourcesAssociated.Count -gt 0) {
                            $arrayManagedIdentityAssociatedResourcesOpt = @($azureResourcesAssociated | Select-Object -Property resource* | Sort-Object -Property resourceId)
                        }

                        if ($miResourceId -and $htMIUserAssignedFederatedCreds[$miResourceId]) {
                            $arrayManagedIdentityFederatedIdentityCredentialOpt = @($htMIUserAssignedFederatedCreds[$miResourceId].federatedCredentials | Select-Object -ExcludeProperty miResourceId)
                        }
                    }

                    $htOptInfo.type = $object.subtype
                    $htOptInfo.alternativeName = $miResourceId
                    $htOptInfo.resourceType = $resourceType
                    $htOptInfo.resourceScope = $miResourceScope
                    $htOptInfo.relict = $relict
                    if ($object.subtype -eq 'User assigned' -and $miResourceId -and $htUserAssignedIdentitiesSettings[$miResourceId.ToLower()]) {
                        $uamiSettings = $htUserAssignedIdentitiesSettings[$miResourceId.ToLower()]
                        $htOptInfo.isolationScope = $uamiSettings.isolationScope
                        #$null: not returned by the api-version (not collected); empty array: unrestricted
                        $htOptInfo.assignmentRestrictions = $null
                        if ($uamiSettings.assignmentRestrictionsCollected) {
                            $htOptInfo.assignmentRestrictions = [string[]]@($uamiSettings.assignmentRestrictions)
                        }
                    }
                    $null = $arrayManagedIdentityOpt.Add($htOptInfo)
                    $durationPerfTrackManagedIdentity = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })
                }
                #endregion ManagedIdentity
            }

            #region Application
            if ($object.Application) {
                #Write-host "SP type:                : Application - objId: $($object.Application.ApplicationDetails.id) appId: $($object.Application.ApplicationDetails.appId)"

                #region ApplicationAADRoleAssignedOn
                $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
                $arrayApplicationAADRoleAssignedOnOpt = [System.Collections.ArrayList]@()
                if ($object.Application.ApplicationAADRoleAssignedOn) {
                    foreach ($aadRoleAssignedOn in $object.Application.ApplicationAADRoleAssignedOn | Sort-Object -Property roleDefinitionId, id) {
                        $hlperAadRoleDefinition = $htAadRoleDefinitions[$aadRoleAssignedOn.roleDefinitionId]

                        $htOptInfo = [ordered] @{
                            id = $aadRoleAssignedOn.id
                            roleName = $hlperAadRoleDefinition.displayName
                            roleId = $aadRoleAssignedOn.roleDefinitionId
                            roleDescription = $hlperAadRoleDefinition.description
                            principalId = $aadRoleAssignedOn.principalId
                            principalDisplayName = $aadRoleAssignedOn.principalDisplayName
                        }

                        if ($aadRoleAssignedOn.principalType -eq 'User') {
                            $htOptInfo.principalType = $aadRoleAssignedOn.principalUserType
                        }
                        elseif ($aadRoleAssignedOn.principalType -eq 'ServicePrincipal') {
                            $hlperSP = $htSpLookup[$aadRoleAssignedOn.principalId]
                            $htOptInfo.principalType = $hlperSP.objectTypeConcatinated
                        }
                        else {
                            $htOptInfo.principalType = $aadRoleAssignedOn.principalType
                        }
                        $null = $arrayApplicationAADRoleAssignedOnOpt.Add($htOptInfo)
                    }
                    $durationPerfTrackApplicationAADRoleAssignedOn = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })
                }
                #endregion ApplicationAADRoleAssignedOn

                #region ApplicationOwner
                $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
                $arrayApplicationOwnerOpt = [System.Collections.ArrayList]@()
                if ($htAppOwners[$object.Application.ApplicationDetails.id]) {
                    foreach ($appOwner in $htAppOwners[$object.Application.ApplicationDetails.id]) {
                        $arrayApplicationOwner = [System.Collections.ArrayList]@()
                        if ($htSPOwnersFinal[$appOwner.id]) {

                            foreach ($servicePrincipalOwner in $htSPOwnersFinal[$appOwner.id]) {
                                $htOptInfo = [ordered] @{
                                    id = $servicePrincipalOwner.id
                                    displayName = $servicePrincipalOwner.displayName
                                    principalType = $servicePrincipalOwner.principalType
                                    applicability = $servicePrincipalOwner.applicability
                                }

                                $arrayOwnedBy = @()

                                foreach ($owner in $servicePrincipalOwner.ownedBy) {
                                    if ($owner -ne 'noOwner') {
                                        if ($htSPOwnersFinal[$owner.id]) {
                                            $arrayOwnedBy += ($htSPOwnersFinal[$owner.id])
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
                        $htOptInfo = [ordered] @{
                            id = $appOwner.id
                            displayName = $appOwner.displayName
                        }

                        if ($appOwner.'@odata.type' -eq '#microsoft.graph.servicePrincipal') {
                            $htOptInfo.principalType = $htSpLookup[$appOwner.id].objectTypeConcatinated
                        }
                        if ($appOwner.'@odata.type' -eq '#microsoft.graph.user') {
                            $htOptInfo.principalType = $htPrincipalsResolved[$appOwner.id].typeOnly
                        }
                        $htOptInfo.applicability = 'direct'
                        if ($appOwner.'@odata.type' -ne '#microsoft.graph.user') {
                            $htOptInfo.ownedBy = $arrayApplicationOwner
                        }
                        $null = $arrayApplicationOwnerOpt.Add($htOptInfo)
                    }
                    $durationPerfTrackApplicationOwner = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })
                }
                #endregion ApplicationOwner

                #region ApplicationFederatedIdentityCredentials
                $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
                $arrayFederatedIdentityCredentialsOpt = [System.Collections.ArrayList]@()
                if ($htFederatedIdentityCredentials[$object.Application.ApplicationDetails.id]) {

                    foreach ($federatedIdentityCredential in $htFederatedIdentityCredentials[$object.Application.ApplicationDetails.id]) {
                        $htOptInfo = [ordered] @{
                            name = $federatedIdentityCredential.name
                            description = $federatedIdentityCredential.description
                            id = $federatedIdentityCredential.id
                            issuer = $federatedIdentityCredential.issuer
                            subject = $federatedIdentityCredential.subject
                            audiences = $federatedIdentityCredential.audiences
                            #review R-27 (added): flexible federated identity credentials (beta) - an expression instead of a subject
                            claimsMatchingExpression = $federatedIdentityCredential.claimsMatchingExpression
                        }

                        $null = $arrayFederatedIdentityCredentialsOpt.Add($htOptInfo)
                    }
                    $durationPerfTrackFederatedIdentityCredentials = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })
                }
                #endregion ApplicationFederatedIdentityCredentials

                #region ApplicationSecrets
                $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
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

                            #review F-25: a credential without an end date threw here and dropped the whole object (R-26 guarded the SP side)
                            $passwordCredentialExpiryTotalDays = $(if ($null -ne $hlperApplicationPasswordCredential.endDateTime) { (New-TimeSpan -Start $currentDateUTC -End $hlperApplicationPasswordCredential.endDateTime).TotalDays })
                            $expiryApplicationPasswordCredential = $(if ($null -ne $passwordCredentialExpiryTotalDays) { [math]::Round($passwordCredentialExpiryTotalDays, 0) })
                            if ($null -eq $passwordCredentialExpiryTotalDays) {
                                $expiryApplicationPasswordCredential = 'no end date'
                            }
                            elseif ($passwordCredentialExpiryTotalDays -lt 0) {
                                $expiryApplicationPasswordCredential = 'expired'
                            }
                            elseif ($passwordCredentialExpiryTotalDays -lt $ApplicationSecretExpiryWarning) {
                                $expiryApplicationPasswordCredential = "expires soon (less than grace period $ApplicationSecretExpiryWarning days)"
                            }
                            else {
                                if ($passwordCredentialExpiryTotalDays -gt $ApplicationSecretExpiryMax) {
                                    $expiryApplicationPasswordCredential = "expires in more than $ApplicationSecretExpiryMax days"
                                }
                                else {
                                    $expiryApplicationPasswordCredential = "expires in $ApplicationSecretExpiryWarning to $ApplicationSecretExpiryMax days"
                                }
                            }

                            $htOptInfo = [ordered] @{
                                keyId = $hlperApplicationPasswordCredential.keyId
                                displayName = $displayName
                                expiryInfo = $expiryApplicationPasswordCredential
                                endDateTime = $hlperApplicationPasswordCredential.endDateTime
                                endDateTimeFormated = $(if ($null -ne $hlperApplicationPasswordCredential.endDateTime) { ($hlperApplicationPasswordCredential.endDateTime).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss 'UTC'", [cultureinfo]::InvariantCulture) } else { '' })
                                startDateTime = $hlperApplicationPasswordCredential.startDateTime
                            }

                            $null = $arrayApplicationPasswordCredentialsOpt.Add($htOptInfo)
                        }
                    }
                    $durationPerfTrackApplicationSecrets = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })
                }
                #endregion ApplicationSecrets

                #region ApplicationCertificates
                $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
                $arrayApplicationKeyCredentialsOpt = [System.Collections.ArrayList]@()
                if ($object.Application.ApplicationKeyCredentials) {
                    $appKeyCredentialsCount = ($object.Application.ApplicationKeyCredentials).count
                    if ($appKeyCredentialsCount -gt 0) {

                        foreach ($appKeyCredential in $object.Application.ApplicationKeyCredentials.Values | Sort-Object -Property keyId) {
                            $hlperApplicationKeyCredential = $appKeyCredential

                            #review F-25: see the secrets above
                            $keyCredentialExpiryTotalDays = $(if ($null -ne $hlperApplicationKeyCredential.endDateTime) { (New-TimeSpan -Start $currentDateUTC -End $hlperApplicationKeyCredential.endDateTime).TotalDays })
                            $expiryApplicationKeyCredential = $(if ($null -ne $keyCredentialExpiryTotalDays) { [math]::Round($keyCredentialExpiryTotalDays, 0) })

                            if ($null -eq $keyCredentialExpiryTotalDays) {
                                $expiryApplicationKeyCredential = 'no end date'
                            }
                            elseif ($keyCredentialExpiryTotalDays -lt 0) {
                                $expiryApplicationKeyCredential = 'expired'
                            }
                            elseif ($keyCredentialExpiryTotalDays -lt $ApplicationCertificateExpiryWarning) {
                                $expiryApplicationKeyCredential = "expires soon (less than grace period $ApplicationCertificateExpiryWarning days)"
                            }
                            else {
                                if ($keyCredentialExpiryTotalDays -gt $ApplicationCertificateExpiryMax) {
                                    $expiryApplicationKeyCredential = "expires in more than $ApplicationCertificateExpiryMax days"
                                }
                                else {
                                    $expiryApplicationKeyCredential = "expires in $ApplicationCertificateExpiryWarning to $ApplicationCertificateExpiryMax days"
                                }
                            }

                            $htOptInfo = [ordered] @{
                                keyId = $hlperApplicationKeyCredential.keyId
                                displayName = $hlperApplicationKeyCredential.displayName
                                customKeyIdentifier = $hlperApplicationKeyCredential.customKeyIdentifier
                                expiryInfo = $expiryApplicationKeyCredential
                                endDateTime = $hlperApplicationKeyCredential.endDateTime
                                endDateTimeFormated = $(if ($null -ne $hlperApplicationKeyCredential.endDateTime) { $hlperApplicationKeyCredential.endDateTime.ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss 'UTC'", [cultureinfo]::InvariantCulture) } else { '' })
                                startDateTime = $hlperApplicationKeyCredential.startDateTime
                            }

                            $null = $arrayApplicationKeyCredentialsOpt.Add($htOptInfo)
                        }
                    }
                    $durationPerfTrackApplicationCertificates = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })
                }
                #endregion ApplicationCertificates
            }
            #endregion Application


            #region finalArray
            $start = $(if ($PerfTracking) { Get-Date })   #P-9: timing only with -PerfTracking
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
                        $arraySPOauth2PermissionGrantedTo = @($arraySPOauth2PermissionGrantedTo | Sort-Object { $_.servicePrincipalDisplayName }, { $_.scope }, { $_.permissionId }, { $_.consentType })
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
                    $arraySPOauth2PermissionGrantedTo = @($arraySPOauth2PermissionGrantedTo | Sort-Object { $_.servicePrincipalDisplayName }, { $_.scope }, { $_.permissionId }, { $_.consentType })
                }

                #one object, not an ArrayList holding the object (consistent with the other object types in $cu)
                $customObjectMi = [PSCustomObject]@{
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
                }

                if ($object.objectTypeConcatinated -eq 'SP MI User assigned') {
                    $customObjectMi | Add-Member -NotePropertyName ManagedIdentityAssociatedAzureResources -NotePropertyValue $arrayManagedIdentityAssociatedResourcesOpt
                    $customObjectMi | Add-Member -NotePropertyName ManagedIdentityFederatedIdentityCredentials -NotePropertyValue $arrayManagedIdentityFederatedIdentityCredentialOpt
                }

                $null = $script:cu.Add($customObjectMi)
            }
            else {
                #Write-Host "$($object.ServicePrincipalDetails.displayName) is neither App, nore MI"
                if ($arraySPOauth2PermissionGrantedTo.Count -gt 0) {
                    $arraySPOauth2PermissionGrantedTo = @($arraySPOauth2PermissionGrantedTo | Sort-Object { $_.servicePrincipalDisplayName }, { $_.scope }, { $_.permissionId }, { $_.consentType })
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
            $durationPerfTrackFinalArray = $(if ($PerfTracking) { [math]::Round((New-TimeSpan -Start $start -End (Get-Date)).TotalMilliseconds) })

            #endregion finalArray

            $processedServicePrincipalsCount++
            if ($PerfTracking) {
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
            }

            ($enrichmentProcessCounter).counter++
            if (($enrichmentProcessCounter).counter % $enrichmentProcessindicator -eq 0) {
                Write-Host "processed: $(($enrichmentProcessCounter).counter)"
            }

            #endregion parallel
        }
        catch {
            $failedItem = "$(if ($spOrAppWithoutSP.ServicePrincipalDetails) { "$($spOrAppWithoutSP.ServicePrincipalDetails.displayName -replace '##', '# #' -replace '[\r\n]+', ' ') ($($spOrAppWithoutSP.ServicePrincipalDetails.id))" } else { "$($spOrAppWithoutSP.Application.ApplicationDetails.displayName -replace '##', '# #' -replace '[\r\n]+', ' ') ($($spOrAppWithoutSP.Application.ApplicationDetails.id))" })"
            $htParallelFailures["ServicePrincipal enrichment|$failedItem"] = [PSCustomObject]@{ Phase = 'ServicePrincipal enrichment'; Item = $failedItem; Error = $_.Exception.Message; ObjectId = [string]$(if ($spOrAppWithoutSP.ServicePrincipalDetails) { $spOrAppWithoutSP.ServicePrincipalDetails.id } else { $spOrAppWithoutSP.Application.ApplicationDetails.id }) }
            Write-Host "  ServicePrincipal enrichment: '$failedItem' failed - skipped: $($_.Exception.Message)" -ForegroundColor Yellow
        }
    }


} -ThrottleLimit $enrichmentThrottleLimit
checkParallelFailures -phase 'ServicePrincipal enrichment' -itemCount ($servicePrincipalsAndAppsOnlyEnriched.Count)
Write-Host "Enrichment completed: $($servicePrincipalsAndAppsOnlyEnriched.Count) ServicePrincipals processed"
$endEnrichmentSP = Get-Date
$duration = New-TimeSpan -Start $startEnrichmentSP -End $endEnrichmentSP
Write-Host "Service Principals enrichment duration: $($duration.TotalMinutes) minutes ($($duration.TotalSeconds) seconds)"

#region insights data
#(review P-11: note properties via PSObject.Properties - the same as Add-Member -Force, a replaced property is added at the end, at
# about half the cost per call on the main thread) (from the collected raw objects; no API calls)
#raw objects by id (the output objects keep only selected fields)
$htRawSPById = @{}
$htRawAppById = @{}
foreach ($rawEntry in @($htServicePrincipalsAndAppsOnlyEnriched.Values)) {
    if ($rawEntry.ServicePrincipalDetails.id) {
        $htRawSPById[$rawEntry.ServicePrincipalDetails.id] = $rawEntry.ServicePrincipalDetails
    }
    if ($rawEntry.Application.ApplicationDetails.id) {
        $htRawAppById[$rawEntry.Application.ApplicationDetails.id] = $rawEntry.Application.ApplicationDetails
    }
}
foreach ($entry in $cu) {
    #credentials stored on the service principal itself (not on the app registration; not shown in the app's 'Certificates & secrets')
    $rawSP = if ($entry.SP) { $htRawSPById[@($entry.SP)[0].SPObjectId] }
    if ($rawSP) {
        #review F-30: the pipelines only for SPs with credentials (most have none)
        $spPasswordCredentials = @()
        $spKeyCredentials = @()
        if ($rawSP.passwordCredentials) {
            $spPasswordCredentials = @(foreach ($credential in @($rawSP.passwordCredentials).where({ $_ }) | Sort-Object -Property keyId) {
                    [ordered]@{
                        keyId = $credential.keyId
                        displayName = $(if ($credential.displayName) { $credential.displayName } else { 'notGiven' })
                        customKeyIdentifier = $credential.customKeyIdentifier
                        #review R-26: a credential without an end date must not stop the run here (after the whole collection)
                        expiryInfo = $(if ($credential.endDateTime) { getCredentialExpiryInfo -endDateTime $credential.endDateTime -warningDays $ApplicationSecretExpiryWarning -maxDays $ApplicationSecretExpiryMax } else { 'no end date' })
                        endDateTime = $credential.endDateTime
                        endDateTimeFormated = $(if ($credential.endDateTime) { ([datetime]$credential.endDateTime).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss 'UTC'", [cultureinfo]::InvariantCulture) } else { '' })
                        startDateTime = $credential.startDateTime
                    }
                })
        }
        if ($rawSP.keyCredentials) {
            $spKeyCredentials = @(foreach ($credential in @($rawSP.keyCredentials).where({ $_ }) | Sort-Object -Property keyId) {
                    [ordered]@{
                        keyId = $credential.keyId
                        displayName = $credential.displayName
                        customKeyIdentifier = $credential.customKeyIdentifier
                        type = $credential.type
                        usage = $credential.usage
                        expiryInfo = $(if ($credential.endDateTime) { getCredentialExpiryInfo -endDateTime $credential.endDateTime -warningDays $ApplicationCertificateExpiryWarning -maxDays $ApplicationCertificateExpiryMax } else { 'no end date' })
                        endDateTime = $credential.endDateTime
                        endDateTimeFormated = $(if ($credential.endDateTime) { ([datetime]$credential.endDateTime).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss 'UTC'", [cultureinfo]::InvariantCulture) } else { '' })
                        startDateTime = $credential.startDateTime
                    }
                })
        }
        $null = $entry.PSObject.Properties.Remove('SPPasswordCredentials'); $entry.PSObject.Properties.Add([psnoteproperty]::new('SPPasswordCredentials', $spPasswordCredentials))
        $null = $entry.PSObject.Properties.Remove('SPKeyCredentials'); $entry.PSObject.Properties.Add([psnoteproperty]::new('SPKeyCredentials', $spKeyCredentials))
    }

    #app configuration: redirect URIs per platform, implicit grant, public client flows
    $rawApp = if ($entry.APP) { $htRawAppById[@($entry.APP)[0].APPObjectId] }
    if ($rawApp) {
        $redirectUris = @(foreach ($platform in 'web', 'spa', 'publicClient') {
                foreach ($uri in @($rawApp.$platform.redirectUris).where({ $_ })) {
                    [ordered]@{ platform = $platform; uri = $uri; findings = @(getRedirectUriFindings -platform $platform -uri $uri) }
                }
            })
        $appObject = @($entry.APP)[0]
        $null = $appObject.PSObject.Properties.Remove('APPRedirectUris'); $appObject.PSObject.Properties.Add([psnoteproperty]::new('APPRedirectUris', $redirectUris))
        $null = $appObject.PSObject.Properties.Remove('APPIdentifierUris'); $appObject.PSObject.Properties.Add([psnoteproperty]::new('APPIdentifierUris', @($rawApp.identifierUris)))
        $null = $appObject.PSObject.Properties.Remove('APPImplicitGrantAccessTokenIssuance'); $appObject.PSObject.Properties.Add([psnoteproperty]::new('APPImplicitGrantAccessTokenIssuance', ($rawApp.web.implicitGrantSettings.enableAccessTokenIssuance -eq $true)))
        $null = $appObject.PSObject.Properties.Remove('APPImplicitGrantIdTokenIssuance'); $appObject.PSObject.Properties.Add([psnoteproperty]::new('APPImplicitGrantIdTokenIssuance', ($rawApp.web.implicitGrantSettings.enableIdTokenIssuance -eq $true)))
        $null = $appObject.PSObject.Properties.Remove('APPIsFallbackPublicClient'); $appObject.PSObject.Properties.Add([psnoteproperty]::new('APPIsFallbackPublicClient', ($rawApp.isFallbackPublicClient -eq $true)))
    }

    #federated identity credentials: who can sign in (issuer type, parsed subject)
    $fics = @(@($entry.APPFederatedIdentityCredentials).where({ $_ }))
    $ficTrusts = @(foreach ($fic in $fics) { getFederatedCredentialTrust -issuer $fic.issuer -subject $fic.subject -claimsMatchingExpression $fic.claimsMatchingExpression.value -signInAudience @($entry.APP)[0].APPSignInAudience -cloudEnvironment $azAPICallConf['checkContext'].Environment.Name })
    setAzureDevOpsConvertedFinding $ficTrusts
    for ($i = 0; $i -lt $fics.Count; $i++) {
        $fic = $fics[$i]; $ficTrust = $ficTrusts[$i]
        $fic.issuerType = $ficTrust.issuerType
        $fic.trust = $ficTrust.trust
        $fic.trustFinding = $ficTrust.finding
    }
    $fics = @(@($entry.ManagedIdentityFederatedIdentityCredentials).where({ $_ }))
    $ficTrusts = @(foreach ($fic in $fics) { getFederatedCredentialTrust -issuer $fic.properties.issuer -subject $fic.properties.subject -cloudEnvironment $azAPICallConf['checkContext'].Environment.Name })
    setAzureDevOpsConvertedFinding $ficTrusts
    for ($i = 0; $i -lt $fics.Count; $i++) {
        $fic = $fics[$i]; $ficTrust = $ficTrusts[$i]
        $null = $fic.PSObject.Properties.Remove('issuerType'); $fic.PSObject.Properties.Add([psnoteproperty]::new('issuerType', $ficTrust.issuerType))
        $null = $fic.PSObject.Properties.Remove('trust'); $fic.PSObject.Properties.Add([psnoteproperty]::new('trust', $ficTrust.trust))
        $null = $fic.PSObject.Properties.Remove('trustFinding'); $fic.PSObject.Properties.Add([psnoteproperty]::new('trustFinding', $ficTrust.finding))
    }
}
$htEffectivePrivileges = getEffectivePrivileges -entries $cu
foreach ($entry in $cu.where({ $_.SP })) {
    $null = $entry.PSObject.Properties.Remove('SPEffectivePrivilege'); $entry.PSObject.Properties.Add([psnoteproperty]::new('SPEffectivePrivilege', $htEffectivePrivileges[@($entry.SP)[0].SPObjectId]))
}
Write-Host " Effective privilege: $(@($htEffectivePrivileges.Values.where({ $_.level -eq 'critical' })).Count) critical, $(@($htEffectivePrivileges.Values.where({ $_.level -eq 'high' })).Count) high"
#endregion insights data

#optional data (only collected with the optional permission: then the property exists, possibly as an empty list)
if ($null -ne $htEntraRecommendationsBySPId -or $null -ne $htCustomSecurityAttributesBySPId) {
    foreach ($entry in $cu) {
        $entrySPObjectId = @($entry.SP)[0].SPObjectId
        if ($null -ne $htEntraRecommendationsBySPId) {
            $entry | Add-Member -NotePropertyName SPEntraRecommendations -NotePropertyValue @(if ($entrySPObjectId -and $htEntraRecommendationsBySPId[$entrySPObjectId]) { $htEntraRecommendationsBySPId[$entrySPObjectId] }) -Force
        }
        if ($null -ne $htCustomSecurityAttributesBySPId) {
            $entry | Add-Member -NotePropertyName SPCustomSecurityAttributes -NotePropertyValue @(if ($entrySPObjectId -and $htCustomSecurityAttributesBySPId[$entrySPObjectId]) { $htCustomSecurityAttributesBySPId[$entrySPObjectId] }) -Force
        }
    }
}

#agent objects (Microsoft Entra Agent ID): marked on the output objects
if ($htAgentObjectsById.Count -gt 0) {
    foreach ($entry in $cu) {
        $agentObject = $(if ($entry.SP) { $htAgentObjectsById[@($entry.SP)[0].SPObjectId] }) ?? $(if ($entry.APP) { $htAgentObjectsById[@($entry.APP)[0].APPObjectId] })
        if ($agentObject) {
            $entry | Add-Member -NotePropertyName AgentType -NotePropertyValue $agentObject.agentType -Force
        }
    }
}

#review F-56: pipelines - a JSON folder of earlier runs is removed also with -NoJsonExport (it stayed in the repository and was
#ingested again as new data by every run)
if ($NoJsonExport -and $azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions -and (Test-Path -LiteralPath "$($outputPath)$($DirectorySeparatorChar)JSON_SP_$($fileNameMGRef)")) {
    Write-Host ' Removing the JSON folder of an earlier run (Pipeline only, parameter NoJsonExport)'
    Remove-Item -LiteralPath "$($outputPath)$($DirectorySeparatorChar)JSON_SP_$($fileNameMGRef)" -Recurse -Force
}
if (-not $NoJsonExport) {
    if ($azAPICallConf['htParameters'].onAzureDevOpsOrGitHubActions) {
        $JSONPath = "JSON_SP_$($fileNameMGRef)"
        if (Test-Path -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($JSONPath)") {
            Write-Host ' Cleaning old state (Pipeline only)'
            Remove-Item -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($JSONPath)" -Recurse -Force   #R-28: [ ] in the path are no wildcards
        }
    }
    else {
        #no new timestamp here: all output files (and the transcript) use the timestamp of the start of the run
        #(a new one here made the HTML/CSV/JSON names differ from the transcript name)
        $JSONPath = "JSON_SP_$($fileNameMGRef)_$($fileTimestamp)"
        Write-Host " Creating new state ($($JSONPath)) (local only))"
    }

    $null = New-Item -Name $JSONPath -ItemType directory -Path $outputPath
    #review P-7: [IO.File]::WriteAllText (same bytes as Set-Content -Encoding utf8: UTF-8 without BOM, trailing newline) in parallel
    #chunks; '| Set-Content' per file cost ~4 ms per object (file handling, not ConvertTo-Json)
    $jsonFolder = "$($outputPath)$($DirectorySeparatorChar)$($JSONPath)"
    $jsonThrottleLimit = [math]::Max(1, [math]::Min($ThrottleLimitLocal, [Environment]::ProcessorCount))
    #review F-31: a file that cannot be written (sharing violation, disk full) is reported; the report and the change history follow
    $jsonExportFailures = [System.Collections.ArrayList]::Synchronized([System.Collections.ArrayList]::new())
    getParallelBatches -Items $cu -BatchSize ([math]::Ceiling($cu.Count / $jsonThrottleLimit)) | ForEach-Object -Parallel {
        $utf8NoBom = [System.Text.UTF8Encoding]::new($false)
        foreach ($entry in $_.Group) {
            try {
                [System.IO.File]::WriteAllText("$($using:jsonFolder)$($using:DirectorySeparatorChar)$($entry.ObjectId)_$($entry.ObjectType -replace ' ', '-').json", (ConvertTo-Json -InputObject $entry -Depth 99) + [Environment]::NewLine, $utf8NoBom)
            }
            catch {
                $null = ($using:jsonExportFailures).Add("$($entry.ObjectId): $($_.Exception.Message)")
            }
        }
    } -ThrottleLimit $jsonThrottleLimit
    if ($jsonExportFailures.Count -gt 0) {
        Write-Host " JSON export: $($jsonExportFailures.Count) of $($cu.Count) files not written" -ForegroundColor Yellow
        foreach ($jsonExportFailure in @($jsonExportFailures | Select-Object -First 20)) { Write-Host "  $($jsonExportFailure)" -ForegroundColor Yellow }
        writePipelineWarning "$($Product) JSON export: $($jsonExportFailures.Count) of $($cu.Count) files not written (see the console output)"
    }
}

#region change history (A7): compare with the previous run, append the changes, keep the state (README: Change history)
$script:changeHistoryLogPath = "$($outputPath)$($DirectorySeparatorChar)$($Product)_$($fileNameMGRef)_ChangeHistory.jsonl"
$changeHistoryRunRecord = $null
$changeHistoryUpdateIssue = ''
if (-not $NoChangeHistory) {
    $startChangeHistory = Get-Date
    Write-Host 'Change history'
    try {
        $changeHistoryScope = getChangeHistoryScope -TenantId $azAPICallConf['checkContext'].Tenant.Id -ManagementGroupIds $ManagementGroupId -SubscriptionQuotaIdWhitelist $SubscriptionQuotaIdWhitelist -NoAzureResourceSideRelations ([bool]$NoAzureResourceSideRelations) -OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes ([bool]$OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes)
        #review R-9: what was not collected completely keeps its previous state (was: any failure skipped the whole comparison)
        $changeHistoryCarryForwardObjects = @{}
        foreach ($failure in @($htParallelFailures.Values)) {
            if ($failure.ObjectId) { $changeHistoryCarryForwardObjects[[string]$failure.ObjectId] = "$($failure.Phase): $($failure.Error)" }
        }
        foreach ($incompleteId in @($htDataIncomplete.Keys)) {
            $changeHistoryCarryForwardObjects[[string]$incompleteId] = [string]$htDataIncomplete[$incompleteId]
        }
        $changeHistoryCarryForwardRelations = @(
            #a failed management group / subscription: Azure roles (and the effective privilege derived from them) are incomplete
            #review F-21: a failed resolve of the groups with Azure role assignments - the roles through these groups are missing
            if (@($htParallelFailures.Values.where({ $_.Phase -in 'ManagementGroups custom data', 'Subscriptions custom data', 'Azure role assignment groups resolve' })).Count -gt 0) { 'azureRoles'; 'azureRoleConditions'; 'level' }
            if (@($htParallelFailures.Values.where({ $_.Phase -eq 'AAD Groups resolve' })).Count -gt 0) { 'groups' }
            #a failed UAMI federated credential request (review P-14): the federated credentials keep their previous value
            if (@($htParallelFailures.Values.where({ $_.Phase -eq 'UAMI federated credentials' })).Count -gt 0) { 'federatedCredentials' }
        )
        $changeHistoryRunRecords = @(updateChangeHistory -StatePath "$($outputPath)$($DirectorySeparatorChar)$($Product)_$($fileNameMGRef)_ChangeHistoryState.json" -LogPath $script:changeHistoryLogPath -Scope $changeHistoryScope -CurrentState (getChangeHistoryState -entries $cu) -Version $ProductVersion -Incomplete $false -Time ((Get-Date).ToUniversalTime().ToString('o', [cultureinfo]::InvariantCulture)) -CarryForwardObjects $changeHistoryCarryForwardObjects -CarryForwardRelations $changeHistoryCarryForwardRelations)
        $changeHistoryRunRecord = @($changeHistoryRunRecords.where({ $_.category -eq 'run' }))[-1]
        Write-Host " $($changeHistoryRunRecord.change): $($changeHistoryRunRecord.detail)"
    }
    catch {
        #the change history must not fail the run (the other outputs are written)
        Write-Host " Change history not updated: $($_.Exception.Message)" -ForegroundColor Yellow
        #review F-35: also on the report (overview tile and section)
        $changeHistoryUpdateIssue = "this run was not added to the change history: $($_.Exception.Message)"
    }
    Write-Host " Change history duration: $((New-TimeSpan -Start $startChangeHistory -End (Get-Date)).TotalSeconds) seconds"
}
#endregion change history

#endregion enrichedAADSPData

if ($PerfTracking) {
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
}


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

#Export perf csv (only with -PerfTracking; without it the file was always written, empty)
if (-not $NoCsvExport -and $PerfTracking) {
    $arrayPerformanceTracking | protectCsvFormula | Export-Csv -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName)_perf.csv" -Delimiter "$csvDelimiter" -Encoding utf8 -NoTypeInformation -UseQuotes $csvUseQuotes
}



Write-Host 'Building HTML'

#region AG Grid runtime (browser): ported from Azure Governance Visualizer 7.0.0 (MIT License, Copyright (c) 2020 Julian Hayward)
$spiGridStyle = @'
        /* the variables of the toolbar and menu styles */
        :root { --az-radius: 4px; --az-bg: #ffffff; --az-card-bg: #ffffff; --az-border: #e1dfdd; --az-border-strong: #c8c6c4; --az-text: #323130; --az-text-secondary: #605e5c; --az-hover: #f3f2f1; --az-pressed: #edebe9; --az-accent: #0078d4; --az-accent-soft: #deecf9; --az-shadow-8: 0 3.2px 7.2px rgba(0,0,0,.13), 0 .6px 1.8px rgba(0,0,0,.11); }
        .spiToolbarHost { margin: 4px 0 2px 0; }
        /* the report stylesheet floats every div ('div { float: left }'): floated elements shrink to their content, which collapses the grid */
        .spiToolbarHost, .spiToolbarHost div, .spiGrid, .spiGrid div, .spiMenu, .spiMenu div, #spiCellPanel, #spiCellPanel div { float: none; }
        .spiToolbarHost, .spiGrid { clear: both; width: 100%; }
        /* toolbar and menus: the font of the grid (the sections around them use other fonts) */
        .spiToolbarHost, .spiMenu { font-family: "Segoe UI", "Segoe UI Web (West European)", -apple-system, BlinkMacSystemFont, Roboto, "Helvetica Neue", Arial, sans-serif; font-size: 12.5px; }
        /* custom grid chrome; this element is copied into the pop out window */
        .spiGrid .spiSelectFloatingFilter { width: 100%; height: 24px; font-family: inherit; font-size: inherit; border-radius: 4px; border: 1px solid var(--az-border-strong, #c8c6c4); background: var(--az-card-bg, #ffffff); color: inherit; }
        .spiGrid .spiResetFiltersButton { margin-right: auto; display: inline-flex; align-items: center; gap: 5px; height: 26px; padding: 0 8px; font-family: inherit; font-size: inherit; cursor: pointer; color: var(--az-accent, #0078d4); background: transparent; border: 1px solid transparent; border-radius: 4px; }
        .spiGrid .spiResetFiltersButton .fa { font-size: 14px; }
        .spiGrid .spiResetFiltersButton .spiResetFiltersCount:empty { display: none; }
        .spiGrid .spiResetFiltersButton .spiResetFiltersCount { font-size: 11px; font-weight: 600; line-height: 16px; padding: 0 6px; border-radius: 8px; color: #ffffff; background: var(--az-accent, #0078d4); }
        .spiGrid .spiResetFiltersButton:hover:not(:disabled) { background: var(--az-hover, #f3f2f1); border-color: var(--az-border-strong, #c8c6c4); }
        .spiGrid .spiResetFiltersButton:disabled { opacity: 0.4; cursor: default; }
        .spiGrid .ag-cell-wrap-text { word-break: break-word; line-height: 17px; padding-top: 5px; padding-bottom: 5px; }
        /* no ellipsis: cells and headers show the full text; a wrapped cell with a very long value scrolls inside the cell,
           one line per row (Wrap cells off): the value scrolls sideways */
        .spiGrid .ag-cell-value, .spiGrid .ag-header-cell-text { text-overflow: clip; }
        .spiGrid .ag-header-cell-text { overflow-wrap: anywhere; }
        .spiGrid .ag-cell-wrap-text .ag-cell-value { max-height: 132px; overflow-x: hidden; overflow-y: auto; }
        .spiGrid .ag-cell:not(.ag-cell-wrap-text) .ag-cell-wrapper { height: 100%; align-items: flex-start; }
        .spiGrid .ag-cell:not(.ag-cell-wrap-text) .ag-cell-value { display: block; height: 100%; box-sizing: border-box; padding-top: 4px; line-height: 16px; overflow-x: auto; overflow-y: hidden; }
        .spiGrid .ag-cell:not(.ag-cell-wrap-text) .ag-cell-value::-webkit-scrollbar { height: 6px; }
        .spiGrid .ag-cell:not(.ag-cell-wrap-text) .ag-cell-value::-webkit-scrollbar-thumb { background: var(--az-scrollbar, #c8c6c4); border: 0; border-radius: 3px; }
        .spiGrid .ag-cell:not(.ag-cell-wrap-text) .ag-cell-value::-webkit-scrollbar-track { background: transparent; }
        @supports not selector(::-webkit-scrollbar) { .spiGrid .ag-cell:not(.ag-cell-wrap-text) .ag-cell-value { scrollbar-width: thin; } }
        .spiGrid a { color: var(--az-accent, #0078d4); }
        .spiGrid .spiDim { color: var(--az-text-secondary, #605e5c); }
        /* bullet lists in cells (getAgGridListHtml); one comma separated line when the cells do not wrap */
        .spiGrid ul.spiList { margin: 0; padding-left: 16px; }
        .spiGrid .ag-cell:not(.ag-cell-wrap-text) ul.spiList { display: inline; padding: 0; }
        .spiDefLink { margin-left: 5px; color: var(--az-accent, #0078d4); text-decoration: none; white-space: nowrap; }
        .spiDefLink:hover { opacity: .75; text-decoration: none; }
        .spiDefLink .fa { font-size: 12px; }
        .spiGrid .ag-cell:not(.ag-cell-wrap-text) ul.spiList li { display: inline; }
        .spiGrid .ag-cell:not(.ag-cell-wrap-text) ul.spiList li + li::before { content: ', '; }
        /* readable cells (getSpiItemsHtml): one entry per line, details dimmed, key/value lines; one line per row: inline */
        .spiGrid ul.spiItems { margin: 0 0 2px; padding-left: 16px; }
        .spiGrid .spiItemsLabel { display: block; font-weight: 600; margin-top: 2px; }
        .spiGrid .ag-cell:not(.ag-cell-wrap-text) .spiItemsLabel { display: inline; margin: 0 4px 0 0; }
        .spiGrid .ag-cell:not(.ag-cell-wrap-text) .spiItemsLabel ~ .spiItemsLabel { margin-left: 8px; }
        .spiGrid .spiKv { display: block; color: var(--az-text-secondary, #605e5c); font-size: 11.5px; line-height: 15px; }
        .spiGrid .spiKvLine { display: block; overflow-wrap: anywhere; }
        .spiGrid .spiKvKey { font-weight: 600; }
        .spiGrid .ag-cell:not(.ag-cell-wrap-text) .spiKv, .spiGrid .ag-cell:not(.ag-cell-wrap-text) .spiKvLine { display: inline; }
        .spiGrid .ag-cell:not(.ag-cell-wrap-text) .spiKv::before { content: ' ('; }
        .spiGrid .ag-cell:not(.ag-cell-wrap-text) .spiKv::after { content: ')'; }
        .spiGrid .ag-cell:not(.ag-cell-wrap-text) .spiKvLine + .spiKvLine::before { content: '; '; }
        .spiGrid .spiItem-muted, .spiGrid .spiItem-muted a { color: var(--az-text-secondary, #605e5c); }
        .spiGrid .spiItem-warn { color: var(--az-warning-strong, #d83b01); }
        .spiGrid .spiCritical { color: var(--az-warning-strong, #d83b01); font-size: 10px; margin-right: 4px; }
        .spiGrid .spiClassDot { display: inline-block; width: 8px; height: 8px; border-radius: 50%; margin-right: 5px; vertical-align: 1px; }
        /* long lists (getAgGridScrollListHtml) scroll inside the wrapped cell */
        .spiGrid .ag-cell-wrap-text .spiScrollList { max-height: 132px; overflow: auto; padding-right: 4px; }
        .spiGrid .ag-cell:not(.ag-cell-wrap-text) .spiScrollList { display: inline; }
        /* grid toolbar and menus (spiRenderGridToolbar): in the report above the grid, in the pop out window above its grid; the --az-* variables: :root above or the pop out\'s own style */
        .spiToolbar { display: flex; flex-wrap: wrap; align-items: center; gap: 2px 4px; }
        .spiTbBtn { display: inline-flex; align-items: center; gap: 6px; height: 28px; padding: 0 9px; border: 1px solid transparent; border-radius: var(--az-radius); background: transparent; color: var(--az-text); font: inherit; font-size: 12.5px; cursor: pointer; white-space: nowrap; }
        .spiTbBtn .fa { color: var(--az-accent); font-size: 13px; width: 14px; text-align: center; }
        .spiTbBtn:hover:not(:disabled) { background: var(--az-hover); }
        .spiTbBtn:active:not(:disabled) { background: var(--az-pressed); }
        .spiTbBtn:disabled { opacity: .45; cursor: default; }
        .spiTbBtn[aria-pressed="true"] { background: var(--az-accent-soft); border-color: var(--az-accent-soft); }
        .spiTbBtn:focus-visible { outline: 2px solid var(--az-accent); outline-offset: 1px; }
        .spiTbBtn .spiResetFiltersCount:empty { display: none; }
        .spiTbBtn .spiResetFiltersCount { font-size: 11px; font-weight: 600; line-height: 16px; padding: 0 6px; border-radius: 8px; color: #fff; background: var(--az-accent); }
        .spiTbCount { margin-left: auto; font-size: 12px; color: var(--az-text-secondary); white-space: nowrap; padding: 0 6px; }
        .spiTbPopout { color: var(--az-text-secondary); }
        .spiMenu { position: fixed; z-index: 180; min-width: 220px; max-width: 360px; max-height: 70vh; overflow-y: auto; padding: 4px 0; background: var(--az-card-bg); color: var(--az-text); border: 1px solid var(--az-border); border-radius: var(--az-radius); box-shadow: var(--az-shadow-8); font-size: 12.5px; }
        .spiMenuItem { display: flex; align-items: center; gap: 8px; width: 100%; padding: 6px 12px; border: 0; background: transparent; color: var(--az-text); font: inherit; text-align: left; cursor: pointer; }
        .spiMenuItem:hover { background: var(--az-hover); }
        .spiMenuItem input { margin: 0; accent-color: var(--az-accent); }
        .spiMenuItem span { min-width: 0; overflow-wrap: anywhere; }
        .spiMenuSep { height: 1px; margin: 4px 0; background: var(--az-border); }
        .spiMenuHead { display: flex; gap: 4px; padding: 2px 8px 4px; }
        .spiMenuLink { border: 0; background: transparent; color: var(--az-accent); font: inherit; font-size: 12px; cursor: pointer; padding: 2px 4px; border-radius: var(--az-radius); }
        .spiMenuLink:hover { background: var(--az-hover); }
        .spiMenuRow { display: flex; align-items: center; }
        .spiMenuRow:hover { background: var(--az-hover); }
        .spiMenuRow .spiMenuItem { flex: 1 1 auto; min-width: 0; }
        .spiMenuRow .spiMenuItem:hover { background: transparent; }
        .spiMenuGo { flex: 0 0 auto; border: 0; background: transparent; color: var(--az-accent); font: inherit; font-size: 12px; cursor: pointer; padding: 5px 10px; border-radius: var(--az-radius); }
        .spiMenuGo:hover { background: var(--az-pressed); }
        .spiMenuFilter { display: flex; align-items: center; gap: 4px; padding: 4px 8px 4px 12px; }
        .spiMenuFilter:hover { background: var(--az-hover); }
        .spiMenuFilterText { flex: 1 1 auto; min-width: 0; }
        .spiMenuFilterName { font-weight: 600; overflow-wrap: anywhere; }
        .spiMenuFilterCond { font-size: 11.5px; color: var(--az-text-secondary); overflow-wrap: anywhere; }
        .spiMenuNote { color: var(--az-text-secondary); cursor: default; }
        .spiMenuNote:hover { background: transparent; }
        /* 'Go to' of the active filters menu: the header and the filter cell of the column flash */
        @keyframes spiFlashFrames { 0%, 60% { box-shadow: inset 0 0 0 2px var(--az-accent); } 100% { box-shadow: none; } }
        .spiGrid .spiFlash { animation: spiFlashFrames 1.6s ease-out; }
        /* side panel (full cell values), docked to the right edge of the window; shared with the pop out window */
        #spiCellPanel { position: fixed; top: var(--az-topbar-h, 0px); right: 0; height: calc(100% - var(--az-topbar-h, 0px)); width: 45%; min-width: 360px; display: none; flex-direction: column; background: var(--az-card-bg, #ffffff); color: var(--az-text, #323130); border-left: 1px solid var(--az-border, #e1dfdd); box-shadow: 0 0.6px 1.8px rgba(0, 0, 0, .10), 0 3.2px 7.2px rgba(0, 0, 0, .13); z-index: 10000; font-family: "Segoe UI", -apple-system, BlinkMacSystemFont, Roboto, "Helvetica Neue", Arial, sans-serif; font-size: 12px; }
        #spiCellPanel .spiCellPanelHeader { display: flex; align-items: center; gap: 8px; padding: 8px 12px; border-bottom: 1px solid var(--az-border, #e1dfdd); }
        #spiCellPanel .spiCellPanelTitle { flex: 1; min-width: 0; font-weight: 600; font-size: 13px; overflow-wrap: anywhere; }
        #spiCellPanel .spiPanelButton { font-family: inherit; font-size: inherit; line-height: 16px; padding: 2px 8px; cursor: pointer; color: var(--az-accent, #0078d4); background: var(--az-card-bg, #ffffff); border: 1px solid var(--az-border-strong, #c8c6c4); border-radius: 4px; }
        #spiCellPanel .spiPanelButton:hover { background: var(--az-hover, #f3f2f1); }
        #spiCellPanel .spiCellPanelBody { flex: 1; margin: 0; padding: 8px 12px; overflow: auto; white-space: pre; font-family: "Cascadia Code", Consolas, "Courier New", monospace; font-size: 11px; line-height: 1.4; }
        #spiCellPanel.spiPanelText .spiCellPanelBody { white-space: pre-wrap; overflow-wrap: anywhere; font-family: inherit; font-size: 12.5px; line-height: 1.5; }
'@
$spiGridSelectFilter = @'
    /* AG Grid Community has no set filter; this floating filter renders a <select> (like the TableFilter 'select' columns)
       and drives the column's text filter with an 'equals' match. The options are the distinct values of the column. */
    class spiSelectFloatingFilter {
        init(params) {
            this.params = params;
            this.eGui = document.createElement('select');
            this.eGui.className = 'ag-floating-filter-input spiSelectFloatingFilter';
            this.eGui.addEventListener('change', () => {
                const value = this.eGui.value;
                this.params.parentFilterInstance((instance) => {
                    if (value === '') {
                        instance.onFloatingFilterChanged(null, null);
                    }
                    else {
                        //a multi value column (params.separator) is filtered by 'contains' the selected item
                        instance.onFloatingFilterChanged(this.params.separator ? 'contains' : 'equals', value);
                    }
                });
            });
            this.populate();
            if (!this.params.values && this.eGui.options.length <= 1) {
                //the row data may not be available yet when the grid builds its floating filters
                this.onFirstDataRendered = () => this.populate();
                params.api.addEventListener('firstDataRendered', this.onFirstDataRendered);
            }
        }
        populate() {
            const values = new Set();
            const separator = this.params.separator;
            const add = (value) => {
                //a multi value column lists its distinct items instead of the distinct joined strings
                if (separator) { String(value).split(separator).forEach((item) => values.add(item.trim())); } else { values.add(String(value)); }
            };
            if (this.params.values) {
                //dictionary encoded column, the dictionary already is the set of distinct values
                this.params.values.forEach(add);
            }
            else {
                const colId = this.params.column.getColId();
                this.params.api.forEachNode((node) => {
                    const value = node.data ? node.data[colId] : null;
                    if (value !== null && value !== undefined) {
                        add(value);
                    }
                });
            }
            values.delete('');
            const selected = this.eGui.value;
            this.eGui.replaceChildren(new Option('', ''));
            /* one collator for the sort (localeCompare with options builds one per comparison: 20,000 values 1.8 s -> 20 ms; review P-19) */
            const collator = new Intl.Collator(undefined, { sensitivity: 'base' });
            Array.from(values).sort(collator.compare).forEach((value) => {
                this.eGui.appendChild(new Option(value, value));
            });
            this.eGui.value = selected;
        }
        onParentModelChanged(parentModel) {
            //keep the dropdown in sync when the filter is set or cleared elsewhere
            this.eGui.value = (parentModel && (parentModel.type === 'equals' || parentModel.type === 'contains') && parentModel.filter != null) ? parentModel.filter : '';
        }
        getGui() {
            return this.eGui;
        }
        destroy() {
            if (this.onFirstDataRendered) {
                this.params.api.removeEventListener('firstDataRendered', this.onFirstDataRendered);
            }
        }
    }
'@
$spiGridSupportScript = @'
    /* ---------- theme (AG Grid Theming API) ----------
       typography and sizes of the report (.summary); the theme object is passed to every grid as the 'theme' option */
    var spiGridTheme = agGrid.themeQuartz.withParams({
        fontFamily: '"Segoe UI", "Segoe UI Web (West European)", -apple-system, BlinkMacSystemFont, Roboto, "Helvetica Neue", Arial, sans-serif',
        fontSize: 12.5,
        rowHeight: 30,
        headerHeight: 34,
        listItemHeight: 24,
        cellHorizontalPadding: 10,
        borderRadius: 4,
        wrapperBorderRadius: 4,
        backgroundColor: '#ffffff',
        foregroundColor: '#323130',
        headerBackgroundColor: '#faf9f8',
        headerTextColor: '#323130',
        oddRowBackgroundColor: '#faf9f8',
        rowHoverColor: '#f3f2f1',
        borderColor: '#e1dfdd',
        accentColor: '#0078d4',
        chromeBackgroundColor: '#faf9f8',
        browserColorScheme: 'light'
    }).withParams({
        backgroundColor: '#252423',
        foregroundColor: '#f3f2f1',
        headerBackgroundColor: '#2d2c2b',
        headerTextColor: '#f3f2f1',
        oddRowBackgroundColor: '#292827',
        rowHoverColor: '#323130',
        borderColor: '#3b3a39',
        accentColor: '#2899f5',
        chromeBackgroundColor: '#2d2c2b',
        inputBackgroundColor: '#1b1a19',
        inputTextColor: '#f3f2f1',
        inputBorder: { color: '#605e5c' },
        textColor: '#f3f2f1',
        browserColorScheme: 'dark'
    }, 'dark');

    /* ---------- layout ----------
       A grid with few rows grows with its rows (no scrollbar inside the card, the page scrolls); a grid with many
       rows gets a height that fits the viewport and scrolls inside. The pop out window always fills its window. */
    var spiAutoHeightRows = 40;
    function spiGridHeightCap() {
        return Math.min(1000, Math.max(360, window.innerHeight - 230));
    }
    function spiApplyGridLayout(element, options) {
        var rows = options.rowData ? options.rowData.length : 0;
        if (rows > spiAutoHeightRows) {
            element.style.height = spiGridHeightCap() + 'px';
            return;
        }
        options.domLayout = 'autoHeight';
        element.style.height = '';
        /* wrapped cells can make even a few rows taller than the viewport: then the grid gets the cap and scrolls inside */
        var previous = options.onGridReady;
        options.onGridReady = function (event) {
            if (previous) { previous(event); }
            if (typeof ResizeObserver === 'undefined') { return; }
            var observer = new ResizeObserver(function () {
                var cap = spiGridHeightCap();
                if (element.offsetHeight > cap) {
                    observer.disconnect();
                    element.style.height = cap + 'px';
                    event.api.setGridOption('domLayout', 'normal');
                }
            });
            observer.observe(element);
        };
    }

    /* ---------- row data ----------
       The row data of a grid is embedded as an inert compressed block (spiData4<id>) and inflated once, when the grid
       is first shown, exported or popped out; the expanded data is cached as window.rowData4<id> (the pop out window
       reads it from its opener). */
    /* computed grids: a provider returns the row data ({ rows, dictionaries, columns }, plain
       string dictionaries) instead of the embedded block; spiResetGrid drops the data and the grid, the next
       spiCreateGrid asks the provider again */
    var spiGridDataProviders = {};
    function spiLoadGridData(id) {
        var name = 'rowData4' + id;
        if (window[name]) { return Promise.resolve(window[name]); }
        if (spiGridDataProviders[id]) {
            return Promise.resolve(spiGridDataProviders[id]()).then(function (data) { window[name] = data; return data; });
        }
        return Promise.all([spiInflateElement('spiData4' + id), spiLoadStrings()]).then(function (results) {
            var encoded = JSON.parse(results[0]);
            var getString = results[1];
            for (var columnIndex = 0; columnIndex < encoded.dictionaries.length; columnIndex++) {
                var dictionary = encoded.dictionaries[columnIndex];
                for (var valueIndex = 0; valueIndex < dictionary.length; valueIndex++) {
                    if (typeof dictionary[valueIndex] === 'number') { dictionary[valueIndex] = getString(dictionary[valueIndex]); }
                }
            }
            window[name] = encoded;
            return encoded;
        });
    }
    function spiGridError(element, error) {
        element.style.height = '';
        element.innerHTML = '<div style="padding: 8px; color: var(--az-error, #a4262c);"><i class="fa fa-exclamation-triangle" aria-hidden="true"></i> ' + spiEscapeHtml(error && error.message ? error.message : String(error)) + '</div>';
    }
    function spiCreateGrid(id) {
        var grid = spiGrids[id];
        if (!grid) { return Promise.reject(new Error('unknown grid ' + id)); }
        if (grid.ready) { return grid.ready; }
        var element = document.getElementById(id);
        grid.ready = spiLoadGridData(id).then(function (data) {
            var options = spiGridDefs[grid.key](spiCreateHighlighter(), data, element);
            grid.rows = data.rows.length;
            grid.baseDefs = options.columnDefs;
            /* small grids start without the filter row (the toolbar brings it back) */
            if (!grid.filtersTouched) { grid.state.filters = grid.rows > 10; }
            spiPrepareOptions(options, grid.state, element.ownerDocument);
            grid.options = options;
            if (!grid.popout) { spiApplyGridLayout(element, options); }
            var api = agGrid.createGrid(element, options);
            grid.api = api;
            window['api4' + id] = api;
            var sync = function () { spiSyncToolbar(grid); };
            api.addEventListener('filterChanged', sync);
            api.addEventListener('modelUpdated', sync);
            sync();
            return api;
        });
        grid.ready.catch(function (error) { spiGridError(element, error); });
        return grid.ready;
    }
    function spiResetGrid(id) {
        var grid = spiGrids[id];
        if (!grid) { return; }
        window['rowData4' + id] = null;
        if (grid.api) { grid.api.destroy(); grid.api = null; window['api4' + id] = null; }
        grid.ready = null;
        var element = document.getElementById(id);
        if (element) { element.innerHTML = ''; }
    }
    function spiExportGrid(id, separator) {
        spiCreateGrid(id).then(function (api) {
            api.exportDataAsCsv({ columnSeparator: separator, fileName: 'export_' + id + '_' + new Date().toLocaleDateString('en-CA') + '.csv', processCellCallback: spiCsvCell });
        }).catch(function () { /* shown in the grid element */ });
    }

    /* a CSV cell: number columns export their text (e.g. 'n/a'), line breaks become a space, whitespace runs one space;
       CSV injection: spreadsheet apps evaluate values that start with = + - @ (or tab / carriage return) as formulas */
    function spiCsvCell(params) {
        var value = params.column && params.column.getColDef().valueFormatter && params.formatValue ? params.formatValue(params.value) : params.value;
        var data = (value === null || value === undefined) ? '' : String(value);
        data = data.replace(/(\r\n|\n|\r)/g, ' ').replace(/\s+/g, ' ').trim();
        if (/^[=+\-@\t\r]/.test(data)) { data = "'" + data; }
        return data;
    }

    /* ---------- text filter match highlighting ----------
       Every text filter condition of a column gets its own color slot and its matches are wrapped
       in <mark class="filter-match fm-N"> by the column cell renderers. */
    var spiFilterHighlightPalette = [
        { bg: '#FFF59D', fg: '#B71C1C' },
        { bg: '#BBDEFB', fg: '#0D3B66' },
        { bg: '#C8E6C9', fg: '#1B5E20' },
        { bg: '#FFE0B2', fg: '#5D2E00' },
        { bg: '#F8BBD0', fg: '#880E4F' },
        { bg: '#B2EBF2', fg: '#004D40' },
        { bg: '#E1BEE7', fg: '#4A148C' },
        { bg: '#E0E0E0', fg: '#212121' }
    ];

    function spiEscapeRegex(text) {
        return text.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    }

    function spiEscapeHtml(text) {
        return String(text).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
    }

    function spiTagRanges(text) {
        var ranges = [];
        /* tags and character references (&amp; &#39; &middot;): a highlight inside one showed it as literal text (review R-31) */
        var re = /<[^>]*>|&#?[A-Za-z0-9]+;/g;
        var m;
        while ((m = re.exec(text)) !== null) {
            ranges.push([m.index, m.index + m[0].length]);
        }
        return ranges;
    }

    /* review F-47: one collator for every comparison of the text sort (numeric: 'sp2' before 'sp10') */
    var spiTextCollator = new Intl.Collator(undefined, { sensitivity: 'base', numeric: true });
    function spiTextComparator(a, b) {
        var aEmpty = a === null || a === undefined || a === '';
        var bEmpty = b === null || b === undefined || b === '';
        if (aEmpty || bEmpty) { return aEmpty === bEmpty ? 0 : (aEmpty ? -1 : 1); }
        /* number columns (spiColumnNumberValueGetter) */
        if (typeof a === 'number' && typeof b === 'number') { return a - b; }
        return spiTextCollator.compare(String(a), String(b));
    }

    function spiBuildHighlighted(text, entry, isHtml) {
        var emit = function (chunk) { return isHtml ? chunk : spiEscapeHtml(chunk); };
        if (!entry || !entry.slots.length) {
            return emit(text);
        }
        //matches must not land inside the markup produced by a cell renderer
        var tagRanges = isHtml ? spiTagRanges(text) : null;
        var matches = [];
        for (var i = 0; i < entry.slots.length; i++) {
            var re = entry.slots[i].re;
            re.lastIndex = 0;
            var m;
            /* review F-14: the tag ranges and the matches are both sorted by position - a pointer per condition (a scan of all
               ranges per match was O(matches x ranges): a list cell with 5,000 items took seconds per render) */
            var tagIndex = 0;
            while ((m = re.exec(text)) !== null) {
                if (m[0].length === 0) { re.lastIndex++; continue; }
                var start = m.index;
                var end = m.index + m[0].length;
                if (tagRanges) {
                    while (tagIndex < tagRanges.length && tagRanges[tagIndex][1] <= start) { tagIndex++; }
                    if (tagIndex < tagRanges.length && tagRanges[tagIndex][0] < end) { continue; }
                }
                matches.push({ start: start, end: end, idx: entry.slots[i].idx, order: i });
            }
        }
        if (!matches.length) {
            return emit(text);
        }
        //earliest position wins, ties are resolved by condition order so colors stay stable
        matches.sort(function (a, b) { return a.start - b.start || a.order - b.order; });
        var out = '';
        var pos = 0;
        for (var k = 0; k < matches.length; k++) {
            var match = matches[k];
            if (match.start < pos) { continue; }
            out += emit(text.slice(pos, match.start));
            out += '<mark class="filter-match fm-' + match.idx + '">' + emit(text.slice(match.start, match.end)) + '</mark>';
            pos = match.end;
        }
        out += emit(text.slice(pos));
        return out;
    }

    /* ---------- grid registry ----------
       The report holds one grid definition per column signature (spiGridDefs[key], shared by every grid with the same
       columns) and one line per grid: spiRegisterGrid(id, key, title).
       The row data of a grid and the string table of the file are gzip compressed inert blocks (application/octet-stream,
       base64) that are inflated with DecompressionStream when a grid is first shown. */
    var spiGridDefs = {};
    var spiGrids = {};
    function spiRegisterGrid(id, key, title) {
        spiGrids[id] = { key: key, title: title, ready: null, api: null, rows: 0, state: { wrap: spiPrefs.wrap, pin: spiPrefs.pin, filters: true } };
        window['createag' + id] = function () { return spiCreateGrid(id); };
        window['loadag' + id] = function () { setTimeout(function () { spiCreateGrid(id); }, 0); };
        window['exportag' + id] = function (separator) { spiExportGrid(id, separator); };
        window['popoutag' + id] = function () { spiPopoutGrid(id); };
    }
    function spiBase64ToBytes(text) {
        var binary = atob(text.replace(/\s+/g, ''));
        var bytes = new Uint8Array(binary.length);
        for (var i = 0; i < binary.length; i++) { bytes[i] = binary.charCodeAt(i); }
        return bytes;
    }
    function spiInflateElement(id) {
        /* review F-54: inside the promise - a damaged block (atob throws synchronously) made spiCreateGrid throw, the remaining
           grids of the page were not created and no error was shown; now the grid shows the error */
        return Promise.resolve().then(function () {
            var element = document.getElementById(id);
            if (!element) { return ''; }
            if (element.getAttribute('type') !== 'application/octet-stream') { return element.textContent; }
            if (typeof DecompressionStream === 'undefined') {
                throw new Error('This browser cannot inflate the report data (DecompressionStream is not available) - please open the report with a current Edge, Chrome, Firefox or Safari.');
            }
            var stream = new Blob([spiBase64ToBytes(element.textContent)]).stream().pipeThrough(new DecompressionStream('gzip'));
            return new Response(stream).text();
        });
    }
    /* the string table of the file: the dictionaries of the grids reference it by index; the table repeats the same
       markup (e.g. the AzAdvertizer link) for thousands of values, the emitter therefore replaces those fragments with
       a single private use character indexing into 'fragments' */
    var spiStringsPromise = null;
    function spiLoadStrings() {
        if (!spiStringsPromise) {
            spiStringsPromise = spiInflateElement('spiStrings').then(function (text) {
                var table = text ? JSON.parse(text) : { fragments: [], strings: [] };
                var fragments = table.fragments || [];
                var expand = function (token) {
                    var index = token.charCodeAt(0) - 0xE000;
                    return index < fragments.length ? fragments[index] : token;
                };
                var strings = table.strings || [];
                /* \uE0FF + a character: that character itself (a private use character of the data, review R-16) */
                var tokenOrEscaped = /\uE0FF([\uE000-\uE0FF])|[\uE000-\uE0FE]/g;
                var restore = function (match, literal) { return literal !== undefined ? literal : expand(match); };
                /* expanded on first use (review P-19): a grid expands only its own strings, each string once - the whole
                   report-wide table was expanded on the first open of any grid (1.4 s and memory for ~100k objects) */
                var expanded = new Uint8Array(strings.length);
                var getString = function (index) {
                    if (!expanded[index]) {
                        expanded[index] = 1;
                        if (strings[index]) { strings[index] = strings[index].replace(tokenOrEscaped, restore); }
                    }
                    return strings[index];
                };
                return getString;
            });
        }
        return spiStringsPromise;
    }

    /* Dictionary encoded row data: a row is an array of integers, each one an index into the dictionary of its column.
       This keeps the payload small and avoids materializing one object per row. */
    function spiColumnValueGetter(encoded, columnIndex) {
        var dictionary = encoded.dictionaries[columnIndex];
        return function (params) {
            return params.data ? dictionary[params.data[columnIndex]] : undefined;
        };
    }

    //renders an html column (the sortable/filterable plain text lives in a separate column)
    function spiColumnHtmlRenderer(encoded, columnIndex, highlighter) {
        var dictionary = encoded.dictionaries[columnIndex];
        return function (params) {
            return highlighter.html(dictionary[params.data[columnIndex]], params.column.getColId());
        };
    }

    //an object linked to its Microsoft Entra page in the Azure portal; the link is built here from the ids of the row (not stored
    //in the report) and only from GUIDs. kind: servicePrincipal (enterprise application; aux column: application id), application
    //(app registration; aux column: application id, else the cell), user, group, principal (aux column: the principal type,
    //'User...' or 'Group...'; other types are not linked)
    function spiColumnPortalLinkRenderer(encoded, columnIndex, auxIndex, kind, highlighter) {
        var dictionary = encoded.dictionaries[columnIndex];
        var aux = auxIndex >= 0 ? encoded.dictionaries[auxIndex] : null;
        var guid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
        var portal = 'https://portal.azure.com/#view/';
        return function (params) {
            var value = dictionary[params.data[columnIndex]];
            var text = highlighter.text(value, params.column.getColId());
            if (!value || !guid.test(value)) { return text; }
            var auxValue = aux ? aux[params.data[auxIndex]] : value;
            var cellKind = kind;
            if (kind === 'principal') {
                cellKind = /^user/i.test(auxValue || '') ? 'user' : (/^group/i.test(auxValue || '') ? 'group' : '');
            }
            var href = '';
            if (cellKind === 'servicePrincipal' && auxValue && guid.test(auxValue)) {
                href = portal + 'Microsoft_AAD_IAM/ManagedAppMenuBlade/~/Overview/objectId/' + value + '/appId/' + auxValue;
            }
            else if (cellKind === 'application' && auxValue && guid.test(auxValue)) {
                href = portal + 'Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/~/Overview/appId/' + auxValue;
            }
            else if (cellKind === 'user') {
                href = portal + 'Microsoft_AAD_UsersAndTenants/UserProfileMenuBlade/~/overview/userId/' + value;
            }
            else if (cellKind === 'group') {
                href = portal + 'Microsoft_AAD_IAM/GroupDetailsMenuBlade/~/Overview/groupId/' + value + '/menuId/';
            }
            if (!href) { return text; }
            return '<a class="externallink" href="' + href + '" target="_blank" rel="noopener">' + text + '</a>';
        };
    }

    //numeric column; values that are no numbers (e.g. 'skipped') do not participate in sorting and number filtering
    function spiColumnNumberValueGetter(encoded, columnIndex) {
        var dictionary = encoded.dictionaries[columnIndex];
        return function (params) {
            if (!params.data) { return undefined; }
            var value = dictionary[params.data[columnIndex]];
            if (value === null || value === undefined || value === '') { return null; }
            var number = Number(value);
            return isNaN(number) ? null : number;
        };
    }

    //the text of a number column (export and tooltips; the value getter returns the number or null)
    function spiColumnTextFormatter(encoded, columnIndex) {
        var dictionary = encoded.dictionaries[columnIndex];
        return function (params) {
            return params.data ? dictionary[params.data[columnIndex]] : '';
        };
    }

    //renders the dictionary value of a column instead of the cell value, keeps non numeric values of number columns visible
    function spiColumnTextRenderer(encoded, columnIndex, highlighter) {
        var dictionary = encoded.dictionaries[columnIndex];
        return function (params) {
            return highlighter.text(dictionary[params.data[columnIndex]], params.column.getColId());
        };
    }

    function spiCreateHighlighter() {
        var byField = Object.create(null);

        function collectTerm(condition, out) {
            if (!condition) { return; }
            var type = condition.type;
            //negative and blank conditions have nothing to highlight
            if (type === 'notEqual' || type === 'notContains' || type === 'blank' || type === 'notBlank') { return; }
            if (condition.filter === null || condition.filter === undefined || condition.filter === '') { return; }
            /* review F-49: a number or date condition (greaterThan 5) is no text to mark in the cells */
            if (condition.filterType && condition.filterType !== 'text') { return; }
            out.push(String(condition.filter));
        }

        function extractTerms(entry) {
            var out = [];
            if (!entry) { return out; }
            if (entry.conditions && entry.conditions.length) {
                for (var i = 0; i < entry.conditions.length; i++) {
                    var tmp = [];
                    collectTerm(entry.conditions[i], tmp);
                    if (tmp.length) { out.push(tmp[0]); }
                }
                return out;
            }
            collectTerm(entry, out);
            return out;
        }

        return {
            //returns the fields whose terms changed so that only those columns need to be refreshed
            update: function (model) {
                var changed = [];
                var seen = Object.create(null);
                if (model) {
                    for (var field in model) {
                        seen[field] = true;
                        var terms = extractTerms(model[field]);
                        var sig = terms.join('\u0001');
                        var prev = byField[field];
                        if (prev && prev.sig === sig) { continue; }
                        if (!terms.length) {
                            if (prev) {
                                byField[field] = null;
                                changed.push(field);
                            }
                            continue;
                        }
                        var slots = [];
                        for (var ti = 0; ti < terms.length; ti++) {
                            slots.push({ re: new RegExp(spiEscapeRegex(terms[ti]), 'ig'), idx: ti % spiFilterHighlightPalette.length });
                        }
                        byField[field] = { sig: sig, slots: slots };
                        changed.push(field);
                    }
                }
                for (var prevField in byField) {
                    if (seen[prevField] || !byField[prevField]) { continue; }
                    byField[prevField] = null;
                    changed.push(prevField);
                }
                return changed;
            },
            //plain cell value, the result is html encoded
            text: function (value, field) {
                return spiBuildHighlighted((value === null || value === undefined) ? '' : String(value), byField[field], false);
            },
            //cell value that already is an html fragment
            html: function (value, field) {
                return spiBuildHighlighted((value === null || value === undefined) ? '' : String(value), byField[field], true);
            }
        };
    }

    (function injectSpiHighlightStyles() {
        if (document.getElementById('spi-filter-match-style')) { return; }
        var rules = ['mark.filter-match { font-weight: bold; padding: 0; border-radius: 2px; }'];
        for (var i = 0; i < spiFilterHighlightPalette.length; i++) {
            rules.push('mark.filter-match.fm-' + i + ' { background-color: ' + spiFilterHighlightPalette[i].bg + '; color: ' + spiFilterHighlightPalette[i].fg + '; }');
        }
        var style = document.createElement('style');
        style.id = 'spi-filter-match-style';
        style.textContent = rules.join('\n');
        document.head.appendChild(style);
    })();

    /* ---------- date column support ----------
       Date values are rendered by the report as invariant culture strings ('MM/dd/yyyy HH:mm:ss'). */
    function spiParseGridDate(value) {
        if (value === null || value === undefined || value === '') { return null; }
        var m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?/.exec(String(value));
        if (m) {
            return new Date(Number(m[3]), Number(m[1]) - 1, Number(m[2]), Number(m[4] || 0), Number(m[5] || 0), Number(m[6] || 0));
        }
        var parsed = new Date(String(value));
        return isNaN(parsed.getTime()) ? null : parsed;
    }

    var spiDateFilterParams = {
        browserDatePicker: true,
        comparator: function (filterLocalDateAtMidnight, cellValue) {
            var cellDate = spiParseGridDate(cellValue);
            if (cellDate === null) { return -1; }
            var cellMidnight = new Date(cellDate.getFullYear(), cellDate.getMonth(), cellDate.getDate());
            if (cellMidnight < filterLocalDateAtMidnight) { return -1; }
            if (cellMidnight > filterLocalDateAtMidnight) { return 1; }
            return 0;
        }
    };

    function spiDateSortComparator(valueA, valueB) {
        var dateA = spiParseGridDate(valueA);
        var dateB = spiParseGridDate(valueB);
        if (dateA === null && dateB === null) { return 0; }
        if (dateA === null) { return -1; }
        if (dateB === null) { return 1; }
        return dateA.getTime() - dateB.getTime();
    }

    /* ---------- display preferences ----------
       wrap: cells wrap and rows grow (default; a very long value scrolls inside its cell); off: one line per row, the value
       scrolls sideways (no ellipsis), the full value also on double click
       pin: the identifying columns (SP / App displayName) stay visible while scrolling wide grids */
    var spiPrefs = { wrap: true, pin: false };
    (function spiReadPrefs() {
        try {
            var w = localStorage.getItem('azadspi.gridWrap'); if (w === '1' || w === '0') { spiPrefs.wrap = w === '1'; }
            var p = localStorage.getItem('azadspi.gridPin'); if (p === '1' || p === '0') { spiPrefs.pin = p === '1'; }
        } catch (e) { /* storage may be unavailable */ }
    })();
    function spiStorePref(name, value) {
        try { localStorage.setItem('azadspi.grid' + name, value ? '1' : '0'); } catch (e) { /* ignore */ }
    }
    var spiPinHeaders = { 'SP displayName': 1, 'App displayName': 1 };
    /* the leaf columns of a column definition list (column groups carry their columns in 'children') */
    function spiLeafColumnDefs(defs) {
        var out = [];
        (defs || []).forEach(function (def) {
            if (def.children) { out = out.concat(spiLeafColumnDefs(def.children)); } else { out.push(def); }
        });
        return out;
    }
    function spiPinCandidates(defs) {
        /* the leading identifying columns of a wide grid (at most 3, within the first 6 columns) */
        var leaves = spiLeafColumnDefs(defs);
        var out = [];
        for (var i = 0; i < leaves.length && i < 6 && out.length < 3; i++) {
            if (spiPinHeaders[leaves[i].headerName]) { out.push(leaves[i].colId); }
        }
        return leaves.length >= 10 ? out : [];
    }
    function spiDecorateColumnDefs(defs, state, forCreate) {
        var pins = spiPinCandidates(defs);
        var decorate = function (list) {
            return list.map(function (def) {
                var out = Object.assign({}, def);
                if (def.children) { out.children = decorate(def.children); return out; }
                out.wrapText = !!state.wrap;
                out.autoHeight = !!state.wrap;
                out.floatingFilter = state.filters !== false;
                if (forCreate) { out.pinned = state.pin && pins.indexOf(def.colId) >= 0 ? 'left' : null; }
                return out;
            });
        };
        return decorate(defs);
    }
    /* applies the display state to the options of a grid definition (report grids and the pop out window alike);
       state is the live state object of the grid, the renderers read it when a cell is drawn */
    function spiPrepareOptions(options, state, doc) {
        options.columnDefs = spiDecorateColumnDefs(options.columnDefs, state, true);
        options.maintainColumnOrder = true;
        /* tooltips: the header tooltips only (headerTooltip of a column); cells have none - their text is never cut (cells
           wrap, or scroll sideways in one-line mode), the full value of a cell is shown on double click */
        options.tooltipShowDelay = 400;
        options.onCellDoubleClicked = function (event) {
            /* the full content of a cell (links kept) in the side panel */
            if (!event.data || !event.event) { return; }
            var cell = event.event.target && event.event.target.closest ? event.event.target.closest('.ag-cell') : null;
            var def = event.colDef || {};
            var html = cell ? cell.innerHTML : spiEscapeHtml(String(event.value === null || event.value === undefined ? '' : event.value));
            spiShowPanel(doc || document, { title: def.headerName || '', html: html });
        };
        return options;
    }
    function spiUpdateColumnDefs(grid) {
        var api = grid.api;
        if (!api) { return; }
        api.setGridOption('columnDefs', spiDecorateColumnDefs(grid.baseDefs, grid.state, false));
        api.resetRowHeights();
        api.refreshCells({ force: true });
        spiSyncToolbar(grid);
    }
    function spiSetWrap(grid, on) {
        grid.state.wrap = !!on;
        spiPrefs.wrap = grid.state.wrap;
        spiStorePref('Wrap', grid.state.wrap);
        spiUpdateColumnDefs(grid);
    }
    function spiSetFilters(grid, on) {
        grid.state.filters = !!on;
        grid.filtersTouched = true;
        spiUpdateColumnDefs(grid);
    }
    function spiSetPin(grid, on) {
        grid.state.pin = !!on;
        spiPrefs.pin = grid.state.pin;
        spiStorePref('Pin', grid.state.pin);
        if (!grid.api) { return; }
        var pins = spiPinCandidates(grid.baseDefs);
        grid.api.applyColumnState({ state: pins.map(function (colId) { return { colId: colId, pinned: grid.state.pin ? 'left' : null }; }) });
        spiSyncToolbar(grid);
    }

    /* ---------- toolbar (report and pop out window) ----------
       The report renders this toolbar above each grid (spiRegisterReportGrid); the pop out window renders the same
       toolbar above its grid (without the pop out button). */
    var spiOpenMenu = null;
    function spiCloseMenu() { if (spiOpenMenu) { spiOpenMenu.remove(); spiOpenMenu = null; } }
    document.addEventListener('click', function (event) { if (spiOpenMenu && !spiOpenMenu.contains(event.target) && !(spiOpenMenu.spiAnchor && spiOpenMenu.spiAnchor.contains(event.target))) { spiCloseMenu(); } });
    document.addEventListener('keydown', function (event) { if (event.key === 'Escape') { spiCloseMenu(); } });
    function spiShowMenu(anchor, build) {
        if (spiOpenMenu && spiOpenMenu.spiAnchor === anchor) { spiCloseMenu(); return; }
        spiCloseMenu();
        var menu = document.createElement('div');
        menu.className = 'spiMenu';
        menu.setAttribute('role', 'menu');
        build(menu);
        var rect = anchor.getBoundingClientRect();
        menu.style.left = rect.left + 'px';
        menu.style.top = (rect.bottom + 2) + 'px';
        menu.spiAnchor = anchor;
        document.body.appendChild(menu);
        var mr = menu.getBoundingClientRect();
        if (mr.right > window.innerWidth - 8) { menu.style.left = Math.max(8, window.innerWidth - 8 - mr.width) + 'px'; }
        if (mr.bottom > window.innerHeight - 8) { menu.style.maxHeight = Math.max(120, window.innerHeight - rect.bottom - 12) + 'px'; }
        spiOpenMenu = menu;
        return menu;
    }
    function spiMenuItem(menu, label, onClick, options) {
        var o = options || {};
        var item = document.createElement(o.checkbox ? 'label' : 'button');
        item.className = 'spiMenuItem' + (o.cls ? ' ' + o.cls : '');
        if (o.checkbox) {
            var box = document.createElement('input'); box.type = 'checkbox'; box.checked = !!o.checked;
            box.addEventListener('change', function () { onClick(box.checked); });
            item.appendChild(box);
            var text = document.createElement('span'); text.textContent = label; item.appendChild(text);
        }
        else {
            item.type = 'button';
            item.textContent = label;
            item.addEventListener('click', function () { onClick(); if (!o.keepOpen) { spiCloseMenu(); } });
        }
        if (o.goTo) {
            var row = document.createElement('div');
            row.className = 'spiMenuRow';
            row.appendChild(item);
            var go = document.createElement('button');
            go.type = 'button';
            go.className = 'spiMenuGo';
            go.title = 'Go to the column';
            go.setAttribute('aria-label', 'Go to the column ' + label);
            go.innerHTML = '<i class="fa fa-arrow-right" aria-hidden="true"></i>';
            go.addEventListener('click', function (event) { event.preventDefault(); o.goTo(); });
            row.appendChild(go);
            menu.appendChild(row);
            return row;
        }
        menu.appendChild(item);
        return item;
    }
    function spiToolbarButton(cls, icon, label, title) {
        var button = document.createElement('button');
        button.type = 'button';
        button.className = 'spiTbBtn ' + cls;
        button.innerHTML = (icon ? '<i class="fa ' + icon + '" aria-hidden="true"></i>' : '') + '<span>' + spiEscapeHtml(label) + '</span>';
        if (title) { button.title = title; }
        return button;
    }
    function spiRenderGridToolbar(id, host) {
        var grid = spiGrids[id];
        if (!grid) { return null; }
        var bar = document.createElement('div');
        bar.className = 'spiToolbar';
        var withApi = function (fn) { spiCreateGrid(id).then(fn).catch(function () { /* shown in the grid element */ }); };
        var csv = spiToolbarButton('spiTbCsv', 'fa-download', 'Download CSV', 'Download the rows as CSV - the download respects the filters and the column order applied in the grid');
        csv.addEventListener('click', function () {
            spiShowMenu(csv, function (menu) {
                spiMenuItem(menu, 'Semicolon separated (;)', function () { spiExportGrid(id, ';'); });
                spiMenuItem(menu, 'Comma separated (,)', function () { spiExportGrid(id, ','); });
            });
        });
        bar.appendChild(csv);
        var columns = spiToolbarButton('spiTbColumns', 'fa-columns', 'Columns', 'Show or hide columns');
        columns.addEventListener('click', function () {
            withApi(function (api) {
                spiShowMenu(columns, function (menu) {
                    var pins = spiPinCandidates(grid.baseDefs);
                    if (pins.length) {
                        spiMenuItem(menu, 'Keep the identifying columns visible (pinned)', function (checked) { spiSetPin(grid, checked); }, { checkbox: true, checked: grid.state.pin });
                        menu.appendChild(Object.assign(document.createElement('div'), { className: 'spiMenuSep' }));
                    }
                    var head = document.createElement('div'); head.className = 'spiMenuHead';
                    var all = document.createElement('button'); all.type = 'button'; all.className = 'spiMenuLink'; all.textContent = 'Show all';
                    all.addEventListener('click', function () { api.setColumnsVisible(api.getColumns().map(function (c) { return c.getColId(); }), true); menu.querySelectorAll('input').forEach(function (i) { if (i.spiColumn) { i.checked = true; } }); });
                    var reset = document.createElement('button'); reset.type = 'button'; reset.className = 'spiMenuLink'; reset.textContent = 'Reset';
                    reset.addEventListener('click', function () { api.resetColumnState(); spiSetPin(grid, grid.state.pin); spiCloseMenu(); });
                    head.appendChild(all); head.appendChild(reset);
                    menu.appendChild(head);
                    api.getColumnState().forEach(function (st) {
                        var column = api.getColumn(st.colId);
                        if (!column) { return; }
                        var label = column.getColDef().headerName || st.colId;
                        var item = spiMenuItem(menu, label, function (checked) { api.setColumnsVisible([st.colId], checked); }, { checkbox: true, checked: !st.hide, goTo: function () { spiCloseMenu(); spiGoToColumn(grid, api, id, st.colId, { focusFilter: false }); } });
                        item.querySelector('input').spiColumn = true;
                    });
                });
            });
        });
        bar.appendChild(columns);
        var wrap = spiToolbarButton('spiTbWrap', 'fa-align-left', 'Wrap cells', 'Wrap long cell values (rows grow; very long values scroll inside the cell); off: one line per row, long values scroll sideways');
        wrap.setAttribute('aria-pressed', grid.state.wrap ? 'true' : 'false');
        wrap.addEventListener('click', function () { withApi(function () { spiSetWrap(grid, !grid.state.wrap); }); });
        bar.appendChild(wrap);
        var filters = spiToolbarButton('spiTbFilters', 'fa-filter', 'Filters', 'Show the filter row of the columns');
        filters.setAttribute('aria-pressed', grid.state.filters ? 'true' : 'false');
        filters.addEventListener('click', function () { withApi(function () { spiSetFilters(grid, !grid.state.filters); }); });
        bar.appendChild(filters);
        var reset = spiToolbarButton('spiTbReset', 'fa-filter', 'Active filters', 'No column filters applied');
        reset.disabled = true;
        reset.appendChild(Object.assign(document.createElement('span'), { className: 'spiResetFiltersCount' }));
        reset.addEventListener('click', function () { withApi(function (api) { spiShowMenu(reset, function (menu) { spiBuildFilterMenu(grid, api, id, menu); }); }); });
        bar.appendChild(reset);
        var count = document.createElement('span'); count.className = 'spiTbCount'; bar.appendChild(count);
        if (!grid.popout) {
            var popout = spiToolbarButton('spiTbPopout', 'fa-external-link', 'Pop out', 'Open the grid in a new window');
            popout.addEventListener('click', function () { spiPopoutGrid(id); });
            bar.appendChild(popout);
        }
        grid.toolbar = bar;
        host.appendChild(bar);
        spiSyncToolbar(grid);
        return bar;
    }
    function spiSyncToolbar(grid) {
        var bar = grid.toolbar;
        if (!bar) { return; }
        bar.querySelector('.spiTbWrap').setAttribute('aria-pressed', grid.state.wrap ? 'true' : 'false');
        bar.querySelector('.spiTbFilters').setAttribute('aria-pressed', grid.state.filters ? 'true' : 'false');
        var reset = bar.querySelector('.spiTbReset');
        var count = bar.querySelector('.spiTbCount');
        if (!grid.api) { count.textContent = ''; return; }
        var model = grid.api.getFilterModel();
        var n = model ? Object.keys(model).length : 0;
        reset.disabled = n === 0;
        reset.title = n === 0 ? 'No column filters applied' : n + ' column filter' + (n === 1 ? '' : 's') + ' applied - show, go to, clear';
        reset.querySelector('.spiResetFiltersCount').textContent = n === 0 ? '' : String(n);
        if (n === 0) { spiCloseMenuOf(reset); }
        var total = grid.rows, shown = grid.api.getDisplayedRowCount();
        count.textContent = shown === total ? total + ' rows' : shown + ' of ' + total + ' rows';
    }
    function spiCloseMenuOf(anchor) {
        if (spiOpenMenu && spiOpenMenu.spiAnchor === anchor) { spiCloseMenu(); }
    }
    /* ---------- active filters menu ----------
       One line per filtered column: header, condition and value; 'Go to' shows the column (hidden columns and collapsed
       groups are opened) and focuses its floating filter so the value can be changed there; 'Clear' removes one filter.
       The columns menu uses the same 'go to' without the filter focus. */
    var spiFilterTypeWords = {
        contains: 'contains', notContains: 'does not contain', equals: 'is', notEqual: 'is not', startsWith: 'starts with', endsWith: 'ends with',
        blank: 'is blank', notBlank: 'is not blank', lessThan: 'less than', lessThanOrEqual: 'at most', greaterThan: 'greater than',
        greaterThanOrEqual: 'at least', inRange: 'between', empty: 'is empty'
    };
    function spiDescribeFilterCondition(condition) {
        if (!condition) { return ''; }
        var words = spiFilterTypeWords[condition.type] || condition.type || '';
        var value = '';
        if (condition.filterType === 'date') {
            value = condition.type === 'inRange' ? (condition.dateFrom || '') + ' and ' + (condition.dateTo || '') : (condition.dateFrom || '');
        }
        else if (condition.type === 'inRange') {
            value = String(condition.filter) + ' and ' + String(condition.filterTo);
        }
        else if (condition.filter !== undefined && condition.filter !== null) {
            value = String(condition.filter);
        }
        return value === '' ? words : words + ': ' + value;
    }
    function spiDescribeFilter(model) {
        if (!model) { return ''; }
        if (model.conditions && model.conditions.length) {
            return model.conditions.map(spiDescribeFilterCondition).join(' ' + (model.operator || 'AND').toLowerCase() + ' ');
        }
        return spiDescribeFilterCondition(model);
    }
    function spiGoToColumn(grid, api, id, colId, options) {
        var o = options || {};
        var focusFilter = o.focusFilter !== false;
        var column = api.getColumn(colId);
        if (!column) { return; }
        if (!column.isVisible()) { api.setColumnsVisible([colId], true); }
        /* a column of a collapsed group (columnGroupShow 'open') is only shown when the group is opened */
        var parent = column.getParent();
        while (parent) {
            if (parent.getProvidedColumnGroup && parent.getProvidedColumnGroup().isExpandable && parent.getProvidedColumnGroup().isExpandable() && !parent.isExpanded()) {
                api.setColumnGroupOpened(parent.getProvidedColumnGroup(), true);
            }
            parent = parent.getParent ? parent.getParent() : null;
        }
        if (focusFilter && !grid.state.filters) { spiSetFilters(grid, true); }
        setTimeout(function () {
            api.ensureColumnVisible(colId, 'middle');
            var root = document.getElementById(id) || document;
            setTimeout(function () {
                var header = root.querySelector('.ag-header-cell[col-id="' + colId + '"]');
                var input = focusFilter ? root.querySelector('.ag-floating-filter[col-id="' + colId + '"] input, .ag-floating-filter[col-id="' + colId + '"] select') : null;
                if (input) { input.focus(); }
                [header, input ? input.closest('.ag-floating-filter') : null].forEach(function (cell) {
                    if (!cell) { return; }
                    cell.classList.remove('spiFlash');
                    void cell.offsetWidth;
                    cell.classList.add('spiFlash');
                    setTimeout(function () { cell.classList.remove('spiFlash'); }, 1600);
                });
            }, 60);
        }, 0);
    }
    function spiBuildFilterMenu(grid, api, id, menu) {
        var model = api.getFilterModel() || {};
        var colIds = Object.keys(model);
        if (!colIds.length) {
            spiMenuItem(menu, 'No column filters applied', function () { }, { cls: 'spiMenuNote' });
            return;
        }
        colIds.forEach(function (colId) {
            var column = api.getColumn(colId);
            var def = column ? column.getColDef() : {};
            var header = def.headerName || colId;
            var row = document.createElement('div');
            row.className = 'spiMenuFilter';
            var text = document.createElement('div');
            text.className = 'spiMenuFilterText';
            var name = document.createElement('div'); name.className = 'spiMenuFilterName';
            name.textContent = header + (column && !column.isVisible() ? ' (hidden)' : '');
            var cond = document.createElement('div'); cond.className = 'spiMenuFilterCond';
            cond.textContent = spiDescribeFilter(model[colId]);
            cond.title = cond.textContent;
            text.appendChild(name); text.appendChild(cond);
            row.appendChild(text);
            var goTo = document.createElement('button'); goTo.type = 'button'; goTo.className = 'spiMenuLink'; goTo.textContent = 'Go to'; goTo.title = 'Show the column and focus its filter';
            goTo.addEventListener('click', function () { spiCloseMenu(); spiGoToColumn(grid, api, id, colId); });
            var clear = document.createElement('button'); clear.type = 'button'; clear.className = 'spiMenuLink'; clear.textContent = 'Clear'; clear.title = 'Remove this filter';
            clear.addEventListener('click', function () {
                var result = api.setColumnFilterModel(colId, null);
                var apply = function () { api.onFilterChanged(); row.remove(); if (!menu.querySelector('.spiMenuFilter')) { spiCloseMenu(); } };
                if (result && result.then) { result.then(apply); } else { apply(); }
            });
            row.appendChild(goTo); row.appendChild(clear);
            menu.appendChild(row);
        });
        menu.appendChild(Object.assign(document.createElement('div'), { className: 'spiMenuSep' }));
        spiMenuItem(menu, 'Clear all filters', function () { api.setFilterModel(null); });
    }
    function spiOnFirstDataRendered(api, element) {
        var id = element.id;
        var grid = spiGrids[id];
        if (grid && grid.toolbar) { spiSyncToolbar(grid); }
    }

    var spiPopoutWatchers = Object.create(null);

    /* Opens the grid on its own in a new window. The window is a standalone document that reuses the
       AG Grid assets and the grid definition of this report; only the row data is read from the opener. */
    function spiPopoutGrid(id) {
        var grid = spiGrids[id];
        if (!grid) { return; }
        var title = grid.title;
        var gridDefScriptId = 'spiGridDef_' + grid.key;
        var popout = window.open('', 'spiPopout_' + id, 'width=1600,height=900,resizable=yes,scrollbars=yes');
        if (!popout) {
            alert('The browser blocked the pop out window - please allow pop ups for this page.');
            return;
        }

        var render = function () {
            var assets = '';
            document.querySelectorAll('link[href*="ag-grid"], script[src*="ag-grid"], link[href*="font-awesome"]').forEach(function (element) {
                assets += element.outerHTML;
            });
            /* the brand line at the right of the pop out toolbar: version, tenant and report date, the GitHub and LinkedIn links */
            var brandHtml = '<b>AzADServicePrincipalInsights</b>'
                + ' <a href="https://github.com/JulianHayward/AzADServicePrincipalInsights" target="_blank" rel="noopener" title="AzADServicePrincipalInsights on GitHub" aria-label="AzADServicePrincipalInsights on GitHub"><i class="fa fa-github" aria-hidden="true"></i></a>'
                + ' <a href="https://www.linkedin.com/in/julianhayward" target="_blank" rel="noopener" title="Julian Hayward on LinkedIn" aria-label="Julian Hayward on LinkedIn"><i class="fa fa-linkedin-square" aria-hidden="true"></i></a>';
            ['spiAgGridStyle', 'spiAgGridSelectFilter', 'spiAgGridSupport', gridDefScriptId].forEach(function (id) {
                var element = document.getElementById(id);
                if (element) { assets += element.outerHTML; }
            });
            /* the pop out registers the grid in its own runtime (the copied support script): the row data comes from the
               opener, the toolbar and the grid are built by the same functions as in the report */
            var bootstrap = 'window[' + JSON.stringify('rowData4' + id) + '] = window.opener[' + JSON.stringify('rowData4' + id) + '];'
                + ' spiRegisterGrid(' + JSON.stringify(id) + ', ' + JSON.stringify(grid.key) + ', ' + JSON.stringify(title) + ');'
                + ' var spiPopoutEntry = spiGrids[' + JSON.stringify(id) + '];'
                + ' spiPopoutEntry.popout = true; spiPopoutEntry.filtersTouched = true;'
                + ' spiPopoutEntry.state = ' + JSON.stringify({ wrap: grid.state.wrap, pin: grid.state.pin, filters: true }) + ';'
                + ' spiRenderGridToolbar(' + JSON.stringify(id) + ', document.getElementById("spiPopoutToolbar"));'
                + ' spiCreateGrid(' + JSON.stringify(id) + ');';
            /* review F-51: '<' only occurs inside the JSON strings (the title): as \u003c it cannot end the script element */
            bootstrap = bootstrap.replace(/</g, '\\u003c');
            var mode = document.documentElement.getAttribute('data-ag-theme-mode') || 'light';
            var vars = mode === 'dark'
                ? '--az-radius: 4px; --az-bg: #1b1a19; --az-card-bg: #252423; --az-border: #484644; --az-border-strong: #605e5c; --az-text: #f3f2f1; --az-text-secondary: #c8c6c4; --az-hover: #323130; --az-pressed: #3b3a39; --az-accent: #2899f5; --az-accent-soft: #0c2f4e; --az-shadow-8: 0 3.2px 7.2px rgba(0,0,0,.5), 0 .6px 1.8px rgba(0,0,0,.4);'
                : '--az-radius: 4px; --az-bg: #ffffff; --az-card-bg: #ffffff; --az-border: #e1dfdd; --az-border-strong: #c8c6c4; --az-text: #323130; --az-text-secondary: #605e5c; --az-hover: #f3f2f1; --az-pressed: #edebe9; --az-accent: #0078d4; --az-accent-soft: #deecf9; --az-shadow-8: 0 3.2px 7.2px rgba(0,0,0,.13), 0 .6px 1.8px rgba(0,0,0,.11);';
            var doc = '<!doctype html><html lang="en" data-theme="' + mode + '" data-ag-theme-mode="' + mode + '" data-spi-popout="1"><head><meta charset="utf-8"><title>' + spiEscapeHtml(title) + '</title>'
                + assets
                + '<style>html { ' + vars + ' } html, body { margin: 0; padding: 0; height: 100%; overflow: hidden; background: var(--az-bg); color: var(--az-text); color-scheme: ' + mode + '; }'
                + ' body { display: flex; flex-direction: column; font-family: "Segoe UI", "Segoe UI Web (West European)", -apple-system, BlinkMacSystemFont, Roboto, "Helvetica Neue", Arial, sans-serif; font-size: 12.5px; }'
                + ' #spiPopoutToolbar { flex: 0 0 auto; display: flex; flex-wrap: wrap; align-items: center; gap: 4px 12px; padding: 6px 8px; border-bottom: 1px solid var(--az-border); }'
                + ' #spiPopoutToolbar .spiToolbar { flex: 1 1 auto; }'
                + ' #spiPopoutBrand { flex: 0 0 auto; order: 2; margin-left: auto; font-size: 11.5px; color: var(--az-text-secondary); white-space: nowrap; }'
                + ' #spiPopoutBrand a { color: var(--az-accent); text-decoration: none; font-size: 14px; margin-left: 2px; } #spiPopoutBrand a:hover { text-decoration: none; opacity: .8; }'
                + ' #' + id + ' { flex: 1 1 auto; min-height: 0; width: 100%; }</style>'
                + '</head><body><div id="spiPopoutToolbar"><span id="spiPopoutBrand">' + brandHtml + '</span></div><div id="' + id + '" class="spiGrid"></div>'
                + '<script>' + bootstrap + '<\/script></body></html>';
            popout.document.open();
            popout.document.write(doc);
            popout.document.close();
        };

        popout.document.open();
        popout.document.write('<!doctype html><html lang="en"><head><meta charset="utf-8"><title>' + spiEscapeHtml(title) + '</title></head><body style="font-family: Segoe UI, sans-serif; padding: 16px;">Loading the grid data ...</body></html>');
        popout.document.close();
        popout.focus();
        spiLoadGridData(id).then(function () {
            render();
            //the pop out content is written by this document, so a reload of the pop out would leave it empty
            if (spiPopoutWatchers[id]) { clearInterval(spiPopoutWatchers[id]); }
            spiPopoutWatchers[id] = setInterval(function () {
                try {
                    if (popout.closed) {
                        clearInterval(spiPopoutWatchers[id]);
                        delete spiPopoutWatchers[id];
                        return;
                    }
                    if (popout.document.readyState === 'complete' && !popout.document.getElementById(id)) {
                        render();
                    }
                }
                catch (e) { /* the pop out is navigating, retry on the next tick */ }
            }, 500);
        }).catch(function (error) {
            try { popout.document.body.textContent = error && error.message ? error.message : String(error); } catch (e) { /* ignore */ }
        });
    }
    /* ---------- side panel ----------
       the full value of a cell (double click), docked to the right; works in the report and in the pop out window alike */


    function spiGetCellPanel(doc) {
        var panel = doc.getElementById('spiCellPanel');
        if (panel) { return panel; }
        panel = doc.createElement('div');
        panel.id = 'spiCellPanel';
        panel.innerHTML = '<div class="spiCellPanelHeader"><span class="spiCellPanelTitle"></span>'
            + '<button type="button" class="spiPanelButton spiCellPanelCopy" title="Copy to the clipboard">Copy</button>'
            + '<button type="button" class="spiPanelButton spiCellPanelClose" title="Close">&#x2715;</button></div>'
            + '<pre class="spiCellPanelBody"></pre>';
        panel.querySelector('.spiCellPanelClose').addEventListener('click', function () { panel.style.display = 'none'; });
        panel.querySelector('.spiCellPanelCopy').addEventListener('click', function () {
            var button = this;
            var label = button.textContent;
            var done = function () {
                button.textContent = 'Copied';
                setTimeout(function () { button.textContent = label; }, 1500);
            };
            /* review F-53: navigator.clipboard is undefined outside a secure context (report served over http) */
            if (navigator.clipboard && navigator.clipboard.writeText) {
                navigator.clipboard.writeText(panel.spiCellText || '').then(done, done);
            }
            else {
                var area = doc.createElement('textarea');
                area.value = panel.spiCellText || '';
                doc.body.appendChild(area);
                area.select();
                try { doc.execCommand('copy'); } catch (e) { /* not supported */ }
                area.remove();
                done();
            }
        });
        doc.addEventListener('keydown', function (event) { if (event.key === 'Escape') { panel.style.display = 'none'; } });
        doc.body.appendChild(panel);
        return panel;
    }

    /* content: { title, html } - the full value of a cell */
    function spiShowPanel(doc, content) {
        var panel = spiGetCellPanel(doc);
        var body = panel.querySelector('.spiCellPanelBody');
        var title = content.title || '';
        panel.classList.add('spiPanelText');
        body.innerHTML = content.html || '';
        if (!body.textContent.trim() && !body.querySelector('img')) { body.innerHTML = '<span style="color: var(--az-text-secondary, #605e5c);">(empty cell)</span>'; }
        body.querySelectorAll('mark.filter-match').forEach(function (m) { m.replaceWith(doc.createTextNode(m.textContent)); });
        panel.spiCellText = body.textContent;
        panel.querySelector('.spiCellPanelCopy').textContent = 'Copy';
        panel.querySelector('.spiCellPanelTitle').textContent = title;
        panel.querySelector('.spiCellPanelTitle').title = title;
        panel.style.display = 'flex';
        body.scrollTop = 0;
    }

    /* ---------- AzADSPI: grids in collapsible sections ----------
       A grid created in a hidden container has no size: a grid is created when its section is opened (any click may
       open a section - the section toggles are the report's own scripts - so the visible grids are checked after a click). */
    function spiCreateVisibleGrids() {
        document.querySelectorAll('.spiGrid').forEach(function (element) {
            var grid = spiGrids[element.id];
            if (grid && !grid.ready && element.offsetParent !== null) { spiCreateGrid(element.id); }
        });
    }
    document.addEventListener('click', function () { setTimeout(spiCreateVisibleGrids, 0); });
    document.addEventListener('DOMContentLoaded', spiCreateVisibleGrids);
    /* registration plus the toolbar above the grid */
    function spiRegisterReportGrid(id, key, title) {
        spiRegisterGrid(id, key, title);
        var host = document.getElementById('spiToolbar4' + id);
        if (host) { spiRenderGridToolbar(id, host); }
    }

    /* ---------- AzADSPI: chart click filters ----------
       The charts filter a grid by the clicked slice. The terms are the former TableFilter terms: '[empty]', '[nonempty]',
       '!value' (does not contain) and 'a && b'; select columns match the value exactly, the others by 'contains'. */
    function spiTermToFilterModel(term, isSelect) {
        term = String(term);
        if (term === '[empty]') { return { filterType: 'text', type: 'blank' }; }
        if (term === '[nonempty]') { return { filterType: 'text', type: 'notBlank' }; }
        var condition = function (part) {
            return part.charAt(0) === '!' ? { filterType: 'text', type: 'notContains', filter: part.slice(1) } : { filterType: 'text', type: isSelect ? 'equals' : 'contains', filter: part };
        };
        var parts = term.split(/\s*&&\s*/);
        if (parts.length === 1) { return condition(parts[0]); }
        return { filterType: 'text', operator: 'AND', conditions: parts.map(condition) };
    }
    /* terms: [[column index, term], ...] - the column index of the former table */
    function spiChartFilter(id, terms) {
        return spiCreateGrid(id).then(function (api) {
            var columns = api.getColumns() || [];
            var model = {};
            terms.forEach(function (entry) {
                var column = columns[Number(entry[0])];
                if (!column) { return; }
                model[column.getColId()] = spiTermToFilterModel(entry[1], !!column.getColDef().floatingFilterComponent);
            });
            api.setFilterModel(model);
            return api;
        });
    }
'@
#endregion AG Grid runtime

#date of the report (top bar, Overview, registry)
$spiReportDate = (Get-Date).ToUniversalTime().ToString("yyyy-MM-dd HH:mm:ss 'UTC'", [cultureinfo]::InvariantCulture)
$html = $null
$html += @"
<!doctype html>
<html lang="en">
<head>
    <meta charset="utf-8" />
    <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
    <meta http-equiv="Pragma" content="no-cache" />
    <meta http-equiv="Expires" content="0" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>$($Product)</title>
$(getSpiPortalHeadHtml)
    <!-- third-party libraries: exact versions from public CDNs with Subresource Integrity -->
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/7.3.1/css/all.min.css" integrity="sha512-QeR2VH+lsBE5LSAe1Q5EnTBbe7XTBubt8dG93Y7gidSgdMCr8nVqKcfKAMyN96SV8KDbZVTDXChatu5G2KQGzg==" crossorigin="anonymous" referrerpolicy="no-referrer" />
    <!-- the report uses Font Awesome 4 icon names (fa fa-*) -->
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/7.3.1/css/v4-shims.min.css" integrity="sha512-sDE2ecb/xYM2k7bAcpJnLHaPquu8BHLjYjaLX+ua4d/rwTLFP7+ggQuVb4KBeCs3Z3/w5eqwLJ4B7vufQ+Mt5w==" crossorigin="anonymous" referrerpolicy="no-referrer" />
    <script src="https://cdn.jsdelivr.net/npm/ag-grid-community@36.2.0/dist/ag-grid-community.min.js" integrity="sha512-uFpNwWaA9t4Qeratr3lYNbFE0B23FWNje7H15lJriUHgYxNw4IhLPFBAOJ5EqqqsdKTviDv8byhOab5/ZTF65A==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>
    <style id="spiAgGridStyle">
$spiGridStyle
    </style>
    <script id="spiAgGridSelectFilter">
$spiGridSelectFilter
    </script>
    <script id="spiAgGridSupport">
$spiGridSupportScript
    </script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.6.1/chart.umd.min.js" integrity="sha512-vMRyUfXe50uUhhDC+fk6UYBA/1VPj+I7Jn+wK0A3+wSVeUgT9OCz/xoF2M8KFvrmUOlb4Anv0FQUw8NafuzYPw==" crossorigin="anonymous" referrerpolicy="no-referrer"></script>
</head>
<body>
$(getSpiShellStartHtml -TenantId $azAPICallConf['checkContext'].Tenant.Id -ReportDate $spiReportDate -UpdateHtml $newerVersionAvailableHTML)
"@

#review P-21: the raw API data and the collection lookups are not used by the report build (the output objects in $cu are):
#released before it to lower the peak memory (CSV/JSON are written already)
$memoryBeforeRelease = [System.GC]::GetTotalMemory($false)
#review F-13: also the other variables that still referenced the raw objects (the prefetch results, the SP lists and batches with
#the raw SP as 'Details', the enrichment input, the role assignment and grant lists, the SP lookups with 'spDetails')
foreach ($releasedVariable in 'htServicePrincipalsAndAppsOnlyEnriched', 'htRawSPById', 'htRawAppById', 'htSPOwnersTmp', 'htOwnedByEnriched', 'htOwnedBy', 'getServicePrincipals', 'getServicePrincipalsFromAPI', 'htSpLookup', 'htAllServicePrincipalsById', 'getApplicationsFromAPI', 'htApplicationsByAppId', 'htOauth2PermissionGrantsByClientId',
    'htGraphPrefetch', 'arraySPsAndAppsWithoutSP', 'SPsAndAppsWithoutSPBatch', 'servicePrincipalsAndAppsOnlyEnriched', 'servicePrincipalsAndAppsOnlyEnrichedSPBatch', 'prefetchAADRoleAssignments', 'prefetchAADRoleAssignmentsExpanded', 'prefetchOauth2PermissionGrants',
    'htApplications', 'htAADRoleAssignmentsByPrincipalId', 'htAADRoleAssignmentsByResourceScope', 'htServicePrincipalsAppRoles', 'htServicePrincipalsPublishedPermissionScopes') {
    Set-Variable -Name $releasedVariable -Value $null -Scope Script -ErrorAction SilentlyContinue
}
[System.GC]::Collect()
Write-Host " Memory before the report: $([math]::Round($memoryBeforeRelease / 1MB)) MB -> $([math]::Round([System.GC]::GetTotalMemory($true) / 1MB)) MB (raw collection data released)"

$startSummary = Get-Date

summary

$endSummary = Get-Date
Write-Host " Building Summary duration: $((New-TimeSpan -Start $startSummary -End $endSummary).TotalMinutes) minutes ($((New-TimeSpan -Start $startSummary -End $endSummary).TotalSeconds) seconds)"

#the pages (Overview, Service Principals, ...) with the sections written by function summary
$html += getSpiPagesHtml

$html += @'
    </main>
    </div><!--az-shell-->
'@

#AG Grid: the grid definitions (one per column signature) and the string table of the report
$html += buildSpiGridAssetsBlock

#the section registry of the portal shell (navigation, search, counts, deep links, run parameters)
$html += getSpiRegistryHtml -Report ([ordered]@{
        product = $Product
        version = $ProductVersion
        latestVersion = $(if ($newerVersionAvailable) { $versionOnRepositoryFull } else { $null })
        tenantId = $azAPICallConf['checkContext'].Tenant.Id
        date = $spiReportDate
        name = $fileName
        parameters = @($paramsUsed)
    })

$html += @'
</body>
</html>
'@

$html | Set-Content -LiteralPath "$($outputPath)$($DirectorySeparatorChar)$($fileName).html" -Encoding utf8 -Force

$endBuildHTML = Get-Date
Write-Host "Building HTML total duration: $((New-TimeSpan -Start $startBuildHTML -End $endBuildHTML).TotalMinutes) minutes ($((New-TimeSpan -Start $startBuildHTML -End $endBuildHTML).TotalSeconds) seconds)"
#endregion BuildHTML

#endregion createoutputs

#APITracking
#AzAPICall tracks every request in $azAPICallConf['arrayAPICallTracking']; TargetEndpoint uses AzAPICall's endpoint keys ('ARM', 'MicrosoftGraph')
$apiCallTracking = $azAPICallConf['arrayAPICallTracking']
$APICallTrackingCount = $apiCallTracking.Count
$APICallTrackingManagementCount = $apiCallTracking.where( { $_.TargetEndpoint -eq 'ARM' } ).Count
$APICallTrackingGraphCount = $apiCallTracking.where( { $_.TargetEndpoint -eq 'MicrosoftGraph' } ).Count
#TryCounter is 1 for the first attempt (nextLink pages do not increment it)
$APICallTrackingRetriesCount = $apiCallTracking.where( { $_.TryCounter -gt 1 } ).Count
$APICallTrackingRestartDueToDuplicateNextlinkCounterCount = $apiCallTracking.where( { $_.RestartDueToDuplicateNextlinkCounter -gt 0 } ).Count
Write-Host "$($Product) APICalls total count: $APICallTrackingCount ($APICallTrackingManagementCount ManagementAPI; $APICallTrackingGraphCount MSGraphAPI; $APICallTrackingRetriesCount retries; $APICallTrackingRestartDueToDuplicateNextlinkCounterCount nextLinkReset)"
if ($PerfTracking) {
    #API calls per task type (ids, names and batch numbers removed) to see where the calls go
    Write-Host ' APICalls by task:'
    $apiCallTracking.CurrentTask | ForEach-Object { ($_ -replace '[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}', '<id>' -replace "'[^']*'", "'<name>'" -replace '\(.*\)', '(..)' -replace '#\d+/\d+', '#n').Trim() } | Group-Object | Sort-Object Count -Descending | ForEach-Object {
        Write-Host "  $($_.Count.ToString().PadLeft(6)) $($_.Name)"
    }
}

$endProduct = Get-Date
$durationProduct = New-TimeSpan -Start $startProduct -End $endProduct
Write-Host "$($Product) duration: $(($durationProduct).TotalMinutes) minutes ($(($durationProduct).TotalSeconds) seconds)"

#end
$endTime = (Get-Date).ToString('dd-MMM-yyyy HH:mm:ss', [cultureinfo]::InvariantCulture)
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
#Usage statistics (README: Telemetry). The identifier counts unique tenants: a SHA-256 hash of the product name and the
#tenant ID - one value per tenant, whoever runs the script (no account, no user name); the tenant ID cannot be read
#back from it. With -StatsOptOut only a random ID is sent (a run count, not linked to the tenant or the account).
function getTelemetryIdentifier {
    param(
        [string]
        $TenantId
    )
    $sha256 = [System.Security.Cryptography.SHA256]::Create()
    try {
        $hash = $sha256.ComputeHash([System.Text.Encoding]::UTF8.GetBytes("AzADServicePrincipalInsights|$($TenantId.Trim().ToLowerInvariant())"))
    }
    finally {
        $sha256.Dispose()
    }
    return ([System.BitConverter]::ToString($hash) -replace '-')
}

if (-not $StatsOptOut) {

    $identifier = getTelemetryIdentifier -TenantId ([string]$azAPICallConf['checkContext'].Tenant.Id)

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
    "time": "$((Get-Date).ToUniversalTime().ToString('o'))",
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
            $null = Invoke-WebRequest -Uri 'https://dc.services.visualstudio.com/v2/track' -Method 'POST' -Body $statusBody -TimeoutSec 10
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
    "time": "$((Get-Date).ToUniversalTime().ToString('o'))",
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
            $null = Invoke-WebRequest -Uri 'https://dc.services.visualstudio.com/v2/track' -Method 'POST' -Body $statusBody -TimeoutSec 10
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
