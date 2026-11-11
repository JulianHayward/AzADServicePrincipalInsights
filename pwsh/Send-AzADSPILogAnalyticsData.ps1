#version 2.1.0
[CmdletBinding()]
param (
    [Parameter(Mandatory = $true)]
    [string]$ImportPath,

    [Parameter(Mandatory = $true)]
    [string]$DataCollectionRuleSubscriptionId,

    [Parameter(Mandatory = $true)]
    [string]$DataCollectionRuleResourceGroup,

    [Parameter(Mandatory = $true)]
    [string]$DataCollectionRuleName,

    [Parameter(Mandatory = $true)]
    [string]$LogAnalyticsCustomLogTableName,

    #review F-57: 0 divided by zero below
    [Parameter(Mandatory = $false)]
    [ValidateRange(1, 100)]
    [int]$ThrottleLimitMonitor = 5,

    #Logs Ingestion API: max 1 MB per call; records are sent in arrays up to this size (UTF-8 bytes of the JSON body)
    [Parameter(Mandatory = $false)]
    [ValidateRange(10KB, 1MB)]
    [int]$MaxRequestBytes = 900KB
)

Write-Host "Ingesting to Log Analytics Custom Log Table '$($LogAnalyticsCustomLogTableName)'"
Write-Host " DataCollectionRuleSubscriptionId '$($DataCollectionRuleSubscriptionId)'"
Write-Host " DataCollectionRuleResourceGroup '$($DataCollectionRuleResourceGroup)'"
Write-Host " DataCollectionRuleName: '$($DataCollectionRuleName)'"
Write-Host " LogAnalyticsCustomLogTableName: '$($LogAnalyticsCustomLogTableName)'"
Write-Host " ThrottleLimitMonitor: '$($ThrottleLimitMonitor)'"
Write-Host " MaxRequestBytes: '$($MaxRequestBytes)'"

# Get AzADServicePrincipalInsights JSON files
#review F-57: a missing folder or no files is an error (the 'nothing to do' branch was unreachable: @($null) has one element);
#e.g. the main script ran with -NoJsonExport
if (-not (Test-Path -LiteralPath $ImportPath -PathType Container)) {
    throw "ImportPath '$($ImportPath)' does not exist - the ingestion needs the JSON export of AzADServicePrincipalInsights (parameter NoJsonExport not set)"
}
$AzADSPInsightsJsonFiles = @(Get-ChildItem -LiteralPath $ImportPath -Recurse -Filter '*.json' | ForEach-Object { $_.FullName })
$AzADSPInsightsJsonFilesCount = $AzADSPInsightsJsonFiles.Count
Write-Host "Found $($AzADSPInsightsJsonFilesCount) JSON files in directory '$($ImportPath)'"

#one run per ingestion: a parent folder with several JSON_SP_* folders (e.g. local runs with timestamps) would ingest every run
$jsonDirectories = @($AzADSPInsightsJsonFiles | ForEach-Object { Split-Path -Parent $_ } | Sort-Object -Unique)
if ($jsonDirectories.Count -gt 1) {
    Write-Host "The JSON files are in $($jsonDirectories.Count) different folders:"
    $jsonDirectories | Select-Object -First 10 | ForEach-Object { Write-Host "  $_" }
    throw "ImportPath '$($ImportPath)' contains more than one AzADServicePrincipalInsights run - use the JSON_SP_* folder of one run"
}

if ($AzADSPInsightsJsonFilesCount -eq 0) {
    throw "No JSON files in '$($ImportPath)' - the ingestion needs the JSON export of AzADServicePrincipalInsights (parameter NoJsonExport not set)"
}
else {
    #the subscription of the Data Collection Rule as context: no check of the default subscription of the sign-in (it may have none)
    $azAPICallConf = initAzAPICall -SubscriptionId4AzContext $DataCollectionRuleSubscriptionId

    $UTC = (Get-Date).ToUniversalTime()
    $logTimeGenerated = $UTC.ToString('o')
    $runId = $UTC.ToString('yyyyMMddHHmmss')
    Write-Host "RunId: $($runId)"

    #api-version 2024-03-11 (since 2023-03-11): the DCR has its own logs ingestion endpoint (properties.endpoints.logsIngestion) if it was created without
    #a Data Collection Endpoint ('kind: Direct'); 2022-06-01 does not return it
    $currentTask = "Get Data Collection Rule $($DataCollectionRuleName)"
    $uriDCR = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/subscriptions/$($DataCollectionRuleSubscriptionId)/resourceGroups/$($DataCollectionRuleResourceGroup)/providers/Microsoft.Insights/dataCollectionRules/$($DataCollectionRuleName)?api-version=2024-03-11"
    try {
        $DCR = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uriDCR -method 'Get' -listenOn Content -currentTask $currentTask
    }
    catch {
        #reading the rule needs Microsoft.Insights/dataCollectionRules/read - 'Monitoring Metrics Publisher' has no read permission
        throw "The Data Collection Rule '$($DataCollectionRuleName)' (subscription '$($DataCollectionRuleSubscriptionId)', resource group '$($DataCollectionRuleResourceGroup)') could not be read ($($_.Exception.Message)). Check the three values in the variables file; on 403 / AuthorizationFailed: the identity '$($azAPICallConf['checkContext'].Account.Id)' needs the role 'Monitoring Reader' on the rule (the template assigns it with principalId) - a new role assignment can take up to about 30 minutes to work. Guide: https://github.com/JulianHayward/AzADServicePrincipalInsights/blob/main/setup/log-analytics.md#if-something-doesnt-work"
    }
    if (-not $DCR.properties) {
        throw "The Data Collection Rule '$($DataCollectionRuleName)' could not be read (empty response) - check DataCollectionRuleSubscriptionId, DataCollectionRuleResourceGroup and DataCollectionRuleName"
    }

    #the stream of the table and its columns: a property without a column is dropped by the rule without an error (e.g. a rule of an
    #earlier version)
    $streamName = "Custom-$($LogAnalyticsCustomLogTableName)"
    $streamDeclaration = $DCR.properties.streamDeclarations.$streamName
    if (-not $streamDeclaration) {
        throw "The Data Collection Rule '$($DataCollectionRuleName)' has no stream '$($streamName)' (streams: $(@($DCR.properties.streamDeclarations.PSObject.Properties.Name) -join ', ')) - LogAnalyticsCustomLogTableName must be the tableName of the template deployment"
    }
    $streamColumns = @($streamDeclaration.columns.name)
    #the properties of the first record (an unreadable file is reported by the batches below)
    $recordProperties = try { @((Get-Content -LiteralPath $AzADSPInsightsJsonFiles[0] -Raw | ConvertFrom-Json).PSObject.Properties.Name) + 'TimeGenerated', 'RunId' } catch { @() }
    $missingColumns = @($recordProperties.where({ $_ -cnotin $streamColumns }))
    if ($missingColumns.Count -gt 0) {
        Write-Warning "The Data Collection Rule '$($DataCollectionRuleName)' has no column for: $($missingColumns -join ', ') - these values are dropped. Deploy setup/log-analytics-dcr.json of this version (setup/log-analytics.md)"
        if ($env:GITHUB_ACTIONS -eq 'true') { Write-Host "::warning::Data Collection Rule '$($DataCollectionRuleName)': no column for $($missingColumns -join ', ') - deploy setup/log-analytics-dcr.json of this version" }
        elseif ($env:TF_BUILD -eq 'True') { Write-Host "##vso[task.logissue type=warning]Data Collection Rule '$($DataCollectionRuleName)': no column for $($missingColumns -join ', ') - deploy setup/log-analytics-dcr.json of this version" }
    }

    if ($DCR.properties.endpoints.logsIngestion) {
        $dceIngestEndpointUrl = $DCR.properties.endpoints.logsIngestion
        Write-Host " Using the Data Collection Rule's own logs ingestion endpoint"
    }
    elseif ($DCR.properties.dataCollectionEndpointId) {
        $dataCollectionEndpointId = $DCR.properties.dataCollectionEndpointId
        $currentTask = "Get Data Collection Endpoint $($dataCollectionEndpointId)"
        $uriDCE = "$($azAPICallConf['azAPIEndpointUrls'].ARM)$($dataCollectionEndpointId)?api-version=2024-03-11"
        $dceResourceJson = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uriDCE -method 'Get' -listenOn Content -currentTask $currentTask
        $dceIngestEndpointUrl = $dceResourceJson.properties.logsIngestion.endpoint
        Write-Host " Using Data Collection Endpoint '$($dataCollectionEndpointId)'"
    }
    else {
        #before: an empty DCE id produced a malformed URL and an unclear error
        throw "Data Collection Rule '$($DataCollectionRuleName)' has neither a logs ingestion endpoint nor a Data Collection Endpoint"
    }

    $postUri = "$dceIngestEndpointUrl/dataCollectionRules/$($DCR.properties.immutableId)/streams/$($streamName)?api-version=2023-01-01"

    createBearerToken -targetEndPoint 'MonitorIngest' -AzAPICallConfiguration $azAPICallConf

    $batchSize = [math]::ceiling($AzADSPInsightsJsonFilesCount / $ThrottleLimitMonitor)
    Write-Host "Optimal batch size: $($batchSize)"
    $counterBatch = [PSCustomObject] @{ Value = 0 }
    #@(): a single group has a Count of its own (the number of files)
    $filesBatch = @($AzADSPInsightsJsonFiles | Group-Object -Property { [math]::Floor($counterBatch.Value++ / $batchSize) })
    Write-Host "Ingesting data in $($filesBatch.Count) batches"

    #results per batch (summed at the end: '+=' on a shared counter is not atomic across runspaces)
    $htIngestResults = @{ Batches = [System.Collections.ArrayList]::Synchronized([System.Collections.ArrayList]@()); Skipped = [System.Collections.ArrayList]::Synchronized([System.Collections.ArrayList]@()); Failed = [System.Collections.ArrayList]::Synchronized([System.Collections.ArrayList]@()) }

    $filesBatch | ForEach-Object -Parallel {
        $logTimeGenerated = $using:logTimeGenerated
        $runId = $using:runId
        $postUri = $using:postUri
        $azAPICallConf = $using:azAPICallConf
        $maxRequestBytes = $using:MaxRequestBytes
        $htIngestResults = $using:htIngestResults

        #one POST per array of records up to $maxRequestBytes (was: one POST per file)
        $chunk = [System.Collections.Generic.List[string]]::new()
        $chunkBytes = 2   #'[' and ']'
        $chunkInfo = [System.Collections.Generic.List[string]]::new()
        $batchRecords = 0
        $batchRequests = 0
        $postChunk = {
            if ($chunk.Count -eq 0) {
                return
            }
            $body = '[' + ($chunk -join ',') + ']'
            $currentTask = "Batch#$($_.Name); Ingesting $($chunk.Count) records ($([math]::Round($chunkBytes / 1KB)) KB)"
            Write-Host $currentTask
            try {
                $null = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $postUri -method 'Post' -body $body -currentTask $currentTask
                $batchRecords += $chunk.Count
                $batchRequests++
            }
            catch {
                $null = $htIngestResults.Failed.Add("$($chunk.Count) records ($($chunkInfo -join '; ')): $($_.Exception.Message)")
            }
            #dot-sourced: these are the variables of the batch scope
            $chunk.Clear(); $chunkInfo.Clear(); $chunkBytes = 2
        }

        foreach ($jsonFilePath in $_.Group) {
            $jsonRaw = Get-Content -LiteralPath $jsonFilePath -Raw
            try {
                $jsonObject = $jsonRaw | ConvertFrom-Json
                $spInfo = "$($jsonObject.ObjectType) $($jsonObject.SP.SPDisplayName)$($jsonObject.APP.APPDisplayName) $($jsonObject.ObjectId)"
                # Add TimeGenerated to JSON data
                $jsonObject | Add-Member -NotePropertyName TimeGenerated -NotePropertyValue $logTimeGenerated -Force
                $jsonObject | Add-Member -NotePropertyName RunId -NotePropertyValue $runId -Force
                #-Depth 100: the main script writes the JSON with -Depth 99 (-Depth 10 would cut deeper objects to type names)
                #ASCII only (\uXXXX): the request has no charset - PowerShell before 7.4 does not send UTF-8 by default
                $record = $jsonObject | ConvertTo-Json -Compress -Depth 100 -EscapeHandling EscapeNonAscii
            }
            catch {
                #review F-65: recorded (the throw ended the batch silently with ErrorActionPreference Continue: its unsent records were
                #dropped and the script reported success); the other files of the batch are still sent
                $null = $htIngestResults.Failed.Add("$($jsonFilePath): cannot be read as JSON ($($_.Exception.Message))")
                continue
            }

            $recordBytes = [System.Text.Encoding]::UTF8.GetByteCount($record) + 1   #+1 for the ','
            if ($recordBytes + 2 -gt $maxRequestBytes) {
                #a single record above the API limit would be rejected
                Write-Warning "Skipping $($spInfo): $([math]::Round($recordBytes / 1KB)) KB exceeds the maximum request size ($([math]::Round($maxRequestBytes / 1KB)) KB)"
                $null = $htIngestResults.Skipped.Add("$($spInfo) ($([math]::Round($recordBytes / 1KB)) KB)")
                continue
            }
            if ($chunkBytes + $recordBytes -gt $maxRequestBytes) {
                . $postChunk
            }
            $chunk.Add($record)
            $chunkInfo.Add($spInfo)
            $chunkBytes += $recordBytes
        }
        . $postChunk
        $null = $htIngestResults.Batches.Add([PSCustomObject]@{ Records = $batchRecords; Requests = $batchRequests })
    } -ThrottleLimit $ThrottleLimitMonitor

    $ingestedRecords = [int]($htIngestResults.Batches | Measure-Object Records -Sum).Sum
    Write-Host "Ingested $($ingestedRecords) of $($AzADSPInsightsJsonFilesCount) records in $(($htIngestResults.Batches | Measure-Object Requests -Sum).Sum) requests"
    if ($htIngestResults.Skipped.Count -gt 0) {
        Write-Warning "$($htIngestResults.Skipped.Count) records skipped (too large): $($htIngestResults.Skipped -join ', ')"
    }
    if ($htIngestResults.Failed.Count -gt 0) {
        $htIngestResults.Failed | ForEach-Object { Write-Host "  failed: $_" -ForegroundColor Red }
        #a send request failed (not only unreadable files): AzAPICall printed the status above and threw a generic message - the most
        #common cause is a 403 'does not have access to ingest data' (missing role, or a role assignment that is not active yet)
        if (@($htIngestResults.Failed | Where-Object { $_ -notmatch 'cannot be read as JSON' }).Count -gt 0) {
            $signedInAs = $azAPICallConf['checkContext'].Account.Id
            Write-Host "If the output above shows 403 / 'does not have access to ingest data': the identity '$($signedInAs)' needs the role 'Monitoring Metrics Publisher' on the Data Collection Rule '$($DCR.id)' (or its resource group). A new role assignment can take up to about 30 minutes to work - then run it again. Guide: https://github.com/JulianHayward/AzADServicePrincipalInsights/blob/main/setup/log-analytics.md#step-2---allow-your-pipeline-identity-to-send" -ForegroundColor Yellow
        }
        throw "$($htIngestResults.Failed.Count) ingestion requests or files failed"
    }
    #review F-65: every file is ingested or skipped (too large) - anything else is an error, also outside pipelines
    if ($ingestedRecords + $htIngestResults.Skipped.Count -ne $AzADSPInsightsJsonFilesCount) {
        throw "Ingested $($ingestedRecords) and skipped $($htIngestResults.Skipped.Count) of $($AzADSPInsightsJsonFilesCount) records - $($AzADSPInsightsJsonFilesCount - $ingestedRecords - $htIngestResults.Skipped.Count) records are missing (a batch ended with an error)"
    }
}
