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
    $azAPICallConf = initAzAPICall

    $UTC = (Get-Date).ToUniversalTime()
    $logTimeGenerated = $UTC.ToString('o')
    $runId = $UTC.ToString('yyyyMMddHHmmss')
    Write-Host "RunId: $($runId)"

    #api-version 2024-03-11 (since 2023-03-11): the DCR has its own logs ingestion endpoint (properties.endpoints.logsIngestion) if it was created without
    #a Data Collection Endpoint ('kind: Direct'); 2022-06-01 does not return it
    $currentTask = "Get Data Collection Rule $($DataCollectionRuleName)"
    $uriDCR = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/subscriptions/$($DataCollectionRuleSubscriptionId)/resourceGroups/$($DataCollectionRuleResourceGroup)/providers/Microsoft.Insights/dataCollectionRules/$($DataCollectionRuleName)?api-version=2024-03-11"
    $DCR = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uriDCR -method 'Get' -listenOn Content -currentTask $currentTask

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

    $postUri = "$dceIngestEndpointUrl/dataCollectionRules/$($DCR.properties.immutableId)/streams/Custom-$($LogAnalyticsCustomLogTableName)?api-version=2023-01-01"

    createBearerToken -targetEndPoint 'MonitorIngest' -AzAPICallConfiguration $azAPICallConf

    $batchSize = [math]::ceiling($AzADSPInsightsJsonFilesCount / $ThrottleLimitMonitor)
    Write-Host "Optimal batch size: $($batchSize)"
    $counterBatch = [PSCustomObject] @{ Value = 0 }
    $filesBatch = ($AzADSPInsightsJsonFiles) | Group-Object -Property { [math]::Floor($counterBatch.Value++ / $batchSize) }
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
                $record = $jsonObject | ConvertTo-Json -Compress -Depth 100
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
        throw "$($htIngestResults.Failed.Count) ingestion requests or files failed"
    }
    #review F-65: every file is ingested or skipped (too large) - anything else is an error, also outside pipelines
    if ($ingestedRecords + $htIngestResults.Skipped.Count -ne $AzADSPInsightsJsonFilesCount) {
        throw "Ingested $($ingestedRecords) and skipped $($htIngestResults.Skipped.Count) of $($AzADSPInsightsJsonFilesCount) records - $($AzADSPInsightsJsonFilesCount - $ingestedRecords - $htIngestResults.Skipped.Count) records are missing (a batch ended with an error)"
    }
}
