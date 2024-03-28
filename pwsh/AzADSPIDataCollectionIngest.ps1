#version 2.0.0
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

    [Parameter(Mandatory = $false)]
    [int]$ThrottleLimitMonitor = 5
)

Write-Host "Ingesting to Log Analytics Custom Log Table '$($LogAnalyticsCustomLogTableName)'"
Write-Host " DataCollectionRuleSubscriptionId '$($DataCollectionRuleSubscriptionId)'"
Write-Host " DataCollectionRuleResourceGroup '$($DataCollectionRuleResourceGroup)'"
Write-Host " DataCollectionRuleName: '$($DataCollectionRuleName)'"
Write-Host " LogAnalyticsCustomLogTableName: '$($LogAnalyticsCustomLogTableName)'"
Write-Host " ThrottleLimitMonitor: '$($ThrottleLimitMonitor)'"

# Get AzADServicePrincipalInsights JSON files
$AzADSPInsightsJsonFiles = (Get-ChildItem -Path $ImportPath -Recurse -Filter '*.json').FullName
$AzADSPInsightsJsonFilesCount = $AzADSPInsightsJsonFiles.Count
Write-Host "Found $($AzADSPInsightsJsonFilesCount) JSON files in directory '$($ImportPath)'"

if ($AzADSPInsightsJsonFilesCount -eq 0) {
    #may also be handled as an error
    Write-Host 'Nothing to do!?'
}
else {
    $azAPICallConf = initAzAPICall

    $UTC = (Get-Date).ToUniversalTime()
    $logTimeGenerated = $UTC.ToString('o')
    $runId = $UTC.ToString('yyyyMMddHHmmss')
    Write-Host "RunId: $($runId)"

    $currentTask = "Get Data Collection Rule $($DataCollectionRuleName)"
    $uriDCR = "$($azAPICallConf['azAPIEndpointUrls'].ARM)/subscriptions/$($DataCollectionRuleSubscriptionId)/resourceGroups/$($DataCollectionRuleResourceGroup)/providers/Microsoft.Insights/dataCollectionRules/$($DataCollectionRuleName)?api-version=2022-06-01"
    $DCR = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uriDCR -method 'Get' -listenOn Content -currentTask $currentTask

    $dataCollectionEndpointId = $DCR.properties.dataCollectionEndpointId
    $currentTask = "Get Data Collection Endpoint $($dataCollectionEndpointId)"
    $uriDCE = "$($azAPICallConf['azAPIEndpointUrls'].ARM)$($dataCollectionEndpointId)?api-version=2022-06-01"
    $dceResourceJson = AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $uriDCE -method 'Get' -listenOn Content -currentTask $currentTask
    $dceIngestEndpointUrl = $dceResourceJson.properties.logsIngestion.endpoint

    $postUri = "$dceIngestEndpointUrl/dataCollectionRules/$($DCR.properties.immutableId)/streams/Custom-$($LogAnalyticsCustomLogTableName)?api-version=2023-01-01"

    createBearerToken -targetEndPoint 'MonitorIngest' -AzAPICallConfiguration $azAPICallConf

    $batchSize = [math]::ceiling($AzADSPInsightsJsonFilesCount / $ThrottleLimitMonitor)
    Write-Host "Optimal batch size: $($batchSize)"
    $counterBatch = [PSCustomObject] @{ Value = 0 }
    $filesBatch = ($AzADSPInsightsJsonFiles) | Group-Object -Property { [math]::Floor($counterBatch.Value++ / $batchSize) }
    Write-Host "Ingesting data in $($filesBatch.Count) batches"

    $filesBatch | ForEach-Object -Parallel {
        $logTimeGenerated = $using:logTimeGenerated
        $runId = $using:runId
        $postUri = $using:postUri
        $azAPICallConf = $using:azAPICallConf

        $filesProcessCounter = 0
        foreach ($jsonFilePath in $_.Group) {
            $filesProcessCounter++
            $jsonRaw = Get-Content -Path $jsonFilePath -Raw
            try {
                $jsonObject = $jsonRaw | ConvertFrom-Json
                $spInfoObj = [ordered]@{
                    ObjectType = $jsonObject.ObjectType
                    SPDisplayName = $jsonObject.SP.SPDisplayName
                    SPObjectId = $jsonObject.SP.SPObjectId
                    SPAppId = $jsonObject.SP.SPAppId
                }
                if ($jsonObject.APP) {
                    $spInfoObj.APPDisplayName = $jsonObject.APP.APPDisplayName
                    $spInfoObj.APPObjectId = $jsonObject.APP.APPObjectId
                    $spInfoObj.APPAppId = $jsonObject.APP.APPAppClientId
                }
                $spInfo = ($spInfoObj.Keys | ForEach-Object { "$($_)=$($spInfoObj.($_))" }) -join ', '
                # Add TimeGenerated to JSON data
                $jsonObject | Add-Member -NotePropertyName TimeGenerated -NotePropertyValue $logTimeGenerated -Force
                $jsonObject | Add-Member -NotePropertyName RunId -NotePropertyValue $runId -Force
                $jsonRawAsArray = $jsonObject | ConvertTo-Json -AsArray -Compress -Depth 10
            }
            catch {
                Write-Error 'Cannot convert jsonRaw content to jsonObject'
                throw $_
            }

            $currentTask = "Batch#$($_.Name); Process file $($filesProcessCounter)/$($_.Count); Ingesting data for $($spInfo)"
            Write-Host $currentTask
            AzAPICall -AzAPICallConfiguration $azAPICallConf -uri $postUri -method 'Post' -body $jsonRawAsArray -currentTask $currentTask
        }
    } -ThrottleLimit $ThrottleLimitMonitor
}
