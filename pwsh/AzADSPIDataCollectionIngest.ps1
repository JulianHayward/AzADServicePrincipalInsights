[CmdletBinding()]
param (
    [Parameter(Mandatory = $true)]
    [string]$ImportPath
    ,
    [Parameter(Mandatory = $true)]
    [System.String]$DataCollectionSubscriptionId
    ,
    [Parameter(Mandatory = $true)]
    [System.String]$DataCollectionResourceGroup
    ,
    [Parameter(Mandatory = $true)]
    [System.String]$DataCollectionEndpointName
    ,
    [Parameter(Mandatory = $false)]
    [System.String]$TenantId = (Get-AzContext).Tenant.Id
    ,
    [Parameter(Mandatory = $False)]
    [System.String]$TableName = 'AzADServicePrincipalInsights_CL'
    ,
    [Parameter(Mandatory = $false)]
    [System.Boolean]$SampleDataOnly = $false
)

# Getting all content from AzADServicePrincipalInsights
$AzADSPInsights = (Get-ChildItem -Path $ImportPath -Recurse -Filter '*.json').FullName

Write-Output $($DataCollectionEndpointName)
Write-Output $($DataCollectionResourceGroup)

$AzADSPInsights | ForEach-Object -Parallel {

    #region Define function to Call Ingestion API (outsourced from EntraOps PowerShell Module)
    function Push-AzADSPILogsIngestionAPI {

        [CmdletBinding()]
        param (
            [Parameter(Mandatory = $True)]
            [object]$JsonContent
            ,
            [Parameter(Mandatory = $True)]
            [System.String]$DataCollectionSubscriptionId
            ,
            [Parameter(Mandatory = $True)]
            [System.String]$DataCollectionResourceGroup
            ,
            [Parameter(Mandatory = $True)]
            [System.String]$DataCollectionEndpointName
            ,
            [Parameter(Mandatory = $true)]
            [System.String]$TableName
            ,
            [Parameter(Mandatory = $false)]
            [System.Boolean]$SampleDataOnly = $false
        )

        # Azure Connection
        Set-AzContext -SubscriptionId $DataCollectionSubscriptionId | Out-Null

        # Authentication
        $AccessToken = (Get-AzAccessToken -ResourceUrl 'https://monitor.azure.com/').Token
        $headers = @{'Authorization' = "Bearer $AccessToken"; 'Content-Type' = 'application/json' }

        # Add Timestamp to JSON data
        try {
            $json = $JsonContent | ConvertFrom-Json -Depth 10
            Write-Output "$($json.SP.SPDisplayName)"
            $json | ForEach-Object {
                $_ | Add-Member -NotePropertyName TimeGenerated -NotePropertyValue (Get-Date).ToUniversalTime().ToString('o') -Force
            }
            $json = $json | ConvertTo-Json -AsArray -Depth 10
        }
        catch {
            Write-Error 'Cannot convert JSON content to JSON object'
            throw $_
        }

        if ($SampleDataOnly -eq $false) {
            # Data Collection Endpoint
            $DceArmEndpoint = 'https://management.azure.com' + '/subscriptions/' + $DataCollectionSubscriptionId + '/resourceGroups/' + $DataCollectionResourceGroup + '/providers/Microsoft.Insights/dataCollectionEndpoints/' + $DataCollectionEndpointName + '?api-version=2022-06-01'
            $DceResourceJson = ((Invoke-AzRestMethod -Method 'Get' -Uri $DceArmEndpoint).Content | ConvertFrom-Json).properties
            $DceIngestEndpointUrl = $DceResourceJson.logsIngestion.endpoint

            if ($DceIngestEndpointUrl -eq $null) {
                Write-Error 'No DCE endpoint found!'
            }

            # Table information
            $DataFlows = 'Custom-' + $TableName

            # Data Collection Rule
            $Dcr = Get-AzDataCollectionRule | Where-Object { $_.DataFlows.Streams -eq $DataFlows }

            if ($Dcr -eq $Null) {
                Write-Error 'No data collection rule found!'
            }
            $DcrArmEndpoint = 'https://management.azure.com' + $Dcr.Id + '?api-version=2022-06-01'
            $DcrResourceJson = ((Invoke-AzRestMethod -Method Get -Uri $($DcrArmEndpoint)).Content | ConvertFrom-Json).properties

            # Post Information
            $PostUri = "$DceIngestEndpointUrl/dataCollectionRules/$($DcrResourceJson.immutableId)/streams/$($DataFlows)?api-version=2021-11-01-preview"

            $uploadResponse = Invoke-RestMethod -Uri $PostUri -Method 'Post' -Body $json -Headers $headers -Verbose

            # Let's see how the response looks
            Write-Output $uploadResponse
            Write-Output '---------------------'
        }
        else {
            return $json
        }
    }
    #endregion

    $Json = Get-Content -Path $_
    Push-AzADSPILogsIngestionAPI `
        -TableName $using:TableName `
        -JsonContent $json `
        -DataCollectionSubscriptionId $using:DataCollectionSubscriptionId `
        -DataCollectionResourceGroup $using:DataCollectionResourceGroup `
        -DataCollectionEndpointName $using:DataCollectionEndpointName `
        -SampleDataOnly $using:SampleDataOnly
}