#Pester 5+ tests for pwsh/Send-AzADSPILogAnalyticsData.ps1 (review P2, theme 17). No Azure calls: a fake AzAPICall records the requests.

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:IngestPath = Join-Path $script:RepoRoot 'pwsh/Send-AzADSPILogAnalyticsData.ps1'

    function global:AzAPICall {
        param($AzAPICallConfiguration, $uri, $method, $currentTask, $listenOn, $body)
        if ($method -eq 'Post') {
            if ($global:AzADSPITestFailMarker -and $body.Contains($global:AzADSPITestFailMarker)) { throw 'Error - check the last console output for details' }
            $null = $global:AzADSPITestBodies.Add($body); return
        }
        foreach ($key in $global:AzADSPITestFakeResponses.Keys) { if ($uri -like "*$key*") { return $global:AzADSPITestFakeResponses[$key] } }
    }

    #a record nested 12 levels deep (deeper than the former -Depth 10)
    function New-Record([string]$Id, [int]$PaddingBytes) {
        $deep = 'leaf'; for ($i = 0; $i -lt 12; $i++) { $deep = @{ "level$i" = $deep } }
        [ordered]@{ ObjectType = 'SP APP INT'; ObjectId = $Id; SP = @(@{ SPDisplayName = "sp $Id"; SPObjectId = $Id }); Deep = $deep; Padding = ('x' * $PaddingBytes) }
    }

    function Invoke-IngestBlock([string[]]$Files, [int]$MaxRequestBytes) {
        $block = Get-ParallelBlock -Path $script:IngestPath -Marker 'maxRequestBytes = $using:MaxRequestBytes'
        $logTimeGenerated = '2026-11-06T00:00:00.0000000Z'; $runId = '20261106000000'; $postUri = 'https://dce.example/ingest'; $azAPICallConf = @{}
        $htIngestResults = @{ Batches = [System.Collections.ArrayList]::Synchronized([System.Collections.ArrayList]@()); Skipped = [System.Collections.ArrayList]::Synchronized([System.Collections.ArrayList]@()); Failed = [System.Collections.ArrayList]::Synchronized([System.Collections.ArrayList]@()) }
        $global:AzADSPITestBodies = [System.Collections.ArrayList]@()
        $null = [pscustomobject]@{ Name = 0; Group = $Files } | ForEach-Object -Process $block.ScriptBlock 6>$null 3>$null
        $htIngestResults
    }

    $script:Dir = Join-Path ([IO.Path]::GetTempPath()) "azadspi-ingest-$([guid]::NewGuid().ToString('n'))"
    $null = New-Item -ItemType Directory -Path $script:Dir
    $script:Files = foreach ($i in 1..30) { $f = Join-Path $script:Dir "sp$i.json"; New-Record "id$i" 3000 | ConvertTo-Json -Depth 99 | Set-Content -LiteralPath $f; $f }
    $script:Oversized = Join-Path $script:Dir 'big.json'; New-Record 'idBig' 80000 | ConvertTo-Json -Depth 99 | Set-Content -LiteralPath $script:Oversized
}

AfterAll {
    Remove-Item function:global:AzAPICall -ErrorAction SilentlyContinue
    Remove-Variable -Name AzADSPITestBodies, AzADSPITestFailMarker, AzADSPITestFakeResponses -Scope Global -ErrorAction SilentlyContinue
    Remove-Item -LiteralPath $script:Dir -Recurse -Force -ErrorAction SilentlyContinue
}

Describe 'ingest batching' {
    BeforeAll {
        $global:AzADSPITestFailMarker = $null
        $script:Result = Invoke-IngestBlock -Files (@($script:Files) + $script:Oversized) -MaxRequestBytes 50KB
        $script:Bodies = @($global:AzADSPITestBodies)
        $script:Records = @($script:Bodies | ForEach-Object { $_ | ConvertFrom-Json -Depth 100 } | ForEach-Object { $_ })
    }
    It 'sends several records per request (was one request per file)' {
        $script:Bodies.Count | Should -BeLessThan 10
        $script:Bodies.Count | Should -BeGreaterThan 1
    }
    It 'keeps every request within the size limit and sends valid JSON arrays' {
        @($script:Bodies | Where-Object { [Text.Encoding]::UTF8.GetByteCount($_) -gt 50KB }) | Should -BeNullOrEmpty
        @($script:Bodies | Where-Object { -not $_.StartsWith('[') -or -not $_.EndsWith(']') }) | Should -BeNullOrEmpty
    }
    It 'sends each record exactly once, with TimeGenerated and RunId' {
        @($script:Records.ObjectId | Sort-Object) | Should -Be @(1..30 | ForEach-Object { "id$_" } | Sort-Object)
        #raw text: ConvertFrom-Json would turn TimeGenerated into a DateTime
        $joined = $script:Bodies -join ''
        ([regex]::Matches($joined, '"TimeGenerated":"2026-11-06T00:00:00.0000000Z"')).Count | Should -Be 30
        ([regex]::Matches($joined, '"RunId":"20261106000000"')).Count | Should -Be 30
    }
    It 'keeps objects nested deeper than 10 levels (was -Depth 10)' {
        $x = $script:Records[0].Deep; for ($i = 11; $i -ge 0; $i--) { $x = $x."level$i" }
        $x | Should -Be 'leaf'
    }
    It 'skips a record above the size limit and reports it' {
        @($script:Result.Skipped).Count | Should -Be 1
        $script:Result.Skipped[0] | Should -Match 'idBig'
        @($script:Result.Batches | Measure-Object Records -Sum).Sum | Should -Be 30
    }
}

Describe 'ingest failures' {
    It 'F-65: a file that is no JSON is recorded; the other records are still sent' {
        $bad = Join-Path $script:Dir 'bad.json.txt'
        Set-Content -LiteralPath $bad -Value '{ not json'
        try {
            $r = Invoke-IngestBlock -Files @($script:Files[0], $bad, $script:Files[1]) -MaxRequestBytes 900KB
        }
        finally { Remove-Item -LiteralPath $bad -ErrorAction SilentlyContinue }
        $r.Failed.Count | Should -Be 1
        $r.Failed[0] | Should -Match 'cannot be read as JSON'
        ($r.Batches | Measure-Object Records -Sum).Sum | Should -Be 2
    }
    It 'records a failed request and still sends the other requests' {
        $global:AzADSPITestFailMarker = '"id1"'
        $r = Invoke-IngestBlock -Files $script:Files -MaxRequestBytes 20KB
        $global:AzADSPITestFailMarker = $null
        @($r.Failed).Count | Should -Be 1
        @($global:AzADSPITestBodies).Count | Should -BeGreaterThan 0
    }
}

Describe 'ingest endpoint selection' {
    BeforeAll {
        $ast = Get-ScriptAst -Path $script:IngestPath
        $script:EndpointCode = $ast.Find({ param($n) $n -is [System.Management.Automation.Language.IfStatementAst] -and $n.Clauses[0].Item1.Extent.Text -eq '$DCR.properties.endpoints.logsIngestion' }, $true).Extent.Text
        function Get-Endpoint($DCR) { $azAPICallConf = @{ azAPIEndpointUrls = @{ ARM = 'https://management.azure.com' } }; . ([scriptblock]::Create($script:EndpointCode)) 6>$null; $dceIngestEndpointUrl }
    }
    It 'uses the DCR''s own logs ingestion endpoint (kind Direct)' {
        Get-Endpoint ([pscustomobject]@{ properties = [pscustomobject]@{ endpoints = [pscustomobject]@{ logsIngestion = 'https://dcr-direct.ingest.monitor.azure.com' } } }) | Should -Be 'https://dcr-direct.ingest.monitor.azure.com'
    }
    It 'uses the Data Collection Endpoint otherwise' {
        $global:AzADSPITestFakeResponses = @{ '/dataCollectionEndpoints/dce1' = [pscustomobject]@{ properties = [pscustomobject]@{ logsIngestion = [pscustomobject]@{ endpoint = 'https://dce1.ingest.monitor.azure.com' } } } }
        Get-Endpoint ([pscustomobject]@{ properties = [pscustomobject]@{ dataCollectionEndpointId = '/subscriptions/s/resourceGroups/rg/providers/Microsoft.Insights/dataCollectionEndpoints/dce1' } }) | Should -Be 'https://dce1.ingest.monitor.azure.com'
    }
    It 'fails clearly if there is neither' {
        { Get-Endpoint ([pscustomobject]@{ properties = [pscustomobject]@{} }) } | Should -Throw '*neither a logs ingestion endpoint nor a Data Collection Endpoint*'
    }
}

Describe 'ingest ImportPath' {
    It 'F-57: a missing folder or a folder without JSON files is an error (NoJsonExport?)' {
        $empty = Join-Path ([IO.Path]::GetTempPath()) "azadspi-empty-$([guid]::NewGuid().ToString('n'))"
        $null = New-Item -ItemType Directory -Path $empty
        try {
            { & $script:IngestPath -ImportPath $empty -DataCollectionRuleSubscriptionId s -DataCollectionRuleResourceGroup rg -DataCollectionRuleName dcr -LogAnalyticsCustomLogTableName t 6>$null } | Should -Throw '*No JSON files*'
            { & $script:IngestPath -ImportPath (Join-Path $empty 'missing') -DataCollectionRuleSubscriptionId s -DataCollectionRuleResourceGroup rg -DataCollectionRuleName dcr -LogAnalyticsCustomLogTableName t 6>$null } | Should -Throw '*does not exist*'
            { & $script:IngestPath -ImportPath $empty -DataCollectionRuleSubscriptionId s -DataCollectionRuleResourceGroup rg -DataCollectionRuleName dcr -LogAnalyticsCustomLogTableName t -ThrottleLimitMonitor 0 6>$null } | Should -Throw '*ThrottleLimitMonitor*'
        }
        finally { Remove-Item -LiteralPath $empty -Recurse -Force -ErrorAction SilentlyContinue }
    }
    It 'stops if ImportPath contains more than one run (before any Azure call)' {
        $root = Join-Path ([IO.Path]::GetTempPath()) "azadspi-two-runs-$([guid]::NewGuid().ToString('n'))"
        foreach ($d in 'JSON_SP_mg_20261105_100000', 'JSON_SP_mg_20261106_100000') { $null = New-Item -ItemType Directory -Path (Join-Path $root $d) -Force; '{}' | Set-Content -LiteralPath (Join-Path $root "$d/a.json") }
        try {
            { & $script:IngestPath -ImportPath $root -DataCollectionRuleSubscriptionId s -DataCollectionRuleResourceGroup rg -DataCollectionRuleName dcr -LogAnalyticsCustomLogTableName t 6>$null } | Should -Throw '*more than one AzADServicePrincipalInsights run*'
        }
        finally { Remove-Item -LiteralPath $root -Recurse -Force -ErrorAction SilentlyContinue }
    }
}
