#Pester 5+ tests for parallel processing (review P1-21): one failing item must not abort the run or lose its batch.

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    foreach ($name in 'getParallelBatchSize', 'checkParallelFailures', 'writePipelineWarning') {
        $fn = $ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name }, $true)
        if (-not $fn) { throw "$name not found in main script" }
        . ([scriptblock]::Create($fn.Extent.Text))
    }

    function global:AzAPICall {
        param($AzAPICallConfiguration, $uri, $method, $currentTask, $listenOn, $consistencyLevel, $noPaging, $skipOnErrorCode, $body, $validateAccess)
        if ($uri -like '*/servicePrincipals/sp-bad/*') { throw 'Error - check the last console output for details' }
        return @()
    }
}

AfterAll {
    Remove-Item function:global:AzAPICall -ErrorAction SilentlyContinue
}

Describe 'P1-21 getParallelBatchSize' {
    It 'creates about 4 batches per thread: <Items> items / throttle <Throttle> -> <Expected>' -ForEach @(
        @{ Items = 1198; Throttle = 30; Expected = 10 }
        @{ Items = 10; Throttle = 30; Expected = 1 }
        @{ Items = 0; Throttle = 30; Expected = 1 }
        @{ Items = 100000; Throttle = 30; Expected = 50 }
    ) {
        getParallelBatchSize -itemCount $Items -throttleLimit $Throttle | Should -Be $Expected
    }
}

Describe 'P1-21 checkParallelFailures' {
    BeforeEach { $htParallelFailures = [hashtable]::Synchronized(@{}) }
    It 'reports and continues when few items failed' {
        1..3 | ForEach-Object { $htParallelFailures["p|$_"] = [pscustomobject]@{ Phase = 'p'; Item = "$_"; Error = 'x' } }
        { checkParallelFailures -phase 'p' -itemCount 1000 6>$null } | Should -Not -Throw
    }
    It 'stops when more than max(10, 5%) items failed' {
        1..60 | ForEach-Object { $htParallelFailures["p|$_"] = [pscustomobject]@{ Phase = 'p'; Item = "$_"; Error = 'x' } }
        { checkParallelFailures -phase 'p' -itemCount 1000 6>$null } | Should -Throw '*too many failed items (60 of 1000, maximum 50)*'
    }
    It 'only counts failures of its own phase' {
        1..60 | ForEach-Object { $htParallelFailures["other|$_"] = [pscustomobject]@{ Phase = 'other'; Item = "$_"; Error = 'x' } }
        { checkParallelFailures -phase 'p' -itemCount 1000 6>$null } | Should -Not -Throw
    }
    It 'R-4: stops when all <Count> items of a small phase failed' -ForEach @(@{ Count = 1 }, @{ Count = 8 }, @{ Count = 10 }) {
        1..$Count | ForEach-Object { $htParallelFailures["p|$_"] = [pscustomobject]@{ Phase = 'p'; Item = "$_"; Error = 'x' } }
        { checkParallelFailures -phase 'p' -itemCount $Count 6>$null } | Should -Throw "*all $Count items failed*"
    }
    It 'R-4: continues when some items of a small phase failed' {
        1..3 | ForEach-Object { $htParallelFailures["p|$_"] = [pscustomobject]@{ Phase = 'p'; Item = "$_"; Error = 'x' } }
        { checkParallelFailures -phase 'p' -itemCount 8 6>$null } | Should -Not -Throw
    }
    It 'R-4: still stops above max(10, 5%) in a large phase' {
        1..11 | ForEach-Object { $htParallelFailures["p|$_"] = [pscustomobject]@{ Phase = 'p'; Item = "$_"; Error = 'x' } }
        { checkParallelFailures -phase 'p' -itemCount 100 6>$null } | Should -Throw '*too many failed items (11 of 100, maximum 10)*'
    }
}

Describe 'R-7 every parallel block that calls AzAPICall uses the AzAPICall version of the main runspace' {
    It 'imports the module from the path of the loaded version; blocks without API calls do not (P-4)' {
        $source = Get-Content -Raw -LiteralPath $script:MainScriptPath
        $ast = Get-ScriptAst -Path $script:MainScriptPath
        $import = 'if (-not (Get-Module -Name $using:AzAPICallModuleName)) { Import-Module -Name $using:azAPICallModuleImportPath -ErrorAction Stop }'
        $parallelBlocks = @($ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.CommandAst] -and $n.GetCommandName() -eq 'ForEach-Object' -and @($n.CommandElements | Where-Object { $_ -is [System.Management.Automation.Language.CommandParameterAst] -and $_.ParameterName -eq 'Parallel' }).Count -gt 0 }, $true))
        $parallelBlocks.Count | Should -BeGreaterThan 4
        $withApi = 0
        foreach ($block in $parallelBlocks) {
            $text = $block.Extent.Text
            if ($text -match 'AzAPICall -AzAPICallConfiguration') {
                $withApi++
                $text.Contains($import) | Should -BeTrue -Because $text.Substring(0, 80)
            }
            else {
                $text.Contains($import) | Should -BeFalse -Because $text.Substring(0, 80)
            }
        }
        $withApi | Should -BeGreaterThan 3
        $source | Should -Match '\$azAPICallModuleImportPath = Join-Path -Path \$azAPICallModuleLoaded\.ModuleBase'
    }
}

Describe 'P-8 getParallelBatches' {
    BeforeAll {
        $ast2 = Get-ScriptAst -Path $script:MainScriptPath
        . ([scriptblock]::Create($ast2.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'getParallelBatches' }, $true).Extent.Text))
    }
    It '<Count> items in batches of <Size>: the same groups as Group-Object with the counter' -ForEach @(
        @{ Count = 0; Size = 3 }, @{ Count = 1; Size = 3 }, @{ Count = 7; Size = 3 }, @{ Count = 9; Size = 3 }, @{ Count = 25; Size = 50 }, @{ Count = 1000; Size = 12 }
    ) {
        $items = @(1..$Count | Where-Object { $Count -gt 0 } | ForEach-Object { "item$_" })
        $counter = [PSCustomObject]@{ Value = 0 }
        $expected = @($items | Group-Object -Property { [math]::Floor($counter.Value++ / $Size) } | ForEach-Object { , @($_.Group) })
        $actual = @(getParallelBatches -Items $items -BatchSize $Size)
        $actual.Count | Should -Be $expected.Count
        for ($b = 0; $b -lt $actual.Count; $b++) {
            ($actual[$b].Group -join ',') | Should -BeExactly ($expected[$b] -join ',')
        }
    }
    It 'a hashtable key collection and a single item' {
        $ht = @{ a = 1; b = 2; c = 3 }
        @(getParallelBatches -Items $ht.Keys -BatchSize 2).Count | Should -Be 2
        $single = @(getParallelBatches -Items 'x' -BatchSize 5)
        $single.Count | Should -Be 1
        $single[0].Group.Count | Should -Be 1
    }
}

Describe 'P1-21 a failing SP does not abort the SP collection or lose the rest of its batch' {
    BeforeAll {
        $block = Get-ParallelBlock -Path $script:MainScriptPath -Marker 'AppWithoutSP_'
        foreach ($name in $block.UsingNames) { Set-Variable -Name $name -Value ([hashtable]::Synchronized(@{})) }
        $skipApplicationRequests = $false   #R-22 flag (not a hashtable)
        $htApplicationsByAppId = $null; $htAADRoleAssignmentsByPrincipalId = $null; $htAADRoleAssignmentsByResourceScope = $null; $htOauth2PermissionGrantsByClientId = $null
        $azAPICallConf = @{ azAPIEndpointUrls = @{ MicrosoftGraph = 'https://graph.microsoft.com' }; checkContext = @{ Tenant = @{ Id = 'tenant1' } } }
        $indicator = 100
        $ErrorActionPreference = 'Stop'
        $items = foreach ($id in 'sp-bad', 'sp-ok') { [pscustomobject]@{ SPOrAppWithoutSP = 'SP'; Details = [pscustomobject]@{ id = $id; appId = "app-$id"; displayName = $id; servicePrincipalType = 'ManagedIdentity'; appOwnerOrganizationId = $null; alternativeNames = @() } } }
        $script:Threw = $null
        try { $null = [pscustomobject]@{ Group = @($items) } | ForEach-Object -Process $block.ScriptBlock 6>$null } catch { $script:Threw = $_ }
        $script:Failures = $htParallelFailures
        $script:Enriched = $htServicePrincipalsAndAppsOnlyEnriched
    }
    It 'does not throw' {
        $script:Threw | Should -BeNullOrEmpty
    }
    It 'records the failed SP' {
        #(.Item on an array would be the array's indexer, hence ForEach-Object)
        @($script:Failures.Values | ForEach-Object { $_.Item }) | Should -Be @('sp-bad (sp-bad)')
        @($script:Failures.Values | ForEach-Object { $_.Phase }) | Should -Be @('ServicePrincipal collection')
    }
    It 'still processes the next SP of the same batch' {
        $script:Enriched.ContainsKey('sp-ok') | Should -BeTrue
        $script:Enriched['sp-ok'].SPOrAppOnly | Should -Be 'SP'
    }
}
