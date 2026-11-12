#Review R-4: items that failed during the collection are shown on the report overview and as a pipeline warning

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    $cls = $ast.Find({ param($n) $n -is [System.Management.Automation.Language.TypeDefinitionAst] -and $n.Name -eq 'HtmlSafe' }, $true)
    #loads the class definition of the script under test
    Invoke-Expression $cls.Extent.Text # DevSkim: ignore DS104456
    foreach ($name in 'addFindingsOverview', 'writePipelineWarning', 'checkParallelFailures') {
        . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name }, $true).Extent.Text))
    }
    $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    #stand-ins for the report builders
    $script:Builder = [System.Text.StringBuilder]::new()
    function useSpiPage { param($Id) $script:Builder }
    function useSpiSection { param($Id, [switch]$Open) $script:Builder }
    function getSpiCardHeadHtml { param($TitleHtml, [switch]$Open) '' }
    $script:EnvNames = @('SYSTEM_TEAMPROJECTID', 'BUILD_REPOSITORY_ID', 'GITHUB_ACTIONS')
}

Describe 'R-4 overview warning' {
    BeforeEach { $null = $script:Builder.Clear() }
    It 'no failures: no warning' {
        addFindingsOverview -overviewRows @() -keyFigures @() -collectionFailures @()
        $script:Builder.ToString() | Should -Not -Match 'spiCollectionFailures'
    }
    It 'failures: a warning with the count per phase and the failed items, encoded' {
        $failures = @(
            [pscustomobject]@{ Phase = 'Subscriptions custom data'; Item = 'sub-1'; Error = 'AuthorizationFailed <b>' }
            [pscustomobject]@{ Phase = 'Subscriptions custom data'; Item = 'sub-2'; Error = 'x' }
            [pscustomobject]@{ Phase = 'ServicePrincipal enrichment'; Item = 'sp <img src=x>'; Error = 'y' }
        )
        addFindingsOverview -overviewRows @() -keyFigures @() -collectionFailures $failures
        $html = $script:Builder.ToString()
        $html | Should -Match 'id="spiCollectionFailures"'
        $html | Should -Match '3 item\(s\) failed during the collection'
        $html | Should -Match 'ServicePrincipal enrichment: 1; Subscriptions custom data: 2'
        $html | Should -Match 'sp &lt;img src=x&gt;'
        $html | Should -Match 'AuthorizationFailed &lt;b&gt;'
        $html | Should -Not -Match '<img src=x>'
    }
    It 'the report passes the failures of the run' {
        $script:Source | Should -Match '-keyFigures \$keyFigures -collectionFailures @\(\$htParallelFailures\.Values\)'
    }
}

Describe 'R-4 pipeline warning' {
    BeforeEach {
        $script:Saved = @{}
        foreach ($n in $EnvNames) { $Saved[$n] = [Environment]::GetEnvironmentVariable($n); [Environment]::SetEnvironmentVariable($n, $null) }
        $script:Lines = [System.Collections.Generic.List[string]]::new()
        Mock Write-Host { $script:Lines.Add([string]$Object) }
    }
    AfterEach { foreach ($n in $EnvNames) { [Environment]::SetEnvironmentVariable($n, $Saved[$n]) } }

    It 'Azure DevOps: one task.logissue warning line; no ; or ] that would end the command' {
        $env:SYSTEM_TEAMPROJECTID = 'p'; $env:BUILD_REPOSITORY_ID = 'r'
        writePipelineWarning "a;b]c`r`nnext"
        $script:Lines | Should -Be @('##vso[task.logissue type=warning]a,b)c next')
    }
    It 'GitHub Actions: ::warning:: line, % escaped' {
        $env:GITHUB_ACTIONS = 'true'
        writePipelineWarning '100% failed'
        $script:Lines | Should -Be @('::warning::100%25 failed')
    }
    It 'elsewhere: nothing' {
        writePipelineWarning 'x'
        $script:Lines.Count | Should -Be 0
    }
    It 'checkParallelFailures warns when items failed and the run continues' {
        $env:GITHUB_ACTIONS = 'true'
        $Product = 'AzADServicePrincipalInsights'
        $htParallelFailures = [hashtable]::Synchronized(@{})
        $htParallelFailures['p|1'] = [pscustomobject]@{ Phase = 'p'; Item = '1'; Error = 'x' }
        { checkParallelFailures -phase 'p' -itemCount 100 } | Should -Not -Throw
        @($script:Lines.where({ $_ -like '::warning::*p: 1 of 100 items failed*' })).Count | Should -Be 1
    }
}
