#Pester 5+ tests for CSV export hygiene (review P2, theme 13): formula injection guard, consistent quoting, report CSV download.

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:Raw = Get-Content -LiteralPath $script:MainScriptPath -Raw
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    $filter = $ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'protectCsvFormula' }, $true)
    if (-not $filter) { throw 'protectCsvFormula not found in main script' }
    . ([scriptblock]::Create($filter.Extent.Text))
}

Describe 'protectCsvFormula' {
    It 'prefixes values that spreadsheet apps would evaluate as formulas: <Value>' -ForEach @(
        @{ Value = '=SUM(A1)' }, @{ Value = '+1' }, @{ Value = '-cmd' }, @{ Value = '@admin' }, @{ Value = "`tx" }, @{ Value = "`rx" }
    ) {
        ([pscustomobject]@{ a = $Value } | protectCsvFormula).a | Should -BeExactly ("'" + $Value)
    }
    It 'leaves other values, non-string values and the property order unchanged' {
        $out = [pscustomobject]@{ name = 'App = 1'; count = -5; flag = $true; empty = ''; none = $null } | protectCsvFormula
        @($out.PSObject.Properties.Name) | Should -Be @('name', 'count', 'flag', 'empty', 'none')
        $out.name | Should -BeExactly 'App = 1'
        $out.count | Should -Be -5
        $out.flag | Should -BeTrue
        $out.empty | Should -BeExactly ''
        $out.none | Should -BeNullOrEmpty
    }
}

Describe 'CSV exports' {
    It 'every Export-Csv pipeline uses protectCsvFormula and the common quoting setting' {
        $lines = @($script:Raw -split "`r?`n" | Where-Object { $_ -match '\| Export-Csv ' })
        $lines.Count | Should -BeGreaterThan 15
        @($lines | Where-Object { $_ -notmatch '\| protectCsvFormula \| Export-Csv ' }) | Should -BeNullOrEmpty
        @($lines | Where-Object { $_ -notmatch '-UseQuotes \$csvUseQuotes' }) | Should -BeNullOrEmpty
    }
    It 'CSV values built by the script contain no HTML markup' {
        $script:Raw | Should -Not -Match "\`$ownerOwnedBy = `"[^`"]*<br>"
        $script:Raw | Should -Not -Match "\`$miAssignedToResources = 'check <i>"
    }
}

Describe 'report CSV download (JavaScript)' {
    BeforeAll {
        #the grids export with AG Grid (exportDataAsCsv); spiCsvCell applies the cell rules of the former table download
        $script:Js = [regex]::Match($script:Raw, '(?s)    function spiCsvCell\(params\) \{.*??
    \}').Value
    }
    It 'has one cell function and no AzGovViz leftover branch' {
        $script:Js | Should -Not -BeNullOrEmpty
        $script:Raw | Should -Not -Match 'helpertfConfig4TenantSummary_roleAssignmentsAll'
        $script:Raw | Should -Match 'exportDataAsCsv\(\{[^}]*processCellCallback: spiCsvCell'
    }
    It 'converts cells correctly (line breaks, whitespace, formula guard; number columns export their text)' -Skip:(-not (Get-Command node -ErrorAction SilentlyContinue)) {
        $file = Join-Path ([IO.Path]::GetTempPath()) "azadspi-js-$([guid]::NewGuid().ToString('n')).js"
        Set-Content -LiteralPath $file -Encoding utf8NoBOM -Value @'
eval(process.env.AZADSPI_JS);
const col = (formatter) => ({ getColDef: () => (formatter ? { valueFormatter: formatter } : {}) });
const cells = [
  { value: 'a\nb', column: col() },
  { value: '  x   y  ', column: col() },
  { value: '=1+1', column: col() },
  { value: 'say "hi"', column: col() },
  { value: null, column: col(() => 'n/a'), formatValue: () => 'n/a' },
  { value: undefined, column: col() }
];
process.stdout.write(cells.map(spiCsvCell).join('|'));
'@
        try { $env:AZADSPI_JS = $script:Js; $out = (node $file) -join "`n" } finally { Remove-Item Env:AZADSPI_JS -ErrorAction SilentlyContinue; Remove-Item -LiteralPath $file -ErrorAction SilentlyContinue }
        #quoting and the doubled quotes are done by AG Grid's CSV export
        $out | Should -BeExactly "a b|x y|'=1+1|say ""hi""|n/a|"
    }
}

