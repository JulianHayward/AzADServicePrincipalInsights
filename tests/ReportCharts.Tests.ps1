#Pester 5+ tests for chart helpers in summary() (review item P1-8).

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    $fn = $ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'getChartColors' }, $true)
    if (-not $fn) { throw 'getChartColors not found in main script' }
    . ([scriptblock]::Create($fn.Extent.Text))
    #small palette so wrap-around is easy to see (the script uses 64 colors)
    $categoryColorsMax = @('c0', 'c1', 'c2', 'c3')
}

Describe 'P1-8 getChartColors' {
    It 'returns count colors starting at offset' {
        getChartColors 1 2 | Should -Be @('c1', 'c2')
    }
    It 'wraps around the palette instead of returning nothing past its end' {
        getChartColors 3 3 | Should -Be @('c3', 'c0', 'c1')
    }
    It 'returns no colors for count 0 (the old [0..(0-1)] slice returned 2)' {
        @(getChartColors 0 0).Count | Should -Be 0
    }
    It 'gives the MI chart colors after the Org and SP type colors (no overlap)' {
        $org = getChartColors 0 1; $spType = getChartColors 1 1; $mi = getChartColors 2 2
        @($mi | Where-Object { $_ -in @($org) + @($spType) }).Count | Should -Be 0
    }
}
