#Pester 5+ tests for HTML encoding of report data (review item P0-1).

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:Ast = Get-ScriptAst -Path $script:MainScriptPath

    #load class HtmlSafe from the main script without running the script
    $cls = $script:Ast.Find({ param($n) $n -is [System.Management.Automation.Language.TypeDefinitionAst] -and $n.Name -eq 'HtmlSafe' }, $true)
    if (-not $cls) { throw 'class HtmlSafe not found in main script' }
    Invoke-Expression $cls.Extent.Text
    function ConvertTo-HtmlSafe { param($Value) [HtmlSafe]::Encode($Value) }

    #<td> cells in summary() whose content interpolates a value without [HtmlSafe]::Encode
    function Get-UnencodedTdCells {
        param([string]$Path)
        $lines = [IO.File]::ReadAllLines($Path)
        $start = [array]::FindIndex($lines, [Predicate[string]] { param($x) $x -match '^function summary\(\)' })
        $end = [array]::FindIndex($lines, [Predicate[string]] { param($x) $x -match '^function getOwner' })
        #cells whose value is an HTML fragment built from encoded parts
        $allowed = '$($spAADRoleAssignments)', '$($SPAAADRoleAssignedOn)', '$($APPAAADRoleAssignedOn)', '$($SPAppRoleAssignments)',
        '$($SPOauth2PermissionGrants)', '$($SPAzureRoleAssignments)', '$($hipo.permission4HTML)', '$($ownerOwnedByHtml)', '$($miAssignedToResourcesHtml)',
        '$($arrayOwnedObjects.Count) ($($arrayOwnedObjects -join '', ''))'
        for ($i = $start; $i -lt $end; $i++) {
            foreach ($m in [regex]::Matches($lines[$i], '<td[^>]*>(?<c>.*?)</td>')) {
                $c = $m.Groups['c'].Value
                if ($c -match '\$' -and $c -notmatch '^\$\(\[HtmlSafe\]::Encode\(' -and $allowed -notcontains $c) { "line $($i + 1): $c" }
            }
            #AG Grid: the html twin of a cell (the line after '$spiGridRow.c<n>Html = @"'); plain values are encoded by the grid renderer
            if ($lines[$i] -match '^\s*\$spiGridRow\.c\d+Html = @"$') {
                $c = $lines[$i + 1]
                if ($c -match '\$' -and $c -notmatch '^\$\(\[HtmlSafe\]::Encode\(' -and $allowed -notcontains $c) { "line $($i + 2): $c" }
            }
        }
    }
}

Describe '[HtmlSafe]::Encode' {
    It 'encodes the five significant characters (lt, gt, amp, double and single quote)' {
        ConvertTo-HtmlSafe "<img src=x onerror=alert(1)><b>B</b> & `"q`" 'a'" |
            Should -BeExactly '&lt;img src=x onerror=alert(1)&gt;&lt;b&gt;B&lt;/b&gt; &amp; &quot;q&quot; &#39;a&#39;'
    }
    It 'encodes & first (no double encoding of its own output)' {
        ConvertTo-HtmlSafe '&lt;' | Should -BeExactly '&amp;lt;'
    }
    It 'returns an empty string for $null' {
        ConvertTo-HtmlSafe $null | Should -BeExactly ''
    }
    It 'leaves other characters (umlauts, GUIDs, paths) unchanged' {
        ConvertTo-HtmlSafe 'Müller /subscriptions/0f1e2d3c-0000-4000-8000-000000000000' | Should -BeExactly 'Müller /subscriptions/0f1e2d3c-0000-4000-8000-000000000000'
    }
    It 'converts like string interpolation: arrays joined with a space, booleans and numbers as text' {
        ConvertTo-HtmlSafe @('a<', 'b') | Should -BeExactly 'a&lt; b'
        ConvertTo-HtmlSafe $true | Should -BeExactly 'True'
        ConvertTo-HtmlSafe 14 | Should -BeExactly '14'
    }
    It 'round-trips through HtmlDecode' {
        $s = "x<y>&`"'z"
        [System.Net.WebUtility]::HtmlDecode((ConvertTo-HtmlSafe $s)) | Should -BeExactly $s
    }
}

Describe 'P0-1 every table cell value in summary() is encoded' {
    It 'the grids carry html twins (the check above is not empty) and no td element is left in summary()' {
        $source = [IO.File]::ReadAllText($script:MainScriptPath)
        $summary = $source.Substring($source.IndexOf('function summary()'), $source.IndexOf('function getOwner($owner)') - $source.IndexOf('function summary()'))
        #here-strings (fragments built from encoded parts) and the readable-cell helpers (getSpi*Html encode every value)
        ([regex]::Matches($summary, '\$spiGridRow\.c\d+Html = @"')).Count | Should -BeGreaterOrEqual 4
        ([regex]::Matches($summary, '\$spiGridRow\.c\d+Html = getSpi\w+Html')).Count | Should -BeGreaterOrEqual 20
        $summary | Should -Not -Match '<td[ >]'
    }
    It 'main script has no unencoded td / grid html twin interpolation (except fragments built from encoded parts)' {
        Get-UnencodedTdCells -Path $script:MainScriptPath | Should -BeNullOrEmpty
    }
    It 'the check finds unencoded cells in original code d332d98' {
        @(Get-UnencodedTdCells -Path (Get-MainScriptAtCommit -Commit 'd332d98')).Count | Should -BeGreaterThan 100
    }
}
