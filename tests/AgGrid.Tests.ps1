#Pester 5+ tests for the AG Grid emitter (TableFilter -> AG Grid migration): row data round trip through the dictionary
#encoding, the string table with its fragments and gzip/base64; column definitions per filter type; shared definitions.

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.TypeDefinitionAst] -and $n.Name -eq 'HtmlSafe' }, $true).Extent.Text))
    $script:EmitterCode = [regex]::Match($source, '(?s)#region AG Grid emitter\r?\n.*?#endregion AG Grid emitter').Value
    function Expand-Block([string]$base64) {
        $reader = [IO.StreamReader]::new([IO.Compression.GZipStream]::new([IO.MemoryStream]::new([Convert]::FromBase64String($base64)), [IO.Compression.CompressionMode]::Decompress), [Text.Encoding]::UTF8)
        try { $reader.ReadToEnd() } finally { $reader.Dispose() }
    }
    #decodes the grids of an html fragment the way the browser runtime does (U+E0FF + character: that character, review R-16)
    function Read-Grids([string]$html) {
        $table = Expand-Block ([regex]::Match($html, 'id="spiStrings">([^<]*)<').Groups[1].Value) | ConvertFrom-Json -AsHashtable -DateKind String
        $fragments = @($table.fragments)
        $strings = @(foreach ($value in $table.strings) { if ($value) { [regex]::Replace($value, '\uE0FF([\uE000-\uE0FF])|[\uE000-\uE0FE]', { param($x) if ($x.Groups[1].Success) { $x.Groups[1].Value } else { $fragments[[int][char]$x.Value - 0xE000] } }) } else { $value } })
        $grids = @{}
        foreach ($m in [regex]::Matches($html, "spiRegisterReportGrid\('(\w+)', '(\w+)'")) {
            $data = Expand-Block ([regex]::Match($html, "id=`"spiData4$($m.Groups[1].Value)`">([^<]*)<").Groups[1].Value) | ConvertFrom-Json -AsHashtable -DateKind String
            $rows = @(foreach ($row in $data.rows) {
                    $o = [ordered]@{}
                    for ($c = 0; $c -lt $data.columns.Count; $c++) { $o[$data.columns[$c]] = $strings[$data.dictionaries[$c][$row[$c]]] }
                    [pscustomobject]$o
                })
            $grids[$m.Groups[1].Value] = @{ Key = $m.Groups[2].Value; Rows = $rows; Columns = $data.columns }
        }
        $grids
    }
    function New-Emitter {
        $CsvDelimiterOpposite = ','
        . ([scriptblock]::Create($script:EmitterCode))
        $script:CsvDelimiterOpposite = ','
    }
}

Describe 'AG Grid emitter' {
    BeforeAll {
        $CsvDelimiterOpposite = ','
        . ([scriptblock]::Create($script:EmitterCode))
        $rows = @(
            [pscustomobject]@{ Name = 'azadspi-xss <img src=x onerror=alert(1)><b>B</b> & "q" ''a'''; Type = 'SP APP INT'; Count = '3'; Enabled = $true; Role = 'Owner'; RoleHtml = '<a class="externallink" href="https://www.azadvertizer.net/azrolesadvertizer/8e3af657.html" target="_blank">Owner</a>' }
            [pscustomobject]@{ Name = '2026-11-06T10:00:00Z'; Type = 'SP APP EXT'; Count = 'n/a'; Enabled = $false; Role = ''; RoleHtml = '' }
            [pscustomobject]@{ Name = 'third'; Type = 'SP APP INT'; Count = '10'; Enabled = $true; Role = 'Reader, Owner'; RoleHtml = '<b>Reader</b><br><b>Owner</b>' }
        )
        $columns = @(
            @{ header = 'SP displayName'; property = 'Name' }
            @{ header = 'Type'; property = 'Type'; filter = 'select' }
            @{ header = 'Count'; property = 'Count'; filter = 'number' }
            @{ header = 'Enabled'; property = 'Enabled' }
            @{ header = 'Roles'; property = 'Role'; htmlProperty = 'RoleHtml'; filter = 'selectMulti'; tooltip = "it's" }
        )
        $script:Html = (buildSpiGrid -HtmlTableId 'TenantSummary_A' -Rows $rows -ColumnDefinitions $columns -PopoutTitle "A 'grid'") +
                       (buildSpiGrid -HtmlTableId 'TenantSummary_B' -Rows @($rows[0]) -ColumnDefinitions $columns -PopoutTitle 'B') +
                       (buildSpiGrid -HtmlTableId 'TenantSummary_Empty' -Rows @() -ColumnDefinitions @(@{ header = 'X'; property = 'X' }) -PopoutTitle 'E') +
                       (buildSpiGridAssetsBlock)
        $script:Grids = Read-Grids $script:Html
    }
    It 'round trip: the values come back unchanged (markup, quotes, dates; booleans as PowerShell writes them, as the former table cells)' {
        $g = $script:Grids['TenantSummary_A']
        $g.Rows.Count | Should -Be 3
        $g.Rows[0].Name | Should -BeExactly 'azadspi-xss <img src=x onerror=alert(1)><b>B</b> & "q" ''a'''
        $g.Rows[1].Name | Should -BeExactly '2026-11-06T10:00:00Z'
        $g.Rows[0].Enabled | Should -BeExactly 'True'
        $g.Rows[1].Enabled | Should -BeExactly 'False'
        $g.Rows[0].RoleHtml | Should -BeExactly '<a class="externallink" href="https://www.azadvertizer.net/azrolesadvertizer/8e3af657.html" target="_blank">Owner</a>'
        $g.Rows[2].RoleHtml | Should -BeExactly '<b>Reader</b><br><b>Owner</b>'
        $g.Rows[1].Count | Should -BeExactly 'n/a'
    }
    It 'the html twin is a data column before its plain column' {
        $script:Grids['TenantSummary_A'].Columns | Should -Be @('Name', 'Type', 'Count', 'Enabled', 'RoleHtml', 'Role')
    }
    It 'repeated markup is stored as fragments (private use characters) in the string table' {
        $table = Expand-Block ([regex]::Match($script:Html, 'id="spiStrings">([^<]*)<').Groups[1].Value) | ConvertFrom-Json -AsHashtable
        #once in the fragment list, not in the values
        (@($table.fragments) -match 'azadvertizer\.net/azrolesadvertizer').Count | Should -Be 1
        (@($table.strings) -match 'azadvertizer\.net').Count | Should -Be 0
        (@($table.strings) -match '[\uE000-\uE0FF]').Count | Should -BeGreaterThan 0
    }
    It 'grids with the same columns share one definition' {
        $script:Grids['TenantSummary_A'].Key | Should -Be $script:Grids['TenantSummary_B'].Key
        ([regex]::Matches($script:Html, "spiGridDefs\['")).Count | Should -Be 2
    }
    It 'column definitions per filter type; quotes escaped for JavaScript' {
        $def = [regex]::Match($script:Html, "(?s)spiGridDefs\['$($script:Grids['TenantSummary_A'].Key)'\].*?columnDefs: \[(.*?)\n    \],").Groups[1].Value
        $lines = $def -split "`n" | Where-Object { $_ -match 'headerName' }
        $lines[0] | Should -Match "valueGetter: spiColumnValueGetter\(spiRowData, 0\)"
        $lines[1] | Should -Match 'floatingFilterComponent: spiSelectFloatingFilter.*values: spiRowData\.dictionaries\[1\]'
        $lines[2] | Should -Match "spiColumnNumberValueGetter\(spiRowData, 2\).*valueFormatter: spiColumnTextFormatter\(spiRowData, 2\).*filter: 'agNumberColumnFilter'"
        $lines[4] | Should -Match "spiColumnValueGetter\(spiRowData, 5\).*spiColumnHtmlRenderer\(spiRowData, 4,.*separator: ','.*headerTooltip: 'it\\'s'"
        $script:Html | Should -Match "spiRegisterReportGrid\('TenantSummary_A', '\w+', 'A \\'grid\\''\);"
    }
    It 'an empty grid has no rows and still a valid block' {
        $script:Grids['TenantSummary_Empty'].Rows.Count | Should -Be 0
    }
    It 'every grid has a toolbar host and an element, the data is an inert block' {
        foreach ($id in 'TenantSummary_A', 'TenantSummary_B') {
            $script:Html | Should -Match "<div class=`"spiToolbarHost`" id=`"spiToolbar4$id`"></div>"
            $script:Html | Should -Match "<div id=`"$id`" class=`"spiGrid`""
            $script:Html | Should -Match "<script type=`"application/octet-stream`" id=`"spiData4$id`">[A-Za-z0-9+/=]+</script>"
        }
    }
}

Describe 'R-16 private use characters in the data are not expanded into markup' {
    BeforeAll {
        $CsvDelimiterOpposite = ','
        . ([scriptblock]::Create($script:EmitterCode))
        $script:Pua = "name$([char]0xE000)x$([char]0xE005)y$([char]0xE0FF)z"
        $rows = @(
            [pscustomobject]@{ Name = $script:Pua; Role = 'Owner'; RoleHtml = "<b>$($script:Pua)</b><br><a class=`"externallink`" href=`"https://www.azadvertizer.net/azrolesadvertizer/8e3af657.html`" target=`"_blank`">Owner</a>" }
        )
        $columns = @(
            @{ header = 'SP displayName'; property = 'Name' }
            @{ header = 'Roles'; property = 'Role'; htmlProperty = 'RoleHtml' }
        )
        $script:PuaHtml = (buildSpiGrid -HtmlTableId 'TenantSummary_Pua' -Rows $rows -ColumnDefinitions $columns -PopoutTitle 'P') + (buildSpiGridAssetsBlock)
        $script:PuaGrids = Read-Grids $script:PuaHtml
    }
    It 'plain and html values come back with the characters themselves; the markup fragments still expand' {
        $g = $script:PuaGrids['TenantSummary_Pua']
        $g.Rows[0].Name | Should -BeExactly $script:Pua
        $g.Rows[0].RoleHtml | Should -BeExactly "<b>$($script:Pua)</b><br><a class=`"externallink`" href=`"https://www.azadvertizer.net/azrolesadvertizer/8e3af657.html`" target=`"_blank`">Owner</a>"
    }
    It 'the browser runtime restores them the same way (node)' {
        $node = Get-Command node -ErrorAction SilentlyContinue
        if (-not $node) { Set-ItResult -Skipped -Because 'node is not available'; return }
        $source = Get-Content -Raw -LiteralPath $script:MainScriptPath
        $runtime = [regex]::Match($source, '(?s)var fragments = table\.fragments \|\| \[\];.*?return getString;').Value
        $runtime | Should -Not -BeNullOrEmpty
        $tableJson = Expand-Block ([regex]::Match($script:PuaHtml, 'id="spiStrings">([^<]*)<').Groups[1].Value)
        $file = Join-Path $TestDrive 'restore.js'
        $script = "var table = JSON.parse(require('fs').readFileSync(process.argv[2], 'utf8'));`n" + 'var getString = (function () {' + "`n" + $runtime + "`n" + '})();' + "`nvar result = (table.strings || []).map(function (_, i) { return getString(i); });`nprocess.stdout.write(JSON.stringify(result));"
        [System.IO.File]::WriteAllText($file, $script)
        $tableFile = Join-Path $TestDrive 'table.json'
        [System.IO.File]::WriteAllText($tableFile, $tableJson, [System.Text.UTF8Encoding]::new($false))
        #node writes UTF-8; Windows decodes native output with the console code page (OEM) unless set - the characters became '?'
        $savedEncoding = [Console]::OutputEncoding
        try {
            [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
            $strings = (& $node.Source $file $tableFile) | ConvertFrom-Json
        }
        finally {
            [Console]::OutputEncoding = $savedEncoding
        }
        @($strings) | Should -Contain $script:Pua
        @($strings.where({ $_ -like '<b>*' }))[0] | Should -BeExactly "<b>$($script:Pua)</b><br><a class=`"externallink`" href=`"https://www.azadvertizer.net/azrolesadvertizer/8e3af657.html`" target=`"_blank`">Owner</a>"
    }
}

Describe 'AG Grid helpers for the migrated sections' {
    BeforeAll {
        . ([scriptblock]::Create($script:EmitterCode))
    }
    It 'the plain text of an html cell: tags become a space, entities decoded (as the browser shows it)' {
        ConvertTo-SpiPlainText '<span class="odd"><b>BuiltIn</b> ''<a href="x">Owner</a>'' &amp; &lt;img&gt;</span><br>next' | Should -BeExactly "BuiltIn ' Owner ' & <img> next"
        ConvertTo-SpiPlainText $null | Should -BeExactly ''
    }
    It 'the title of a header fragment' {
        getSpiHtmlTitle 'MI Relict <abbr title="relict: deleted &amp; gone"><i class="fa"></i></abbr>' | Should -BeExactly 'relict: deleted & gone'
        getSpiHtmlTitle 'plain' | Should -BeNullOrEmpty
    }
    It 'every helper the migrated sections call exists in the emitter region' {
        foreach ($helperName in 'buildSpiGrid', 'buildSpiGridAssetsBlock', 'ConvertTo-SpiPlainText', 'getSpiHtmlTitle') {
            $script:EmitterCode.Contains("function $helperName") | Should -BeTrue -Because "$helperName is called by the migrated sections"
        }
    }
}

Describe 'AG Grid runtime in the report' {
    It 'AG Grid is loaded with SRI, the runtime parts are embedded' {
        $source | Should -Match 'ag-grid-community@36\.2\.0/dist/ag-grid-community\.min\.js" integrity="sha512-uFpNwWaA9t4Qeratr3lYNbFE0B23FWNje7H15lJriUHgYxNw4IhLPFBAOJ5EqqqsdKTviDv8byhOab5/ZTF65A=="'
        foreach ($id in 'spiAgGridStyle', 'spiAgGridSelectFilter', 'spiAgGridSupport') { $source | Should -Match "id=`"$id`"" }
        $source | Should -Match '\$html \+= buildSpiGridAssetsBlock'
    }
    It 'grid cells have no tooltip (their text is never cut); header tooltips stay' {
        $source | Should -Not -Match 'tooltipValueGetter|tooltipField|whenTruncated'
        $source | Should -Match "options\.tooltipShowDelay = 400;"
        $source | Should -Match "headerTooltip: '"
    }
    It 'the grid styles undo the report stylesheet float on divs' {
        $source | Should -Match '\.spiGrid div[^{]*\{ float: none; \}'
    }
}
