<#
Compares two AzADServicePrincipalInsights runs in one output folder (CSV, JSON, HTML tables).
Prints a summary to the console and writes details (examples of differing rows) to a report file.
Runs are identified by their output timestamp (yyyyMMdd_HHmmss); by default the two newest runs are compared.
#>
[CmdletBinding()]
param(
    #default: zzzzz_Output in the repository root (as tests/Invoke-LocalTestRun.ps1)
    [string]$OutputDir = (Join-Path -Path (Split-Path -Parent $PSScriptRoot) -ChildPath 'zzzzz_Output'),
    [string]$OldTs,
    [string]$NewTs,
    [string]$ReportPath,
    [string[]]$IgnoreJsonProperty = @(),
    [string[]]$IgnoreCsvColumn = @(),
    [int]$MaxExamples = 5,
    [string]$Delimiter = ';'
)
$ErrorActionPreference = 'Stop'

$htmlFiles = Get-ChildItem -LiteralPath $OutputDir -File -Filter '*.html'
$allTs = @($htmlFiles | ForEach-Object { if ($_.Name -match '_(\d{8}_\d{6})_') { $Matches[1] } } | Sort-Object -Unique)
if (-not $OldTs) { $OldTs = $allTs[-2] }
if (-not $NewTs) { $NewTs = $allTs[-1] }
if (-not $OldTs -or -not $NewTs -or $OldTs -eq $NewTs) { throw "Need two runs; found timestamps: $($allTs -join ', ')" }
if (-not $ReportPath) { $ReportPath = Join-Path $OutputDir "compare_${OldTs}_vs_${NewTs}.txt" } #output folder is gitignored

$report = [System.Collections.Generic.List[string]]::new()
function Out-Both([string]$line) { Write-Host $line; $report.Add($line) }
function Out-Report([string]$line) { $report.Add($line) }
function Limit([string]$s, [int]$n = 400) { if ($s.Length -gt $n) { $s.Substring(0, $n) + "… (+$($s.Length - $n) chars)" } else { $s } }

# Multiset difference of two string lists -> @{ OnlyOld = [..]; OnlyNew = [..] }
function Compare-Multiset([string[]]$old, [string[]]$new) {
    $counts = [System.Collections.Generic.Dictionary[string, int]]::new([StringComparer]::Ordinal)
    foreach ($s in $old) { $counts[$s] = $(if ($counts.ContainsKey($s)) { $counts[$s] } else { 0 }) + 1 }
    foreach ($s in $new) { $counts[$s] = $(if ($counts.ContainsKey($s)) { $counts[$s] } else { 0 }) - 1 }
    $onlyOld = [System.Collections.Generic.List[string]]::new(); $onlyNew = [System.Collections.Generic.List[string]]::new()
    foreach ($kv in $counts.GetEnumerator()) {
        if ($kv.Value -gt 0) { for ($i = 0; $i -lt $kv.Value; $i++) { $onlyOld.Add($kv.Key) } }
        elseif ($kv.Value -lt 0) { for ($i = 0; $i -lt -$kv.Value; $i++) { $onlyNew.Add($kv.Key) } }
    }
    @{ OnlyOld = $onlyOld; OnlyNew = $onlyNew }
}

# Canonical JSON string: object keys sorted, arrays sorted (parallel collection makes order nondeterministic).
function Get-Canon($x) {
    if ($null -eq $x) { return 'null' }
    if ($x -is [System.Collections.IDictionary]) {
        $parts = foreach ($k in ($x.Keys | Sort-Object)) {
            if ($IgnoreJsonProperty -contains $k) { continue }
            (ConvertTo-Json ([string]$k) -Compress) + ':' + (Get-Canon $x[$k])
        }
        return '{' + ($parts -join ',') + '}'
    }
    if ($x -is [System.Collections.IList]) {
        [string[]]$parts = @(foreach ($i in $x) { Get-Canon $i })
        [Array]::Sort($parts, [StringComparer]::Ordinal)
        return '[' + ($parts -join ',') + ']'
    }
    if ($x -is [string]) { return (ConvertTo-Json $x -Compress) }
    if ($x -is [bool]) { return $x.ToString().ToLowerInvariant() }
    if ($x -is [datetime]) { return '"' + $x.ToUniversalTime().ToString('o') + '"' }
    if ($x -is [System.IFormattable]) { return $x.ToString($null, [cultureinfo]::InvariantCulture) }
    return [string]$x
}

function Get-RunFiles([string]$ts) {
    $map = @{}
    foreach ($f in Get-ChildItem -LiteralPath $OutputDir -File) {
        if ($f.Name -like '*_Log.txt') { continue }   # transcript carries a different timestamp
        if ($f.Name -match "_${ts}_[0-9a-fA-F-]{36}(.*)$") { $map[$Matches[1]] = $f.FullName }
    }
    $json = Get-ChildItem -LiteralPath $OutputDir -Directory -Filter "JSON_SP_*_$ts" | Select-Object -First 1
    @{ Files = $map; JsonDir = $json.FullName }
}

$old = Get-RunFiles $OldTs
$new = Get-RunFiles $NewTs
Out-Both "Compare runs: OLD=$OldTs  NEW=$NewTs  (folder $OutputDir)"
$diffCount = 0

#region files present
foreach ($k in $old.Files.Keys | Where-Object { -not $new.Files.ContainsKey($_) }) { Out-Both "FILE only in OLD: *$k"; $diffCount++ }
foreach ($k in $new.Files.Keys | Where-Object { -not $old.Files.ContainsKey($_) }) { Out-Both "FILE only in NEW: *$k"; $diffCount++ }
#endregion

#region CSV
Out-Both ''
Out-Both '== CSV'
foreach ($k in ($old.Files.Keys | Where-Object { $_ -like '*.csv' -and $new.Files.ContainsKey($_) } | Sort-Object)) {
    $o = @(Import-Csv -LiteralPath $old.Files[$k] -Delimiter $Delimiter)
    $n = @(Import-Csv -LiteralPath $new.Files[$k] -Delimiter $Delimiter)
    $colsO = @(if ($o.Count) { $o[0].PSObject.Properties.Name } else { (Get-Content -LiteralPath $old.Files[$k] -TotalCount 1) -split [regex]::Escape($Delimiter) })
    $colsN = @(if ($n.Count) { $n[0].PSObject.Properties.Name } else { (Get-Content -LiteralPath $new.Files[$k] -TotalCount 1) -split [regex]::Escape($Delimiter) })
    $colsO = @($colsO | ForEach-Object { $_.Trim('"') } | Where-Object { $_ })
    $colsN = @($colsN | ForEach-Object { $_.Trim('"') } | Where-Object { $_ })
    $colNote = ''
    if (($colsO -join '|') -ne ($colsN -join '|')) {
        $colNote = "  COLUMNS CHANGED: -[$((@($colsO | Where-Object { $_ -notin $colsN })) -join ',')] +[$((@($colsN | Where-Object { $_ -notin $colsO })) -join ',')]"
    }
    $cols = @($colsO | Where-Object { $_ -in $colsN -and $IgnoreCsvColumn -notcontains $_ })
    $rowStr = { param($r) ($cols | ForEach-Object { "$_=$($r.$_)" }) -join ' | ' }
    $d = Compare-Multiset @($o | ForEach-Object { & $rowStr $_ }) @($n | ForEach-Object { & $rowStr $_ })
    $status = if ($d.OnlyOld.Count -or $d.OnlyNew.Count -or $colNote) { 'DIFF'; $diffCount++ } else { 'same' }
    Out-Both ("  {0,-4} *{1}: rows {2} -> {3} (-{4} / +{5}){6}" -f $status, $k, $o.Count, $n.Count, $d.OnlyOld.Count, $d.OnlyNew.Count, $colNote)
    if ($status -eq 'DIFF') {
        Out-Report "    --- CSV *$k"
        $d.OnlyOld | Select-Object -First $MaxExamples | ForEach-Object { Out-Report ('    - ' + (Limit $_)) }
        $d.OnlyNew | Select-Object -First $MaxExamples | ForEach-Object { Out-Report ('    + ' + (Limit $_)) }
    }
}
#endregion

#region JSON
Out-Both ''
Out-Both '== JSON'
if (-not $old.JsonDir -or -not $new.JsonDir) { Out-Both '  JSON folder missing in one run - skipped' }
else {
    $jo = @{}; Get-ChildItem -LiteralPath $old.JsonDir -File -Filter *.json | ForEach-Object { $jo[$_.Name] = $_.FullName }
    $jn = @{}; Get-ChildItem -LiteralPath $new.JsonDir -File -Filter *.json | ForEach-Object { $jn[$_.Name] = $_.FullName }
    $onlyO = @($jo.Keys | Where-Object { -not $jn.ContainsKey($_) }); $onlyN = @($jn.Keys | Where-Object { -not $jo.ContainsKey($_) })
    $propDiff = @{}; $changedFiles = 0; $examples = 0
    foreach ($name in ($jo.Keys | Where-Object { $jn.ContainsKey($_) } | Sort-Object)) {
        $rawO = [IO.File]::ReadAllText($jo[$name]); $rawN = [IO.File]::ReadAllText($jn[$name])
        if ($rawO -eq $rawN -and $IgnoreJsonProperty.Count -eq 0) { continue }
        $ho = ConvertFrom-Json $rawO -AsHashtable -Depth 100 -NoEnumerate
        $hn = ConvertFrom-Json $rawN -AsHashtable -Depth 100 -NoEnumerate
        $keys = @(@($ho.Keys) + @($hn.Keys) | Sort-Object -Unique | Where-Object { $IgnoreJsonProperty -notcontains $_ })
        $changed = @(foreach ($p in $keys) {
                $co = if ($ho.Contains($p)) { Get-Canon $ho[$p] } else { '<absent>' }
                $cn = if ($hn.Contains($p)) { Get-Canon $hn[$p] } else { '<absent>' }
                if ($co -ne $cn) { [pscustomobject]@{ Prop = $p; Old = $co; New = $cn } }
            })
        if ($changed.Count) {
            $changedFiles++
            foreach ($c in $changed) { $propDiff[$c.Prop] = 1 + $(if ($propDiff.ContainsKey($c.Prop)) { $propDiff[$c.Prop] } else { 0 }) }
            if ($examples -lt $MaxExamples * 4) {
                $examples++
                Out-Report "    --- JSON $name"
                foreach ($c in $changed) { Out-Report "    [$($c.Prop)]"; Out-Report ('      - ' + (Limit $c.Old 600)); Out-Report ('      + ' + (Limit $c.New 600)) }
            }
        }
    }
    if ($onlyO.Count -or $onlyN.Count -or $changedFiles) { $diffCount++ }
    Out-Both ("  files: OLD {0}, NEW {1}; only OLD {2}, only NEW {3}; changed {4}" -f $jo.Count, $jn.Count, $onlyO.Count, $onlyN.Count, $changedFiles)
    $onlyO | Select-Object -First $MaxExamples | ForEach-Object { Out-Both "    only OLD: $_" }
    $onlyN | Select-Object -First $MaxExamples | ForEach-Object { Out-Both "    only NEW: $_" }
    foreach ($kv in ($propDiff.GetEnumerator() | Sort-Object Value -Descending)) { Out-Both ("    property {0}: differs in {1} files" -f $kv.Key, $kv.Value) }
}
#endregion

#region HTML tables
Out-Both ''
Out-Both '== HTML tables'
#rendered text of an html fragment: tags stripped, entities decoded (so '&amp;' and '&' compare equal)
function ConvertTo-CellText([string]$fragment) { [System.Net.WebUtility]::HtmlDecode((($fragment -replace '<[^>]+>', ' ') -replace '\s+', ' ').Trim()) }
function Expand-GridBlock([string]$base64) {
    $in = [IO.MemoryStream]::new([Convert]::FromBase64String(($base64 -replace '\s', '')))
    $reader = [IO.StreamReader]::new([IO.Compression.GZipStream]::new($in, [IO.Compression.CompressionMode]::Decompress), [Text.Encoding]::UTF8)
    try { $reader.ReadToEnd() } finally { $reader.Dispose() }
}
$getTables = {
    param($path)
    $html = [IO.File]::ReadAllText($path)
    $t = [ordered]@{}
    #TableFilter tables: header, filter type per column (TableFilter config: col_N select/multiple, col_types number), cell text
    foreach ($m in [regex]::Matches($html, '(?s)<table id="([^"]+)".*?</table>')) {
        $rows = @([regex]::Matches($m.Value, '(?s)<tr[^>]*>(.*?)</tr>') | Where-Object { $_.Value -match '<td' } | ForEach-Object {
                $cells = @([regex]::Matches($_.Groups[1].Value, '(?s)<td[^>]*>(.*?)</td>') | ForEach-Object { ConvertTo-CellText $_.Groups[1].Value })
                "[$($cells.Count)] " + ($cells -join ' | ')
            })
        $headers = @([regex]::Matches($m.Value, '(?s)<th[^>]*>(.*?)</th>') | ForEach-Object { ConvertTo-CellText $_.Groups[1].Value })
        $types = @($headers | ForEach-Object { 'text' })
        $config = [regex]::Match($html.Substring($m.Index + $m.Length, [math]::Min(6000, $html.Length - $m.Index - $m.Length)), "(?s)var tfConfig4$([regex]::Escape($m.Groups[1].Value)) = \{(.*?)\};").Groups[1].Value
        $colTypes = [regex]::Match($config, '(?s)col_types:\s*\[(.*?)\]').Groups[1].Value -split ',' | ForEach-Object { $_.Trim().Trim("'") }
        for ($i = 0; $i -lt $types.Count; $i++) { if (@($colTypes)[$i] -eq 'number') { $types[$i] = 'number' } }
        foreach ($sm in [regex]::Matches($config, "col_(\d+):\s*'(select|multiple)'")) { $i = [int]$sm.Groups[1].Value; if ($i -lt $types.Count) { $types[$i] = 'select' } }
        $t[$m.Groups[1].Value] = @{ Th = $headers.Count; Headers = $headers; Types = $types; Rows = $rows }
    }
    #AG Grid tables: definition (headers, filter types, value/html column per header), gzip data block, string table
    $stringsMatch = [regex]::Match($html, '<script type="application/octet-stream" id="spiStrings">([^<]*)</script>')
    if ($stringsMatch.Success) {
        $table = Expand-GridBlock $stringsMatch.Groups[1].Value | ConvertFrom-Json -AsHashtable -DateKind String
        $fragments = @($table.fragments)
        $strings = @(foreach ($value in $table.strings) { if ($value) { [regex]::Replace($value, '[-]', { param($x) $i = [int][char]$x.Value - 0xE000; if ($i -lt $fragments.Count) { $fragments[$i] } else { $x.Value } }) } else { $value } })
        $defs = @{}
        foreach ($dm in [regex]::Matches($html, "(?s)spiGridDefs\['(\w+)'\] = function \(.*?columnDefs:\s*\[(.*?)\r?\n\s*\],")) {
            $defs[$dm.Groups[1].Value] = @(foreach ($line in ($dm.Groups[2].Value -split "\r?\n" | Where-Object { $_ -match 'headerName:' -and $_ -notmatch 'hide: true' })) {
                    @{
                        Header = ([regex]::Match($line, "headerName: '((?:\\.|[^'\\])*)'").Groups[1].Value -replace "\\'", "'" -replace '\\\\', '\')
                        Value = [int][regex]::Match($line, 'spiColumn(?:Number)?ValueGetter\(spiRowData, (\d+)\)').Groups[1].Value
                        Html = $(if ($line -match 'spiColumnHtmlRenderer\(spiRowData, (\d+)') { [int]$Matches[1] } else { -1 })
                        Type = $(if ($line -match 'agNumberColumnFilter') { 'number' } elseif ($line -match 'spiSelectFloatingFilter') { 'select' } elseif ($line -match 'agDateColumnFilter') { 'date' } else { 'text' })
                    }
                })
        }
        foreach ($rm in [regex]::Matches($html, "spiRegisterReportGrid\('(\w+)', '(\w+)'")) {
            $gridId = $rm.Groups[1].Value; $columns = $defs[$rm.Groups[2].Value]
            $data = Expand-GridBlock ([regex]::Match($html, "id=`"spiData4$([regex]::Escape($gridId))`">([^<]*)<").Groups[1].Value) | ConvertFrom-Json -AsHashtable -DateKind String
            $dictionaries = @(foreach ($dictionary in $data.dictionaries) { , @(foreach ($index in $dictionary) { $strings[$index] }) })
            $rows = @(foreach ($row in $data.rows) {
                    $cells = @(foreach ($column in $columns) {
                            if ($column.Html -ge 0) { ConvertTo-CellText ([string]$dictionaries[$column.Html][$row[$column.Html]]) }
                            else { (([string]$dictionaries[$column.Value][$row[$column.Value]]) -replace '\s+', ' ').Trim() }
                        })
                    "[$($cells.Count)] " + ($cells -join ' | ')
                })
            $t[$gridId] = @{ Th = $columns.Count; Headers = @($columns.Header); Types = @($columns.Type); Rows = $rows; Grid = $true }
        }
    }
    $t
}
$htmlKey = $old.Files.Keys | Where-Object { $_ -like '*.html' } | Select-Object -First 1
if ($htmlKey -and $new.Files.ContainsKey($htmlKey)) {
    $to = & $getTables $old.Files[$htmlKey]; $tn = & $getTables $new.Files[$htmlKey]
    foreach ($id in (@($to.Keys) + @($tn.Keys) | Sort-Object -Unique)) {
        if (-not $to.Contains($id)) { Out-Both "  DIFF table only in NEW: $id"; $diffCount++; continue }
        if (-not $tn.Contains($id)) { Out-Both "  DIFF table only in OLD: $id"; $diffCount++; continue }
        $a = $to[$id]; $b = $tn[$id]
        $d = Compare-Multiset $a.Rows $b.Rows
        $tdO = (@($a.Rows | ForEach-Object { ($_ -split ' ')[0] }) | Sort-Object -Unique) -join '/'
        $tdN = (@($b.Rows | ForEach-Object { ($_ -split ' ')[0] }) | Sort-Object -Unique) -join '/'
        $headersSame = ($a.Headers -join '|') -ceq ($b.Headers -join '|')
        $typesSame = ($a.Types -join '|') -eq ($b.Types -join '|')
        $status = if ($d.OnlyOld.Count -or $d.OnlyNew.Count -or $a.Th -ne $b.Th -or -not $headersSame -or -not $typesSame) { 'DIFF'; $diffCount++ } else { 'same' }
        $kind = "$(if ($a.Grid) { 'grid' } else { 'table' })->$(if ($b.Grid) { 'grid' } else { 'table' })"
        Out-Both ("  {0,-4} {1} [{10}]: th {2}->{3}, td {4}->{5}, rows {6} -> {7} (-{8} / +{9})" -f $status, $id, $a.Th, $b.Th, $tdO, $tdN, $a.Rows.Count, $b.Rows.Count, $d.OnlyOld.Count, $d.OnlyNew.Count, $kind)
        if (-not $headersSame) { Out-Both "    headers: $($a.Headers -join ' | ')"; Out-Both "          -> $($b.Headers -join ' | ')" }
        if (-not $typesSame) { Out-Both "    filter types: $($a.Types -join ',')"; Out-Both "               -> $($b.Types -join ',')" }
        if ($b.Rows.Count -and $tdN -ne "[$($b.Th)]") { Out-Both "  WARN $id (NEW): header has $($b.Th) columns but rows have $tdN cells" }
        if ($status -eq 'DIFF') {
            Out-Report "    --- HTML $id"
            $d.OnlyOld | Select-Object -First $MaxExamples | ForEach-Object { Out-Report ('    - ' + (Limit $_)) }
            $d.OnlyNew | Select-Object -First $MaxExamples | ForEach-Object { Out-Report ('    + ' + (Limit $_)) }
        }
    }
}
#endregion

Out-Both ''
Out-Both "Sections with differences: $diffCount. Details: $ReportPath"
$report | Set-Content -LiteralPath $ReportPath -Encoding utf8
