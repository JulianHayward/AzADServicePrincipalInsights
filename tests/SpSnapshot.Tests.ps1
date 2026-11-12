#Pester 5+ tests for the SP snapshot (feature zoom-in): the emitter's description of the grids (columns, object id columns),
#every report grid names its object id columns, and the browser code (index, object, cards, find) run in node on grids
#built by the emitter.

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.TypeDefinitionAst] -and $n.Name -eq 'HtmlSafe' }, $true).Extent.Text))
    $script:EmitterCode = [regex]::Match($source, '(?s)#region AG Grid emitter\r?\n.*?#endregion AG Grid emitter').Value
    $script:Runtime = [regex]::Match($source, "(?s)\`$spiGridSupportScript = @'\r?\n(.*?)\r?\n'@").Groups[1].Value -replace '\r\n', "`n"
    function Expand-Block([string]$base64) {
        $reader = [IO.StreamReader]::new([IO.Compression.GZipStream]::new([IO.MemoryStream]::new([Convert]::FromBase64String($base64)), [IO.Compression.CompressionMode]::Decompress), [Text.Encoding]::UTF8)
        try { $reader.ReadToEnd() } finally { $reader.Dispose() }
    }
    #the grids of an html fragment as the browser has them after spiLoadGridData: the description (meta) and the data with the
    #dictionary values as strings
    function Read-SnapshotGrids([string]$html) {
        $table = Expand-Block ([regex]::Match($html, 'id="spiStrings">([^<]*)<').Groups[1].Value) | ConvertFrom-Json -AsHashtable
        $fragments = @($table.fragments)
        $strings = @(foreach ($value in $table.strings) { if ($value) { [regex]::Replace($value, '\uE0FF([\uE000-\uE0FF])|[\uE000-\uE0FE]', { param($x) if ($x.Groups[1].Success) { $x.Groups[1].Value } else { $fragments[[int][char]$x.Value - 0xE000] } }) } else { $value } })
        $grids = [ordered]@{}
        foreach ($m in [regex]::Matches($html, "spiRegisterReportGrid\('(\w+)', '\w+', '(?:[^'\\]|\\.)*', (\{.*?\})\);</script>")) {
            $data = Expand-Block ([regex]::Match($html, "id=`"spiData4$($m.Groups[1].Value)`">([^<]*)<").Groups[1].Value) | ConvertFrom-Json -AsHashtable
            $data.dictionaries = @(foreach ($dictionary in $data.dictionaries) { , @(foreach ($index in $dictionary) { $strings[$index] }) })
            $grids[$m.Groups[1].Value] = [ordered]@{ meta = ($m.Groups[2].Value | ConvertFrom-Json -AsHashtable); data = $data }
        }
        $grids
    }
}

Describe 'SP snapshot: the emitter describes the grids' {
    BeforeAll {
        $CsvDelimiterOpposite = ','
        . ([scriptblock]::Create($script:EmitterCode))
        $rows = @([pscustomobject]@{ c0 = '11111111-1111-1111-1111-111111111111'; c1 = 'aaaaaaaa-1111-1111-1111-111111111111'; c2 = 'App One'; c3 = 'x'; c3Html = '<b>x</b>'; c4 = 'bbbbbbbb-1111-1111-1111-111111111111'; c5 = 'aaaaaaaa-1111-1111-1111-111111111111'; c6 = 'cccccccc-1111-1111-1111-111111111111 dddddddd-1111-1111-1111-111111111111' })
        $script:Html = (buildSpiGrid -HtmlTableId 'TenantSummary_T' -Rows $rows -PopoutTitle 'T' -ColumnDefinitions @(
                @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                @{ header = 'SP application Id'; property = 'c1' }
                @{ header = 'SP displayName'; property = 'c2'; snapshot = 'name' }
                @{ header = 'a</script>b'; property = 'c3'; htmlProperty = 'c3Html' }
                @{ header = 'App object Id'; property = 'c4'; portal = 'application'; appIdProperty = 'c5' }
                @{ header = 'App client Id'; property = 'c5' }
                @{ header = 'API object Ids'; property = 'c6'; data = $true; snapshot = 'api' }
            )) + (buildSpiGridAssetsBlock)
        $script:Grid = (Read-SnapshotGrids $script:Html)['TenantSummary_T']
    }
    It 'object id columns: a portal Service Principal column is sp, an application column app, their application id columns appId; explicit roles' {
        $columns = $script:Grid.data.columns
        $keys = @(foreach ($key in $script:Grid.meta.k) { "$($columns[$key[0]])=$($key[1])" })
        $keys | Should -Be @('c0=sp', 'c1=appId', 'c4=app', 'c5=appId', 'c6=api')
    }
    It 'columns: header, value, html twin, portal kind with its aux column, role' {
        $columns = $script:Grid.data.columns
        $c0 = $script:Grid.meta.c[0]
        $c0[0] | Should -BeExactly 'SP object Id'
        $columns[$c0[1]] | Should -BeExactly 'c0'
        $c0[2] | Should -Be -1
        $c0[3] | Should -BeExactly 'servicePrincipal'
        $columns[$c0[4]] | Should -BeExactly 'c1'
        $c0[5] | Should -BeExactly 'sp'
        $c3 = @($script:Grid.meta.c.where({ $_[0] -eq 'a</script>b' }))[0]
        $columns[$c3[2]] | Should -BeExactly 'c3Html'
        $script:Grid.meta.c[2][5] | Should -BeExactly 'name'
    }
    It 'the name column carries the snapshot icon of its object; the id column then has none' {
        $script:Html | Should -Match "colId: 'c2', valueGetter: spiColumnValueGetter\(spiRowData, 2\), cellRenderer: spiColumnSnapNameRenderer\(spiRowData, 2, 0, spiHighlighter\)"
        $script:Html | Should -Match "cellRenderer: spiColumnPortalLinkRenderer\(spiRowData, 0, 1, 'servicePrincipal', spiHighlighter, true\)"
        $script:Html | Should -Match "cellRenderer: spiColumnPortalLinkRenderer\(spiRowData, 5, 6, 'application', spiHighlighter\)" -Because 'no name column of the application in this grid'
    }
    It 'a data column is in the row data and the description, not in the cards; API ids and scopes are hidden grid columns (Show in report filters on them)' {
        $script:Grid.data.columns | Should -Contain 'c6'
        @($script:Grid.meta.c.where({ $_[0] -eq 'API object Ids' })).Count | Should -Be 0
        $script:Html | Should -Match "\{ headerName: 'API object Ids', colId: 'c6', valueGetter: spiColumnValueGetter\(spiRowData, 7\), hide: true \}"
        $script:Html | Should -Match "colId: 'c5'"
    }
    It 'a header with a closing script tag does not end the inline script' {
        $script:Html | Should -Not -Match '</script>b'
        $script:Html.Contains('a' + [char]92 + 'u003c/script>b') | Should -BeTrue
    }
}

Describe 'SP snapshot: every report grid names its object id columns' {
    It 'buildSpiGrid and addReportSection calls have a Service Principal / application portal column, a snapshot role or the SP columns' {
        $ast = Get-ScriptAst -Path $script:MainScriptPath
        $calls = $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.CommandAst] -and $n.GetCommandName() -in 'buildSpiGrid', 'addReportSection' }, $true)
        $calls.Count | Should -BeGreaterThan 25
        #not about one object: the tenant consent settings
        $notAboutAnObject = @('TenantConsentSettings')
        $missing = foreach ($call in $calls) {
            $text = $call.Extent.Text
            if ($call.GetCommandName() -eq 'addReportSection' -and $text -notmatch '-Columns') { continue }
            if ($call.GetCommandName() -eq 'buildSpiGrid' -and $text -match '-ColumnDefinitions \$gridColumns') { continue }
            $name = [regex]::Match($text, "-Region '(\w+)'|-PopoutTitle '([^']+)'").Value
            if ($name -match ($notAboutAnObject -join '|')) { continue }
            if ($text -notmatch "(?i)portal = 'servicePrincipal'|portal = 'application'|snapshot = '|getSPColumns") { $name }
        }
        $missing | Should -BeNullOrEmpty
    }
    It 'the Service Principals grid names the name, type and application name columns (the object of a snapshot)' {
        $grid = [regex]::Match($source, "(?s)-PopoutTitle 'Service Principals' -ColumnDefinitions @\((.*?)\)\)\)").Groups[1].Value
        $grid | Should -Match "snapshot = 'name'"
        $grid | Should -Match "snapshot = 'type'"
        $grid | Should -Match "snapshot = 'appName'"
    }
    It 'the application and delegated permission grids carry the API ids (the clients of an API)' {
        ([regex]::Matches($source, "@\{ header = 'API object Ids'; property = 'c9'; data = \`$true; snapshot = 'api' \}")).Count | Should -Be 2
    }
}

Describe 'SP snapshot: browser code (node)' {
    BeforeAll {
        $script:Node = Get-Command node -ErrorAction SilentlyContinue
        if (-not $script:Node) { return }
        $CsvDelimiterOpposite = ','
        . ([scriptblock]::Create($script:EmitterCode))
        $a1 = '11111111-1111-1111-1111-111111111111'; $aid1 = 'aaaaaaaa-1111-1111-1111-111111111111'; $ao1 = 'a0a0a0a0-1111-1111-1111-111111111111'
        $g1 = '22222222-2222-2222-2222-222222222222'; $gid = '00000003-0000-0000-c000-000000000000'
        $ao2 = 'a0a0a0a0-2222-2222-2222-222222222222'; $aid2 = 'aaaaaaaa-2222-2222-2222-222222222222'
        $u1 = '33333333-3333-3333-3333-333333333333'; $grp = '44444444-4444-4444-4444-444444444444'
        $rg1 = '/subscriptions/55555555-5555-5555-5555-555555555555/resourcegroups/rg1'; $sub1 = '/subscriptions/55555555-5555-5555-5555-555555555555'
        $m1 = '66666666-6666-6666-6666-666666666666'; $mid1 = 'aaaaaaaa-6666-6666-6666-666666666666'; $site1 = "$rg1/providers/microsoft.web/sites/app1"
        #a second API with the same display name (SN-1), a removed object (SN-2), an app owned by its own Service Principal (SN-12),
        #the Tenant Root Group (SN-10)
        $g2 = '77777777-7777-7777-7777-777777777777'; $gid2 = 'aaaaaaaa-7777-7777-7777-777777777777'
        $r1 = '88888888-8888-8888-8888-888888888888'
        $s3 = '99999999-3333-3333-3333-333333333333'; $ao3 = 'a0a0a0a0-3333-3333-3333-333333333333'; $aid3 = 'aaaaaaaa-3333-3333-3333-333333333333'
        $tenant = 'abcdef00-0000-0000-0000-000000000000'; $mgRoot = "/providers/microsoft.management/managementgroups/$tenant"; $mg1 = '/providers/microsoft.management/managementgroups/mg1'
        $script:Ids = @{ a1 = $a1; aid1 = $aid1; ao1 = $ao1; g1 = $g1; g2 = $g2; ao2 = $ao2; u1 = $u1; grp = $grp; rg1 = $rg1; sub1 = $sub1; res1 = "$rg1/providers/microsoft.keyvault/vaults/kv1"; m1 = $m1; site1 = $site1; r1 = $r1; s3 = $s3; ao3 = $ao3; mgRoot = $mgRoot; mg1 = $mg1 }
        $spColumns = @(
            @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
            @{ header = 'SP application Id'; property = 'c1' }
            @{ header = 'SP displayName'; property = 'c2'; snapshot = 'name' }
            @{ header = 'SP notes'; property = 'c3' }
            @{ header = 'Type'; property = 'c6'; filter = 'select'; snapshot = 'type' }
            @{ header = 'App object Id'; property = 'c8'; portal = 'application'; appIdProperty = 'c9' }
            @{ header = 'App application (client) Id'; property = 'c9' }
            @{ header = 'App displayName'; property = 'c10'; snapshot = 'appName' }
            @{ header = 'MI resource (system assigned)'; property = 'c21'; data = $true; snapshot = 'resource' }
        )
        $spRows = @(
            [pscustomobject]@{ c0 = $a1; c1 = $aid1; c2 = 'App One'; c3 = 'note one'; c6 = 'SP APP INT'; c8 = $ao1; c9 = $aid1; c10 = 'App One' }
            [pscustomobject]@{ c0 = $g1; c1 = $gid; c2 = 'Microsoft Graph'; c3 = ''; c6 = 'SP APP EXT'; c8 = ''; c9 = ''; c10 = '' }
            [pscustomobject]@{ c0 = ''; c1 = ''; c2 = ''; c3 = ''; c6 = 'Application'; c8 = $ao2; c9 = $aid2; c10 = 'Lonely <img src=x onerror=alert(1)> App' }
            [pscustomobject]@{ c0 = $m1; c1 = $mid1; c2 = 'site identity'; c3 = ''; c6 = 'SP MI System assigned'; c8 = ''; c9 = ''; c10 = ''; c21 = $site1 }
            [pscustomobject]@{ c0 = $g2; c1 = $gid2; c2 = 'Microsoft Graph'; c3 = ''; c6 = 'SP APP INT'; c8 = ''; c9 = ''; c10 = '' }
            [pscustomobject]@{ c0 = $s3; c1 = $aid3; c2 = 'Self Owner'; c3 = ''; c6 = 'SP APP INT'; c8 = $ao3; c9 = $aid3; c10 = 'Self Owner' }
        )
        $appRoleRows = @([pscustomobject]@{ c0 = $a1; c1 = $aid1; c2 = 'App One'; c8 = 'Microsoft Graph (User.Read.All), Microsoft Graph (Write.All)'; c8Html = "<b class=`"spiItemsLabel`" data-spi-api=`"$g1`">Microsoft Graph (1)</b><ul class=`"spiList spiItems`"><li>User.Read.All</li></ul><b class=`"spiItemsLabel`" data-spi-api=`"$g2`">Microsoft Graph (1)</b><ul class=`"spiList spiItems`"><li>Write.All</li></ul>"; c9 = "$g1 $g2" })
        $cleanupRows = @([pscustomobject]@{ SPObjectId = $a1; SPAppId = $aid1; SPDisplayName = 'App One'; Reason = 'expired credentials' })
        $appOwnerRows = @(
            [pscustomobject]@{ c0 = $ao2; c1 = $aid2; c2 = 'Lonely App'; c5 = 'App One'; c6 = 'SP APP INT'; c7 = $a1 }
            [pscustomobject]@{ c0 = $ao2; c1 = $aid2; c2 = 'Lonely App'; c5 = 'Una User'; c6 = 'User (Member)'; c7 = $u1 }
            [pscustomobject]@{ c0 = $ao3; c1 = $aid3; c2 = 'Self Owner'; c5 = 'Self Owner'; c6 = 'SP APP INT'; c7 = $s3 }
        )
        #SP owners: a direct owner, an indirect one (through the direct owner) - the graph separates them (SN-4); the managed identity
        #owns another object indirectly (SN-9)
        $spOwnerRows = @(
            [pscustomobject]@{ c0 = $a1; c1 = $aid1; c2 = 'App One'; c5 = 'site identity'; c6 = 'SP MI System assigned'; c7 = $m1; c8 = 'direct'; c10 = '' }
            [pscustomobject]@{ c0 = $a1; c1 = $aid1; c2 = 'App One'; c5 = 'Una User'; c6 = 'User (Member)'; c7 = $u1; c8 = 'indirect'; c10 = 'owner of site identity' }
            [pscustomobject]@{ c0 = $g1; c1 = $gid; c2 = 'Microsoft Graph'; c5 = 'site identity'; c6 = 'SP MI System assigned'; c7 = $m1; c8 = 'indirect'; c10 = 'owner of App One' }
        )
        #an Entra role scoped to App One, held by the user: the holder by its marker, a single item (SN-6, SN-7)
        $holderRows = @([pscustomobject]@{ c0 = $a1; c1 = $aid1; c2 = 'App One'; c9 = 'Application Administrator on Una User'; c9Html = "<span class=`"spiItem`"><a class=`"externallink`" href=`"https://www.azadvertizer.net/azentraidroles/x.html`" target=`"_blank`">Application Administrator</a><span class=`"spiPrincipal`" data-spi-principal=`"$u1`" data-spi-principal-name=`"Una User`" data-spi-principal-type=`"User`"></span> <span class=`"spiDim`">assigned to Una User &middot; User &middot; $u1</span></span>" })
        $secretRows = @([pscustomobject]@{ c0 = $a1; c1 = $aid1; c2 = 'App One'; c4 = $ao1; c5 = $aid1; c6 = 'App One'; c7 = '2'; c7Html = '<ul class="spiList spiItems"><li class="spiItem-muted">old secret <span class="spiDim">expired &middot; 2025-01-01 &middot; key id x</span></li><li>new secret <span class="spiDim">expires in 200 days &middot; 2027-01-01 &middot; key id y</span></li></ul>' })
        $ficRows = @([pscustomobject]@{ SPObjectId = $a1; SPAppId = $aid1; SPDisplayName = 'App One'; CredentialOn = 'app registration'; Name = 'deploy'; IssuerType = 'GitHub'; Trust = 'repo org/app, branch main' })
        $groupRows = @([pscustomobject]@{ c0 = $a1; c1 = $aid1; c2 = 'App One'; c7 = 'Grp One'; c7Html = "<a class=`"externallink`" href=`"https://portal.azure.com/#view/Microsoft_AAD_IAM/GroupDetailsMenuBlade/~/Overview/groupId/$grp/menuId/`" target=`"_blank`">Grp One</a>" })
        $rbacRows = @(
            [pscustomobject]@{ c0 = $a1; c1 = $aid1; c2 = 'App One'; c4 = "CostCenterOwnerGroup:$grp"; c4Html = "<span class=`"spiTag`">CostCenterOwnerGroup:$grp</span>"; c7 = 'critical'; c9 = 'Reader (ResourceGroup rg1), Contributor (Subscription sub1)'; c9Html = "<b class=`"spiItemsLabel`">rg1 (1)</b><ul class=`"spiList spiItems`"><li>Reader<span class=`"spiScope`" data-spi-scope=`"$rg1`" data-spi-scope-name=`"rg1`"></span></li></ul><b class=`"spiItemsLabel`">sub1 (1)</b><ul class=`"spiList spiItems`"><li>Contributor<span class=`"spiScope`" data-spi-scope=`"$sub1`" data-spi-scope-name=`"sub1`"></span><span class=`"spiPrincipal`" data-spi-principal=`"$grp`" data-spi-principal-name=`"Grp One`" data-spi-principal-type=`"Group`"></span> <span class=`"spiDim`">sub1 &middot; PIM &middot; through group Grp One ($grp)</span></li></ul>"; c10 = "$rg1`n$sub1" }
            [pscustomobject]@{ c0 = $m1; c1 = $mid1; c2 = 'site identity'; c4 = ''; c4Html = ''; c7 = ''; c9 = 'Owner (Tenant Root Group)'; c9Html = "<span class=`"spiItem`">Owner<span class=`"spiScope`" data-spi-scope=`"$mgRoot`" data-spi-scope-name=`"Tenant Root Group`"></span></span>"; c10 = $mgRoot }
        )
        $changeRows = @(
            [pscustomobject]@{ Time = '2026-11-10'; Change = 'owner added'; ObjectType = 'ServicePrincipal'; DisplayName = 'App One'; ObjectId = $a1 }
            [pscustomobject]@{ Time = '2026-11-11'; Change = 'credential added'; ObjectType = 'ServicePrincipal'; DisplayName = 'App One'; ObjectId = $a1 }
            [pscustomobject]@{ Time = '2026-11-11'; Change = 'object added'; ObjectType = 'ServicePrincipal'; DisplayName = 'Microsoft Graph'; ObjectId = $g1 }
            [pscustomobject]@{ Time = '2026-11-11'; Change = 'object removed'; ObjectType = 'ServicePrincipal'; DisplayName = 'Removed App'; ObjectId = $r1 }
            #a row under the application object id of App One (SN-28: the focus filters on both ids)
            [pscustomobject]@{ Time = '2026-11-12'; Change = 'app renamed'; ObjectType = 'Application'; DisplayName = 'App One'; ObjectId = $ao1 }
        )
        $html = (buildSpiGrid -HtmlTableId 'TenantSummary_ServicePrincipals' -Rows $spRows -PopoutTitle 'Service Principals' -ColumnDefinitions $spColumns) +
            (buildSpiGrid -HtmlTableId 'TenantSummary_ServicePrincipalsAppRoleAssignments' -Rows $appRoleRows -PopoutTitle 'A' -ColumnDefinitions @(
                @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                @{ header = 'SP application Id'; property = 'c1' }
                @{ header = 'SP displayName'; property = 'c2' }
                @{ header = 'App role assignments (application permissions)'; property = 'c8'; htmlProperty = 'c8Html' }
                @{ header = 'API object Ids'; property = 'c9'; data = $true; snapshot = 'api' }
            )) +
            (buildSpiGrid -HtmlTableId 'TenantSummary_CleanupCandidates' -Rows $cleanupRows -PopoutTitle 'C' -ColumnDefinitions @(
                @{ header = 'SP object Id'; property = 'SPObjectId'; portal = 'servicePrincipal'; appIdProperty = 'SPAppId' }
                @{ header = 'SP application Id'; property = 'SPAppId' }
                @{ header = 'SP displayName'; property = 'SPDisplayName' }
                @{ header = 'Reason'; property = 'Reason' }
            )) +
            (buildSpiGrid -HtmlTableId 'TenantSummary_ApplicationOwners' -Rows $appOwnerRows -PopoutTitle 'O' -ColumnDefinitions @(
                @{ header = 'App object Id'; property = 'c0'; portal = 'application'; appIdProperty = 'c1' }
                @{ header = 'App application Id'; property = 'c1' }
                @{ header = 'App displayName'; property = 'c2' }
                @{ header = 'Owner DisplayName'; property = 'c5'; snapshot = 'principalName' }
                @{ header = 'Owner PrincipalType'; property = 'c6'; filter = 'select' }
                @{ header = 'Owner Id'; property = 'c7'; portal = 'principal'; typeProperty = 'c6' }
            )) +
            (buildSpiGrid -HtmlTableId 'TenantSummary_ServicePrincipalOwners' -Rows $spOwnerRows -PopoutTitle 'SO' -ColumnDefinitions @(
                @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                @{ header = 'SP application Id'; property = 'c1' }
                @{ header = 'SP displayName'; property = 'c2' }
                @{ header = 'Owner DisplayName'; property = 'c5'; snapshot = 'principalName' }
                @{ header = 'Owner PrincipalType'; property = 'c6'; filter = 'select' }
                @{ header = 'Owner Id'; property = 'c7'; portal = 'principal'; typeProperty = 'c6' }
                @{ header = 'Owner Applicability'; property = 'c8'; filter = 'select'; snapshot = 'applicability' }
                @{ header = 'Indirect via'; property = 'c10'; snapshot = 'via' }
            )) +
            (buildSpiGrid -HtmlTableId 'TenantSummary_ServicePrincipalsAADRoleAssignedOn' -Rows $holderRows -PopoutTitle 'RH' -ColumnDefinitions @(
                @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                @{ header = 'SP application Id'; property = 'c1' }
                @{ header = 'SP displayName'; property = 'c2' }
                @{ header = 'Role assignments on it'; property = 'c9'; htmlProperty = 'c9Html' }
            )) +
            (buildSpiGrid -HtmlTableId 'TenantSummary_ApplicationSecrets' -Rows $secretRows -PopoutTitle 'S' -ColumnDefinitions @(
                @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                @{ header = 'SP application Id'; property = 'c1' }
                @{ header = 'SP displayName'; property = 'c2' }
                @{ header = 'Application ObjectId'; property = 'c4'; portal = 'application'; appIdProperty = 'c5' }
                @{ header = 'Application (client) Id'; property = 'c5' }
                @{ header = 'Application DisplayName'; property = 'c6' }
                @{ header = 'Application Secrets'; property = 'c7'; htmlProperty = 'c7Html' }
            )) +
            (buildSpiGrid -HtmlTableId 'TenantSummary_FederatedIdentityCredentialTrust' -Rows $ficRows -PopoutTitle 'F' -ColumnDefinitions @(
                @{ header = 'SP object Id'; property = 'SPObjectId'; portal = 'servicePrincipal'; appIdProperty = 'SPAppId' }
                @{ header = 'SP application Id'; property = 'SPAppId' }
                @{ header = 'SP displayName'; property = 'SPDisplayName' }
                @{ header = 'Credential on'; property = 'CredentialOn'; snapshot = 'credOn' }
                @{ header = 'Name'; property = 'Name'; snapshot = 'credName' }
                @{ header = 'Issuer type'; property = 'IssuerType'; snapshot = 'credType' }
                @{ header = 'Trust'; property = 'Trust'; snapshot = 'credDetail' }
            )) +
            (buildSpiGrid -HtmlTableId 'TenantSummary_ServicePrincipalsGroupMemberships' -Rows $groupRows -PopoutTitle 'G' -ColumnDefinitions @(
                @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                @{ header = 'SP application Id'; property = 'c1' }
                @{ header = 'SP displayName'; property = 'c2' }
                @{ header = 'Group memberships'; property = 'c7'; htmlProperty = 'c7Html' }
            )) +
            (buildSpiGrid -HtmlTableId 'TenantSummary_ServicePrincipalsAzureRoleAssignments' -Rows $rbacRows -PopoutTitle 'R' -ColumnDefinitions @(
                @{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' }
                @{ header = 'SP application Id'; property = 'c1' }
                @{ header = 'SP displayName'; property = 'c2' }
                @{ header = 'SP Tags'; property = 'c4'; htmlProperty = 'c4Html' }
                @{ header = 'Classification'; property = 'c7'; filter = 'select' }
                @{ header = 'Azure role assignments'; property = 'c9'; htmlProperty = 'c9Html' }
                @{ header = 'Azure role assignment scopes'; property = 'c10'; data = $true; snapshot = 'scope' }
            )) +
            (buildSpiGrid -HtmlTableId 'TenantSummary_ChangeHistory' -Rows $changeRows -PopoutTitle 'H' -ColumnDefinitions @(
                @{ header = 'Time'; property = 'Time'; snapshot = 'time' }
                @{ header = 'Change'; property = 'Change' }
                @{ header = 'Object type'; property = 'ObjectType'; snapshot = 'objectType' }
                @{ header = 'Display name'; property = 'DisplayName'; snapshot = 'objectName' }
                @{ header = 'Object Id'; property = 'ObjectId'; snapshot = 'object' }
            )) + (buildSpiGridAssetsBlock)
        $fixture = [ordered]@{
            registry = [ordered]@{
                report = @{ tenantId = $tenant }
                pages = @(
                    @{ id = 'Overview'; title = 'Overview'; fa = 'fa-gauge-high' }, @{ id = 'Changes'; title = 'Changes'; fa = 'fa-clock-rotate-left' }
                    @{ id = 'ServicePrincipals'; title = 'Service Principals'; fa = 'fa-cubes' }, @{ id = 'Ownership'; title = 'Ownership'; fa = 'fa-user-tie' }
                    @{ id = 'ApiPermissions'; title = 'API permissions'; fa = 'fa-plug' }, @{ id = 'AzureRbac'; title = 'Azure RBAC'; fa = 'fa-shield-halved' }
                    @{ id = 'Groups'; title = 'Groups'; fa = 'fa-users' }, @{ id = 'Credentials'; title = 'Credentials'; fa = 'fa-key' }, @{ id = 'Risk'; title = 'Risk'; fa = 'fa-triangle-exclamation' }
                )
                sections = @(
                    @{ id = 'ChangeHistory'; page = 'Changes'; title = 'Change history'; state = 'ok'; grids = @('TenantSummary_ChangeHistory') }
                    @{ id = 'ServicePrincipals'; page = 'ServicePrincipals'; title = 'Service Principals'; state = 'ok'; grids = @('TenantSummary_ServicePrincipals') }
                    @{ id = 'ServicePrincipalOwners'; page = 'Ownership'; title = 'Service Principal owners'; state = 'ok'; grids = @('TenantSummary_ServicePrincipalOwners') }
                    @{ id = 'ApplicationOwners'; page = 'Ownership'; title = 'Application owners'; state = 'ok'; grids = @('TenantSummary_ApplicationOwners') }
                    @{ id = 'ServicePrincipalsAADRoleAssignedOn'; page = 'Ownership'; title = 'Entra roles assigned on Service Principals'; state = 'ok'; grids = @('TenantSummary_ServicePrincipalsAADRoleAssignedOn') }
                    @{ id = 'ServicePrincipalsAppRoleAssignments'; page = 'ApiPermissions'; title = 'Application permissions (app role assignments)'; state = 'ok'; grids = @('TenantSummary_ServicePrincipalsAppRoleAssignments') }
                    @{ id = 'ServicePrincipalsAzureRoleAssignments'; page = 'AzureRbac'; title = 'Azure role assignments of Service Principals'; state = 'ok'; grids = @('TenantSummary_ServicePrincipalsAzureRoleAssignments') }
                    @{ id = 'ServicePrincipalsGroupMemberships'; page = 'Groups'; title = 'Group memberships of Service Principals'; state = 'ok'; grids = @('TenantSummary_ServicePrincipalsGroupMemberships') }
                    @{ id = 'ApplicationSecrets'; page = 'Credentials'; title = 'Application secrets'; state = 'ok'; grids = @('TenantSummary_ApplicationSecrets') }
                    @{ id = 'FederatedIdentityCredentialTrust'; page = 'Credentials'; title = 'Federated identity credentials: who can sign in'; state = 'ok'; grids = @('TenantSummary_FederatedIdentityCredentialTrust') }
                    @{ id = 'CleanupCandidates'; page = 'Risk'; title = 'Cleanup candidates'; state = 'warning'; grids = @('TenantSummary_CleanupCandidates') }
                )
            }
            grids = Read-SnapshotGrids $html
            ids = $script:Ids
            apiCell = $appRoleRows[0].c8Html
        }
        $fixtureFile = Join-Path $TestDrive 'snapshot-fixture.json'
        [IO.File]::WriteAllText($fixtureFile, ($fixture | ConvertTo-Json -Depth 20), [Text.UTF8Encoding]::new($false))
        $escape = [regex]::Match($script:Runtime, '(?s)    function spiEscapeHtml\(text\) \{.*?\n    \}\n').Value
        $portalHref = [regex]::Match($script:Runtime, "(?s)    function spiPortalHref\(kind, value, auxValue\) \{.*?\n        return '';\n    \}\n").Value
        $snapshot = $script:Runtime.Substring($script:Runtime.IndexOf('    /* ---------- SP snapshot: zoom in'))
        $escape | Should -Not -BeNullOrEmpty
        $portalHref | Should -Not -BeNullOrEmpty
        $codeFile = Join-Path $TestDrive 'snapshot-code.js'
        [IO.File]::WriteAllText($codeFile, $escape + $portalHref + $snapshot, [Text.UTF8Encoding]::new($false))
        $runner = @'
const fs = require('fs'), vm = require('vm');
const fixture = JSON.parse(fs.readFileSync(process.argv[3], 'utf8'));
global.window = global; global.addEventListener = () => {};
global.performance = { now: () => Date.now() };
global.document = {
    addEventListener() {},
    getElementById: (id) => id === 'azadspiRegistry' ? { textContent: JSON.stringify(fixture.registry) } : (id.indexOf('spiData4') === 0 ? {} : null),
    createElement: () => ({ content: { querySelectorAll: () => [] } })
};
global.spiGridDataProviders = {};
global.spiLoadGridData = (id) => Promise.resolve(fixture.grids[id].data);
vm.runInThisContext(fs.readFileSync(process.argv[2], 'utf8'));
Object.keys(fixture.grids).forEach((id) => spiSnapRegister(id, fixture.grids[id].meta));
const cards = (html) => [...html.matchAll(/<h4>(.*?)<\/h4>/g)].map((m) => m[1]);
const groups = (html) => [...html.matchAll(/<h3 class="spiSnapGroupTitle"[^>]*><i[^>]*><\/i> (.*?)<\/h3>/g)].map((m) => m[1]);
spiSnapIndex().then((index) => {
    const ids = fixture.ids, out = {};
    const one = spiSnapHtml(index, ids.a1);
    out.one = { id: one.entity.id, name: one.entity.name, groups: groups(one.html), cards: cards(one.html), html: one.html };
    out.byAppId = spiSnapEntity(index, ids.aid1.toUpperCase()).id;
    out.byAppObject = spiSnapEntity(index, ids.ao1).id;
    const graph = spiSnapHtml(index, ids.g1);
    out.graph = { groups: groups(graph.html), cards: cards(graph.html), html: graph.html };
    const lonely = spiSnapHtml(index, ids.ao2);
    out.lonely = { id: lonely.entity.id, spId: lonely.entity.spId, cards: cards(lonely.html), html: lonely.html };
    out.unknown = spiSnapHtml(index, '99999999-9999-9999-9999-999999999999').html;
    out.findName = spiSnapFind(index, 'micro').map((e) => e.id);
    out.findId = spiSnapFind(index, ids.a1.slice(0, 8)).map((e) => e.id);
    out.decorate = spiSnapDecorate('<a class="externallink" href="https://portal.azure.com/#view/Microsoft_AAD_IAM/ManagedAppMenuBlade/~/Overview/objectId/' + ids.a1 + '/appId/' + ids.aid1 + '" target="_blank">App One</a>');
    const user = spiSnapHtml(index, ids.u1);
    out.user = { kind: user.entity.kind, name: user.entity.name, type: user.entity.type, cards: cards(user.html), html: user.html };
    const group = spiSnapHtml(index, ids.grp);
    out.group = { kind: group.entity.kind, name: group.entity.name, type: group.entity.type, cards: cards(group.html), html: group.html };
    const sub = spiSnapHtml(index, ids.sub1.toUpperCase() + '/');
    out.sub = { kind: sub.entity.kind, id: sub.entity.id, type: sub.entity.type, cards: cards(sub.html), html: sub.html };
    const rg = spiSnapHtml(index, ids.rg1);
    out.rg = { name: rg.entity.name, type: rg.entity.type, cards: cards(rg.html), html: rg.html };
    const res = spiSnapHtml(index, ids.res1);
    out.res = { type: res.entity.type, cards: cards(res.html) };
    out.findGroup = spiSnapFind(index, 'grp').map((e) => e.id);
    out.findScope = spiSnapFind(index, 'rg1').map((e) => e.id);
    out.decorateScope = spiSnapDecorate('<span class="spiItem">Reader<span class="spiScope" data-spi-scope="' + ids.rg1 + '" data-spi-scope-name="rg1"></span></span>');
    out.oneAfter = cards(spiSnapHtml(index, ids.a1).html);
    const groupsOf = (id) => spiGraphGroups(index, spiSnapEntity(index, id)).map((g) => (g.inward ? 'in ' : 'out ') + g.label + ': ' + g.nodes.map((n) => n.kind + ' ' + n.id + (n.details.length ? ' [' + n.details.join(', ') + ']' : '')).join(', '));
    out.graphOne = groupsOf(ids.a1);
    out.graphApi = groupsOf(ids.g1);
    out.graphUser = groupsOf(ids.u1);
    out.graphRg = groupsOf(ids.rg1);
    const graphOne = spiGraphHtml(index, ids.a1).html;
    out.graphNodes = (graphOne.match(/class="spiGraphNode /g) || []).length;
    out.graphRisky = (graphOne.match(/<line [^>]*class="risky/g) || []).length;
    spiGraphBundleAt = 0;
    const bundled = spiGraphHtml(index, ids.a1).html;
    out.graphBundles = (bundled.match(/spiGraphCollapsed/g) || []).length;
    spiSnapState.graphExpanded = { 'Member of': true };
    out.graphOpened = (spiGraphHtml(index, ids.a1).html.match(/spiGraphCollapsed/g) || []).length;
    out.graphArrows = (graphOne.match(/marker-end="url\(#spiGraphArrow(Risky)?\)"/g) || []).length;
    out.graphCreds = spiGraphGroups(index, spiSnapEntity(index, ids.a1)).filter((g) => /Secrets|Federated/.test(g.label)).map((g) => g.label + '|' + g.anchor + '|' + (g.inward ? 'in' : 'out') + '|' + g.nodes.map((n) => n.sub + ':' + n.name + ' [' + n.details.join(', ') + ']' + (n.state ? ' !' + n.state : '')).join('; '));
    out.graphMi = groupsOf(ids.m1);
    out.graphCenters = (spiGraphHtml(index, ids.a1).html.match(/data-center="(sp|app)"/g) || []).join(',') + ' / ' + (spiGraphHtml(index, ids.m1).html.match(/data-center="(sp|app)"/g) || []).join(',') + ' / ' + (spiGraphHtml(index, ids.ao2).html.match(/data-center="(sp|app)"/g) || []).join(',');
    out.graphPair = (spiGraphHtml(index, ids.a1).html.match(/class="spiGraphPair"/g) || []).length;
    const site = spiSnapHtml(index, ids.site1);
    out.site = { name: site.entity.name, type: site.entity.type, cards: cards(site.html) };
    const api2 = spiSnapHtml(index, ids.g2);
    out.api2 = { cards: cards(api2.html), html: api2.html };
    out.apiPermissionsCell = spiSnapApiPermissionsHtml(fixture.apiCell, ids.g1);
    out.apiPermissionsNone = spiSnapApiPermissionsHtml(fixture.apiCell, '12345678-1234-1234-1234-123456789012');
    out.graphApi2 = groupsOf(ids.g2);
    const removed = spiSnapHtml(index, ids.r1);
    out.removed = { kind: removed.entity.kind, name: removed.entity.name, type: removed.entity.type, cards: cards(removed.html), html: removed.html };
    const mi = spiSnapHtml(index, ids.m1);
    out.mi = { cards: cards(mi.html), html: mi.html };
    out.graphS3 = groupsOf(ids.s3);
    out.groupGraph = groupsOf(ids.grp);
    const root = spiSnapHtml(index, ids.mgRoot);
    out.root = { type: root.entity.type, cards: cards(root.html) };
    const mg = spiSnapHtml(index, ids.mg1);
    out.mg = { cards: cards(mg.html), html: mg.html };
    out.graphSub = groupsOf(ids.sub1);
    const plan = spiFocusPlan(index, spiSnapEntity(index, ids.a1));
    out.focusPlan = plan['TenantSummary_ChangeHistory'];
    out.focusModel = spiFocusModel(out.focusPlan);
    out.focusPlanEmpty = plan['TenantSummary_ServicePrincipalsAADRoleAssignedOn'] ? null : 'none';
    out.focusPlanG1 = spiFocusPlan(index, spiSnapEntity(index, ids.g1))['TenantSummary_ServicePrincipalsAppRoleAssignments'];
    spiFocusState.plan = plan; spiFocusState.applied['TenantSummary_ChangeHistory'] = { colId: out.focusPlan.colId, previous: null };
    out.focusKeep = spiFocusKeep('TenantSummary_ChangeHistory', { Change: { filterType: 'text', type: 'contains', filter: 'owner' } });
    out.focusKeepOther = spiFocusKeep('TenantSummary_CleanupCandidates', null);
    spiFocusState.plan = null; spiFocusState.applied = {};
    out.valid = [ids.a1, '/', '/subscriptions/x/resourcegroups/y/', '/providers/microsoft.management/managementgroups/mg1', '/Your tenant is compromised, call 0123', 'javascript:alert(1)', '/'.repeat(3000), 'App One'].map(spiSnapValidId);
    out.itemsFilter = spiSnapItemsFilter(fixture.apiCell, (html) => html.indexOf('Write') >= 0);
    out.itemsFilterSingle = spiSnapItemsFilter('<span class="spiItem">x</span>', () => true);
    out.unknownGraph = spiGraphHtml(index, '99999999-9999-9999-9999-999999999999').html;
    spiGraphBundleAt = 0; spiSnapState.graphExpanded = { 'Owner of (applications)': true };
    out.hostileGraph = spiGraphHtml(index, ids.u1).html + spiGraphHtml(index, ids.ao2).html;
    spiGraphBundleAt = 10; spiSnapState.graphExpanded = {};
    spiGraphBundleAt = 10; spiSnapState.graphExpanded = {};
    const card = (html, title) => { const at = html.indexOf('<h4>' + title + '</h4>'); return at < 0 ? '' : html.slice(at, html.indexOf('</section>', at)); };
    out.glance = (one.html.match(/<div class="spiSnapGlance"[\s\S]*?<\/div>/) || [''])[0].replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ').trim();
    out.control = card(one.html, 'Who can control it').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ');
    out.reachOne = card(one.html, 'What it can reach (2 hops)').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ');
    out.reachMi = card(spiSnapHtml(index, ids.m1).html, 'What it can reach (2 hops)');
    out.byPermission = card(spiSnapHtml(index, ids.g1).html, 'Clients by permission (this object as the API)').replace(/<[^>]*>/g, ' ').replace(/\s+/g, ' ');
    spiGraphHtml(index, ids.a1);
    out.mermaid = spiGraphMermaid();
    out.csv = spiGraphCsv().split('\r\n');
    out.edgeCount = spiGraphEdges().length;
    out.start = cards(spiSnapStartHtml(index));
    const hostileMarker = spiSnapPrincipalMarkers('<span class="spiPrincipal" data-spi-principal="' + ids.u1 + '" data-spi-principal-name="&lt;img src=x onerror=alert(1)&gt; &quot;q&quot;" data-spi-principal-type="User"></span>')[0];
    out.hostileMarker = { name: hostileMarker.name, node: spiGraphNodeHtml({ id: hostileMarker.id, name: hostileMarker.name, type: hostileMarker.type, kind: 'user', details: [hostileMarker.name] }) };
    out.apiAction = (spiSnapHtml(index, ids.g1).html.match(/<h4>Clients with application permissions on this API<\/h4>[\s\S]*?<\/button>/) || [''])[0];
    out.scopeAction = (spiSnapHtml(index, ids.rg1).html.match(/<h4>Role assignments at this scope or below<\/h4>[\s\S]*?<\/button>/) || [''])[0];
    out.userAction = (spiSnapHtml(index, ids.u1).html.match(/<h4>Owner of these applications<\/h4>[\s\S]*?<\/button>/) || [''])[0];
    spiSnapState.graphRiskyOnly = true;
    out.riskyOnly = (spiGraphHtml(index, ids.a1).html.match(/class="spiGraphBubble[ "]/g) || []).length;
    out.riskyNote = /3 other kinds hidden/.test(spiGraphHtml(index, ids.a1).html);
    spiSnapState.graphRiskyOnly = false;
    out.index = { grids: index.grids.length, failed: index.failed };
    console.log(JSON.stringify(out));
}).catch((e) => { console.error(e && e.stack || e); process.exit(1); });
'@
        $runnerFile = Join-Path $TestDrive 'snapshot-run.js'
        [IO.File]::WriteAllText($runnerFile, $runner, [Text.UTF8Encoding]::new($false))
        $savedEncoding = [Console]::OutputEncoding
        try {
            [Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
            $script:Result = (& $script:Node.Source $runnerFile $codeFile $fixtureFile) -join "`n" | ConvertFrom-Json
        }
        finally {
            [Console]::OutputEncoding = $savedEncoding
        }
    }
    BeforeEach {
        if (-not $script:Node) { Set-ItResult -Skipped -Because 'node is not available' }
    }
    It 'indexes every grid' {
        $script:Result.index.grids | Should -Be 11
        @($script:Result.index.failed).Count | Should -Be 0
    }
    It 'finds the object by its object id, application id (any case) and application object id' {
        $script:Result.one.id | Should -Be $script:Ids.a1
        $script:Result.one.name | Should -BeExactly 'App One'
        $script:Result.byAppId | Should -Be $script:Ids.a1
        $script:Result.byAppObject | Should -Be $script:Ids.a1
    }
    It 'cards: identity, findings first (a warning section), the page groups, the history last' {
        @($script:Result.one.groups) | Should -Be @('Identity', 'Control and reach', 'Findings', 'Ownership', 'API permissions', 'Azure RBAC', 'Groups', 'Credentials', 'History')
        @($script:Result.one.cards) | Should -Be @('Service Principal and application', 'Who can control it', 'What it can reach (2 hops)', 'Cleanup candidates', 'Service Principal owners', 'Entra roles assigned on Service Principals', 'Owner of these applications', 'Application permissions (app role assignments)', 'Azure role assignments of Service Principals', 'Group memberships of Service Principals', 'Application secrets', 'Federated identity credentials: who can sign in', 'Change history')
    }
    It 'a card leaves out the columns that only repeat the object; several rows are a table' {
        $cleanup = [regex]::Match($script:Result.one.html, '(?s)<h4>Cleanup candidates</h4>.*?</section>').Value
        $cleanup | Should -Match '<dt>Reason</dt><dd>expired credentials</dd>'
        $cleanup | Should -Not -Match 'SP displayName|SP object Id|SP application Id'
        $history = [regex]::Match($script:Result.one.html, '(?s)<h4>Change history</h4>.*?</section>').Value
        $history | Should -Match '<span class="spiSnapCount"[^>]*>3</span>'
        $history | Should -Match '<table class="spiSnapTable">'
        $history | Should -Not -Match 'object added'
    }
    It 'an API lists its clients, with a snapshot link to each' {
        @($script:Result.graph.cards) | Should -Contain 'Clients with application permissions on this API'
        $script:Result.graph.html | Should -Match "data-spi-snap=`"$($script:Ids.a1)`""
        $script:Result.graph.html | Should -Match 'User\.Read\.All'
    }
    It 'SN-1: the permissions of a client on an API are matched by the API id - two APIs with the same display name stay apart' {
        $script:Result.graph.html | Should -Not -Match 'Write\.All'
        $script:Result.api2.html | Should -Match 'Write\.All'
        $script:Result.api2.html | Should -Not -Match 'User\.Read\.All'
        $script:Result.apiPermissionsCell | Should -Match '^<b class="spiItemsLabel" data-spi-api="22222222-2222-2222-2222-222222222222">Microsoft Graph \(1\)</b><ul class="spiList spiItems"><li>User\.Read\.All</li></ul>$'
        $script:Result.apiPermissionsNone | Should -BeExactly '' -Because 'no group of the API: nothing, not the whole cell'
        @($script:Result.graphApi2) | Should -Be @("in Clients (application permissions): sp $($script:Ids.a1) [Write.All]")
    }
    It 'SN-2: an id outside the Service Principals list (a removed object) shows the rows about it, its name from them' {
        $script:Result.removed.kind | Should -BeExactly 'principal'
        $script:Result.removed.name | Should -BeExactly 'Removed App'
        @($script:Result.removed.cards) | Should -Be @('Change history')
        $script:Result.removed.html | Should -Match 'object removed'
        $script:Result.removed.html | Should -Not -Match 'not named in any section'
    }
    It 'SN-3/SN-11: a group is matched by its id in markup (marker, link), never in text (a tag naming its id); filtered cards drop the summary columns' {
        @($script:Result.group.cards) | Should -Be @('Azure role assignments through this group', 'Service Principals that are members')
        $script:Result.group.html | Should -Match 'Contributor'
        $script:Result.group.html | Should -Not -Match 'Reader' -Because 'only the item through the group'
        $script:Result.group.html | Should -Not -Match 'SP Tags|Classification|CostCenterOwnerGroup'
        @($script:Result.groupGraph) | Should -Be @("out Role assignments through this group: sp $($script:Ids.a1) [Contributor]", "in Members: sp $($script:Ids.a1)")
    }
    It 'SN-4/SN-9: direct and indirect owners in separate bubbles (with the path); a Service Principal sees what it owns, also indirectly' {
        @($script:Result.graphOne)[0..1] | Should -Be @(
            "in Owners: sp $($script:Ids.m1)"
            "in Owners (indirect): user $($script:Ids.u1) [indirect via owner of site identity]"
        )
        @($script:Result.graphApi)[0] | Should -BeExactly "in Owners (indirect): sp $($script:Ids.m1) [indirect via owner of App One]"
        @($script:Result.mi.cards) | Should -Contain 'Owner of these Service Principals'
        @($script:Result.graphMi) | Should -Contain "out Owner of (indirect): sp $($script:Ids.g1) [indirect via owner of App One]"
        @($script:Result.graphUser)[0] | Should -BeExactly "out Owner of (indirect): sp $($script:Ids.a1) [indirect via owner of site identity]"
    }
    It 'SN-6/SN-7: an Entra role holder is the node (the role its detail); a single-item cell is kept on the holder''s snapshot' {
        @($script:Result.graphOne)[2] | Should -BeExactly "in Entra role holders on it: user $($script:Ids.u1) [Application Administrator]"
        @($script:Result.user.cards) | Should -Contain 'Holds an Entra role scoped to these Service Principals'
        $script:Result.user.html | Should -Match 'Application Administrator'
        @($script:Result.graphUser) | Should -Contain "out Holds an Entra role on: sp $($script:Ids.a1) [Application Administrator]"
    }
    It 'SN-12: a relation to itself (an app owned by its own Service Principal) is a note, not dropped' {
        @($script:Result.graphS3) | Should -Be @('in Application owners: note self:sp')
    }
    It 'SN-10: the Tenant Root Group is above every scope; a management group shows the assignments at it, with a note' {
        $script:Result.root.type | Should -BeExactly 'Management group (Tenant Root Group)'
        @($script:Result.root.cards) | Should -Contain 'Role assignments in the tenant (at the root and below)'
        @($script:Result.mg.cards) | Should -Be @('Inherited from above (tenant root)')
        $script:Result.mg.html | Should -Match 'not known in this report'
        @($script:Result.graphSub) | Should -Contain "in Inherited from above: sp $($script:Ids.m1) [Owner]"
    }
    It 'an application without a Service Principal: its own snapshot, the owner Service Principal linked; names are encoded' {
        $script:Result.lonely.id | Should -Be $script:Ids.ao2
        $script:Result.lonely.spId | Should -BeNullOrEmpty
        @($script:Result.lonely.cards) | Should -Contain 'Application owners'
        $script:Result.lonely.html | Should -Match "data-spi-snap=`"$($script:Ids.a1)`""
        $script:Result.lonely.html | Should -Match 'Lonely &lt;img src=x onerror=alert\(1\)&gt; App'
        $script:Result.lonely.html | Should -Not -Match '<img'
    }
    It 'a user: the rows that name it (owner columns), its name and type from the owner columns' {
        $script:Result.user.kind | Should -BeExactly 'principal'
        $script:Result.user.name | Should -BeExactly 'Una User'
        $script:Result.user.type | Should -BeExactly 'User (Member)'
        @($script:Result.user.cards) | Should -Be @('Owner of these Service Principals', 'Owner of these applications', 'Holds an Entra role scoped to these Service Principals')
        $script:Result.user.html | Should -Match "data-spi-snap=`"$($script:Ids.ao2)`""
        $script:Result.user.html | Should -Not -Match 'Owner DisplayName|Owner Id'
    }
    It 'a group: the rows whose list cells link it, its name from the link' {
        $script:Result.group.name | Should -BeExactly 'Grp One'
        $script:Result.group.type | Should -BeExactly 'Group'
        @($script:Result.group.cards) | Should -Contain 'Service Principals that are members'
        $script:Result.group.html | Should -Match "data-spi-snap=`"$($script:Ids.a1)`""
    }
    It 'an Azure scope: role assignments at the scope or below, inherited from above; the id normalized' {
        $script:Result.sub.kind | Should -BeExactly 'scope'
        $script:Result.sub.id | Should -BeExactly $script:Ids.sub1
        $script:Result.sub.type | Should -BeExactly 'Subscription'
        @($script:Result.sub.cards) | Should -Be @('Managed identities (system assigned) of resources here or below', 'Role assignments at this scope or below', 'Inherited from above (tenant root, subscription, resource group)')
        $script:Result.rg.name | Should -BeExactly 'rg1'
        $script:Result.rg.type | Should -BeExactly 'Resource group'
        $script:Result.rg.html | Should -Match "data-spi-snap=`"$($script:Ids.sub1)`"" -Because 'the subscription above is linked'
        $script:Result.res.type | Should -BeExactly 'microsoft.keyvault/vaults' -Because 'a resource: its type'
        @($script:Result.res.cards) | Should -Be @('Inherited from above (tenant root, subscription, resource group)')
    }
    It 'find: users, groups and scopes too; the scope marker of a cell becomes its snapshot icon' {
        @($script:Result.findGroup) | Should -Be @($script:Ids.grp)
        @($script:Result.findScope) | Should -Contain $script:Ids.rg1
        $script:Result.decorateScope | Should -Match "Reader <a class=`"spiSnapLink`" href=`"\?sp=%2Fsubscriptions%2F55555555-5555-5555-5555-555555555555%2Fresourcegroups%2Frg1`" data-spi-snap=`"$([regex]::Escape($script:Ids.rg1))`""
        $script:Result.decorateScope | Should -Not -Match 'data-spi-scope='
    }
    It 'an owner column (principal) does not make a row about the owner: no Ownership card for the owning Service Principal' {
        @($script:Result.oneAfter) | Should -Not -Contain 'Application owners'
    }
    It 'graph: the relations (direction, Azure roles at a scope, permissions on an API) of a Service Principal, an API, a user, a scope' {
        @($script:Result.graphOne).Count | Should -Be 8 -Because 'owners (direct, indirect), role holders, APIs, Azure, groups and the two credential bubbles'
        @($script:Result.graphOne)[3..5] | Should -Be @(
            "out APIs (application permissions): sp $($script:Ids.g1) [User.Read.All], sp $($script:Ids.g2) [Write.All]"
            "out Azure role assignments: scope $($script:Ids.rg1) [Reader], scope $($script:Ids.sub1) [Contributor (PIM; through group Grp One ($($script:Ids.grp)))]"
            "out Member of: group $($script:Ids.grp)"
        )
        @($script:Result.graphApi)[1] | Should -BeExactly "in Clients (application permissions): sp $($script:Ids.a1) [User.Read.All]"
        @($script:Result.graphUser) | Should -Contain "out Owner of (applications): app $($script:Ids.ao2)"
        @($script:Result.graphRg) | Should -Be @(
            "in Role assignments at this scope or below: sp $($script:Ids.a1) [Reader]"
            "in Inherited from above: sp $($script:Ids.a1) [Contributor (PIM; through group Grp One ($($script:Ids.grp)))], sp $($script:Ids.m1) [Owner]"
            "out Managed identities (system assigned): sp $($script:Ids.m1)"
            "out In: scope $($script:Ids.sub1)"
        )
    }
    It 'graph: a node per object plus the centre, an arrow per bubble, control relations marked, large bubbles closed until opened' {
        $script:Result.graphNodes | Should -Be 13 -Because 'the enterprise application, the app registration and 11 related objects'
        $script:Result.graphArrows | Should -Be 8
        $script:Result.graphPair | Should -Be 1
        $script:Result.graphRisky | Should -Be 5 -Because 'the owners (direct, indirect), the role holders, the Azure role assignments and the federated credential'
        $script:Result.graphBundles | Should -Be 8
        $script:Result.graphOpened | Should -Be 7
    }
    It 'graph: credentials - the secrets of the app registration (expiry, expired muted), its federated credentials (who can sign in)' {
        @($script:Result.graphCreds) | Should -Be @(
            'Secrets|app|in|secret:old secret [expired, 2025-01-01] !muted; secret:new secret [expires in 200 days, 2027-01-01]'
            'Federated credentials (app registration)|app|in|fic:deploy [repo org/app, branch main]'
        )
    }
    It 'graph: the centre - enterprise application and app registration, a managed identity alone, an app registration alone' {
        $script:Result.graphCenters | Should -BeExactly 'data-center="sp",data-center="app" / data-center="sp" / data-center="app"'
    }
    It 'graph and snapshot: the Azure resource of a system-assigned managed identity' {
        @($script:Result.graphMi)[0] | Should -BeExactly "in Managed identity of (Azure resource): scope $($script:Ids.site1)"
        $script:Result.site.name | Should -BeExactly 'app1'
        $script:Result.site.type | Should -BeExactly 'microsoft.web/sites'
        @($script:Result.site.cards) | Should -Be @('Managed identities (system assigned) of resources here or below', 'Inherited from above (tenant root, subscription, resource group)')
    }
    It 'SN-28/SN-20: the focus filters on the object''s ids in one column (two ids: an OR filter), counts what it shows, and is kept in a new filter model' {
        $script:Result.focusPlan.colId | Should -BeExactly 'ObjectId'
        @($script:Result.focusPlan.values) | Should -Be @($script:Ids.a1, $script:Ids.ao1)
        $script:Result.focusPlan.count | Should -Be 3
        $script:Result.focusModel.operator | Should -BeExactly 'OR'
        @($script:Result.focusModel.conditions.filter) | Should -Be @($script:Ids.a1, $script:Ids.ao1)
        $script:Result.focusPlanG1.count | Should -Be 0 -Because 'no rows about the API itself: a filter that leaves the grid empty'
        @($script:Result.focusPlanG1.values) | Should -Be @($script:Ids.g1)
        $script:Result.focusKeep.Change.filter | Should -BeExactly 'owner'
        $script:Result.focusKeep.ObjectId.operator | Should -BeExactly 'OR'
        $script:Result.focusKeepOther | Should -BeNullOrEmpty -Because 'a grid without a focus filter keeps its model'
    }
    It 'SN-33: ?sp= opens only an object id or an Azure scope' {
        @($script:Result.valid) | Should -Be @($true, $true, $true, $true, $false, $false, $false, $false)
    }
    It 'list cells: items filtered by a string scan of the markup - the label count follows, a single item is kept or dropped whole' {
        $script:Result.itemsFilter | Should -BeExactly '<b class="spiItemsLabel" data-spi-api="77777777-7777-7777-7777-777777777777">Microsoft Graph (1)</b><ul class="spiList spiItems"><li>Write.All</li></ul>'
        $script:Result.itemsFilterSingle | Should -BeExactly '<span class="spiItem">x</span>'
    }
    It 'SN-38: the graph of an id the report does not know: the note, no node of its own' {
        $script:Result.unknownGraph | Should -Match 'not named in any section of this report'
        $script:Result.unknownGraph | Should -Not -Match 'spiGraphNode'
    }
    It 'names reach the graph encoded (centre, nodes, bubbles opened)' {
        $script:Result.hostileGraph | Should -Match 'Lonely &lt;img src=x onerror=alert\(1\)&gt; App'
        $script:Result.hostileGraph | Should -Not -Match '<img src=x|onerror=alert\(1\)>'
    }
    It 'SE-1: at a glance - effective privilege, findings, credentials, control, reach, the last change' {
        $script:Result.glance | Should -Match 'Findings 1 in 1 section'
        $script:Result.glance | Should -Match 'Credentials 3, 1 expired'
        $script:Result.glance | Should -Match 'Controlled by 1 direct owner, 1 indirect, 1 role holder'
        $script:Result.glance | Should -Match 'Reaches 2 Azure scopes'
        $script:Result.glance | Should -Match 'Last change 2026-11-12'
    }
    It 'SE-12: who can control it - owners (direct, indirect with the path), role holders, who signs in as it' {
        $script:Result.control | Should -Match 'Owner of the enterprise application'
        $script:Result.control | Should -Match 'Controls an owner of the enterprise application indirect via owner of site identity'
        $script:Result.control | Should -Match 'Entra role scoped to the enterprise application Application Administrator'
        $script:Result.control | Should -Match 'Signs in as it \(federated credential on the app registration\)'
        $script:Result.control | Should -Match 'Not in this report'
    }
    It 'SE-18: what it can reach - its privileged relations and those of the objects it owns' {
        $script:Result.reachOne | Should -Match 'Azure role assignments 2'
        $script:Result.reachOne | Should -Match 'APIs \(application permissions\) 2'
        $script:Result.reachMi | Should -Match 'Owner of \(indirect\)'
        $script:Result.reachMi | Should -Match "data-spi-snap=`"$($script:Ids.g1)`""
    }
    It 'SE-13: an API lists its clients by permission' {
        $script:Result.byPermission | Should -Match 'User\.Read\.All Application'
        $script:Result.byPermission | Should -Match '1 client'
        $script:Result.byPermission | Should -Not -Match 'Write\.All'
    }
    It 'SE-16: the graph as Mermaid and as a CSV edge list (every relation, the direction of the arrows)' {
        $script:Result.mermaid | Should -Match '^graph LR'
        $script:Result.mermaid | Should -Match '-->\|"Owners"\|'
        @($script:Result.csv)[0] | Should -BeExactly 'From,From type,From id,Relation,To,To type,To id,Details'
        @($script:Result.csv).Count | Should -Be ($script:Result.edgeCount + 1)
    }
    It 'SE-17: a hostile name in a marker is decoded as text and encoded again in the graph' {
        $script:Result.hostileMarker.name | Should -BeExactly '<img src=x onerror=alert(1)> "q"'
        $script:Result.hostileMarker.node | Should -Not -Match '<img src=x'
        $script:Result.hostileMarker.node | Should -Match '&lt;img src=x onerror=alert\(1\)&gt; &quot;q&quot;'
    }
    It 'SE-14: the start page lists the most exposed objects' {
        @($script:Result.start) | Should -Contain 'Most findings'
        @($script:Result.start) | Should -Contain 'Secrets and certificates expiring or expired'
    }
    It 'SE-3: Show in report of an API''s clients, a scope and an owner filters on the API id, the scope, the owner id' {
        $script:Result.apiAction | Should -Match "data-col=`"c9`" data-value=`"$($script:Ids.g1)`""
        $script:Result.scopeAction | Should -Match "data-col=`"c10`" data-value=`"$([regex]::Escape($script:Ids.rg1))`""
        $script:Result.userAction | Should -Match "data-col=`"c7`" data-value=`"$($script:Ids.u1)`""
    }
    It 'SE-15: control and privilege only - the other bubbles hidden, with a note' {
        $script:Result.riskyOnly | Should -Be 5
        $script:Result.riskyNote | Should -BeTrue
    }
    It 'an id that is not in the report says so' {
        $script:Result.unknown | Should -Match 'not named in any section of this report'
    }
    It 'find: by name (contains) and by the start of an id' {
        @($script:Result.findName)[0..1] | Should -Be @($script:Ids.g1, $script:Ids.g2)
        @($script:Result.findId) | Should -Be @($script:Ids.a1)
    }
    It 'html cells: the snapshot icon after the Entra portal link of a Service Principal' {
        $script:Result.decorate | Should -Match "</a> <a class=`"spiSnapLink`" href=`"\?sp=$($script:Ids.a1)`" data-spi-snap=`"$($script:Ids.a1)`""
    }
}
