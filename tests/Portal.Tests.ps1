#Pester 5+ tests for the portal theme (theme 28): pages and sections of the report, the hosted versioned shell assets, the
#design tokens of both themes, the section registry, the shell script.

#The stylesheet, shell script and icons of the report are hosted on azadvertizer.net and not part of the public repository. The
#maintainer keeps them in _local/html (local only); the tests of these files run where that folder exists and are skipped elsewhere (CI)
BeforeDiscovery {
    $script:HasPortalAssets = Test-Path -LiteralPath (Join-Path (Split-Path -Parent $PSScriptRoot) '_local/html')
}

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $script:Ast = Get-ScriptAst -Path $script:MainScriptPath
    $script:HtmlDir = Join-Path $script:RepoRoot '_local/html'
    $script:PortalCode = [regex]::Match($script:Source, '(?s)#region portal shell\r?\n.*?#endregion portal shell').Value
    $script:Version = [regex]::Match($script:PortalCode, "spiPortalAssetVersion = '(\d{3}_\d{3})'").Groups[1].Value
    $script:JsPath = Join-Path $script:HtmlDir "azadspi-portal_$($script:Version).js"
    if (Test-Path -LiteralPath $script:HtmlDir) {
        $script:Css = Get-Content -Raw -LiteralPath (Join-Path $script:HtmlDir "azadspi-portal_$($script:Version).css")
        $script:Js = Get-Content -Raw -LiteralPath $script:JsPath
    }
    function Get-Tokens([string]$block) { @([regex]::Matches($block, '(--az-[\w-]+)\s*:') | ForEach-Object { $_.Groups[1].Value } | Sort-Object -Unique) }
    #loads the portal region (and what it needs) into the caller's scope
    function Import-Portal {
        . ([scriptblock]::Create($script:Ast.Find({ param($n) $n -is [System.Management.Automation.Language.TypeDefinitionAst] -and $n.Name -eq 'HtmlSafe' }, $true).Extent.Text))
        $script:Product = 'AzADServicePrincipalInsights'
        . ([scriptblock]::Create($script:PortalCode))
    }
}

Describe 'portal: pages and sections' {
    It 'the region and a versioned asset name are there' {
        $script:PortalCode | Should -Not -BeNullOrEmpty
        $script:Version | Should -Match '^\d{3}_\d{3}$'
    }
    It 'every section the report writes is in the catalog, on an existing page' {
        . Import-Portal
        $ids = @([regex]::Matches($script:Source, "useSpiSection -Id '(\w+)'") | ForEach-Object { $_.Groups[1].Value }) +
               @([regex]::Matches($script:Source, "addReportSection -Region '(\w+)'") | ForEach-Object { $_.Groups[1].Value }) | Sort-Object -Unique
        $ids.Count | Should -BeGreaterOrEqual 30
        foreach ($sectionId in $ids) {
            $script:spiSectionCatalog.ContainsKey($sectionId) | Should -BeTrue -Because "section '$sectionId' is written by the report"
            $script:spiPages.Contains($script:spiSectionCatalog[$sectionId].page) | Should -BeTrue -Because "the page of '$sectionId'"
        }
        #no catalog entry without a writer
        foreach ($sectionId in $script:spiSectionCatalog.Keys) { $ids + 'FindingsOverview' | Should -Contain $sectionId }
    }
    It 'the former section markup is gone (buttons, content divs, chart toggles, footer, jQuery)' {
        $script:Source | Should -Not -Match 'class="(collapsible|nonCollapsible|decollapsible)"'
        $script:Source | Should -Not -Match 'content TenantSummaryContent'
        $script:Source | Should -Not -Match 'class="footer"|VersionAlert|se-pre-con|toggle_v004|collapsetable_v004|version_v001|azadserviceprincipalinsightsmain_'
        $script:Source | Should -Not -Match '(?i)jquery'
        $script:Source | Should -Not -Match 'id="tenantSummaryPolicy"'
    }
    It 'every page icon is in the repository (to upload with the shell)' -Skip:(-not $script:HasPortalAssets) {
        . Import-Portal
        foreach ($page in $script:spiPages.Values) {
            $page.fa | Should -Match '^fa-[a-z0-9-]+$' -Because "page $($page.id) has a Font Awesome icon (fallback)"
            if ($page.icon) { Join-Path $script:HtmlDir "icon/$($page.icon)" | Should -Exist }
        }
    }
}

Describe 'portal: credit and section titles' {
    It 'the grids that build on the work of Thomas Naunheim credit him, with both repositories' {
        $script:Source | Should -Match 'https://github\.com/Cloud-Architekt/EntraOps'
        $script:Source | Should -Match 'https://github\.com/Cloud-Architekt/AzurePrivilegedIAM'
        @([regex]::Matches($script:Source, 'getThomasCreditHtml -What')).Count | Should -Be 3 -Because 'Entra role assignments, HiPo users, effective privilege'
        $script:Source | Should -Match "addReportSection -Region 'ServicePrincipalEffectivePrivilege' [^
]*-CreditHtml \(getThomasCreditHtml"
    }
    It 'section titles use one naming style (no AAD, no RoleAssignments, no count in the title)' {
        . Import-Portal
        foreach ($section in $script:spiSectionCatalog.Values) {
            $section.title | Should -Not -MatchExactly 'AAD|RoleAssign|Oauth|\(\d+\)' -Because $section.id
        }
        $script:Source | Should -Not -Match "-PopoutTitle '[^']*\(\)'"
    }
}

Describe 'portal: hosted assets' {
    It 'the report loads the versioned stylesheet and shell script of the repository (deferred)' {
        $script:PortalCode | Should -Match 'href="\$\(\$script:spiPortalAssetBase\)/css/azadspi-portal_\$\(\$script:spiPortalAssetVersion\)\.css"'
        $script:PortalCode | Should -Match 'src="\$\(\$script:spiPortalAssetBase\)/js/azadspi-portal_\$\(\$script:spiPortalAssetVersion\)\.js" defer'
        $script:PortalCode | Should -Match "spiPortalAssetBase = 'https://www\.azadvertizer\.net/azadserviceprincipalinsights'"
    }
    It 'the head has the theme snippet and the watchdog (plain report when the shell never ran)' {
        $script:PortalCode | Should -Match "localStorage\.getItem\('azadspi\.theme'\)"
        $script:PortalCode | Should -Match "root\.classList\.add\('az-ready', 'az-noshell'\)"
    }
    It 'light, preference-dark and explicit-dark define the same tokens' -Skip:(-not $script:HasPortalAssets) {
        $light = [regex]::Match($script:Css, '(?s):root \{(.*?)\n\}').Groups[1].Value
        $prefDark = [regex]::Match($script:Css, '(?s)@media \(prefers-color-scheme: dark\) \{\s*:root:not\(\[data-theme="light"\]\) \{(.*?)\n    \}').Groups[1].Value
        $explicitDark = [regex]::Match($script:Css, '(?s):root\[data-theme="dark"\] \{(.*?)\n\}').Groups[1].Value
        $prefDark | Should -Not -BeNullOrEmpty
        $explicitDark | Should -Not -BeNullOrEmpty
        $darkTokens = Get-Tokens $prefDark
        Get-Tokens $explicitDark | Should -Be $darkTokens
        #every themed token of the light block has a dark value (layout tokens are shared)
        $shared = '--az-font', '--az-font-mono', '--az-radius', '--az-topbar-h', '--az-nav-w', '--az-shadow-2', '--az-shadow-8'
        @(Get-Tokens $light | Where-Object { $_ -notin $shared }) | Should -Be $darkTokens
    }
    It 'every token used by the stylesheet is defined' -Skip:(-not $script:HasPortalAssets) {
        $defined = Get-Tokens $script:Css
        $used = @([regex]::Matches($script:Css, 'var\((--az-[\w-]+)') | ForEach-Object { $_.Groups[1].Value } | Sort-Object -Unique)
        #--az-cat and --az-lvl are set on elements (category colour, navigation level)
        @($used | Where-Object { $_ -notin $defined -and $_ -notin '--az-cat', '--az-lvl' }) | Should -BeNullOrEmpty
    }
    It 'no text is cut with an ellipsis (shell and grid chrome): labels wrap, the lists scroll' -Skip:(-not $script:HasPortalAssets) {
        $script:Css | Should -Not -Match 'ellipsis|line-clamp'
        $script:Js | Should -Not -Match 'ellipsis|line-clamp'
        $gridStyle = [regex]::Match($script:Source, "(?s)\`$spiGridStyle = @'.*?\r?\n'@").Value
        $gridStyle | Should -Not -BeNullOrEmpty
        $gridStyle | Should -Not -Match 'text-overflow:\s*ellipsis|line-clamp'
        $script:Css | Should -Match '\.az-nav-list \{ flex: 1 1 auto; overflow: auto;'
    }
    It 'grid cells: no ellipsis (AG Grid default overridden), wrapped by default, long values scroll inside the cell' -Skip:(-not $script:HasPortalAssets) {
        $gridStyle = [regex]::Match($script:Source, "(?s)\`$spiGridStyle = @'.*?\r?\n'@").Value
        $gridStyle | Should -Match '\.spiGrid \.ag-cell-value, \.spiGrid \.ag-header-cell-text \{ text-overflow: clip; \}'
        $gridStyle | Should -Match '\.spiGrid \.ag-cell-wrap-text \.ag-cell-value \{ max-height: 132px; overflow-x: hidden; overflow-y: auto; \}'
        $gridStyle | Should -Match '\.spiGrid \.ag-cell:not\(\.ag-cell-wrap-text\) \.ag-cell-value \{[^}]*overflow-x: auto;'
        $script:Source | Should -Match 'var spiPrefs = \{ wrap: true, pin: false \};'
    }
    It 'the stylesheet has no AzGovViz-only blocks' -Skip:(-not $script:HasPortalAssets) {
        $script:Css | Should -Not -Match 'agvGrid|\.tree\b|--az-tree-|az-scope-|az-limit|az-findings'
        $script:Css | Should -Match 'Azure Governance Visualizer 7\.0\.0'
    }
    It 'the grid theme uses the token values of both themes' -Skip:(-not $script:HasPortalAssets) {
        $theme = [regex]::Match($script:Source, '(?s)var spiGridTheme = agGrid\.themeQuartz\.withParams\(\{(.*?)\}\)\.withParams\(\{(.*?)\}, ''dark''\)')
        $theme.Success | Should -BeTrue
        $theme.Groups[1].Value | Should -Match "backgroundColor: '#ffffff'"
        $theme.Groups[1].Value | Should -Match "accentColor: '#0078d4'"
        $theme.Groups[2].Value | Should -Match "backgroundColor: '#252423'"
        $theme.Groups[2].Value | Should -Match "accentColor: '#2899f5'"
        $script:Css | Should -Match '--az-card-bg: #252423;'
        $script:Css | Should -Match '--az-accent: #2899f5;'
    }
}

Describe 'portal: shell script' -Skip:(-not $script:HasPortalAssets) {
    It 'is valid JavaScript' -Skip:(-not (Get-Command node -ErrorAction SilentlyContinue)) {
        $out = node --check $script:JsPath 2>&1
        $LASTEXITCODE | Should -Be 0 -Because ($out -join "`n")
    }
    It 'uses its own storage keys and registry id, and keeps the fallback' {
        $script:Js | Should -Match "var PREFIX = 'azadspi\.';"
        $script:Js | Should -Not -Match "azgovviz\.|agvSectionRegistry|loadag"
        $script:Js | Should -Match "getElementById\('azadspiRegistry'\)"
        $script:Js | Should -Match "classList\.add\('az-ready', 'az-noshell'\)"
        #every localStorage access is wrapped (private windows, storage disabled)
        @([regex]::Matches($script:Js, 'localStorage\.\w+\(')).Count | Should -Be 3
        $script:Js | Should -Match 'function store\(key, value\) \{ try \{'
        $script:Js | Should -Match 'function stored\(key\) \{ try \{'
    }
    It 'keeps the MIT notice of the fork' {
        $script:Js | Should -Match 'MIT License'
        $script:Js | Should -Match 'Copyright \(c\) 2020 Julian Hayward'
    }
}

Describe 'portal: sections and registry' {
    BeforeAll {
        . Import-Portal
        #the grid emitter counts the rows of the open section
        $CsvDelimiterOpposite = ','
        . ([scriptblock]::Create([regex]::Match($script:Source, '(?s)#region AG Grid emitter\r?\n.*?#endregion AG Grid emitter').Value))
        $b = useSpiSection -Id 'ServicePrincipals'
        [void]$b.AppendLine((getSpiCardHeadHtml -TitleHtml 'Service Principals'))
        [void]$b.AppendLine((buildSpiGrid -HtmlTableId 'TenantSummary_ServicePrincipals' -Rows @([pscustomobject]@{ a = 1 }, [pscustomobject]@{ a = 2 }) -ColumnDefinitions @(@{ header = 'A'; property = 'a' }) -PopoutTitle 'SP'))
        $b = useSpiSection -Id 'CleanupCandidates'
        [void]$b.AppendLine((buildSpiGrid -HtmlTableId 'TenantSummary_CleanupCandidates' -Rows @([pscustomobject]@{ a = '</script><img src=x onerror=alert(1)>' }) -ColumnDefinitions @(@{ header = 'A'; property = 'a' }) -PopoutTitle 'C'))
        $b = useSpiSection -Id 'ThirdPartyAppExposure'
        [void]$b.AppendLine((buildSpiGrid -HtmlTableId 'TenantSummary_ThirdPartyAppExposure' -Rows @() -ColumnDefinitions @(@{ header = 'A'; property = 'a' }) -PopoutTitle 'T'))
        $b = useSpiSection -Id 'ApplicationOwners' -Kind note
        [void]$b.AppendLine('<span>Application Owners</span>')
        $b = useSpiSection -Id 'ManagedIdentityFederatedIdentityCredentials' -Kind note -NoteState disabled
        $script:PagesHtml = getSpiPagesHtml
        $script:RegistryHtml = getSpiRegistryHtml -Report ([ordered]@{ product = 'AzADServicePrincipalInsights'; parameters = @([ordered]@{ name = '-X'; value = '</script><b>' }) })
        $script:Registry = [regex]::Match($script:RegistryHtml, '(?s)<script type="application/json" id="azadspiRegistry">(.*)</script>$').Groups[1].Value | ConvertFrom-Json
    }
    It 'a section is written to its page and closed by the next one' {
        $sp = [regex]::Match($script:PagesHtml, '(?s)<div class="az-page" id="page-ServicePrincipals".*?<!--page-ServicePrincipals-->').Value
        $sp | Should -Match '<section class="az-card" id="card-ServicePrincipals" data-section="ServicePrincipals">'
        ([regex]::Matches($script:PagesHtml, '<section ')).Count | Should -Be ([regex]::Matches($script:PagesHtml, '</section>')).Count
        $risk = [regex]::Match($script:PagesHtml, '(?s)<div class="az-page" id="page-Risk".*?<!--page-Risk-->').Value
        $risk | Should -Match 'id="card-CleanupCandidates"'
        $risk | Should -Match 'id="card-ThirdPartyAppExposure"'
        $script:PagesHtml | Should -Match '<section class="az-note az-note-neutral" id="card-ApplicationOwners"'
        $script:PagesHtml | Should -Match '<section class="az-note az-note-muted" id="card-ManagedIdentityFederatedIdentityCredentials"'
    }
    It 'every page is written, in catalog order, with head and icon' {
        $order = @([regex]::Matches($script:PagesHtml, '<div class="az-page" id="page-(\w+)"') | ForEach-Object { $_.Groups[1].Value })
        $order | Should -Be @($script:spiPages.Keys)
        $script:PagesHtml | Should -Match '<img class="az-page-icon-img" src="https://www\.azadvertizer\.net/azadserviceprincipalinsights/icon/10225-icon-service-Enterprise-Applications\.svg" alt="" data-fa="fa-cubes">'
    }
    It 'counts and states: rows of the grid; a finding section with rows is a warning, without rows empty; notes empty or disabled' {
        $byId = @{}; foreach ($s in $script:Registry.sections) { $byId[$s.id] = $s }
        $byId.ServicePrincipals.count | Should -Be 2
        $byId.ServicePrincipals.state | Should -Be 'ok'
        $byId.ServicePrincipals.grids | Should -Be @('TenantSummary_ServicePrincipals')
        $byId.CleanupCandidates.state | Should -Be 'warning'
        $byId.ThirdPartyAppExposure.count | Should -Be 0
        $byId.ThirdPartyAppExposure.state | Should -Be 'empty'
        $byId.ApplicationOwners.state | Should -Be 'empty'
        $byId.ManagedIdentityFederatedIdentityCredentials.state | Should -Be 'disabled'
        $script:Registry.pages.Count | Should -Be $script:spiPages.Count
    }
    It 'the registry is inert JSON: no markup from the data can end the element' {
        $json = [regex]::Match($script:RegistryHtml, '(?s)id="azadspiRegistry">(.*)</script>$').Groups[1].Value
        $json | Should -Not -Match '<|>'
        $script:Registry.report.parameters[0].value | Should -BeExactly '</script><b>'
    }
    It 'a section outside the catalog is refused' {
        { useSpiSection -Id 'NotInCatalog' } | Should -Throw '*not in the section catalog*'
    }
}
