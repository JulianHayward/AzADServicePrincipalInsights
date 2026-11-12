#Pester 5+ tests for the report's third-party assets, the Chart.js 4 options and the 'App RoleAssignedTo (Users and Groups)' filter (maintainer decisions, theme 18).

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $script:Ast = Get-ScriptAst -Path $script:MainScriptPath
}

Describe 'third-party assets' {
    It 'every asset from a public CDN names an exact version and has Subresource Integrity' {
        $tags = [regex]::Matches($script:Source, '<(script|link)\b[^>]*(cdnjs\.cloudflare\.com|cdn\.jsdelivr\.net)[^>]*>')
        $tags.Count | Should -BeGreaterOrEqual 4 -Because 'Font Awesome (2), AG Grid, Chart.js (jQuery is no longer used since the portal theme)'
        foreach ($t in $tags) {
            $t.Value | Should -Match 'integrity="sha(384|512)-[A-Za-z0-9+/=]+"'
            $t.Value | Should -Match 'crossorigin="anonymous"'
            $t.Value | Should -Match '/\d+\.\d+\.\d+/|@\d+\.\d+\.\d+/'
        }
    }
    It 'no third-party library is loaded from azadvertizer.net any more (and jQuery UI, unused, not at all)' {
        $script:Source | Should -Not -Match 'azadserviceprincipalinsights/js/(jquery|chartjs|fontawesome)'
        $script:Source | Should -Not -Match 'azadserviceprincipalinsights/tablefilter/tablefilter\.js'
        $script:Source | Should -Not -Match 'jquery-ui'
        #TableFilter replaced by AG Grid; the report tables download their CSV through the grid
        $script:Source | Should -Not -Match '(?i)tablefilter@|new TableFilter\(|table_to_csv|download_table_as_csv'
    }
}

Describe 'charts (Chart.js 4)' {
    It 'uses no Chart.js 2 options' {
        $script:Source | Should -Not -Match '\btooltips: \{'
        $script:Source | Should -Not -Match 'bodyFontSize'
        $script:Source | Should -Not -Match 'function \(tooltipItem, data\)'
    }
    It 'every chart click filters by the clicked element (<_>)' -ForEach @('myChart', 'myChart2', 'myChart3', 'myChart4', 'myChart5', 'chartSecretExpiryNoteWorthy', 'chartCertExpiryNoteWorthy') {
        $chart = [regex]::Match($script:Source, "(?s)getElementById\('$_'\);\r?\nvar \w+ = new Chart\(ctx, \{.*?\r?\n\}\);")
        $chart.Success | Should -BeTrue
        $chart.Value | Should -Match 'onClick: \(e, elements, chart\) => \{\s+if \(elements\.length == 0\) \{\s+return;'
        $chart.Value | Should -Match 'chart\.options\.plugins\.tooltip\.callbacks\.label\(\{ dataset: chart\.data\.datasets\[elements\[0\]\.datasetIndex\]'
        $chart.Value | Should -Match 'plugins: \{\s+legend: \{\s+display: false'
    }
}

Describe 'App RoleAssignedTo (Users and Groups)' {
    It 'lists only users and groups, not service principals' {
        $assign = $script:Ast.Find({ param($n) $n -is [System.Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -eq '$appRoleAssignedToUsersAndGroups' }, $true)
        $assign | Should -Not -BeNullOrEmpty
        $sp = [pscustomobject]@{ SPAppRoleAssignedTo = @(
                [pscustomobject]@{ principalType = 'User (Member)'; principalDisplayName = 'u' }
                [pscustomobject]@{ principalType = 'ServicePrincipal'; principalDisplayName = 's' }
                [pscustomobject]@{ principalType = 'Group'; principalDisplayName = 'g' }
            )
        }
        . ([scriptblock]::Create($assign.Extent.Text))
        @($appRoleAssignedToUsersAndGroups.principalDisplayName) | Should -Be @('u', 'g')
        #one assignment: still a list
        $sp = [pscustomobject]@{ SPAppRoleAssignedTo = [pscustomobject]@{ principalType = 'Group'; principalDisplayName = 'g' } }
        . ([scriptblock]::Create($assign.Extent.Text))
        $appRoleAssignedToUsersAndGroups.Count | Should -Be 1
    }
}

Describe 'credential expiry date (endDateTimeFormated)' {
    It 'is written as yyyy-MM-dd HH:mm:ss UTC, independent of culture and of the DateTime kind (<Culture>)' -ForEach @(@{ Culture = 'de-DE' }, @{ Culture = 'en-US' }, @{ Culture = 'ja-JP' }) {
        $assignments = @($script:Ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -eq 'endDateTimeFormated' }, $true))
        if (-not $assignments) {
            #hashtable entries, not assignment statements
            $assignments = @($script:Ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.HashtableAst] }, $true) | ForEach-Object { $_.KeyValuePairs } | Where-Object { $_.Item1.Extent.Text -eq 'endDateTimeFormated' })
        }
        #app secrets/certificates and (theme 23) the credentials on the service principal
        $assignments.Count | Should -Be 4
        $saved = [cultureinfo]::CurrentCulture
        try {
            [cultureinfo]::CurrentCulture = [cultureinfo]::new($Culture)
            $utc = [datetime]::new(2022, 5, 17, 11, 23, 42, [DateTimeKind]::Utc)
            foreach ($a in $assignments) {
                $expr = $a.Item2.Extent.Text
                foreach ($value in @($utc, $utc.ToLocalTime())) {
                    $hlperApplicationPasswordCredential = $hlperApplicationKeyCredential = $credential = [pscustomobject]@{ endDateTime = $value }
                    & ([scriptblock]::Create($expr)) | Should -Be '2022-05-17 11:23:42 UTC'
                }
            }
        }
        finally { [cultureinfo]::CurrentCulture = $saved }
    }
}

Describe 'transcript file name' {
    It 'uses the product name (was AzADServiceprincipalInsights_...)' {
        $fn = $script:Ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'setTranscript' }, $true).Extent.Text
        @([regex]::Matches($fn, 'fileNameTranscript = "\$\(\$Product\)_')).Count | Should -Be 2
    }
}
