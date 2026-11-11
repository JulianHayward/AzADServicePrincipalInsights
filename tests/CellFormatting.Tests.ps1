#Pester 5+ tests for the readable grid cells (theme 28, maintainer 2026-11-06): one entry per line, details dimmed, key/value
#lines, states; data encoded. Only the html twins use these helpers; the plain values and the CSV files keep their text.

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.TypeDefinitionAst] -and $n.Name -eq 'HtmlSafe' }, $true).Extent.Text))
    foreach ($name in 'getSpiItemsHtml', 'getSpiGroupedItemsHtml', 'getSpiCriticalIconHtml', 'getSpiClassificationDotHtml', 'getSpiOwnerItemsHtml', 'getSpiEntraObjectLinkHtml', 'getSpiTagsHtml', 'getSpiCredentialItemsHtml', 'getSpiFicItemsHtml') {
        . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name }, $true).Extent.Text))
    }
    $script:getClassifications = @{ permissionColors = @{ critical = '#ff0000'; medium = '#dc9405' } }
}

Describe 'getSpiItemsHtml' {
    It 'one entry: no list, main encoded, details dimmed and joined' {
        getSpiItemsHtml -Items @(@{ Main = 'a <b>'; Dim = @('User (Member)', '', 'id1') }) | Should -BeExactly '<span class="spiItem">a &lt;b&gt; <span class="spiDim">User (Member) &middot; id1</span></span>'
    }
    It 'several entries: a list; a label counts them; states as classes' {
        $html = getSpiItemsHtml -Items @(@{ Main = 'x'; State = 'muted' }, @{ MainHtml = '<a href="#">y</a>' }) -Label 'direct'
        $html | Should -BeExactly '<b class="spiItemsLabel">direct (2)</b><ul class="spiList spiItems"><li class="spiItem-muted">x</li><li><a href="#">y</a></li></ul>'
    }
    It 'key/value details; empty values left out; nothing at all: empty' {
        $html = getSpiItemsHtml -Items @(@{ Main = 'fic'; Details = [ordered]@{ issuer = 'https://i'; subject = '<s>'; description = '' } })
        $html | Should -Match '<span class="spiKvLine"><span class="spiKvKey">issuer</span> https://i</span><span class="spiKvLine"><span class="spiKvKey">subject</span> &lt;s&gt;</span>'
        $html | Should -Not -Match 'description'
        getSpiItemsHtml -Items @() | Should -BeExactly ''
        getSpiItemsHtml -Items $null | Should -BeExactly ''
    }
    It 'groups in order' {
        $groups = [ordered]@{ subscription = @(@{ Main = 'Owner' }); 'resource group' = @(@{ Main = 'Reader' }, @{ Main = 'Contributor' }) }
        $html = getSpiGroupedItemsHtml -Groups $groups
        $html.IndexOf('subscription (1)') | Should -BeLessThan $html.IndexOf('resource group (2)')
    }
}

Describe 'cell helpers' {
    It 'owners: direct and indirect groups' {
        $html = getSpiOwnerItemsHtml -Owners @(
            [pscustomobject]@{ displayName = 'u1'; principalType = 'User (Member)'; id = '1'; applicability = 'direct' }
            [pscustomobject]@{ displayName = 'sp2'; principalType = 'SP APP INT'; id = '2'; applicability = 'indirect' }
        )
        $html | Should -Match '^<b class="spiItemsLabel">direct \(1\)</b><ul class="spiList spiItems"><li>u1 <span class="spiDim">User \(Member\) &middot; 1</span></li></ul><b class="spiItemsLabel">indirect \(1\)</b>'
    }
    It 'tags: the SP tags without the ones the app propagates; JSON whitespace collapsed' {
        $entry = [pscustomobject]@{ SP = [pscustomobject]@{ SPTags = @('HideApp', "{`n  ""a"": 1`n}") }; APP = [pscustomobject]@{ APPTags = @('HideApp') } }
        getSpiTagsHtml -Entry $entry -Of SP | Should -BeExactly '<span class="spiItem">{ &quot;a&quot;: 1 }</span>'
        getSpiTagsHtml -Entry $entry -Of App | Should -BeExactly '<span class="spiItem">HideApp</span>'
    }
    It 'credentials: expired dimmed, expiring soon highlighted, certificate thumbprint' {
        $html = getSpiCredentialItemsHtml -Credentials @(
            [pscustomobject]@{ keyId = 'k1'; displayName = 'old'; expiryInfo = 'expired'; endDateTimeFormated = '2022-01-01 00:00:00 UTC' }
            [pscustomobject]@{ keyId = 'k2'; displayName = ''; expiryInfo = 'expires soon (in 3 days)'; endDateTimeFormated = '2026-11-09 00:00:00 UTC'; customKeyIdentifier = 'AB' }
        ) -Certificate
        $html | Should -Match '<li class="spiItem-muted">old <span class="spiDim">expired &middot; 2022-01-01 00:00:00 UTC &middot; key id k1</span></li>'
        $html | Should -Match '<li class="spiItem-warn">\(no name\) <span class="spiDim">expires soon \(in 3 days\) &middot; 2026-11-09 00:00:00 UTC &middot; thumbprint AB &middot; key id k2</span></li>'
    }
    It 'federated identity credentials: app (top level properties) and managed identity (properties)' {
        $app = getSpiFicItemsHtml -Credentials @([pscustomobject]@{ name = 'gh'; issuer = 'https://token.actions.githubusercontent.com'; subject = 'repo:o/r:ref:refs/heads/main'; audiences = @('api://AzureADTokenExchange'); description = 'ci' })
        $app | Should -Match '^<span class="spiItem">gh<span class="spiKv">'
        $app | Should -Match 'spiKvKey">description</span> ci'
        $mi = getSpiFicItemsHtml -Credentials @([pscustomobject]@{ name = 'ado'; id = '/subscriptions/x'; properties = [pscustomobject]@{ issuer = 'https://vstoken.dev.azure.com/1'; subject = 'sc://o/p/c'; audiences = @('b', 'a') } }) -ManagedIdentity
        $mi | Should -Match 'spiKvKey">subject</span> sc://o/p/c'
        $mi | Should -Match 'spiKvKey">audiences</span> a, b'
        $mi | Should -Not -Match '/subscriptions/x'
    }
    It 'markers: critical icon, classification dot (colour from the classification file)' {
        getSpiCriticalIconHtml $true | Should -Match 'fa-triangle-exclamation spiCritical'
        getSpiCriticalIconHtml $false | Should -BeExactly ''
        getSpiClassificationDotHtml 'critical' | Should -BeExactly '<span class="spiClassDot" style="background: #ff0000"></span>'
        getSpiClassificationDotHtml 'unclassified' | Should -BeExactly ''
    }
}

Describe 'readable cells in the report sections' {
    It 'every list column has an html twin; plain values keep their former text' {
        foreach ($header in 'SP Owners', 'App Owners', 'Assigned users and groups', 'SP Group memberships', 'Application Secrets', 'Application Certificates', 'Application Federated Identity Credentials', 'Federated Identity Credentials') {
            $script:Source | Should -Match ("@\{ header = '" + [regex]::Escape($header) + "'; property = 'c\d+'; htmlProperty = 'c\d+Html' \}") -Because $header
        }
        @([regex]::Matches($script:Source, "@\{ header = '(SP|App) Tags'; property = 'c\d+'; htmlProperty = 'c\d+Html' \}")).Count | Should -Be 16
        #the former text stays the plain value (filter, sort, grid CSV download)
        foreach ($var in 'SPAAADRoleAssignedOn', 'APPAAADRoleAssignedOn') {
            $script:Source | Should -Match ('\$spiGridRow\.c\d+ = ConvertTo-SpiPlainText \$' + $var + '\r?\n') -Because $var
        }
        #review F-45: Entra roles, app roles and Azure roles: the plain text is built from the values (the markup gave
        #"Microsoft Graph ( ! Directory.ReadWrite.All )")
        foreach ($var in 'spAADRoleAssignments', 'SPAppRoleAssignments', 'SPAzureRoleAssignments') {
            $script:Source | Should -Match ('\$spiGridRow\.c\d+ = \$\(if \(\$' + $var + '\) \{ ') -Because $var
        }
        $script:Source | Should -Not -Match 'ConvertTo-SpiPlainText \$spiGridRow\.c10Html'
        #R-1: the delegated permissions are built as plain text from the values (one entry per permission and consent)
        $script:Source | Should -Match '\$spiGridRow\.c8 = \[string\]\$SPOauth2PermissionGrants '
    }
}
