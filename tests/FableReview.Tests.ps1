#Pester 5+ tests for the fixes of the review 2026-11-09 (findings F-n) that have no better home:
#mostly wiring checks of code that runs only against a tenant

#The stylesheet, shell script and icons of the report are hosted on azadvertizer.net and not part of the public repository. The
#maintainer keeps them in _local/html (local only); the tests of these files run where that folder exists and are skipped elsewhere (CI)
BeforeDiscovery {
    $script:HasPortalAssets = Test-Path -LiteralPath (Join-Path (Split-Path -Parent $PSScriptRoot) '_local/html')
}

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath
}

Describe 'batch 1: run aborts and data loss' {
    It 'F-19: the parallel runspaces import the AzAPICall module the main runspace resolves' {
        $script:Source | Should -Match "\`$azAPICallModuleLoaded = \(Get-Command -Name 'AzAPICall' -Module \`$AzAPICallModuleName -ErrorAction SilentlyContinue \| Select-Object -First 1\)\.Module"
        #the command resolves to the last imported version (Import-Module -RequiredVersion -Force), not to the newest loaded one
        $versions = @(Get-Module -ListAvailable -Name AzAPICall | Sort-Object -Property Version -Unique)
        if ($versions.Count -lt 2) { Set-ItResult -Skipped -Because 'needs two installed AzAPICall versions'; return }
        $older = $versions[0].Version.ToString(); $newer = $versions[-1].Version.ToString()
        $resolved = pwsh -NoProfile -Command "Import-Module AzAPICall -RequiredVersion $newer; Import-Module AzAPICall -RequiredVersion $older -Force; (Get-Command -Name AzAPICall -Module AzAPICall | Select-Object -First 1).Module.Version.ToString()"
        $resolved | Should -Be $older
    }
    It 'F-21: the group resolve of the Azure role assignments records an error string as a failed batch' {
        $script:Source | Should -Match '(?s)\$resolveObjectIdsTypeGroup = AzAPICall .*?if \(\$resolveObjectIdsTypeGroup -is \[string\]\) \{.*?Phase = ''Azure role assignment groups resolve''.*?continue'
        #the change history keeps the Azure roles of the previous run
        $script:Source | Should -Match "Phase -in 'ManagementGroups custom data', 'Subscriptions custom data', 'Azure role assignment groups resolve'"
    }
    It 'F-25: application secrets and certificates without end date do not throw' {
        $script:Source | Should -Match '\$passwordCredentialExpiryTotalDays = \$\(if \(\$null -ne \$hlperApplicationPasswordCredential\.endDateTime\)'
        $script:Source | Should -Match '\$keyCredentialExpiryTotalDays = \$\(if \(\$null -ne \$hlperApplicationKeyCredential\.endDateTime\)'
        ([regex]::Matches($script:Source, "\`$expiryApplication(Password|Key)Credential = 'no end date'")).Count | Should -Be 2
        ([regex]::Matches($script:Source, 'endDateTimeFormated = \$\(if \(\$null -ne \$hlperApplication(Password|Key)Credential\.endDateTime\)')).Count | Should -Be 2
        #the expressions as written: no end date -> no exception
        $currentDateUTC = (Get-Date).ToUniversalTime()
        $hlperApplicationPasswordCredential = [pscustomobject]@{ endDateTime = $null }
        { $null = $(if ($null -ne $hlperApplicationPasswordCredential.endDateTime) { (New-TimeSpan -Start $currentDateUTC -End $hlperApplicationPasswordCredential.endDateTime).TotalDays }) } | Should -Not -Throw
    }
    It 'F-31: the JSON export catches per file and reports, it does not stop the run' {
        $block = Get-ParallelBlock -Path $script:MainScriptPath -Marker '[System.IO.File]::WriteAllText("$($using:jsonFolder)'
        $block.ScriptBlock.ToString() | Should -Match '(?s)try \{\s+\[System\.IO\.File\]::WriteAllText.*?catch \{\s+\$null = \(\$(using:)?jsonExportFailures\)\.Add'
        $script:Source | Should -Match 'writePipelineWarning "\$\(\$Product\) JSON export: '
    }
    It 'F-35: a change history problem is shown on the overview tile and in the section, also without rows' {
        $ast = Get-ScriptAst -Path $script:MainScriptPath
        . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'getFindingsOverviewRows' }, $true).Extent.Text))
        $insightRows = @{ ThirdParty = @(); AppConfiguration = @(); FederatedCredentialTrust = @(); Cleanup = @(); EffectivePrivilege = @(); CredentialRisk = @() }
        $tile = { param($rows) @($rows).where({ $_.section -eq 'ChangeHistory' })[0] }
        $notRead = & $tile (getFindingsOverviewRows -insightRows $insightRows -changeHistoryRows @() -changeHistoryDays 30 -changeHistoryIssue 'x.jsonl could not be read: locked' -changeHistoryNotRead $true)
        $notRead.count | Should -Be 'not read'
        $notRead.state | Should -Be 'warning'
        $notRead.detail | Should -Be 'x.jsonl could not be read: locked'
        $notUpdated = & $tile (getFindingsOverviewRows -insightRows $insightRows -changeHistoryRows @() -changeHistoryDays 30 -changeHistoryIssue 'this run was not added to the change history: locked.')
        $notUpdated.count | Should -Be 0
        $notUpdated.state | Should -Be 'warning'
        $notUpdated.detail | Should -Match 'this run was not added'
        $fine = & $tile (getFindingsOverviewRows -insightRows $insightRows -changeHistoryRows @() -changeHistoryDays 30)
        $fine.state | Should -Be 'info'
        #addReportSection: the warning also in the empty note
        $script:Source | Should -Match 'aria-hidden=`"true`"></i> <span>\$encodedTitle\$\(if \(\$Warning\) \{ " - \$\(\[HtmlSafe\]::Encode\(\$Warning\)\)" \}\)</span>'
    }
}

Describe 'batch 2: report content' {
    BeforeAll {
        $ast = Get-ScriptAst -Path $script:MainScriptPath
        . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.TypeDefinitionAst] -and $n.Name -eq 'HtmlSafe' }, $true).Extent.Text))
        foreach ($name in 'getSpiEntraRoleLinkHtml', 'getSpiAppRoleAssignedToRoleText') {
            . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name }, $true).Extent.Text))
        }
    }
    It 'F-6: Entra role links end in .html and are built only from GUIDs; all four sites use the helper' {
        getSpiEntraRoleLinkHtml -RoleDefinitionId '62e90394-69f5-4237-9190-012177145e10' -RoleName 'Global Administrator' |
            Should -BeExactly '<a class="externallink" href="https://www.azadvertizer.net/azentraidroles/62e90394-69f5-4237-9190-012177145e10.html" target="_blank">Global Administrator</a>'
        getSpiEntraRoleLinkHtml -RoleDefinitionId 'x"><script>' -RoleName 'a<b' | Should -BeExactly 'a&lt;b'
        ([regex]::Matches($script:Source, '\(getSpiEntraRoleLinkHtml -RoleDefinitionId ')).Count | Should -Be 4
        $script:Source | Should -Not -Match 'azentraidroles/\$\(\[HtmlSafe\]'
    }
    It 'F-42: the role of an assignment to a user or group' {
        getSpiAppRoleAssignedToRoleText ([pscustomobject]@{ roleDisplayName = 'Reader'; roleValue = 'App.Read' }) | Should -Be 'Reader'
        getSpiAppRoleAssignedToRoleText ([pscustomobject]@{ roleValue = 'App.Read' }) | Should -Be 'App.Read'
        getSpiAppRoleAssignedToRoleText ([pscustomobject]@{ roleId = '00000000-0000-0000-0000-000000000000' }) | Should -Be 'default access'
        getSpiAppRoleAssignedToRoleText ([pscustomobject]@{ roleId = 'r1' }) | Should -Be 'app role r1'
    }
    It 'F-3: a policy assignment managed identity is relict only when the scope of the assignment was collected' {
        ([regex]::Matches($script:Source, 'if \(\$runTenantRoot -and -not \$policyAssignmentScopesNotCollected\.Contains\("(sub|mg):')).Count | Should -Be 2
        #known scopes of the tenant minus the collected ones (a deleted management group is not 'not collected': its identities stay relict)
        $script:Source | Should -Match 'foreach \(\$tenantEntity in @\(\$arrayEntitiesFromAPI\)\)'
        $script:Source | Should -Match 'foreach \(\$collectedSubscription in @\(\$arrayDataCollectionProgressSub\)\) \{ \$null = \$policyAssignmentScopesNotCollected\.Remove'
        $script:Source | Should -Match '\$policyAssignmentScopesNotCollected = \$using:policyAssignmentScopesNotCollected'
    }
    It 'F-8: HiPo users - one row per resource, permission and consent kind' {
        $script:Source | Should -Match '\$hipoGrantKey = "\$\(\$SPOauth2PermissionGrant\.SPId\)\|\$\(\$SPOauth2PermissionGrant\.permission\)\|'
        $script:Source | Should -Match 'permission = "\$\(\$SPOauth2PermissionGrant\.SPDisplayName\) \(\$\(\$SPOauth2PermissionGrant\.permission\)\)\$\(\$hipoConsentSuffix\)"'
    }
    It 'F-10 / F-33: chart labels are JSON with html escaping; the MI type slice filters the full type in a select column' {
        ConvertTo-Json -InputObject @('Web/sites', "</script><img src=x onerror=alert(1)>'") -Compress -EscapeHandling EscapeHtml | Should -Not -Match "[<>']"
        foreach ($name in 'OrgIds', 'SPTypes', 'MIResTypes', 'MIResTypeTerms') {
            $script:Source | Should -Match ('\$' + $name + ' = ConvertTo-Json -InputObject @\(\$arr\w+\) -Compress -EscapeHandling EscapeHtml') -Because $name
        }
        $script:Source | Should -Match ([regex]::Escape("[['16', clickedDataset.terms ? clickedDataset.terms[elements[0].index] : window.target]]"))
    }
    It 'F-18: out-of-scope subscriptions: one entry each, CSV and overview note' {
        $script:Source | Should -Match '(?s)if \(\(\$sub\.subDetails\.subscriptionPolicies\.quotaId\)\.startswith\(''AAD_''.*?elseif \(\$sub\.subDetails\.state -ne ''Enabled''\)'
        $script:Source | Should -Match '_SubscriptionsOutOfScope\.csv'
        $script:Source | Should -Match '-outOfScopeSubscriptions @\(\$outOfScopeSubscriptions\)'
    }
    It 'F-22: -OnlyProcessSPs...: the granted-to index from the tenant-wide grant list' {
        $script:Source | Should -Match '\$grantedToSources = @\(@\{ ServicePrincipalOauth2PermissionGrants = @\(\$prefetchOauth2PermissionGrants\) \}\)'
    }
    It 'F-36 / F-37 / F-38 / F-39 / F-40 / F-41 / F-43 / F-44 wiring' {
        $script:Source | Should -Match ([regex]::Escape("`$miAssignedToResources = 'not collected (parameter NoAzureResourceSideRelations)'"))
        $script:Source | Should -Match ([regex]::Escape("elseif (`$column.Number) { 'number' }"))
        $script:Source | Should -Match ([regex]::Escape("Property = 'AttachedResourcesCount'; Number = `$true"))
        $script:Source | Should -Match ([regex]::Escape("-or `$spType -eq 'SP INT') {"))
        $script:Source | Should -Match 'Oauth2PermissionGrantScope = \$oauthGrant\.scope'
        $script:Source | Should -Match 'AppRoleId = \$\(if \(\$hlper\) \{ \$hlper\.id \} else \{ \$servicePrincipalAppRoleAssignment\.appRoleId \}\)'
        $script:Source | Should -Not -Match "-PopoutTitle 'SUMMARY"
        $script:Source | Should -Match 'RoleAssignmentScopeId = \$azureroleAss\.roleAssignmentAssignmentScopeId'
        $script:Source | Should -Match 'APPFederatedIdentityCredentialClaimsMatchingExpression = \$fic\.claimsMatchingExpression\.value'
    }
}

Describe 'batch 3: security hardening' {
    It 'F-17: tenant names in console lines and AzAPICall task texts are neutralised for Azure DevOps logging commands' {
        $sanitize = " -replace '##', '# #' -replace '[\r\n]+', ' '"
        "x ##vso[task.setvariable variable=a]b`nnext" -replace '##', '# #' -replace '[\r\n]+', ' ' | Should -Be 'x # #vso[task.setvariable variable=a]b next'
        $lines = $script:Source -split "`r?`n"
        #every console line that prints the display name of an object
        @($lines | Where-Object { $_ -match '^\s*Write-Host .*\$\(\$object\.displayName\)' }).Count | Should -Be 0
        @($lines | Where-Object { $_ -match '^\s*Write-Host .*\$\(\$object\.displayName' + [regex]::Escape($sanitize) + '\)' }).Count | Should -BeGreaterThan 10
        #task texts with management group / subscription names (printed by AzAPICall on retries and errors)
        @($lines | Where-Object { $_ -match '^\s*\$currentTask = ".*\$\((\$mgdetail\.properties\.displayName|\$childMgSubDisplayName|\$scopeDisplayName)\)' }).Count | Should -Be 0
        #failed items (console and report)
        @($lines | Where-Object { $_ -match '^\s*\$failedItem = ".*\$\(\$(mgdetail\.properties\.displayName|childMgSubDetail\.subscriptionName|uami\.name|spOrAppWithoutSP\.Details\.displayName)\)' }).Count | Should -Be 0
        $script:Source | Should -Match ([regex]::Escape('Write-Host "  $($failure.Item' + $sanitize + '): $($failure.Error' + $sanitize + ')"'))
    }
    It 'F-51: titles in inline scripts cannot end the script element' {
        $script:Source | Should -Match ([regex]::Escape(".Replace('<', '\u003c') }"))
        $script:Source | Should -Match ([regex]::Escape("bootstrap = bootstrap.replace(/</g, '\\u003c');"))
    }
    It 'F-48: the portal maps have no prototype' -Skip:(-not $script:HasPortalAssets) {
        $portal = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot '..' '_local' 'html' 'azadspi-portal_001_001.js')
        foreach ($name in 'sectionsById', 'pagesById', 'pages') {
            $portal | Should -Match ('var ' + $name + ' = Object\.create\(null\);') -Because $name
        }
    }
    It 'F-58: the Web App must redirect unauthenticated requests (v1); excluded paths covering the report stop the publish (v2)' {
        $publish = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot '..' 'pwsh' 'Publish-AzADSPIWebApp.ps1')
        $publish | Should -Match "unauthenticatedClientAction -eq 'RedirectToLoginPage'"
        $publish | Should -Match 'globalValidation\.excludedPaths'
        $publish | Should -Match 'excludes paths from the authentication that cover the report'
    }
}

Describe 'batch 4: pipelines' {
    BeforeAll {
        $script:Ado = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot '..' '.azuredevops' 'pipelines' 'AzADServicePrincipalInsights.yml')
        $script:Gh = @('AzADServicePrincipalInsights.yml', 'AzADServicePrincipalInsights_OIDC.yml' | ForEach-Object { Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot '..' '.github' 'workflows' $_) })
    }
    It 'F-11: the run works on the current head of the branch' {
        foreach ($wf in $script:Gh) { $wf | Should -Match 'ref: \$\{\{ github\.ref \}\}' }
        $script:Ado | Should -Match 'fetch --depth 1 origin \$branch'
        $script:Ado | Should -Match 'git checkout --detach FETCH_HEAD'
        $script:Ado.IndexOf('git checkout --detach FETCH_HEAD') | Should -BeLessThan $script:Ado.IndexOf("displayName: 'Run AzADServicePrincipalInsights'")
    }
    It 'F-12: the run and the ingestion get SYSTEM_ACCESSTOKEN (ADO); OIDC logs in again before ingestion/publish' {
        foreach ($step in "displayName: 'Run AzADServicePrincipalInsights'", "displayName: 'Upload data to Log Analytics via Logs ingestion API'") {
            $index = $script:Ado.IndexOf($step)
            $script:Ado.Substring($index, 400) | Should -Match 'SYSTEM_ACCESSTOKEN: \$\(System\.AccessToken\)' -Because $step
        }
        $script:Gh[1] | Should -Match "(?m)^      - name: Connect Azure OIDC \(again\)\r?\n        id: reconnect\r?\n        if: \$\{\{ !cancelled\(\) && steps\.push\.outcome == 'success' && \(steps\.arguments\.outputs\.WebAppPublish == 'true' \|\| steps\.arguments\.outputs\.IngestToLogAnalytics == 'true'\) \}\}"
    }
    It 'F-55: Azure DevOps pushes before the ingestion (as GitHub)' {
        $script:Ado.IndexOf("displayName: 'Push AzADServicePrincipalInsights output to repository'") | Should -BeLessThan $script:Ado.IndexOf("displayName: 'Upload data to Log Analytics via Logs ingestion API'")
    }
    It 'F-64: the git identity is set for the repository only' {
        $script:Ado | Should -Not -Match 'git config --global'
        foreach ($wf in $script:Gh) { $wf | Should -Not -Match 'git config --global' }
    }
    It 'F-56: pipelines remove an earlier JSON folder also with -NoJsonExport' {
        $script:Source | Should -Match ([regex]::Escape('if ($NoJsonExport -and $azAPICallConf[''htParameters''].onAzureDevOpsOrGitHubActions -and (Test-Path -LiteralPath'))
    }
}

Describe 'batch 5: browser' {
    BeforeAll {
        $script:Node = Get-Command node -ErrorAction SilentlyContinue
        $script:Js = foreach ($name in 'spiTagRanges', 'spiEscapeHtml', 'spiBuildHighlighted', 'spiTextComparator') {
            $start = $script:Source.IndexOf("    function $name(")
            $end = $script:Source.IndexOf("`n    }", $start) + 6
            $script:Source.Substring($start, $end - $start)
        }
    }
    It 'F-14: the highlighter gives the same output as the scan of all tag ranges, fast for a long list cell' {
        if (-not $script:Node) { Set-ItResult -Skipped -Because 'node is not available'; return }
        $js = "var spiTextCollator = new Intl.Collator(undefined, { sensitivity: 'base', numeric: true });`n" + ($script:Js -join "`n") + @'

/* the former check, as reference */
function overlaps(ranges, start, end) { for (var i = 0; i < ranges.length; i++) { if (start < ranges[i][1] && end > ranges[i][0]) { return true; } } return false; }
function reference(text, entry) {
    var tagRanges = spiTagRanges(text), matches = [];
    entry.slots.forEach(function (slot, i) { slot.re.lastIndex = 0; var m; while ((m = slot.re.exec(text)) !== null) { if (!m[0].length) { slot.re.lastIndex++; continue; } if (overlaps(tagRanges, m.index, m.index + m[0].length)) { continue; } matches.push({ start: m.index, end: m.index + m[0].length, idx: slot.idx, order: i }); } });
    matches.sort(function (a, b) { return a.start - b.start || a.order - b.order; });
    var out = '', pos = 0;
    matches.forEach(function (x) { if (x.start < pos) { return; } out += text.slice(pos, x.start) + '<mark class="filter-match fm-' + x.idx + '">' + text.slice(x.start, x.end) + '</mark>'; pos = x.end; });
    return out + text.slice(pos);
}
var items = [];
for (var i = 0; i < 3000; i++) { items.push('<li><a class="externallink" href="https://portal.azure.com/#@t/resource/subscriptions/0000' + i + '/x" target="_blank">Owner a&amp;b ' + i + '</a> <span class="spiDim">sub ' + i + ' &middot; PIM</span></li>'); }
var cell = '<ul class="spiList">' + items.join('') + '</ul>';
var same = true;
['a', '0000', 'sub', 'amp', 'li', 'Owner a'].forEach(function (term) {
    var make = function () { return { slots: [{ re: new RegExp(term.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'gi'), idx: 0 }, { re: /PIM/g, idx: 1 }] }; };
    if (spiBuildHighlighted(cell, make(), true) !== reference(cell, make())) { same = false; }
});
var t0 = Date.now();
spiBuildHighlighted(cell, { slots: [{ re: /a/gi, idx: 0 }] }, true);
var ms = Date.now() - t0;
var sorted = ['b', 'B2', 'a', 'sp10', 'sp2', '', 'A1'].sort(spiTextComparator);
console.log(JSON.stringify({ same: same, ms: ms, sorted: sorted, numbers: [10, -5, 3].sort(spiTextComparator) }));
'@
        $file = Join-Path $TestDrive 'highlight14.js'
        Set-Content -LiteralPath $file -Value $js
        $result = (& $script:Node.Source $file) | ConvertFrom-Json
        $result.same | Should -BeTrue
        $result.ms | Should -BeLessThan 1500
        #F-47: case-insensitive, numeric, empty first
        $result.sorted | Should -Be @('', 'a', 'A1', 'b', 'B2', 'sp2', 'sp10')
        $result.numbers | Should -Be @(-5, 3, 10)
    }
    It 'F-47 / F-49 / F-50 / F-52 / F-53 / F-54 wiring' {
        $script:Source | Should -Match 'comparator: spiTextComparator'
        $script:Source | Should -Match ([regex]::Escape("if (condition.filterType && condition.filterType !== 'text') { return; }"))
        $script:Source | Should -Match ([regex]::Escape("`$parts.Add('filterParams: { caseSensitive: true }')"))
        $script:Source | Should -Match ([regex]::Escape('color: var(--az-error, #a4262c);'))
        $script:Source | Should -Match ([regex]::Escape("if (navigator.clipboard && navigator.clipboard.writeText) {"))
        $script:Source | Should -Match ([regex]::Escape("function spiInflateElement(id) {"))
        $script:Source | Should -Match '(?s)function spiInflateElement\(id\) \{.{0,400}return Promise\.resolve\(\)\.then\(function \(\) \{'
    }
    It 'F-15 / F-16: the portal search applies the word test of the result list to the cards of every page' -Skip:(-not $script:HasPortalAssets) {
        $portal = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot '..' '_local' 'html' 'azadspi-portal_001_001.js')
        $portal | Should -Match ([regex]::Escape("document.querySelectorAll('.az-page section[data-title]').forEach(function (card) {"))
        $portal | Should -Match ([regex]::Escape('card.hidden = !!term && !hit(title);'))
        $portal | Should -Not -Match ([regex]::Escape("labelOf(link).toLowerCase().indexOf(term)"))
    }
}

Describe 'batch 6: performance' {
    BeforeAll {
        $ast = Get-ScriptAst -Path $script:MainScriptPath
        . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.TypeDefinitionAst] -and $n.Name -eq 'HtmlSafe' }, $true).Extent.Text))
        foreach ($name in 'getSpiItemsHtml', 'getSpiTagsHtml', 'getSpiOwnerItemsHtml', 'getSpiGroupedItemsHtml', 'getSpiResourceTypeLinkHtml', 'getSpiPortalResourceLinkHtml') {
            . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name }, $true).Extent.Text))
        }
    }
    It 'F-13: the release before the report also covers the other holders of the raw data' {
        foreach ($name in 'htGraphPrefetch', 'arraySPsAndAppsWithoutSP', 'SPsAndAppsWithoutSPBatch', 'servicePrincipalsAndAppsOnlyEnriched', 'prefetchOauth2PermissionGrants', 'htServicePrincipalsAppRoles', 'htServicePrincipalsPublishedPermissionScopes') {
            $script:Source | Should -Match ("(?s)foreach \(\`$releasedVariable in [^{]*'" + $name + "'") -Because $name
        }
        #none of them is used after the release (the report is built from $cu)
        $release = $script:Source.IndexOf('foreach ($releasedVariable in')
        foreach ($name in 'htGraphPrefetch', 'arraySPsAndAppsWithoutSP', 'prefetchOauth2PermissionGrants', 'htServicePrincipalsAppRoles', 'htApplications') {
            $script:Source.IndexOf('$' + $name, $release + 1500) | Should -Be -1 -Because $name
        }
    }
    It 'F-34: helpers return early for empty input; the tag html is the same when cached' {
        getSpiOwnerItemsHtml -Owners $null | Should -BeExactly ''
        getSpiResourceTypeLinkHtml '' | Should -BeExactly ''
        getSpiPortalResourceLinkHtml '' | Should -BeExactly ''
        $entry = [pscustomobject]@{ SP = @([pscustomobject]@{ SPTags = @('WindowsAzureActiveDirectoryIntegratedApp', 'a  b') }); APP = $null }
        $first = getSpiTagsHtml -Entry $entry -Of SP
        $first | Should -Match 'WindowsAzureActiveDirectoryIntegratedApp'
        $first | Should -Match '>a b<'
        getSpiTagsHtml -Entry $entry -Of SP | Should -BeExactly $first
        getSpiTagsHtml -Entry $entry -Of App | Should -BeExactly ''
        getSpiTagsHtml -Entry ([pscustomobject]@{ SP = @([pscustomobject]@{ SPTags = @() }) }) -Of SP | Should -BeExactly ''
    }
    It 'F-30 / F-32 / F-46 wiring' {
        $script:Source | Should -Match ([regex]::Escape('#review F-30: an SP that owns no SP has only its own reasons'))
        $script:Source | Should -Match ([regex]::Escape('if ($rawSP.passwordCredentials) {'))
        $script:Source | Should -Match ([regex]::Escape("foreach (`$pair in `$maps[`$column].GetEnumerator()) { `$dictionary[`$pair.Value] = `$pair.Key }"))
        $script:Source | Should -Not -Match 'htSpiGrid\.strings'
        $script:Source | Should -Match ([regex]::Escape('$roleName = $azureRoleLinkCache[$azureRoleLinkKey]'))
    }
}

Describe 'policy assignments per management group (AzGovViz mg-leaf concept)' {
    It 'atExactScope() below the management groups of -ManagementGroupId, atscope() at them; subscriptions unfiltered' {
        $script:Source | Should -Match ([regex]::Escape("`$policyAssignmentFilter = `$(if (`$mgdetail.Name -in @(`$using:mgId)) { 'atscope()' } else { 'atExactScope()' })"))
        $script:Source | Should -Match ([regex]::Escape('/providers/Microsoft.Authorization/policyAssignments?`$filter=$($policyAssignmentFilter)&api-version=2026-07-01'))
        $script:Source | Should -Match ([regex]::Escape('/subscriptions/$($childMgSubId)/providers/Microsoft.Authorization/policyAssignments?api-version=2026-07-01"'))
    }
}
