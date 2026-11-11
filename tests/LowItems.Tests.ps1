#Review 2026-11-07 low items (findings R-n): R-23, R-27, R-30, R-31, R-34, R-35, R-36 and the owner order

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'getFederatedCredentialTrust' }, $true).Extent.Text))
}

Describe 'R-27 flexible federated identity credentials' {
    It 'an expression instead of a subject is shown and is a finding' {
        $r = getFederatedCredentialTrust -issuer 'https://token.actions.githubusercontent.com' -subject '' -claimsMatchingExpression "claims['sub'] matches 'repo:contoso/*'"
        $r.trust | Should -Be "claims expression: claims['sub'] matches 'repo:contoso/*'"
        $r.finding | Should -BeLike 'flexible:*'
    }
    It 'a credential with a subject is unchanged' {
        $r = getFederatedCredentialTrust -issuer 'https://token.actions.githubusercontent.com' -subject 'repo:contoso/app:ref:refs/heads/main'
        $r.trust | Should -Be 'repository contoso/app, branch main'
        $r.finding | Should -Be ''
    }
    It 'the expression is kept in the output and in the change history value' {
        $script:Source | Should -Match 'claimsMatchingExpression = \$federatedIdentityCredential\.claimsMatchingExpression'
        $script:Source | Should -Match 'claims expression \$\(\$item\.claimsMatchingExpression\.value\)'
    }
}

Describe 'R-30 resource type of a managed identity' {
    BeforeAll {
        $code = [regex]::Match($script:Source, '(?m)^\s*\$resourceTypeSegments = \$s1\.Split\(''/''\)\r?\n\s*\$resourceType = [^\r\n]+').Value
        $script:TypeOf = [scriptblock]::Create("param(`$s1) $code; `$resourceType")
    }
    It '<Id> -> <Type>' -ForEach @(
        @{ Id = 'Microsoft.Web/sites/myapp/slots/staging'; Type = 'Microsoft.Web/sites/slots' }
        @{ Id = 'Microsoft.Web/sites/sites'; Type = 'Microsoft.Web/sites' }
        @{ Id = 'Microsoft.Compute/virtualMachines/vm1'; Type = 'Microsoft.Compute/virtualMachines' }
        @{ Id = 'Microsoft.Authorization/policyAssignments/pa1'; Type = 'Microsoft.Authorization/policyAssignments' }
    ) {
        & $script:TypeOf $Id | Should -Be $Type
    }
}

Describe 'R-31 filter highlight and character references (node)' {
    It 'a match inside &amp; / &#39; is not highlighted; other matches are' {
        $node = Get-Command node -ErrorAction SilentlyContinue
        if (-not $node) { Set-ItResult -Skipped -Because 'node is not available'; return }
        $js = foreach ($name in 'spiTagRanges', 'spiEscapeHtml', 'spiBuildHighlighted') {
            $start = $script:Source.IndexOf("    function $name(")
            $end = $script:Source.IndexOf("`n    }", $start) + 6
            $script:Source.Substring($start, $end - $start)
        }
        $js = ($js -join "`n") + @'

var entry = { slots: [{ re: /amp/gi, idx: 0 }, { re: /39/g, idx: 1 }, { re: /Bob/g, idx: 2 }] };
console.log(JSON.stringify(spiBuildHighlighted('R&amp;D for O&#39;Bob <b>amp</b>', entry, true)));
'@
        $file = Join-Path $TestDrive 'highlight.js'
        Set-Content -LiteralPath $file -Value $js
        $result = (& $node.Source $file) | ConvertFrom-Json
        $result | Should -BeExactly 'R&amp;D for O&#39;<mark class="filter-match fm-2">Bob</mark> <b><mark class="filter-match fm-0">amp</mark></b>'
    }
}

Describe 'R-34 / R-35 / R-36 and R-23, owner order (wiring)' {
    It 'R-20: the resource listing without $expand and without grouping by type' {
        $script:Source | Should -Match '/subscriptions/\$\(\$scopeId\)/resources\?api-version='
        $script:Source | Should -Not -Match 'resources\?`\$expand=createdTime'
        $script:Source | Should -Not -Match 'resourcesSubscriptionResult \| Group-Object'
        #the UAMI settings query depends on a UAMI in the listing (was: the grouping result)
        $script:Source | Should -Not -Match 'resourcesSubscriptionResultGroupedByType'
        $script:Source | Should -Match 'if \(\$subscriptionHasUserAssignedIdentities\) \{'
    }
    It 'R-21: the groups of the SP memberships are resolved by batched getByIds (no GET per group)' {
        $region = [regex]::Match($script:Source, '(?s)#region groupsFromSPs.*?#endregion groupsFromSPs').Value
        $region | Should -Match 'directoryObjects/getByIds'
        $region | Should -Match '"types":\["group"\]'
        $region | Should -Not -Match '/v1\.0/groups/\$\('
        $region | Should -Not -Match 'ForEach-Object -Parallel'
    }
    It 'P-14: UAMI federated credentials in their own parallel phase; a failure keeps the previous federated credentials' {
        $script:Source | Should -Match '\$null = \$script:arrayUserAssignedIdentityIds4FederatedCreds\.Add\('
        $script:Source | Should -Match '@\(\$arrayUserAssignedIdentityIds4FederatedCreds\) \| ForEach-Object -Parallel \{'
        $script:Source | Should -Match "Phase -eq 'UAMI federated credentials' \}\)\)\.Count -gt 0\) \{ 'federatedCredentials' \}"
        $collection = [regex]::Match($script:Source, '(?s)function dataCollectionResources.*?\r?\n    \}\r?\n').Value
        $collection | Should -Not -Match '/federatedIdentityCredentials\?api-version'
    }
    It 'P-3 b: the tenant-wide Graph lists are requested in one parallel block; the processing takes them from there' {
        $script:Source | Should -Match '\$graphPrefetchTasks \| ForEach-Object -Parallel \{'
        foreach ($name in 'servicePrincipals', 'aadRoleDefinitions', 'applications', 'aadRoleAssignments', 'aadRoleAssignmentsExpanded', 'oauth2PermissionGrants', 'entraRecommendations', 'customSecurityAttributes', 'authorizationPolicy', 'adminConsentRequestPolicy', 'permissionGrantPolicies') {
            $script:Source | Should -Match "\`$graphPrefetchTasks\.Add\(@\{ name = '$name';" -Because $name
            $script:Source | Should -Match "\`$htGraphPrefetch\['$name'\]" -Because $name
        }
        $script:Source | Should -Not -Match 'function testTenantWideAccess|function testOptionalAccess'
    }
    It 'P-3 a: the Azure phase is a task of the tenant-wide Graph block (default mode), first with -OnlyProcessSPs...' {
        $script:Source | Should -Match '(?m)^    \$azurePhase = \{\r?\n\s+#region GettingEntities'
        $script:Source | Should -Match '\$azurePhaseDeferred = -not \$OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes'
        $script:Source | Should -Match '(?s)if \(-not \$azurePhaseDeferred\) \{\r?\n\s+\. \$azurePhase'
        $script:Source | Should -Match "\`$graphPrefetchTasks\.Insert\(0, @\{ name = 'azurePhase' \}\)"
        $script:Source | Should -Match "if \(\`$task\.name -eq 'azurePhase'\) \{"
        #the variables of the phase are set in the script scope after the block
        $script:Source | Should -Match 'Set-Variable -Name \$phaseVariableName -Value \$htAzurePhaseResult\[\$phaseVariableName\] -Scope Script'
        #the Azure API call statistics of the phase count only Azure requests
        $script:Source | Should -Match "if \(\`$apiCallTrackingAll\[\`$apiCallTrackingIndex\]\.TargetEndpoint -eq 'ARM'\)"
        #review F-1: a copy of the tracking list (GetRange is a live view that throws when the Graph tasks add entries)
        $script:Source | Should -Match "\`$apiCallTrackingAll = \`$azAPICallConf\['arrayAPICallTracking'\]\.ToArray\(\)"
        $script:Source | Should -Not -Match 'arrayAPICallTracking.\]\.GetRange'
        #the access check per management group stays before (stops early)
        $script:Source.IndexOf('#region permissionsCheck') | Should -BeLessThan $script:Source.IndexOf('$azurePhase = {')
    }
    It 'R-34: without a subscription in the context the subscription validation is skipped; built-in roles from the provider' {
        $script:Source | Should -Match "if \(\`$SubscriptionId4AzContext -eq 'undefined' -and -not \(Get-AzContext\)\.Subscription\) \{"
        #(Azure role conditions: always from the provider - it returns every built-in role)
        $script:Source | Should -Match '\$uri = "\$\(\$azAPICallConf\[''azAPIEndpointUrls''\]\.ARM\)/providers/Microsoft\.Authorization/roleDefinitions\?api-version=\$\(\$roleDefinitionsApiVersionTry\)&`\$filter=type eq ''BuiltInRole''"'
    }
    It 'R-35: the permission check runs for every non-user account' {
        $script:Source | Should -Match "Account\.Type -in 'ServicePrincipal', 'ManagedService', 'ClientAssertion'\) \{"
    }
    It 'R-36: permissionClassification.json also next to the script; read errors with the message' {
        $script:Source | Should -Match "Join-Path -Path \`$PSScriptRoot -ChildPath 'permissionClassification\.json'"
        $script:Source | Should -Match 'could not be read: \$\(\$_\.Exception\.Message\)'
    }
    It 'R-23: no search of the owner list per indirect owner; via without a pipeline' {
        $script:Source | Should -Not -Match "\(\`$arrayOwners\.where\(\{ \`$_\.applicability -eq 'indirect' \}\)\)\.id -notcontains"
        $script:Source | Should -Not -Match "Select-Object -Skip 1 \| ForEach-Object \{ "", which controls"
    }
    It 'owner order: owners sorted by name and id before the walk; owner CSV rows sorted by owner' {
        $script:Source | Should -Match "htSPOwners\[\`$object\.id\] = @\(\`$getSPOwner \| Select-Object id, displayName, '@odata\.type' \| Sort-Object \{ \`$_\.displayName \}, \{ \`$_\.id \}\)"
        $script:Source | Should -Match "htOwnedBy\[\`$sp\]\.ownedBy = @\(\`$ownedByList \| Sort-Object \{ \`$_\.displayName \}, \{ \`$_\.id \}\)"
        $script:Source | Should -Match 'Sort-Object -Property SPDisplayName, SPObjectId, OwnerApplicability, OwnerDisplayName, OwnerId'
        $script:Source | Should -Match 'Sort-Object -Property AppDisplayName, AppObjectId, OwnerApplicability, OwnerDisplayName, OwnerId'
    }
}
