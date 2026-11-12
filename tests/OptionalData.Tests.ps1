#Pester 5+ tests (theme 22): optional data - Entra recommendations and custom security attributes of SPs.
#Runs the real '#region optional data' of the main script against a fake AzAPICall.

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $m = [regex]::Match($source, '(?s)    #region optional data.*?#endregion optional data')
    if (-not $m.Success) { throw 'optional data region not found' }
    $script:OptionalDataCode = [scriptblock]::Create($m.Value)
    #review P-3 b: the requests are made in the tenant-wide parallel block - its real task list and its block body (in-process)
    $tasks = [regex]::Match($source, '(?s)\$graphUrl = \$azAPICallConf\[''azAPIEndpointUrls''\]\.MicrosoftGraph?
\$relationshipSignalPasses.*?(?=\$startGraphPrefetch = Get-Date)')
    if (-not $tasks.Success) { throw 'tenant-wide task list not found' }
    $script:TaskListCode = [scriptblock]::Create($tasks.Value)
    $script:PrefetchBlock = Get-ParallelBlock -Path $script:MainScriptPath -Marker '$htGraphPrefetch[$task.name] = @{'
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'getOwnedResourceConsentSetting' }, $true).Extent.Text))

    function global:AzAPICall {
        param($AzAPICallConfiguration, $uri, $method, $currentTask, $listenOn, [switch]$validateAccess, [switch]$noPaging)
        $null = $global:AzADSPITestCalls.Add($uri)
        if ($validateAccess) { if ($global:AzADSPITestDenied | Where-Object { $uri -like "*$_*" }) { return 'failed' } else { return @() } }
        if ($global:AzADSPITestThrow | Where-Object { $uri -like "*$_*" }) { throw "permanent failure $uri" }
        foreach ($key in $global:AzADSPITestFakeResponses.Keys) { if ($uri -like "*$key") { return $global:AzADSPITestFakeResponses[$key] } }
        return @()
    }
    function Invoke-OptionalData {
        $azAPICallConf = @{ azAPIEndpointUrls = @{ MicrosoftGraph = 'https://graph.microsoft.com' } }
        $global:AzADSPITestCalls = [System.Collections.ArrayList]@()
        #the optional data tasks of the parallel block (no relationship passes)
        $OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes = $true
        . $script:TaskListCode
        $htGraphPrefetch = [hashtable]::Synchronized(@{})
        @($graphPrefetchTasks | Where-Object { $_.name -in 'entraRecommendations', 'customSecurityAttributes', 'authorizationPolicy', 'adminConsentRequestPolicy', 'permissionGrantPolicies' -or $_.name -like 'agent:*' }) | ForEach-Object -Process $script:PrefetchBlock.ScriptBlock 6>$null
        . $script:OptionalDataCode 6>$null
        [pscustomobject]@{ Recommendations = $htEntraRecommendationsBySPId; Attributes = $htCustomSecurityAttributesBySPId; Agents = $htAgentObjectsById; Consent = $tenantConsentSettings; Calls = @($global:AzADSPITestCalls) }
    }
    $global:AzADSPITestFakeResponses = @{
        '/beta/directory/recommendations'                                      = @(
            [pscustomobject]@{ id = 'r1'; recommendationType = 'managedIdentity'; displayName = 'Convert your application(s) to Managed Identity'; priority = 'medium'; status = 'active'; impactType = 'apps' }
            [pscustomobject]@{ id = 'r2'; recommendationType = 'staleApps'; displayName = 'Remove unused applications'; priority = 'medium'; status = 'completedBySystem'; impactType = 'apps' }
            [pscustomobject]@{ id = 'r3'; recommendationType = 'oneAdmin'; displayName = 'Designate more than one global admin'; priority = 'low'; status = 'active'; impactType = 'users' }
        )
        '/beta/directory/recommendations/r1/impactedResources'                 = @(
            [pscustomobject]@{ subjectId = 'sp1'; status = 'active' }
            [pscustomobject]@{ subjectId = 'sp2'; status = 'completedByUser' }
        )
        '/beta/servicePrincipals?$select=id,customSecurityAttributes&$top=999' = @(
            [pscustomobject]@{ id = 'sp1'; customSecurityAttributes = [pscustomobject]@{ privilegedWorkloadIdentity = [pscustomobject]@{ '@odata.type' = '#microsoft.graph.customSecurityAttributeValue'; adminTier = '0'; 'service@odata.type' = '#Collection(String)'; service = @('EntraOps', 'AzADSPI') } } }
            [pscustomobject]@{ id = 'sp2'; customSecurityAttributes = $null }
        )
        '/beta/servicePrincipals/microsoft.graph.agentIdentity?$select=id,displayName' = @([pscustomobject]@{ id = 'agent1'; displayName = 'Agent 1' })
        '/beta/policies/adminConsentRequestPolicy' = [pscustomobject]@{ isEnabled = $true; reviewers = @(1, 2); requestDurationInDays = 30 }
        '/v1.0/policies/permissionGrantPolicies' = @([pscustomobject]@{ id = 'microsoft-user-default-low'; displayName = 'Microsoft low' }, [pscustomobject]@{ id = 'contoso-policy'; displayName = 'Contoso' })
    }
}

AfterAll {
    Remove-Item function:global:AzAPICall -ErrorAction SilentlyContinue
    Remove-Variable -Name AzADSPITestCalls, AzADSPITestFakeResponses, AzADSPITestDenied, AzADSPITestThrow -Scope Global -ErrorAction SilentlyContinue
}

Describe 'optional data' {
    It 'without the optional permissions: not collected ($null), only the probes are requested' {
        $global:AzADSPITestDenied = @('/directory/recommendations', 'customSecurityAttributes', 'microsoft.graph.agent', '/policies/')
        $r = Invoke-OptionalData
        $r.Recommendations | Should -BeNullOrEmpty
        $null -eq $r.Recommendations | Should -BeTrue
        $null -eq $r.Attributes | Should -BeTrue
        $null -eq $r.Agents | Should -BeTrue
        $null -eq $r.Consent | Should -BeTrue
        #recommendations, custom security attributes, 4 agent object types, consent (authorization policy, admin consent workflow,
        #permission grant policies: tested in parallel since P-3 b, before only after the authorization policy)
        $r.Calls.Count | Should -Be 9
    }
    It 'Entra recommendations: active app recommendations, active/postponed impacted SPs only' {
        $global:AzADSPITestDenied = @()
        $r = Invoke-OptionalData
        @($r.Recommendations.Keys) | Should -Be @('sp1')
        $r.Recommendations['sp1'][0].recommendationType | Should -Be 'managedIdentity'
        $r.Recommendations['sp1'][0].status | Should -Be 'active'
        #completed and user-level recommendations: no impactedResources request
        @($r.Calls | Where-Object { $_ -like '*impactedResources*' }).Count | Should -Be 1
    }
    It 'custom security attributes: one entry per attribute, @odata keys dropped, multi-values kept' {
        $global:AzADSPITestDenied = @()
        $r = Invoke-OptionalData
        @($r.Attributes.Keys) | Should -Be @('sp1')
        $a = @($r.Attributes['sp1'])
        $a.Count | Should -Be 2
        ($a | Where-Object attribute -EQ 'adminTier').value | Should -Be @('0')
        ($a | Where-Object attribute -EQ 'service').value | Should -Be @('EntraOps', 'AzADSPI')
        $a[0].attributeSet | Should -Be 'privilegedWorkloadIdentity'
    }
    It 'agent objects: collected per type, the type is recorded' {
        $global:AzADSPITestDenied = @()
        $r = Invoke-OptionalData
        @($r.Agents.Keys) | Should -Be @('agent1')
        $r.Agents['agent1'].agentType | Should -Be 'AgentIdentity'
        @($r.Calls | Where-Object { $_ -like '*microsoft.graph.agent*' -and $_ -notlike '*top=1*' }).Count | Should -Be 4
    }
    It 'consent settings: <Policies> -> <Expected>' -ForEach @(
        @{ Policies = @('ManagePermissionGrantsForSelf.microsoft-user-default-legacy', 'ManagePermissionGrantsForOwnedResource.microsoft-dynamically-managed-permissions-for-team'); Expected = 'yes, to all apps'; GroupOwner = 'yes*' }
        @{ Policies = @('ManagePermissionGrantsForSelf.microsoft-user-default-low'); Expected = 'yes, limited'; GroupOwner = 'no' }
        @{ Policies = @(); Expected = 'no'; GroupOwner = 'no' }
        @{ Policies = @('ManagePermissionGrantsForSelf.contoso-policy'); Expected = 'custom: contoso-policy'; GroupOwner = 'no' }
        #review F-9: the Microsoft-managed policy; other built-in policies are not custom
        @{ Policies = @('ManagePermissionGrantsForSelf.microsoft-user-default-recommended'); Expected = 'yes, limited (Microsoft-managed)'; GroupOwner = 'no' }
        @{ Policies = @('ManagePermissionGrantsForSelf.microsoft-user-default-other'); Expected = 'built-in policy: microsoft-user-default-other'; GroupOwner = 'no' }
    ) {
        $global:AzADSPITestDenied = @()
        $global:AzADSPITestFakeResponses['/v1.0/policies/authorizationPolicy'] = [pscustomobject]@{ defaultUserRolePermissions = [pscustomobject]@{ permissionGrantPoliciesAssigned = $Policies; allowedToCreateApps = $true } }
        $r = Invoke-OptionalData
        ($r.Consent | Where-Object Setting -EQ 'Users can consent to apps').Value | Should -Be $Expected
        ($r.Consent | Where-Object Setting -Like 'Group/team owners*').Value | Should -BeLike $GroupOwner
        ($r.Consent | Where-Object Setting -EQ 'Users can register applications').Value | Should -Be 'yes'
        ($r.Consent | Where-Object Setting -EQ 'Admin consent workflow').Value | Should -BeLike 'enabled (2 reviewers*'
        ($r.Consent | Where-Object Setting -EQ 'Custom permission grant policies').Value | Should -Be 'Contoso (contoso-policy)'
    }
}

#review 2026-11-10 (first real run with Policy.Read.All): the owner consent policies are the Teams/chat resource-specific consent states
#(Set-MgBetaTeamRscConfiguration / Set-MgBetaChatRscConfiguration); the default tenant has both Microsoft-managed policies
Describe 'getOwnedResourceConsentSetting' {
    It '<Name>' -ForEach @(
        @{ Name = 'default tenant: both Microsoft-managed'; Policies = @('ManagePermissionGrantsForOwnedResource.microsoft-dynamically-managed-permissions-for-team', 'ManagePermissionGrantsForOwnedResource.microsoft-dynamically-managed-permissions-for-chat'); Value = 'yes - teams: Microsoft-managed (default); chats: Microsoft-managed (default)'; Note = "*'Microsoft-managed' currently allows it*Set-MgBetaTeamRscConfiguration*" }
        @{ Name = 'EnabledForAllApps for teams, chats disabled'; Policies = @('ManagePermissionGrantsForOwnedResource.microsoft-all-application-permissions-for-team'); Value = 'yes - teams: all apps; chats: not allowed'; Note = 'resource-specific consent:*' }
        @{ Name = 'EnabledForPreApprovedAppsOnly'; Policies = @('ManagePermissionGrantsForOwnedResource.microsoft-pre-approval-apps-for-team', 'ManagePermissionGrantsForOwnedResource.microsoft-pre-approval-apps-for-chat'); Value = 'yes - teams: pre-approved apps only; chats: pre-approved apps only'; Note = 'resource-specific consent:*' }
        @{ Name = 'DisabledForAllApps (no owner policy); user consent policies are ignored'; Policies = @('ManagePermissionGrantsForSelf.microsoft-user-default-low'); Value = 'no'; Note = '*disabled*' }
        @{ Name = 'other owner policies are listed by id'; Policies = @('ManagePermissionGrantsForOwnedResource.microsoft-dynamically-managed-permissions-for-team', 'ManagePermissionGrantsForOwnedResource.contoso-group-owner-policy'); Value = 'yes - teams: Microsoft-managed (default); chats: not allowed; other: contoso-group-owner-policy'; Note = '*Microsoft-managed*' }
        @{ Name = 'no policies at all'; Policies = @(); Value = 'no'; Note = '*disabled*' }
    ) {
        $r = getOwnedResourceConsentSetting -PermissionGrantPoliciesAssigned $Policies
        $r.Value | Should -BeExactly $Value
        $r.Note | Should -BeLike $Note
    }
}

Describe 'Fable review F-2 / F-20: tasks of the tenant-wide block' {
    It 'a signal pass, a tenant-wide list or optional data whose request throws is not permitted - the run goes on' {
        $global:AzADSPITestDenied = @()
        $global:AzADSPITestThrow = @('expand=owners(', '/beta/oauth2PermissionGrants', '/beta/directory/recommendations')
        $global:AzADSPITestCalls = [System.Collections.ArrayList]@()
        $azAPICallConf = @{ azAPIEndpointUrls = @{ MicrosoftGraph = 'https://graph.microsoft.com' } }
        $OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes = $false
        . $script:TaskListCode
        $htGraphPrefetch = [hashtable]::Synchronized(@{})
        $global:AzADSPITestFakeResponses['$expand=memberOf($select=id)'] = @([pscustomobject]@{ id = 'sp1'; memberOf = @([pscustomobject]@{ id = 'g1' }) }, [pscustomobject]@{ id = 'sp2'; memberOf = @() })
        try {
            @($graphPrefetchTasks | Where-Object { $_.name -like 'signal:*' -or $_.name -in 'oauth2PermissionGrants', 'entraRecommendations' }) | ForEach-Object -Process $script:PrefetchBlock.ScriptBlock 6>$null
        }
        finally {
            $global:AzADSPITestThrow = @()
            $global:AzADSPITestFakeResponses.Remove('$expand=memberOf($select=id)')
        }
        $htGraphPrefetch['signal:spOwners'].permitted | Should -BeFalse
        $htGraphPrefetch['signal:appOwners'].permitted | Should -BeFalse
        $htGraphPrefetch['oauth2PermissionGrants'].permitted | Should -BeFalse
        $htGraphPrefetch['entraRecommendations'].permitted | Should -BeFalse
        #F-20: a signal keeps the objects with entries and every object the pass returned
        $htGraphPrefetch['signal:spMemberOf'].permitted | Should -BeTrue
        @($htGraphPrefetch['signal:spMemberOf'].result.withEntries) | Should -Be @('sp1')
        @($htGraphPrefetch['signal:spMemberOf'].result.seen | Sort-Object) | Should -Be @('sp1', 'sp2')
    }
}

