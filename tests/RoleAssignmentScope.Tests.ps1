#Review R-5: the subscription phase labels a role assignment by its scope (subscription, inherited management group or tenant root)

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $start = $source.IndexOf('$roleAssignmentScope = [string]$roleAssignmentFromAPI.properties.scope')
    $end = $source.IndexOf('($script:htCacheAssignmentsRole)[$roleAssignmentFromAPI.id] = @{', $start)
    if ($start -lt 0 -or $end -lt 0) { throw 'R-5 scope labelling not found in main script' }
    $script:LabelCode = [scriptblock]::Create($source.Substring($start, $end - $start))

    function Get-ScopeLabels([string]$Scope, [string]$SubscriptionId = 'sub1') {
        $roleAssignmentFromAPI = [pscustomobject]@{ id = "$($Scope.TrimEnd('/'))/providers/Microsoft.Authorization/roleAssignments/ra1"; properties = [pscustomobject]@{ scope = $Scope; roleDefinitionId = '/providers/Microsoft.Authorization/roleDefinitions/rd1' } }
        $splitAssignment = $roleAssignmentFromAPI.id.Split('/')
        $childMgSubId = $SubscriptionId
        $htManagementGroupsMgPath = @{ 'mg-a' = @{ DisplayName = 'MG A' } }
        $htSubscriptionsMgPath = @{ 'sub1' = @{ DisplayName = 'Subscription one' } }
        . $script:LabelCode 6>$null
        $scopeLabels
    }
}

Describe 'R-5 role assignment scope in the subscription phase' {
    It 'the subscription itself: Sub' {
        $l = Get-ScopeLabels '/subscriptions/sub1'
        $l.assignmentScope | Should -Be 'Sub'
        $l.assignmentScopeId | Should -Be '/subscriptions/sub1'
        $l.assignmentScopeName | Should -Be 'Subscription one'
    }
    It 'an inherited management group assignment: MG with its id and name (was Sub with /subscriptions/Microsoft.Management)' {
        $l = Get-ScopeLabels '/providers/Microsoft.Management/managementGroups/mg-a'
        $l.assignmentScope | Should -Be 'MG'
        $l.assignmentScopeId | Should -Be '/providers/Microsoft.Management/managementGroups/mg-a'
        $l.assignmentScopeName | Should -Be 'MG A'
        $l.assignmentResourceType | Should -Be 'ManagementGroup'
    }
    It 'the tenant root: Ten' {
        $l = Get-ScopeLabels '/'
        $l.assignmentScope | Should -Be 'Ten'
        $l.assignmentResourceType | Should -Be 'Tenant'
    }
}
