#Review R-8: -OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes - Service Principals that are not processed keep their
#type and name in labels; escalation through Microsoft Graph is still detected; SPs with Azure RBAC through groups are included

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    foreach ($name in 'getServicePrincipalTypeLabel', 'getServicePrincipalTypeLabelById', 'getEffectivePrivileges') {
        . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name }, $true).Extent.Text))
    }
    . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -eq '$script:escalationAppRoles' }, $true).Extent.Text))
    $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $script:Tenant = 'tenant1'
    $azAPICallConf = @{ checkContext = @{ Tenant = @{ Id = 'tenant1' } } }
}

Describe 'R-8 type label of a Service Principal that was not processed' {
    It '<Name> -> <Expected>' -ForEach @(
        @{ Name = 'internal app'; SP = @{ servicePrincipalType = 'Application'; appOwnerOrganizationId = 'tenant1' }; Expected = 'SP APP INT' }
        @{ Name = 'multi-tenant app of another organization'; SP = @{ servicePrincipalType = 'Application'; appOwnerOrganizationId = 'other' }; Expected = 'SP APP EXT' }
        @{ Name = 'system assigned managed identity'; SP = @{ servicePrincipalType = 'ManagedIdentity'; alternativeNames = @('isExplicit=False', '/subscriptions/x') }; Expected = 'SP MI System assigned' }
        @{ Name = 'user assigned managed identity'; SP = @{ servicePrincipalType = 'ManagedIdentity'; alternativeNames = @('isExplicit=True') }; Expected = 'SP MI User assigned' }
        @{ Name = 'legacy'; SP = @{ servicePrincipalType = 'Legacy'; appOwnerOrganizationId = 'other' }; Expected = 'SP EXT' }
    ) {
        getServicePrincipalTypeLabel -ServicePrincipal ([pscustomobject]$SP) -TenantId 'tenant1' | Should -Be $Expected
    }
    It 'the label of a processed SP wins; an unknown id: nothing' {
        $htSpLookup = @{ 'p1' = @{ objectTypeConcatinated = 'SP APP INT' } }
        $htAllServicePrincipalsById = @{ 'p1' = [pscustomobject]@{ servicePrincipalType = 'Application'; appOwnerOrganizationId = 'other' }; 'u1' = [pscustomobject]@{ servicePrincipalType = 'Application'; appOwnerOrganizationId = 'other' } }
        getServicePrincipalTypeLabelById -Id 'p1' | Should -Be 'SP APP INT'
        getServicePrincipalTypeLabelById -Id 'u1' | Should -Be 'SP APP EXT'
        getServicePrincipalTypeLabelById -Id 'nope' | Should -BeNullOrEmpty
    }
}

Describe 'R-8 escalation app roles when Microsoft Graph was not processed' {
    It 'the appId of the resource comes from the lookup of all SPs' {
        $htServicePrincipalsAppRoleIdsToAppId = @{ 'graphSpId_role1' = '00000003-0000-0000-c000-000000000000' }
        $entries = @([pscustomobject]@{ ObjectType = 'SP APP INT'; ObjectId = 'A'; SP = @([pscustomobject]@{ SPObjectId = 'A'; SPAppId = 'appA'; SPDisplayName = 'A' }); SPAppRoleAssignments = @([pscustomobject]@{ AppRoleAssignmentResourceId = 'graphSpId'; AppRoleId = 'role1'; AppRoleAssignmentResourceDisplayName = 'Microsoft Graph'; AppRolePermission = 'RoleManagement.ReadWrite.Directory'; AppRolePermissionSensitivity = 'critical' }) })
        $result = getEffectivePrivileges -entries $entries
        $result['A'].level | Should -Be 'critical'
        $result['A'].reasons[0].category | Should -Be 'App role (can escalate)'
    }
}

Describe 'R-8 wiring' {
    It 'Service Principals with Azure RBAC through a group are added (transitive SP members of the groups with role assignments)' {
        $script:Source | Should -Match '/groups/\$\(\$groupWithRoleAssignments\)/transitiveMembers/microsoft\.graph\.servicePrincipal\?`\$select=id'
        $script:Source | Should -Match '\$spsWithAzureResourceRoleAssignmentUnique\.Add\(\$groupMemberId\)'
        $script:Source | Should -Match '\$groupsWithRoleAssignments \| ForEach-Object -Parallel \{'   #P-13
    }
    It 'createdBy: a Service Principal of the tenant that was not processed is labelled with type and name' {
        $script:Source | Should -Match 'elseif \(\$htAllServicePrincipalsById\[\$assignmentCreatedBy\]\) \{'
    }
    It 'owner SPs: type from all SPs, marked when their owners are not known' {
        @([regex]::Matches($script:Source, 'ownersNotCollected = ')).Count | Should -Be 2
        $script:Source | Should -Not -Match '\.principalType = \$htSpLookup\[\$owner\.id\]\.objectTypeConcatinated'
    }
}
