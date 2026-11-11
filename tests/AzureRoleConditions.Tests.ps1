#Pester 5+ tests: Azure role conditions (2026-11-10). A condition on a role definition (built-in roles, returned since api-version
#2022-05-01-preview) or on a role assignment ('constrain roles' delegation) can limit which roles a role assignment write may hand
#out; such a role is critical only if a critical role is among them. Fixture: the built-in role definitions that can write role
#assignments or are named in a condition (public Microsoft data, read with api-version 2023-07-01-preview on 2026-11-10).

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    foreach ($name in 'testRoleCapableForRoleAssignmentsWrite', 'testPermissionCanWriteRoleAssignments', 'getRoleConditionAssignableRoles', 'getRoleConditionEffect', 'getAzureRoleConditionText', 'applyAzureRoleConditions') {
        . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name }, $true).Extent.Text))
    }
    $script:Roles = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot 'fixtures' 'builtInRoleDefinitionsWithConditions.json') | ConvertFrom-Json
    $script:Owner = '8e3af657-a8ff-443c-a75c-2fe8c4bcb635'
    $script:UserAccessAdministrator = '18d7d88d-d35e-4fb5-a5c3-7773c20a72d9'
    $script:RbacAdministrator = 'f58310d9-a9f6-439a-9e8d-f62e7b41a168'
    $script:BlobDataContributor = 'ba92f5b4-2d11-453d-a403-e96b0029c9fe'
    $script:BlobDataReader = '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1'

    #runs applyAzureRoleConditions on the fixture with the given assignments (role id -> assignment condition)
    function Invoke-Conditions([hashtable[]]$Assignments) {
        $htCacheDefinitionsRole = @{}
        foreach ($role in $script:Roles) { $htCacheDefinitionsRole[$role.name] = @{ definition = $role; roleIsCritical = (testRoleCapableForRoleAssignmentsWrite -RoleDefinition $role) } }
        #a role that is not critical (it cannot write role assignments) with an ABAC condition
        if (-not $htCacheDefinitionsRole[$script:BlobDataReader]) { throw 'fixture: Storage Blob Data Reader missing' }
        $htCacheAssignmentsRole = @{}
        $i = 0
        foreach ($assignment in $Assignments) {
            $i++
            $htCacheAssignmentsRole["a$i"] = @{ roleId = $assignment.roleId; roleIsCritical = $htCacheDefinitionsRole[$assignment.roleId].roleIsCritical; assignment = [pscustomobject]@{ properties = [pscustomobject]@{ condition = $assignment.condition } } }
        }
        applyAzureRoleConditions 6>$null
        , @(1..$i | ForEach-Object { $htCacheAssignmentsRole["a$_"] })
    }
    #the portal 'constrain roles' condition: only these roles, for service principals (Microsoft documentation template)
    function New-ConstrainRolesCondition([string[]]$RoleIds) {
        @"
(
 (
  !(ActionMatches{'Microsoft.Authorization/roleAssignments/write'})
 )
 OR
 (
  @Request[Microsoft.Authorization/roleAssignments:RoleDefinitionId] ForAnyOfAnyValues:GuidEquals {$($RoleIds -join ', ')}
  AND
  @Request[Microsoft.Authorization/roleAssignments:PrincipalType] ForAnyOfAnyValues:StringEqualsIgnoreCase {'ServicePrincipal'}
 )
)
AND
(
 (
  !(ActionMatches{'Microsoft.Authorization/roleAssignments/delete'})
 )
 OR
 (
  @Resource[Microsoft.Authorization/roleAssignments:RoleDefinitionId] ForAnyOfAnyValues:GuidEquals {$($RoleIds -join ', ')}
 )
)
"@
    }
}

Describe 'reading a condition' {
    It 'allow list (GUIDs with and without hyphens, multi-line), deny list' {
        $r = getRoleConditionAssignableRoles -Condition (New-ConstrainRolesCondition @($script:BlobDataContributor, ($script:BlobDataReader -replace '-')))
        $r.evaluated | Should -BeTrue
        @($r.allow | Sort-Object) | Should -Be @(($script:BlobDataReader -replace '-'), ($script:BlobDataContributor -replace '-') | Sort-Object)
        $null -eq $r.deny | Should -BeTrue
        $d = getRoleConditionAssignableRoles -Condition "((!(ActionMatches{'Microsoft.Authorization/roleAssignments/write'})) OR (@Request[Microsoft.Authorization/roleAssignments:RoleDefinitionId] ForAnyOfAllValues:GuidNotEquals {$($script:Owner), $($script:UserAccessAdministrator)}))"
        $d.evaluated | Should -BeTrue
        $d.deny.Count | Should -Be 2
    }
    It 'not evaluated: <Case>' -ForEach @(
        @{ Case = 'an OR that is no action guard'; Condition = "(@Request[Microsoft.Authorization/roleAssignments:RoleDefinitionId] ForAnyOfAnyValues:GuidEquals {2a2b9908-6ea1-4ae2-8e65-a410df84e7d1}) OR (@Request[Microsoft.Authorization/roleAssignments:PrincipalType] ForAnyOfAnyValues:StringEqualsIgnoreCase {'Group'})" }
        @{ Case = 'another operator on the role id'; Condition = "@Request[Microsoft.Authorization/roleAssignments:RoleDefinitionId] StringEquals '2a2b9908-6ea1-4ae2-8e65-a410df84e7d1'" }
        @{ Case = 'no role id clause'; Condition = "@Resource[HasObotoken] boolequals true" }
        @{ Case = 'a value that is no GUID'; Condition = "@Request[Microsoft.Authorization/roleAssignments:RoleDefinitionId] ForAnyOfAnyValues:GuidEquals {abc}" }
    ) {
        (getRoleConditionAssignableRoles -Condition $Condition).evaluated | Should -BeFalse
    }
}

Describe 'built-in roles with a condition (fixture)' {
    It 'every built-in role that can write role assignments with a condition is read; all are limited to non-critical roles today' {
        $conditional = @($script:Roles.where({ (testRoleCapableForRoleAssignmentsWrite -RoleDefinition $_) -and @($_.properties.permissions.condition).where({ $_ }).Count -gt 0 }))
        $conditional.Count | Should -BeGreaterOrEqual 20
        $results = Invoke-Conditions @($conditional | ForEach-Object { @{ roleId = $_.name; condition = $null } })
        foreach ($result in $results) {
            $name = ($script:Roles.where({ $_.name -eq $result.roleId }))[0].properties.roleName
            $result.roleConditionEffect | Should -Be 'limited' -Because $name
            $result.roleIsCritical | Should -BeFalse -Because $name
            $result.roleConditionSources | Should -Be 'role definition'
        }
    }
    It 'a role that may assign a role limited by its own condition is limited too (evaluated until nothing changes)' {
        $owner = ($script:Roles.where({ $_.properties.roleName -eq 'Azure Local Migrate Owner' }))[0]
        $expert = ($script:Roles.where({ $_.properties.roleName -eq 'Azure Local Migrate Execute Expert' }))[0]
        $result = (Invoke-Conditions @(@{ roleId = $owner.name; condition = $null }))[0]
        $result.roleConditionAssignableRoles | Should -Match 'Azure Local Migrate Execute Expert'
        $result.roleConditionEffect | Should -Be 'limited'
        (testRoleCapableForRoleAssignmentsWrite -RoleDefinition $expert) | Should -BeTrue -Because 'it can write role assignments by its actions'
    }
    It 'Key Vault Data Access Administrator: limited to the Key Vault roles, with the text for the report' {
        $kv = ($script:Roles.where({ $_.properties.roleName -eq 'Key Vault Data Access Administrator' }))[0]
        $result = (Invoke-Conditions @(@{ roleId = $kv.name; condition = $null }))[0]
        $text = getAzureRoleConditionText $result
        $text | Should -Match '^limited by role definition condition: can assign only Key Vault '
        $text | Should -Not -Match 'Owner|User Access Administrator'
    }
    It 'a role whose own definition condition allows itself is limited (the new assignment carries the same limit)' {
        $sphere = ($script:Roles.where({ $_.properties.roleName -eq 'Azure Sphere Owner' }))[0]
        $result = (Invoke-Conditions @(@{ roleId = $sphere.name; condition = $null }))[0]
        $result.roleConditionEffect | Should -Be 'limited'
        $result.roleConditionAssignableRoles | Should -Match 'Azure Sphere Owner'
    }
    It 'roles without condition keep their flag (Owner critical, Storage Blob Data Reader not)' {
        $results = Invoke-Conditions @(@{ roleId = $script:Owner; condition = $null }, @{ roleId = $script:BlobDataReader; condition = "@Resource[Microsoft.Storage/storageAccounts/blobServices/containers:name] StringEquals 'logs'" })
        $results[0].roleIsCritical | Should -BeTrue
        $results[0].roleConditionEffect | Should -Be ''
        $results[1].roleIsCritical | Should -BeFalse
        $results[1].roleConditionEffect | Should -Be ''
        $results[1].roleAssignmentCondition | Should -Match 'StringEquals'
    }
}

Describe 'conditions on role assignments (constrain roles)' {
    It 'Role Based Access Control Administrator limited to Storage Blob Data roles: limited, not critical' {
        $result = (Invoke-Conditions @(@{ roleId = $script:RbacAdministrator; condition = (New-ConstrainRolesCondition @($script:BlobDataContributor, $script:BlobDataReader)) }))[0]
        $result.roleConditionEffect | Should -Be 'limited'
        $result.roleIsCritical | Should -BeFalse
        getAzureRoleConditionText $result | Should -Be 'limited by role assignment condition: can assign only Storage Blob Data Contributor, Storage Blob Data Reader'
    }
    It 'a constrain-roles condition that allows Owner: critical, Owner named' {
        $result = (Invoke-Conditions @(@{ roleId = $script:UserAccessAdministrator; condition = (New-ConstrainRolesCondition @($script:BlobDataReader, $script:Owner)) }))[0]
        $result.roleConditionEffect | Should -Be 'allows critical'
        $result.roleIsCritical | Should -BeTrue
        getAzureRoleConditionText $result | Should -Be 'role assignment condition allows Owner'
    }
    It 'a condition on the assignment that allows the role itself: critical (the new assignment would not carry the condition)' {
        $result = (Invoke-Conditions @(@{ roleId = $script:RbacAdministrator; condition = (New-ConstrainRolesCondition @($script:RbacAdministrator)) }))[0]
        $result.roleConditionEffect | Should -Be 'allows critical'
    }
    It 'a role that is limited by its own definition counts as not critical when it is allowed' {
        $kv = ($script:Roles.where({ $_.properties.roleName -eq 'Key Vault Data Access Administrator' }))[0]
        $result = (Invoke-Conditions @(@{ roleId = $script:RbacAdministrator; condition = (New-ConstrainRolesCondition @($kv.name)) }))[0]
        $result.roleConditionEffect | Should -Be 'limited'
    }
    It '"all roles except Owner, User Access Administrator, RBAC Administrator": critical while other roles that can assign roles remain' {
        $condition = "((!(ActionMatches{'Microsoft.Authorization/roleAssignments/write'})) OR (@Request[Microsoft.Authorization/roleAssignments:RoleDefinitionId] ForAnyOfAllValues:GuidNotEquals {$($script:Owner), $($script:UserAccessAdministrator), $($script:RbacAdministrator)}))"
        $result = (Invoke-Conditions @(@{ roleId = $script:RbacAdministrator; condition = $condition }))[0]
        $result.roleConditionAssignableRoles | Should -Match '^all roles except Owner, Role Based Access Control Administrator, User Access Administrator$'
        #the only built-in role left that can assign roles without a limiting condition (roles limited by their own definition
        #condition, such as Azure Local Migrate Owner -> Azure Local Migrate Execute Expert -> storage roles, are not critical)
        $result.roleConditionCriticalRoles | Should -Be 'Reservations Administrator'
        $result.roleConditionEffect | Should -Be 'allows critical'
    }
    It 'an unknown role id (custom role of a scope that was not collected) counts as critical; an unreadable condition is "not evaluated"' {
        $unknown = 'aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee'
        $results = Invoke-Conditions @(
            @{ roleId = $script:RbacAdministrator; condition = (New-ConstrainRolesCondition @($unknown)) }
            @{ roleId = $script:RbacAdministrator; condition = "(@Request[Microsoft.Authorization/roleAssignments:RoleDefinitionId] ForAnyOfAnyValues:GuidEquals {$($script:BlobDataReader)}) OR (@Request[Microsoft.Authorization/roleAssignments:PrincipalType] ForAnyOfAnyValues:StringEqualsIgnoreCase {'Group'})" }
        )
        $results[0].roleConditionEffect | Should -Be 'allows critical'
        $results[0].roleConditionCriticalRoles | Should -Be "role $unknown"
        $results[1].roleConditionEffect | Should -Be 'not evaluated'
        $results[1].roleIsCritical | Should -BeTrue
        getAzureRoleConditionText $results[1] | Should -Be 'role assignment condition not evaluated - rated critical'
    }
}

Describe 'wiring' {
    It 'applied after the collection and the lookup of missing role definitions' {
        $script:Source | Should -Match '(?s)dataCollection -mgId \$ManagementGroupId\r?\n\s+resolveMissingRoleDefinitions\r?\n\s+applyAzureRoleConditions'
    }
    It 'report, CSV, JSON, effective privilege, HiPo users and change history use it' {
        $script:Source | Should -Match ([regex]::Escape('getSpiConditionLimitedIconHtml ($azureroleAss.roleConditionEffect -eq ''limited'')'))
        foreach ($column in 'ConditionEffect', 'ConditionAssignableRoles', 'ConditionCriticalRoles', 'RoleDefinitionCondition', 'RoleAssignmentCondition') {
            $script:Source | Should -Match ("\s$column = \`$azureroleAss\.") -Because $column
        }
        foreach ($property in 'roleConditionEffect', 'roleConditionAssignableRoles', 'roleDefinitionCondition', 'roleAssignmentCondition') {
            $script:Source | Should -Match ("\s$property = \[string\]\`$hlproleAssignmentFull\.$property") -Because $property
        }
        $script:Source | Should -Match "category = 'Azure role \(limited by condition\)'"
        $script:Source | Should -Match "'Azure role \(limited by condition\)\|\*' = "
        $script:Source | Should -Match 'permission = "\$\(\$SPAzureRoleAssignment\.roleName\).*getAzureRoleConditionText \$SPAzureRoleAssignment'
        $script:Source | Should -Match "property = 'azureRoleConditions'; category = 'security'; added = 'Azure role condition added'; removed = 'Azure role condition removed'; changed = 'Azure role condition changed'"
        $script:Source | Should -Match '\$script:changeHistoryFormatVersion = 3'
    }
}
