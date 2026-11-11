#Review R-6: role definitions that the custom role listings did not return are read by id after the collection

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    foreach ($name in 'testRoleCapableForRoleAssignmentsWrite', 'resolveMissingRoleDefinitions') {
        . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name }, $true).Extent.Text))
    }
    $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $azAPICallConf = @{ azAPIEndpointUrls = @{ ARM = 'https://management.azure.com' } }

    function global:AzAPICall {
        param($AzAPICallConfiguration, $uri, $method, $currentTask, $listenOn, $skipOnErrorCode)
        $global:AzAPICallUris.Add($uri)
        #the RG-only role can be read at the scope of its assignment only; 'deleted' does not exist
        if ($uri -like '*/resourceGroups/rg1/providers/Microsoft.Authorization/roleDefinitions/rg-only?*') {
            return [pscustomobject]@{ name = 'rg-only'; properties = [pscustomobject]@{ roleName = 'RG role writer'; type = 'CustomRole'; permissions = @([pscustomobject]@{ actions = @('Microsoft.Authorization/roleAssignments/write'); notActions = @() }) } }
        }
        return $null
    }
}
AfterAll { Remove-Item function:global:AzAPICall -ErrorAction SilentlyContinue }

Describe 'R-6 testRoleCapableForRoleAssignmentsWrite' {
    It '<Name>' -ForEach @(
        @{ Name = 'roleAssignments/write'; Actions = @('Microsoft.Authorization/roleAssignments/write'); NotActions = @(); Expected = $true }
        @{ Name = '* without exclusion'; Actions = @('*'); NotActions = @(); Expected = $true }
        @{ Name = '* excluding Microsoft.Authorization/*/write (Contributor)'; Actions = @('*'); NotActions = @('Microsoft.Authorization/*/Write'); Expected = $false }
        @{ Name = 'read only'; Actions = @('*/read'); NotActions = @(); Expected = $false }
    ) {
        testRoleCapableForRoleAssignmentsWrite -RoleDefinition ([pscustomobject]@{ properties = [pscustomobject]@{ permissions = @([pscustomobject]@{ actions = $Actions; notActions = $NotActions }) } }) | Should -Be $Expected
    }
}

Describe 'R-6 resolveMissingRoleDefinitions' {
    BeforeEach {
        $global:AzAPICallUris = [System.Collections.Generic.List[string]]::new()
        $htCacheDefinitionsRole = @{ 'known' = @{ definition = [pscustomobject]@{ properties = [pscustomobject]@{ roleName = 'Known'; type = 'CustomRole' } }; roleIsCritical = $false } }
        $htCacheAssignmentsRole = @{
            'a1' = @{ assignment = [pscustomobject]@{ properties = [pscustomobject]@{ scope = '/subscriptions/s1/resourceGroups/rg1'; roleDefinitionId = '/subscriptions/s1/providers/Microsoft.Authorization/roleDefinitions/rg-only' } }; roleId = 'rg-only'; roleName = $null; type = $null; roleIsCritical = $null }
            'a2' = @{ assignment = [pscustomobject]@{ properties = [pscustomobject]@{ scope = '/subscriptions/s1'; roleDefinitionId = '/subscriptions/s1/providers/Microsoft.Authorization/roleDefinitions/deleted' } }; roleId = 'deleted'; roleName = $null; type = $null; roleIsCritical = $null }
            'a3' = @{ assignment = [pscustomobject]@{ properties = [pscustomobject]@{ scope = '/subscriptions/s1'; roleDefinitionId = '/subscriptions/s1/providers/Microsoft.Authorization/roleDefinitions/known' } }; roleId = 'known'; roleName = 'Known'; type = 'CustomRole'; roleIsCritical = $false }
        }
        Mock Write-Host {}
    }
    It 'fills role name, type and critical flag of assignments whose definition was not listed' {
        resolveMissingRoleDefinitions
        $htCacheAssignmentsRole['a1'].roleName | Should -Be 'RG role writer'
        $htCacheAssignmentsRole['a1'].type | Should -Be 'CustomRole'
        $htCacheAssignmentsRole['a1'].roleIsCritical | Should -BeTrue
        $htCacheDefinitionsRole['rg-only'].roleIsCritical | Should -BeTrue
    }
    It 'tries the roleDefinitionId first, then the assignment scope; never asks for known definitions' {
        resolveMissingRoleDefinitions
        $global:AzAPICallUris[0] | Should -BeLike 'https://management.azure.com/subscriptions/s1/providers/Microsoft.Authorization/roleDefinitions/*'
        @($global:AzAPICallUris.where({ $_ -like '*/known?*' })).Count | Should -Be 0
        @($global:AzAPICallUris.where({ $_ -like '*/resourceGroups/rg1/providers/Microsoft.Authorization/roleDefinitions/rg-only?*' })).Count | Should -Be 1
    }
    It 'labels the assignments of an unresolvable (deleted) definition and reports it' {
        resolveMissingRoleDefinitions
        $htCacheAssignmentsRole['a2'].roleName | Should -Be 'role definition not found (deleted?)'
        $htCacheAssignmentsRole['a2'].roleIsCritical | Should -BeNullOrEmpty
        Should -Invoke Write-Host -ParameterFilter { $Object -like '*not found (deleted definitions?): deleted (1 role assignments)*' }
    }
    It 'runs after the collection of management groups and subscriptions' {
        $script:Source | Should -Match 'dataCollection -mgId \$ManagementGroupId\r?\n\s+resolveMissingRoleDefinitions'
    }
}
