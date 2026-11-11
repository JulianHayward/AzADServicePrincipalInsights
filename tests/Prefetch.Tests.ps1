#Pester 5+ tests for the tenant-wide prefetch (review P1-20): with the indexes, the per-SP requests are not made and the
#data comes from the index; without them (not permitted / failed), the per-SP requests are made as before.
#Runs the real SP collection parallel block in-process against a fake AzAPICall that records the requested URIs.

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')

    function global:AzAPICall {
        param($AzAPICallConfiguration, $uri, $method, $currentTask, $listenOn, $consistencyLevel, $noPaging, $skipOnErrorCode, $body, $validateAccess)
        $null = $global:AzADSPITestCalls.Add($uri)
        foreach ($key in $global:AzADSPITestFakeResponses.Keys) {
            if ($uri.EndsWith($key)) { return $global:AzADSPITestFakeResponses[$key] }
        }
        return @()
    }

    function Invoke-SPCollectionForSP {
        param([hashtable]$Indexes)
        $block = Get-ParallelBlock -Path $script:MainScriptPath -Marker 'AppWithoutSP_'
        foreach ($name in $block.UsingNames) { Set-Variable -Name $name -Value ([hashtable]::Synchronized(@{})) }
        $skipApplicationRequests = $false   #R-22 flag (not a hashtable)
        #prefetch indexes: $null = not available
        $htApplicationsByAppId = $Indexes.Apps
        $htAADRoleAssignmentsByPrincipalId = $Indexes.RoleAssignments
        $htAADRoleAssignmentsByResourceScope = $Indexes.RoleAssignedOn
        $htOauth2PermissionGrantsByClientId = $Indexes.Grants
        $azAPICallConf = @{ azAPIEndpointUrls = @{ MicrosoftGraph = 'https://graph.microsoft.com' }; checkContext = @{ Tenant = @{ Id = 'tenant1' } } }
        $indicator = 100
        $ErrorActionPreference = 'Stop'
        $sp = [pscustomobject]@{ id = 'sp1'; appId = 'appid1'; displayName = 'SP 1'; servicePrincipalType = 'Application'; appOwnerOrganizationId = 'tenant1' }
        $global:AzADSPITestCalls = [System.Collections.ArrayList]@()
        $null = [pscustomobject]@{ Group = @([pscustomobject]@{ SPOrAppWithoutSP = 'SP'; Details = $sp }) } | ForEach-Object -Process $block.ScriptBlock 6>$null
        [pscustomobject]@{ Enriched = $htServicePrincipalsAndAppsOnlyEnriched['sp1']; Calls = @($global:AzADSPITestCalls) }
    }

    $script:Grant = [pscustomobject]@{ id = 'g1'; clientId = 'sp1'; resourceId = 'res1'; scope = 'User.Read'; consentType = 'AllPrincipals' }
    $script:RoleAssignment = [pscustomobject]@{ id = 'ra1'; principalId = 'sp1'; roleDefinitionId = 'rd1'; directoryScopeId = '/'; resourceScope = '/' }
    $script:App = [pscustomobject]@{ id = 'app1'; appId = 'appid1'; displayName = 'App 1'; keyCredentials = @(); passwordCredentials = @(); tags = @() }
}

AfterAll {
    Remove-Item function:global:AzAPICall -ErrorAction SilentlyContinue
    Remove-Variable -Name AzADSPITestFakeResponses, AzADSPITestCalls -Scope Global -ErrorAction SilentlyContinue
}

Describe 'P1-20 prefetch indexes are used' {
    BeforeAll {
        $global:AzADSPITestFakeResponses = @{}
        $script:R = Invoke-SPCollectionForSP -Indexes @{
            Apps = @{ 'appid1' = $script:App }
            RoleAssignments = @{ 'sp1' = [System.Collections.Generic.List[object]]@($script:RoleAssignment) }
            RoleAssignedOn = @{}
            Grants = @{ 'sp1' = [System.Collections.Generic.List[object]]@($script:Grant) }
        }
    }
    It 'makes no per-SP request for <Name>' -ForEach @(
        @{ Name = 'oauth2PermissionGrants'; Pattern = '/oauth2PermissionGrants' }
        @{ Name = 'role assignments by principal'; Pattern = 'roleAssignments\?\$filter=principalId' }
        @{ Name = 'role assignments by resourceScope'; Pattern = 'roleAssignments\?\$filter=resourceScope' }
        @{ Name = 'application by appId'; Pattern = 'applications\?\$filter=appId' }
    ) {
        @($script:R.Calls | Where-Object { $_ -match $Pattern }).Count | Should -Be 0
    }
    It 'stores the prefetched grants, role assignments and application' {
        @($script:R.Enriched.ServicePrincipalOauth2PermissionGrants).id | Should -Be 'g1'
        @($script:R.Enriched.ServicePrincipalAADRoleAssignments).id | Should -Be 'ra1'
        $script:R.Enriched.Application.ApplicationDetails.id | Should -Be 'app1'
    }
    It 'stores the application as a single object (empty tags stay an empty array, not $null)' {
        $script:R.Enriched.Application.ApplicationDetails | Should -BeOfType [pscustomobject]
        , $script:R.Enriched.Application.ApplicationDetails.tags | Should -BeOfType [object[]]
    }
}

Describe 'P1-20 fallback without prefetch indexes' {
    BeforeAll {
        $global:AzADSPITestFakeResponses = @{
            '/beta/servicePrincipals/sp1/oauth2PermissionGrants' = @($script:Grant)
            "/beta/roleManagement/directory/roleAssignments?`$filter=principalId eq 'sp1'" = @($script:RoleAssignment)
            "/v1.0/applications?`$filter=appId eq 'appid1'" = $script:App
        }
        $script:R = Invoke-SPCollectionForSP -Indexes @{ Apps = $null; RoleAssignments = $null; RoleAssignedOn = $null; Grants = $null }
    }
    It 'makes the per-SP request for <Name>' -ForEach @(
        @{ Name = 'oauth2PermissionGrants'; Pattern = '/servicePrincipals/sp1/oauth2PermissionGrants' }
        @{ Name = 'role assignments by principal'; Pattern = "roleAssignments\?\`$filter=principalId eq 'sp1'" }
        @{ Name = 'role assignments by resourceScope'; Pattern = "roleAssignments\?\`$filter=resourceScope eq '/sp1'" }
        @{ Name = 'application by appId'; Pattern = "applications\?\`$filter=appId eq 'appid1'" }
    ) {
        @($script:R.Calls | Where-Object { $_ -match $Pattern }).Count | Should -Be 1
    }
    It 'stores the same data as with the prefetch' {
        @($script:R.Enriched.ServicePrincipalOauth2PermissionGrants).id | Should -Be 'g1'
        @($script:R.Enriched.ServicePrincipalAADRoleAssignments).id | Should -Be 'ra1'
        $script:R.Enriched.Application.ApplicationDetails.id | Should -Be 'app1'
    }
}

Describe 'P0-4 the SPOauth2PermissionGrantedTo index is not built inside the parallel collection anymore' {
    It 'the SP collection parallel block does not write htSPOauth2PermissionGrantedTo' {
        #(Get-ParallelBlock rewrites '$script:x' and '$using:x' to '$x')
        (Get-ParallelBlock -Path $script:MainScriptPath -Marker 'AppWithoutSP_').ScriptBlock.ToString() | Should -Not -Match '\$htSPOauth2PermissionGrantedTo'
    }
    It 'original code d332d98 wrote it from the parallel block (race)' {
        (Get-ParallelBlock -Path (Get-MainScriptAtCommit -Commit 'd332d98') -Marker 'AppWithoutSP_').ScriptBlock.ToString() | Should -Match '\$htSPOauth2PermissionGrantedTo\.\(\$permissionGrant\.resourceId\)\.Add'
    }
}
