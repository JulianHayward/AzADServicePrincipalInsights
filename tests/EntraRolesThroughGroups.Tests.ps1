#Pester 5+ tests (theme 21): Entra role assignments through role-assignable groups, and Microsoft's isPrivileged flag for 'critical'.
#Runs the real code regions of the main script in-process against fake data / a fake AzAPICall.

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $source = Get-Content -Raw -LiteralPath $script:MainScriptPath

    #post-collection step: from the HashSet of group ids to the console line
    $m = [regex]::Match($source, '(?s)    \$htAADRoleAssignmentsOfGroups = @\{\}.*?Write-Host " Entra role assignments through groups:[^\r\n]*')
    if (-not $m.Success) { throw 'post-collection region not found' }
    $script:ThroughGroupsCode = [scriptblock]::Create($m.Value)

    #enrichment region (inside the enrichment parallel block)
    $m = [regex]::Match($source, '(?s)#region ServicePrincipalAADRoleAssignments\r?\n(.*?)#endregion ServicePrincipalAADRoleAssignments')
    if (-not $m.Success) { throw 'enrichment region not found' }
    $script:EnrichmentCode = [scriptblock]::Create($m.Groups[1].Value)

    function global:AzAPICall {
        param($AzAPICallConfiguration, $uri, $method, $currentTask)
        $null = $global:AzADSPITestCalls.Add($uri)
        if ($uri -match "principalId eq '(\w+)'") { return @($global:AzADSPITestGroupRoles[$Matches[1]]) }
        return @()
    }

    $script:RaG1 = [pscustomobject]@{ id = 'raG1'; principalId = 'g1'; roleDefinitionId = 'rdPriv'; directoryScopeId = '/'; resourceScope = '/' }
    function New-Enriched {
        $ht = [hashtable]::Synchronized(@{})
        $ht['sp1'] = @{ ServicePrincipalGroupMemberships = @('g1', 'g2') }
        $ht['sp2'] = @{}
        $ht['sp3'] = @{ MeanWhileDeleted = $true; ServicePrincipalGroupMemberships = @('g3') }
        $ht['sp4'] = @{ ServicePrincipalGroupMemberships = @('g1') }
        $ht
    }
}

AfterAll {
    Remove-Item function:global:AzAPICall -ErrorAction SilentlyContinue
    Remove-Variable -Name AzADSPITestCalls, AzADSPITestGroupRoles -Scope Global -ErrorAction SilentlyContinue
}

Describe 'Entra role assignments through groups (post-collection step)' {
    It 'with the tenant-wide prefetch: from the index, no request' {
        $htServicePrincipalsAndAppsOnlyEnriched = New-Enriched
        $htAADRoleAssignmentsByPrincipalId = @{ g1 = @($script:RaG1); sp1 = @([pscustomobject]@{ id = 'raDirect' }) }
        $azAPICallConf = @{ azAPIEndpointUrls = @{ MicrosoftGraph = 'https://graph.microsoft.com' } }
        $global:AzADSPITestCalls = [System.Collections.ArrayList]@()
        . $script:ThroughGroupsCode 6>$null
        $global:AzADSPITestCalls.Count | Should -Be 0
        @($htServicePrincipalsAndAppsOnlyEnriched['sp1'].ServicePrincipalAADRoleAssignmentsThroughGroups).Count | Should -Be 1
        $htServicePrincipalsAndAppsOnlyEnriched['sp1'].ServicePrincipalAADRoleAssignmentsThroughGroups[0].groupId | Should -Be 'g1'
        $htServicePrincipalsAndAppsOnlyEnriched['sp1'].ServicePrincipalAADRoleAssignmentsThroughGroups[0].assignment.id | Should -Be 'raG1'
        $htServicePrincipalsAndAppsOnlyEnriched['sp4'].ServicePrincipalAADRoleAssignmentsThroughGroups[0].assignment.id | Should -Be 'raG1'
        $htServicePrincipalsAndAppsOnlyEnriched['sp2'].ContainsKey('ServicePrincipalAADRoleAssignmentsThroughGroups') | Should -BeFalse
        #deleted meanwhile: skipped, its group is not looked up
        $htServicePrincipalsAndAppsOnlyEnriched['sp3'].ContainsKey('ServicePrincipalAADRoleAssignmentsThroughGroups') | Should -BeFalse
        $aadRoleAssignmentsThroughGroupsCount | Should -Be 2
    }
    It 'without the prefetch: one request per distinct group (not per SP), same result' {
        $htServicePrincipalsAndAppsOnlyEnriched = New-Enriched
        $htAADRoleAssignmentsByPrincipalId = $null
        $azAPICallConf = @{ azAPIEndpointUrls = @{ MicrosoftGraph = 'https://graph.microsoft.com' } }
        $global:AzADSPITestGroupRoles = @{ g1 = $script:RaG1 }
        $global:AzADSPITestCalls = [System.Collections.ArrayList]@()
        . $script:ThroughGroupsCode 6>$null
        $global:AzADSPITestCalls.Count | Should -Be 2
        @($global:AzADSPITestCalls | Where-Object { $_ -like "*principalId eq 'g1'*" }).Count | Should -Be 1
        $htServicePrincipalsAndAppsOnlyEnriched['sp1'].ServicePrincipalAADRoleAssignmentsThroughGroups[0].assignment.id | Should -Be 'raG1'
        $htServicePrincipalsAndAppsOnlyEnriched['sp4'].ServicePrincipalAADRoleAssignmentsThroughGroups[0].assignment.id | Should -Be 'raG1'
    }
}

Describe 'Entra role assignments (enrichment)' {
    BeforeAll {
        $script:Opt = {
            param($Object)
            $object = $Object
            $htAadRoleDefinitions = @{
                rdPriv   = [pscustomobject]@{ displayName = 'User Administrator'; isBuiltIn = $true; isPrivileged = $true }
                rdPlain  = [pscustomobject]@{ displayName = 'Directory Readers'; isBuiltIn = $true; isPrivileged = $false }
                rdCustom = [pscustomobject]@{ displayName = 'customRole1'; isBuiltIn = $false; isPrivileged = $false }
            }
            $CriticalAADRoles = @('rdCustom')
            $htAaDGroups = @{ g1 = @{ groupDetails = [pscustomobject]@{ displayName = 'RoleGroup1' } } }
            $htSPandAPPHelper4AADRoleAssignmentsWithScope = @{}
            . $script:EnrichmentCode
            , $arrayServicePrincipalAADRoleAssignmentsOpt
        }
    }
    It 'critical = Microsoft isPrivileged or -CriticalAADRoles; direct and through a group' {
        $object = [pscustomobject]@{
            ServicePrincipalAADRoleAssignments                = @(
                [pscustomobject]@{ id = 'ra1'; roleDefinitionId = 'rdPlain'; directoryScopeId = '/'; resourceScope = '/' }
                [pscustomobject]@{ id = 'ra2'; roleDefinitionId = 'rdCustom'; directoryScopeId = '/'; resourceScope = '/' }
            )
            ServicePrincipalAADRoleAssignmentsThroughGroups = @([pscustomobject]@{ groupId = 'g1'; assignment = $script:RaG1 })
        }
        $r = & $script:Opt $object
        $r.Count | Should -Be 3
        $byId = @{}; $r | ForEach-Object { $byId[$_.id] = $_ }
        $byId['ra1'].roleIsCritical | Should -BeFalse
        $byId['ra1'].roleIsPrivileged | Should -BeFalse
        $byId['ra1'].roleAssignmentApplicability | Should -Be 'direct'
        $byId['ra1'].roleAssignmentAppliesThrough | Should -Be ''
        $byId['ra2'].roleIsCritical | Should -BeTrue -Because 'listed in -CriticalAADRoles'
        $byId['ra2'].roleType | Should -Be 'Custom'
        $byId['raG1'].roleIsCritical | Should -BeTrue -Because 'Microsoft marks the role as privileged'
        $byId['raG1'].roleIsPrivileged | Should -BeTrue
        $byId['raG1'].roleAssignmentApplicability | Should -Be 'indirect'
        $byId['raG1'].roleAssignmentAppliesThrough | Should -Be 'RoleGroup1 (g1)'
        $byId['raG1'].roleDefinitionName | Should -Be 'User Administrator'
    }
    It 'no assignments: empty list' {
        $r = & $script:Opt ([pscustomobject]@{})
        @($r).Count | Should -Be 0
    }
}
