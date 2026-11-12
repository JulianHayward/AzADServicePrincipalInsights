#Pester 5+ tests for the change history (feature A7, 2026-11-07): state of the output objects, comparison by ID, the log
#(one JSON line per change, no time limit), the guards against false changes (baseline, scope change, incomplete run, unreadable
#or other format state) and the report rows. No Azure access: synthetic objects in the shape of the JSON output.

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    foreach ($name in 'getChangeHistoryObjectState', 'getChangeHistoryState', 'compareChangeHistoryState', 'getChangeHistoryScope', 'updateChangeHistory', 'convertChangeHistoryTime', 'getChangeHistoryRows', 'getChangeHistoryLogOffset', 'getAzureRoleConditionText') {
        . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name }, $true).Extent.Text))
    }
    $script:changeHistoryFormatVersion = [int]([regex]::Match($script:Source, '\$script:changeHistoryFormatVersion = (\d+)').Groups[1].Value)
    $script:changeHistoryMicrosoftTenants = @('f8cdef31-a31e-4b4a-93e4-5f571e91255a', '72f988bf-86f1-41af-91ab-2d7cd011db47')
    $script:Tenant = '11111111-2222-4333-8444-555555555555'

    #an internal app with its service principal, in the shape of the JSON output
    function New-AppEntry([string]$Id = 'sp1', [string]$Name = 'App one') {
        [pscustomobject]@{
            ObjectId = $Id; ObjectType = 'SP APP INT'
            SP = @([pscustomobject]@{ SPObjectId = $Id; SPDisplayName = $Name; SPAppOwnerOrganizationId = $script:Tenant; SPAccountEnabled = $true })
            APP = @([pscustomobject]@{ APPObjectId = "app-$Id"; APPDisplayName = $Name; APPSignInAudience = 'AzureADMyOrg'; APPRedirectUris = @([pscustomobject]@{ platform = 'web'; uri = 'https://a.example' }); APPIdentifierUris = @('api://one') })
            SPEffectivePrivilege = [pscustomobject]@{ level = 'high'; reasons = @() }
            SPAADRoleAssignments = @([pscustomobject]@{ id = 'ra1'; roleDefinitionName = 'Directory Readers'; roleIsPrivileged = $false; roleAssignmentApplicability = 'direct'; directoryScopeId = '/' })
            SPAzureRoleAssignments = @(
                [pscustomobject]@{ roleAssignmentId = 'az1'; roleName = 'Owner'; roleIsCritical = $true; roleAssignmentApplicability = 'direct'; roleAssignmentAssignmentScope = 'Sub'; roleAssignmentAssignmentResourceType = 'Subscription'; roleAssignmentAssignmentScopeName = 'Subscription one' }
                [pscustomobject]@{ roleAssignmentId = 'az2'; roleName = 'Reader'; roleIsCritical = $false; roleAssignmentApplicability = 'direct'; roleAssignmentAssignmentScope = '/subscriptions/s1' }
            )
            SPAppRoleAssignments = @([pscustomobject]@{ AppRoleAssignmentId = 'ar1'; AppRolePermission = 'User.Read.All'; AppRoleAssignmentResourceDisplayName = 'Microsoft Graph'; AppRolePermissionSensitivity = 'medium' })
            SPOauth2PermissionGrants = @([pscustomobject]@{ id = 'scope-openid'; SPId = 'graph'; scope = 'openid'; permission = 'openid'; SPDisplayName = 'Microsoft Graph'; type = 'User'; consentType = 'AllPrincipals'; principalId = $null; grantId = 'g1'; permissionSensitivity = '' })
            APPPasswordCredentials = @([pscustomobject]@{ keyId = 'k1'; displayName = 'pipeline'; endDateTimeFormated = '2027-01-01 00:00:00 UTC' })
            APPKeyCredentials = @()
            SPPasswordCredentials = @()
            SPKeyCredentials = @()
            APPFederatedIdentityCredentials = @([pscustomobject]@{ id = 'f1'; name = 'gh'; issuer = 'https://token.actions.githubusercontent.com'; subject = 'repo:o/r:ref:refs/heads/main' })
            SPOwners = @(
                [pscustomobject]@{ id = 'u1'; displayName = 'Owner One'; principalType = 'User (Member)'; applicability = 'direct' }
                [pscustomobject]@{ id = 'u9'; displayName = 'Indirect'; principalType = 'User (Member)'; applicability = 'indirect' }
            )
            APPAppOwners = @([pscustomobject]@{ id = 'u2'; displayName = 'Owner Two'; principalType = 'User (Member)'; applicability = 'direct' })
            SPGroupMemberships = @([pscustomobject]@{ ObjectId = 'grp1'; DisplayName = 'Group one' })
        }
    }
    function New-MsEntry([string]$Id) {
        [pscustomobject]@{ ObjectId = $Id; ObjectType = 'SP APP EXT'; SP = @([pscustomobject]@{ SPObjectId = $Id; SPDisplayName = "MS $Id"; SPAppOwnerOrganizationId = 'f8cdef31-a31e-4b4a-93e4-5f571e91255a'; SPAccountEnabled = $true }); SPEffectivePrivilege = [pscustomobject]@{ level = 'none' } }
    }
    function New-MiEntry([string]$Id, [string]$KeyId) {
        [pscustomobject]@{ ObjectId = $Id; ObjectType = 'SP MI System assigned'; SP = @([pscustomobject]@{ SPObjectId = $Id; SPDisplayName = "mi $Id"; SPAppOwnerOrganizationId = 'f8cdef31-a31e-4b4a-93e4-5f571e91255a'; SPAccountEnabled = $true }); SPEffectivePrivilege = [pscustomobject]@{ level = 'none' }; SPKeyCredentials = @([pscustomobject]@{ keyId = $KeyId; endDateTimeFormated = 'x' }) }
    }
    function Get-Changes($Before, $After, [bool]$VersionChanged = $false) {
        $previous = (ConvertTo-Json -InputObject (getChangeHistoryState -entries $Before) -Depth 8) | ConvertFrom-Json -AsHashtable
        , @(compareChangeHistoryState -Previous $previous -Current (getChangeHistoryState -entries $After) -Time '2026-11-07T10:00:00.0000000Z' -Version 'v2' -VersionChanged $VersionChanged)
    }
    function New-Run([string]$Folder) {
        $null = New-Item -ItemType Directory -Path $Folder -Force
        @{ StatePath = Join-Path $Folder 'state.json'; LogPath = Join-Path $Folder 'log.jsonl' }
    }
    function Invoke-Update($Run, $Entries, $Scope, [string]$Version = 'v1', [bool]$Incomplete = $false, [string]$Time = '2026-11-07T10:00:00.0000000Z') {
        , @(updateChangeHistory -StatePath $Run.StatePath -LogPath $Run.LogPath -Scope $Scope -CurrentState (getChangeHistoryState -entries $Entries) -Version $Version -Incomplete $Incomplete -Time $Time)
    }
    $script:Scope = getChangeHistoryScope -TenantId $script:Tenant -ManagementGroupIds @('mg1') -SubscriptionQuotaIdWhitelist @('undefined') -NoAzureResourceSideRelations $false -OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes $false
}

Describe 'comparison' {
    It 'no change: nothing recorded' {
        (Get-Changes @(New-AppEntry) @(New-AppEntry)).Count | Should -Be 0
    }
    It 'records every change type of the list' {
        $after = New-AppEntry -Name 'App renamed'
        $after.SP[0].SPAccountEnabled = $false
        $after.APP[0].APPSignInAudience = 'AzureADMultipleOrgs'
        $after.APP[0].APPRedirectUris = @([pscustomobject]@{ platform = 'web'; uri = 'https://a.example' }, [pscustomobject]@{ platform = 'spa'; uri = 'https://b.example' })
        $after.APP[0].APPIdentifierUris = @()
        $after.SPEffectivePrivilege.level = 'critical'
        $after.SPAADRoleAssignments += [pscustomobject]@{ id = 'ra2'; roleDefinitionName = 'Global Administrator'; roleIsPrivileged = $true; roleAssignmentApplicability = 'direct'; directoryScopeId = '/' }
        $after.SPAzureRoleAssignments = @($after.SPAzureRoleAssignments.where({ $_.roleAssignmentId -ne 'az1' }))
        $after.SPAppRoleAssignments += [pscustomobject]@{ AppRoleAssignmentId = 'ar2'; AppRolePermission = 'RoleManagement.ReadWrite.Directory'; AppRoleAssignmentResourceDisplayName = 'Microsoft Graph'; AppRolePermissionSensitivity = 'critical' }
        $after.SPOauth2PermissionGrants = @()
        $after.APPPasswordCredentials += [pscustomobject]@{ keyId = 'k2'; displayName = 'new'; endDateTimeFormated = '2028-01-01 00:00:00 UTC' }
        $after.SPPasswordCredentials = @([pscustomobject]@{ keyId = 'k3'; displayName = 'on the SP'; endDateTimeFormated = '2028-01-01 00:00:00 UTC' })
        $after.APPFederatedIdentityCredentials[0].subject = 'repo:o/r:ref:refs/heads/dev'
        $after.SPOwners = @($after.SPOwners.where({ $_.id -ne 'u1' }))
        $after.APPAppOwners += [pscustomobject]@{ id = 'u3'; displayName = 'Owner Three'; principalType = 'Serviceprincipal'; applicability = 'direct' }
        $after.SPGroupMemberships += [pscustomobject]@{ ObjectId = 'grp2'; DisplayName = 'Group two' }
        $changes = Get-Changes @(New-AppEntry) @($after)
        $byChange = $changes | Group-Object { $_.change } -AsHashTable -AsString
        @($changes.change | Sort-Object) | Should -Be @(
            'application permission added', 'credential added', 'credential added', 'critical Azure role removed', 'delegated permission removed', 'disabled', 'effective privilege changed',
            'Entra role added', 'federated credential changed', 'group membership added', 'identifier URI removed', 'owner added', 'owner removed', 'redirect URI added', 'renamed', 'renamed', 'sign-in audience changed'
        )
        $byChange['effective privilege changed'][0].detail | Should -BeExactly 'high -> critical'
        $byChange['Entra role added'][0].detail | Should -BeExactly 'Global Administrator (privileged)'
        #R-24: the scope (type and name), not only its level
        $byChange['critical Azure role removed'][0].detail | Should -BeExactly 'Owner on Subscription Subscription one'
        $byChange['federated credential changed'][0].detail | Should -Match 'refs/heads/main -> .*refs/heads/dev'
        $byChange['owner removed'][0].detail | Should -Match '^Owner One .*owner of the service principal$'
        @($byChange['credential added'].detail) | Should -Contain "service principal secret 'on the SP', ends 2028-01-01 00:00:00 UTC"
        $changes[0].displayName | Should -BeExactly 'App renamed'
        @($changes.where({ $_.category -eq 'security' })).Count | Should -Be 10
        @($changes.where({ $_.category -eq 'configuration' })).Count | Should -Be 7
    }
    It 'not recorded: Azure roles that are not critical, indirect owners, changed labels of the same item' {
        $after = New-AppEntry
        $after.SPAzureRoleAssignments += [pscustomobject]@{ roleAssignmentId = 'az3'; roleName = 'Contributor'; roleIsCritical = $false; roleAssignmentApplicability = 'direct'; roleAssignmentAssignmentScope = '/subscriptions/s1/resourceGroups/rg' }
        $after.SPOwners += [pscustomobject]@{ id = 'u8'; displayName = 'Indirect two'; principalType = 'User (Member)'; applicability = 'indirect' }
        $after.SPAppRoleAssignments[0].AppRolePermissionSensitivity = 'critical'   #new classification of the same assignment
        (Get-Changes @(New-AppEntry) @($after)).Count | Should -Be 0
    }
    It 'objects added and removed; Microsoft first-party apps and certificate rotation of managed identities are not recorded' {
        $changes = Get-Changes @((New-AppEntry -Id 'old' -Name 'Old app'), (New-MsEntry 'ms1'), (New-MiEntry 'mi1' 'kA')) @((New-AppEntry -Id 'new' -Name 'New app'), (New-MsEntry 'ms2'), (New-MiEntry 'mi1' 'kB'))
        @($changes | ForEach-Object { "$($_.change)|$($_.displayName)" } | Sort-Object) | Should -Be @('object added|New app', 'object removed|Old app')
        @($changes.where({ $_.change -eq 'object added' }))[0].detail | Should -BeExactly 'SP APP INT, effective privilege high'
    }
    It 'R-1 delegated grants: compared by consent kind (all users / single users), not per user' {
        $userGrant = { param($principal) [pscustomobject]@{ id = 'scope-mail'; SPId = 'graph'; scope = 'Mail.Read'; permission = 'Mail.Read'; SPDisplayName = 'Microsoft Graph'; type = 'User'; consentType = 'Principal'; principalId = $principal; grantId = "g-$principal"; permissionSensitivity = 'critical' } }
        $before = New-AppEntry; $before.SPOauth2PermissionGrants = @(& $userGrant 'u1')
        #another user consents: not a change
        $after = New-AppEntry; $after.SPOauth2PermissionGrants = @((& $userGrant 'u1'), (& $userGrant 'u2'))
        (Get-Changes @($before) @($after)).Count | Should -Be 0
        #the same scope granted for all users while the user grants stay: recorded
        $tenantWide = [pscustomobject]@{ id = 'scope-mail'; SPId = 'graph'; scope = 'Mail.Read'; permission = 'Mail.Read'; SPDisplayName = 'Microsoft Graph'; type = 'User'; consentType = 'AllPrincipals'; principalId = $null; grantId = 'g-all'; permissionSensitivity = 'critical' }
        $after2 = New-AppEntry; $after2.SPOauth2PermissionGrants = @((& $userGrant 'u1'), $tenantWide)
        $changes = Get-Changes @($before) @($after2)
        $changes.change | Should -Be @('delegated permission granted')
        $changes[0].detail | Should -BeExactly 'Mail.Read (Microsoft Graph, all users), critical'
        #the tenant-wide grant removed while a user grant remains: recorded
        (Get-Changes @($after2) @($before)).change | Should -Be @('delegated permission removed')
    }
    It 'Azure role conditions: a narrowed or widened condition of a role that can assign roles is recorded' {
        $before = New-AppEntry
        $before.SPAzureRoleAssignments[0] | Add-Member -NotePropertyMembers @{ roleConditionEffect = 'limited'; roleConditionSources = 'role assignment'; roleConditionAssignableRoles = 'only Reader'; roleConditionCriticalRoles = '' }
        $before.SPAzureRoleAssignments[0].roleIsCritical = $false
        $after = New-AppEntry
        $after.SPAzureRoleAssignments[0] | Add-Member -NotePropertyMembers @{ roleConditionEffect = 'allows critical'; roleConditionSources = 'role assignment'; roleConditionAssignableRoles = 'only Owner, Reader'; roleConditionCriticalRoles = 'Owner' }
        $changes = Get-Changes @($before) @($after)
        $changes.change | Should -Contain 'Azure role condition changed'
        ($changes.where({ $_.change -eq 'Azure role condition changed' }))[0].detail | Should -Match 'limited by role assignment condition: can assign only Reader -> .*role assignment condition allows Owner'
        #the role became critical
        $changes.change | Should -Contain 'critical Azure role added'
    }
    It 'a new script version: the effective privilege change gets a note' {
        $after = New-AppEntry; $after.SPEffectivePrivilege.level = 'critical'
        (Get-Changes @(New-AppEntry) @($after) -VersionChanged $true)[0].note | Should -BeExactly 'script version changed (classification may differ)'
        (Get-Changes @(New-AppEntry) @($after))[0].note | Should -BeNullOrEmpty
    }
}

Describe 'log and state' {
    It 'first run: baseline created, state written, one log line' {
        $run = New-Run (Join-Path $TestDrive 'first')
        $records = Invoke-Update $run @(New-AppEntry) $script:Scope
        $records.change | Should -Be @('baseline created')
        Test-Path $run.StatePath | Should -BeTrue
        @(Get-Content $run.LogPath).Count | Should -Be 1
    }
    It 'next run: changes appended after the previous lines; the state is the new one' {
        $run = New-Run (Join-Path $TestDrive 'next')
        $null = Invoke-Update $run @(New-AppEntry) $script:Scope
        $after = New-AppEntry; $after.APPPasswordCredentials += [pscustomobject]@{ keyId = 'k2'; displayName = 'new'; endDateTimeFormated = 'x' }
        $records = Invoke-Update $run @($after) $script:Scope -Time '2026-11-08T10:00:00.0000000Z'
        $records.change | Should -Be @('credential added', 'compared')
        $records[-1].detail | Should -Match '^1 changes since the run of 2026-11-07 10:00:00 UTC \(v1\)$'
        $lines = @(Get-Content $run.LogPath)
        $lines.Count | Should -Be 3
        ($lines[1] | ConvertFrom-Json).change | Should -BeExactly 'credential added'
        #the same state again: no change
        (Invoke-Update $run @($after) $script:Scope -Time '2026-11-09T10:00:00.0000000Z').change | Should -Be @('compared')
    }
    It 'another scope: baseline reset with the reason, no comparison' {
        $run = New-Run (Join-Path $TestDrive 'scope')
        $null = Invoke-Update $run @(New-AppEntry) $script:Scope
        $otherScope = getChangeHistoryScope -TenantId $script:Tenant -ManagementGroupIds @('mg1', 'mg2') -SubscriptionQuotaIdWhitelist @('undefined') -NoAzureResourceSideRelations $false -OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes $false
        $records = Invoke-Update $run @(New-AppEntry -Id 'x') $otherScope
        $records.change | Should -Be @('baseline reset')
        $records[0].detail | Should -Match "managementGroups: 'mg1' -> 'mg1,mg2'"
        (Get-Content $run.StatePath -Raw | ConvertFrom-Json -AsHashtable).objects.Keys | Should -Be @('x')
    }
    It 'incomplete run: comparison skipped, the previous state is kept' {
        $run = New-Run (Join-Path $TestDrive 'incomplete')
        $null = Invoke-Update $run @(New-AppEntry) $script:Scope
        $stateBefore = Get-Content $run.StatePath -Raw
        $records = Invoke-Update $run @() $script:Scope -Incomplete $true
        $records.change | Should -Be @('comparison skipped')
        Get-Content $run.StatePath -Raw | Should -BeExactly $stateBefore
    }
    It 'unreadable state or another format version: baseline reset' {
        $run = New-Run (Join-Path $TestDrive 'corrupt')
        Set-Content -LiteralPath $run.StatePath -Value '{ not json'
        (Invoke-Update $run @(New-AppEntry) $script:Scope).change | Should -Be @('baseline reset')
        $run2 = New-Run (Join-Path $TestDrive 'format')
        Set-Content -LiteralPath $run2.StatePath -Value '{"formatVersion":0,"scope":{},"objects":{}}'
        $records = Invoke-Update $run2 @(New-AppEntry) $script:Scope
        $records.change | Should -Be @('baseline reset')
        $records[0].detail | Should -Match "state format changed \(0 -> $($script:changeHistoryFormatVersion)\)"
    }
}

Describe 'R-9 collection errors: previous state kept for what is incomplete' {
    It 'a failed object keeps its previous state (no false removed), the other objects are compared' {
        $run = New-Run (Join-Path $TestDrive 'carry-object')
        $null = Invoke-Update $run @((New-AppEntry -Id 'a' -Name 'A'), (New-AppEntry -Id 'b' -Name 'B')) $script:Scope
        #b failed in this run (missing from the output), a changed
        $a = New-AppEntry -Id 'a' -Name 'A'; $a.APPPasswordCredentials += [pscustomobject]@{ keyId = 'k2'; displayName = 'new'; endDateTimeFormated = 'x' }
        $records = @(updateChangeHistory -StatePath $run.StatePath -LogPath $run.LogPath -Scope $script:Scope -CurrentState (getChangeHistoryState -entries @($a)) -Version 'v1' -Incomplete $false -Time '2026-11-08T10:00:00.0000000Z' -CarryForwardObjects @{ b = 'ServicePrincipal enrichment: x' })
        $records.change | Should -Be @('credential added', 'compared')
        $records[-1].detail | Should -Match 'not compared \(previous state kept, reported by the next complete run\): 1 objects with collection errors'
        #b is still in the state: when it is back, nothing is reported for it
        (Get-Content $run.StatePath -Raw | ConvertFrom-Json -AsHashtable).objects.Keys | Should -Contain 'b'
        (Invoke-Update $run @($a, (New-AppEntry -Id 'b' -Name 'B')) $script:Scope -Time '2026-11-09T10:00:00.0000000Z').change | Should -Be @('compared')
    }
    It 'a failed subscription: Azure roles and effective privilege keep their previous values for all objects' {
        $run = New-Run (Join-Path $TestDrive 'carry-relation')
        $null = Invoke-Update $run @(New-AppEntry) $script:Scope
        $after = New-AppEntry; $after.SPAzureRoleAssignments = @(); $after.SPEffectivePrivilege.level = 'none'; $after.SPGroupMemberships += [pscustomobject]@{ ObjectId = 'grp2'; DisplayName = 'Group two' }
        $records = @(updateChangeHistory -StatePath $run.StatePath -LogPath $run.LogPath -Scope $script:Scope -CurrentState (getChangeHistoryState -entries @($after)) -Version 'v1' -Incomplete $false -Time '2026-11-08T10:00:00.0000000Z' -CarryForwardRelations @('azureRoles', 'level'))
        $records.change | Should -Be @('group membership added', 'compared')
        $records[-1].detail | Should -Match 'azureRoles, level of all objects'
    }
}

Describe 'R-11 reading only the period of a long log' {
    BeforeAll {
        function New-Log([string]$Path, [int]$Count, [string[]]$Insert = @(), [switch]$Bom) {
            $start = [datetime]::new(2026, 1, 1, 0, 0, 0, [DateTimeKind]::Utc)
            $lines = [System.Collections.Generic.List[string]]::new()
            for ($i = 0; $i -lt $Count; $i++) {
                $lines.Add('{"time":"' + $start.AddHours($i).ToString('o') + '","category":"security","change":"c' + $i + '","objectId":"o","objectType":"t","displayName":"d","detail":"x","note":null,"version":"v"}')
            }
            foreach ($item in $Insert) { $lines.Insert(10, $item) }
            [System.IO.File]::WriteAllLines($Path, $lines, [System.Text.UTF8Encoding]::new([bool]$Bom))
        }
        #1000 hourly lines from 2026-01-01; 'now' 2026-02-12 00:00 -> 30 days back = 2026-01-13 00:00 = line 288
        $script:Now = [datetime]::new(2026, 2, 12, 0, 0, 0, [DateTimeKind]::Utc)
        $script:SinceLine = '{"time":"' + $script:Now.AddDays(-30).ToString('o')
    }
    It 'the binary search finds the first line of the period; the rows are the same as reading every line' {
        $log = Join-Path $TestDrive 'long.jsonl'
        New-Log $log 1000
        $offset = getChangeHistoryLogOffset -LogPath $log -SinceLine $script:SinceLine
        $lines = [System.IO.File]::ReadAllLines($log)
        #WriteAllLines ends the lines with the platform's line break: CRLF on Windows, LF on Linux (CI, pipelines)
        $expectedOffset = [System.Text.Encoding]::UTF8.GetByteCount((($lines[0..287] -join [Environment]::NewLine) + [Environment]::NewLine))
        $offset | Should -Be $expectedOffset
        $rows = @(getChangeHistoryRows -LogPath $log -Days 30 -NowUtc $script:Now)
        $rows.Count | Should -Be 712
        $rows[-1].Change | Should -Be 'c288'
        $rows[0].Change | Should -Be 'c999'
    }
    It 'a broken line among the old lines only makes the search start earlier; a BOM is fine' {
        $log = Join-Path $TestDrive 'broken.jsonl'
        New-Log $log 1000 -Insert @('not json', '{}') -Bom
        $rows = @(getChangeHistoryRows -LogPath $log -Days 30 -NowUtc $script:Now)
        $rows.Count | Should -Be 712
    }
    It 'nothing in the period; an empty log' {
        $log = Join-Path $TestDrive 'old.jsonl'
        New-Log $log 10
        @(getChangeHistoryRows -LogPath $log -Days 30 -NowUtc $script:Now).Count | Should -Be 0
        $empty = Join-Path $TestDrive 'empty.jsonl'
        [System.IO.File]::WriteAllText($empty, '')
        @(getChangeHistoryRows -LogPath $empty -Days 30 -NowUtc $script:Now).Count | Should -Be 0
    }
}

Describe 'R-25 state file of another shape' {
    It '<Name>: baseline reset, the state is written again (no error on the next run)' -ForEach @(
        @{ Name = 'objects not a dictionary'; Content = '{"formatVersion":3,"time":"2026-11-07T10:00:00Z","scope":{},"objects":[1,2]}' }
        @{ Name = 'no scope'; Content = '{"formatVersion":3,"time":"2026-11-07T10:00:00Z","objects":{}}' }
        @{ Name = 'no time'; Content = '{"formatVersion":3,"scope":{},"objects":{}}' }
        @{ Name = 'an array'; Content = '[1,2,3]' }
    ) {
        $run = New-Run (Join-Path $TestDrive "shape-$($Name -replace '\W', '')")
        Set-Content -LiteralPath $run.StatePath -Value $Content
        $records = Invoke-Update $run @(New-AppEntry) $script:Scope
        $records.change | Should -Be @('baseline reset')
        $records[0].detail | Should -Match 'unexpected content'
        (Invoke-Update $run @(New-AppEntry) $script:Scope -Time '2026-11-08T10:00:00.0000000Z').change | Should -Be @('compared')
        Test-Path -LiteralPath "$($run.StatePath).tmp" | Should -BeFalse
    }
}

Describe 'report rows' {
    It 'the records of the last n days, newest first; lines that are not JSON are skipped' {
        $log = Join-Path $TestDrive 'rows.jsonl'
        Set-Content -LiteralPath $log -Value @(
            '{"time":"2026-10-01T10:00:00.0000000Z","version":"v1","category":"security","change":"old","objectId":"a","objectType":"SP APP INT","displayName":"A","detail":"d","note":null}'
            'not json'
            '{"time":"2026-11-06T10:00:00.0000000Z","version":"v1","category":"security","change":"owner added","objectId":"a","objectType":"SP APP INT","displayName":"A","detail":"d","note":null}'
            '{"time":"2026-11-07T10:00:00.0000000Z","version":"v2","category":"run","change":"compared","objectId":"","objectType":"","displayName":"","detail":"1 changes","note":null}'
        )
        $rows = getChangeHistoryRows -LogPath $log -Days 30 -NowUtc ([datetime]::new(2026, 11, 7, 12, 0, 0, [System.DateTimeKind]::Utc))
        $rows.Change | Should -Be @('compared', 'owner added')
        $rows[1].Time | Should -BeExactly '2026-11-06 10:00:00 UTC'
        @(getChangeHistoryRows -LogPath (Join-Path $TestDrive 'missing.jsonl') -Days 30 -NowUtc ([datetime]::UtcNow)).Count | Should -Be 0
    }
    It 'R-10: JSON lines without a valid time are skipped and counted, never thrown' {
        $log = Join-Path $TestDrive 'broken.jsonl'
        Set-Content -LiteralPath $log -Value @(
            '{}'
            'null'
            '[1,2]'
            '"text"'
            '{"time":"","change":"empty time"}'
            '{"time":"not a date","change":"bad time"}'
            '{"change":"no time"}'
            '{"time":"2026-11-06T10:00:00.0000000Z","category":"security","change":"owner added","objectId":"a"}'
        )
        $rows = @(getChangeHistoryRows -LogPath $log -Days 30 -NowUtc ([datetime]::new(2026, 11, 7, 12, 0, 0, [System.DateTimeKind]::Utc)))
        $rows.Change | Should -Be @('owner added')
        $script:changeHistoryUnreadLines | Should -Be 7
    }
    It 'R-10: the report reads the history inside a try and shows a note' {
        $script:Source | Should -Match '(?s)try \{\s+\$changeHistoryRows = @\(getChangeHistoryRows'
        #review F-35: as a warning of the section (also shown without rows) and on the overview tile
        $script:Source | Should -Match '-Warning \$changeHistoryReadIssue -Rows \$changeHistoryRows'
        $script:Source | Should -Match '-changeHistoryIssue \$changeHistoryReadIssue -changeHistoryNotRead \$changeHistoryNotRead'
        $script:Source | Should -Match '\$changeHistoryUpdateIssue = "this run was not added to the change history: '
    }
}

Describe 'Fable review: change history robustness' {
    It 'F-5: a log that cannot be appended leaves the previous state - the next run reports the change' {
        $run = New-Run (Join-Path $TestDrive 'f5')
        $null = Invoke-Update $run @(New-AppEntry) $script:Scope
        $stateBefore = Get-Content $run.StatePath -Raw
        $after = New-AppEntry; $after.APPPasswordCredentials += [pscustomobject]@{ keyId = 'k2'; displayName = 'new'; endDateTimeFormated = 'x' }
        $lock = [System.IO.File]::Open($run.LogPath, [System.IO.FileMode]::Open, [System.IO.FileAccess]::ReadWrite, [System.IO.FileShare]::None)
        try {
            { Invoke-Update $run @($after) $script:Scope -Time '2026-11-08T10:00:00.0000000Z' } | Should -Throw
        }
        finally {
            $lock.Dispose()
        }
        Get-Content $run.StatePath -Raw | Should -BeExactly $stateBefore
        (Invoke-Update $run @($after) $script:Scope -Time '2026-11-09T10:00:00.0000000Z').change | Should -Be @('credential added', 'compared')
    }
    It 'F-27: a last line without line end (stopped run) does not swallow the next record' {
        $run = New-Run (Join-Path $TestDrive 'f27')
        $null = Invoke-Update $run @(New-AppEntry) $script:Scope
        [System.IO.File]::AppendAllText($run.LogPath, '{"time":"2026-11-07T11:00:00.0000000Z","categ')
        $after = New-AppEntry; $after.APPPasswordCredentials += [pscustomobject]@{ keyId = 'k2'; displayName = 'new'; endDateTimeFormated = 'x' }
        $null = Invoke-Update $run @($after) $script:Scope -Time '2026-11-08T10:00:00.0000000Z'
        $rows = @(getChangeHistoryRows -LogPath $run.LogPath -Days 30 -NowUtc ([datetime]::new(2026, 11, 9, 0, 0, 0, [System.DateTimeKind]::Utc)))
        $rows.Change | Should -Contain 'credential added'
        $script:changeHistoryUnreadLines | Should -Be 1
    }
    It 'F-26: keys that differ only in case are the same (the previous state is read case-sensitively)' {
        $before = New-AppEntry; $before.SPAzureRoleAssignments[0].roleAssignmentId = '/subscriptions/s1/resourceGroups/RG/providers/Microsoft.Authorization/roleAssignments/AZ1'
        $after = New-AppEntry; $after.SPAzureRoleAssignments[0].roleAssignmentId = '/subscriptions/s1/resourcegroups/rg/providers/Microsoft.Authorization/roleAssignments/az1'
        (Get-Changes @($before) @($after)).Count | Should -Be 0
        #through updateChangeHistory (the previous objects are read from the file)
        $run = New-Run (Join-Path $TestDrive 'f26')
        $null = Invoke-Update $run @($before) $script:Scope
        (Invoke-Update $run @($after) $script:Scope -Time '2026-11-08T10:00:00.0000000Z').change | Should -Be @('compared')
    }
}

Describe 'wiring' {
    It 'on by default; ChangeHistoryDays 30; the history files are named after the scope, without timestamp' {
        $script:Source | Should -Match '\[switch\]\$NoChangeHistory,'
        $script:Source | Should -Match '\[ValidateRange\(1, 3650\)\]\[int\]\$ChangeHistoryDays = 30'
        $script:Source | Should -Match '_\$\(\$fileNameMGRef\)_ChangeHistory\.jsonl"'
        $script:Source | Should -Match '_\$\(\$fileNameMGRef\)_ChangeHistoryState\.json"'
    }
    It 'compares after the JSON export and before the report; incomplete = collection errors; never fails the run' {
        #the JSON export (its failure list - the former marker '$entry | ConvertTo-Json -Depth 99 | Set-Content' no longer exists)
        $iJson = $script:Source.IndexOf('$jsonExportFailures = [System.Collections.ArrayList]::Synchronized(')
        $iHistory = $script:Source.IndexOf('$changeHistoryRunRecords = @(updateChangeHistory')
        #LF or CRLF: the line endings depend on the checkout (CI: LF)
        $summaryMatch = [regex]::Match($script:Source, '(?m)^summary\r?$')
        $summaryMatch.Success | Should -BeTrue
        $iSummary = $summaryMatch.Index
        $iJson | Should -Not -Be -1
        $iHistory | Should -Not -Be -1
        $iJson | Should -BeLessThan $iHistory
        $iHistory | Should -BeLessThan $iSummary
        $script:Source | Should -Match '-CarryForwardObjects \$changeHistoryCarryForwardObjects -CarryForwardRelations \$changeHistoryCarryForwardRelations'
        $script:Source | Should -Match 'the change history must not fail the run'
    }
    It 'report: page Changes, section ChangeHistory, overview row' {
        $script:Source | Should -Match "@\{ id = 'Changes'; title = 'Changes'"
        $script:Source | Should -Match "@\{ id = 'ChangeHistory'; page = 'Changes'"
        @([regex]::Matches($script:Source, "addReportSection -Region 'ChangeHistory'")).Count | Should -Be 2
        $script:Source | Should -Match 'finding = "Changes in the last \$changeHistoryDays days"'
    }
}
