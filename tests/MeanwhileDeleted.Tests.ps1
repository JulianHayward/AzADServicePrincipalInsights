#Pester 5+ tests for objects deleted while the script runs (GitHub issue #43, review items P0-5, P0-6, P0-7, P1-12).
#Runs real blocks of the main script in-process against a fake AzAPICall; no tenant needed.
#Each fix is also run against the original code (commit d332d98) to prove the test catches the bug.

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:OriginalScriptPath = Get-MainScriptAtCommit -Commit 'd332d98'

    #fake AzAPICall: answers by URI from $global:AzADSPITestFakeResponses (key = URI suffix)
    function global:AzAPICall {
        param($AzAPICallConfiguration, $uri, $method, $currentTask, $listenOn, $consistencyLevel, $noPaging, $skipOnErrorCode)
        foreach ($key in $global:AzADSPITestFakeResponses.Keys) {
            if ($uri.EndsWith($key)) { return $global:AzADSPITestFakeResponses[$key] }
        }
        return @()
    }

    function Invoke-AppRoleAssignmentsBlock {
        param([string]$Path, [ValidateSet('User', 'Group')][string]$Kind, [string[]]$ObjectIds)
        $marker = if ($Kind -eq 'User') { 'getUser AppRoleAssignments' } else { 'getGroup AppRoleAssignments' }
        $block = Get-ParallelBlock -Path $Path -Marker $marker
        $azAPICallConf = @{ azAPIEndpointUrls = @{ MicrosoftGraph = 'https://graph.microsoft.com' } }
        $ScriptPath = 'pwsh'
        $htUsersAndGroupsAppRoleAssignmentsUser = [hashtable]::Synchronized(@{})
        $htUsersAndGroupsAppRoleAssignmentsGroup = [hashtable]::Synchronized(@{})
        $ErrorActionPreference = 'Stop'
        $null = [pscustomobject]@{ Group = $ObjectIds } | ForEach-Object -Process $block.ScriptBlock 6>$null
        if ($Kind -eq 'User') { $htUsersAndGroupsAppRoleAssignmentsUser } else { $htUsersAndGroupsAppRoleAssignmentsGroup }
    }
}

BeforeAll {
    #runs the SP collection parallel block for one application without SP
    function Invoke-SPCollectionForAppOnly {
        param([string]$Path)
        $block = Get-ParallelBlock -Path $Path -Marker 'AppWithoutSP_'
        #every $using: variable becomes a fresh synchronized hashtable unless set below
        foreach ($name in $block.UsingNames) { Set-Variable -Name $name -Value ([hashtable]::Synchronized(@{})) }
        $skipApplicationRequests = $false   #R-22 flag (not a hashtable)
        #no tenant-wide prefetch: these tests exercise the per-SP requests (an empty hashtable would count as an available index)
        $htApplicationsByAppId = $null; $htAADRoleAssignmentsByPrincipalId = $null; $htAADRoleAssignmentsByResourceScope = $null; $htOauth2PermissionGrantsByClientId = $null
        $azAPICallConf = @{ azAPIEndpointUrls = @{ MicrosoftGraph = 'https://graph.microsoft.com' }; checkContext = @{ Tenant = @{ Id = 'tenant' } } }
        $indicator = 100
        $ErrorActionPreference = 'Stop'
        $item = [pscustomobject]@{ SPOrAppWithoutSP = 'AppOnly'; Details = [pscustomobject]@{ id = 'app1'; appId = 'appid1'; displayName = 'App 1' } }
        $null = [pscustomobject]@{ Group = @($item) } | ForEach-Object -Process $block.ScriptBlock 6>$null
        [pscustomobject]@{ Enriched = $htServicePrincipalsAndAppsOnlyEnriched; MeanwhileDeleted = $htMeanwhileDeleted; AppOwners = $htAppOwners; FICs = $htFederatedIdentityCredentials; DataIncomplete = $htDataIncomplete }
    }
}

AfterAll {
    Remove-Item function:global:AzAPICall -ErrorAction SilentlyContinue
    Remove-Variable -Name AzADSPITestFakeResponses -Scope Global -ErrorAction SilentlyContinue
}

Describe 'P0-7 AppRoleAssignments for users and groups deleted mid-run (issue #43)' {
    BeforeEach {
        $global:AzADSPITestFakeResponses = @{
            '/deleted/appRoleAssignments' = 'Request_ResourceNotFound'
            '/ok/appRoleAssignments' = @([pscustomobject]@{ id = 'a1'; appRoleId = 'r1' }, [pscustomobject]@{ id = 'a2'; appRoleId = 'r2' })
            '/nullid/appRoleAssignments' = @([pscustomobject]@{ id = $null; appRoleId = 'r3' })
            '/empty/appRoleAssignments' = @()
        }
    }

    #root fix (review P0-7 option 3 / P1-19): the per-user and per-group appRoleAssignments requests no longer exist,
    #the appRoleId is taken from the appRoleAssignedTo result; so a principal deleted mid-run cannot crash this step anymore
    It 'main script no longer requests /users/{id}/appRoleAssignments or /groups/{id}/appRoleAssignments' {
        $text = Get-Content -LiteralPath $script:MainScriptPath -Raw
        $text | Should -Not -Match '/(users|Groups|groups)/\$\(\$\w+\)/appRoleAssignments'
        $text | Should -Not -Match 'getUser AppRoleAssignments|getGroup AppRoleAssignments'
    }

    It 'main script takes the appRoleId of User/Group principals from the appRoleAssignedTo record' {
        Get-Content -LiteralPath $script:MainScriptPath -Raw | Should -Match '\$appRoleId = \$servicePrincipalAppRoleAssignedTo\.appRoleId'
    }

    It '<Kind>: original code d332d98 throws the null-key error from issue #43' -ForEach @(@{ Kind = 'User' }, @{ Kind = 'Group' }) {
        { Invoke-AppRoleAssignmentsBlock -Path $script:OriginalScriptPath -Kind $Kind -ObjectIds 'deleted', 'ok' } |
            Should -Throw '*null key is not allowed*'
    }
}

Describe 'P0-6 application without SP (AppOnly) deleted mid-run' {

    It 'appId filter returns nothing: entry AppWithoutSP_app1 is flagged MeanWhileDeleted' {
        $global:AzADSPITestFakeResponses = @{ "/v1.0/applications?`$filter=appId eq 'appid1'" = @() }
        $r = Invoke-SPCollectionForAppOnly -Path $script:MainScriptPath
        $r.MeanwhileDeleted.ContainsKey('app1') | Should -BeTrue
        $r.Enriched['AppWithoutSP_app1']['MeanWhileDeleted'] | Should -BeTrue
        $r.Enriched.ContainsKey('app1') | Should -BeFalse
    }

    It 'appId lookup returns Request_ResourceNotFound: entry AppWithoutSP_app1 is flagged MeanWhileDeleted' {
        $global:AzADSPITestFakeResponses = @{ "/v1.0/applications?`$filter=appId eq 'appid1'" = 'Request_ResourceNotFound' }
        $r = Invoke-SPCollectionForAppOnly -Path $script:MainScriptPath
        $r.Enriched['AppWithoutSP_app1']['MeanWhileDeleted'] | Should -BeTrue
    }

    It 'original code d332d98: an empty result is not detected as deleted' {
        $global:AzADSPITestFakeResponses = @{ "/v1.0/applications?`$filter=appId eq 'appid1'" = @() }
        $r = Invoke-SPCollectionForAppOnly -Path $script:OriginalScriptPath
        $r.Enriched['AppWithoutSP_app1']['MeanWhileDeleted'] | Should -BeNullOrEmpty
    }

    It 'original code d332d98: Request_ResourceNotFound writes the flag to the wrong key and fails' {
        $global:AzADSPITestFakeResponses = @{ "/v1.0/applications?`$filter=appId eq 'appid1'" = 'Request_ResourceNotFound' }
        { Invoke-SPCollectionForAppOnly -Path $script:OriginalScriptPath } | Should -Throw "*property 'MeanWhileDeleted' cannot be found*"
    }
}

Describe 'R-22 an error string from the application request' {
    It 'is not stored as the application (was: null key, the run stopped); the object is marked as incomplete' {
        $global:AzADSPITestFakeResponses = @{ "/v1.0/applications?`$filter=appId eq 'appid1'" = 'skipApplications' }
        { $script:R22 = Invoke-SPCollectionForAppOnly -Path $script:MainScriptPath } | Should -Not -Throw
        $script:R22.DataIncomplete['app1'] | Should -BeLike '*application not read (skipApplications)*'
        $script:R22.Enriched['AppWithoutSP_app1'].Application | Should -BeNullOrEmpty
    }
}

Describe 'P1-16 Request_ResourceNotFound from app sub-requests' {
    BeforeEach {
        #the application still exists for the appId lookup, then is gone for the owners / federatedIdentityCredentials request
        $app = [pscustomobject]@{ id = 'app1'; appId = 'appid1'; displayName = 'App 1'; keyCredentials = @(); passwordCredentials = @() }
        $script:AppLookup = @{ "/v1.0/applications?`$filter=appId eq 'appid1'" = @($app) }
    }

    It 'getAppOwner 404: object flagged meanwhile deleted, no fake owner stored' {
        $global:AzADSPITestFakeResponses = $script:AppLookup + @{ '/v1.0/applications/app1/owners' = 'Request_ResourceNotFound' }
        $r = Invoke-SPCollectionForAppOnly -Path $script:MainScriptPath
        $r.MeanwhileDeleted.ContainsKey('app1') | Should -BeTrue
        $r.AppOwners.ContainsKey('app1') | Should -BeFalse
    }

    It 'original code d332d98: getAppOwner 404 stored a fake owner with empty id' {
        $global:AzADSPITestFakeResponses = $script:AppLookup + @{ '/v1.0/applications/app1/owners' = 'Request_ResourceNotFound' }
        $r = Invoke-SPCollectionForAppOnly -Path $script:OriginalScriptPath
        $r.AppOwners.ContainsKey('app1') | Should -BeTrue
        $r.AppOwners['app1'].id | Should -BeNullOrEmpty
    }

    It 'federatedIdentityCredentials 404: object flagged meanwhile deleted, error string not stored' {
        $global:AzADSPITestFakeResponses = $script:AppLookup + @{ '/beta/applications/app1/federatedIdentityCredentials' = 'Request_ResourceNotFound' }
        $r = Invoke-SPCollectionForAppOnly -Path $script:MainScriptPath
        $r.MeanwhileDeleted.ContainsKey('app1') | Should -BeTrue
        $r.FICs.ContainsKey('app1') | Should -BeFalse
    }

    It 'original code d332d98: federatedIdentityCredentials 404 stored the error string as credentials' {
        $global:AzADSPITestFakeResponses = $script:AppLookup + @{ '/beta/applications/app1/federatedIdentityCredentials' = 'Request_ResourceNotFound' }
        $r = Invoke-SPCollectionForAppOnly -Path $script:OriginalScriptPath
        $r.FICs['app1'] | Should -Be 'Request_ResourceNotFound'
    }

    It 'no 404: owners are stored and nothing is flagged' {
        $global:AzADSPITestFakeResponses = $script:AppLookup + @{ '/v1.0/applications/app1/owners' = @([pscustomobject]@{ id = 'u1'; displayName = 'User 1'; '@odata.type' = '#microsoft.graph.user' }) }
        $r = Invoke-SPCollectionForAppOnly -Path $script:MainScriptPath
        $r.MeanwhileDeleted.ContainsKey('app1') | Should -BeFalse
        $r.AppOwners['app1'].id | Should -Be 'u1'
    }
}

Describe 'Static checks' {
    It 'P0-5: no hashtable literal uses a member expression as key (main script)' {
        $bad = Get-BadHashtableKeys -Path $script:MainScriptPath
        $bad | Should -BeNullOrEmpty
    }

    It 'P0-5: the check finds the broken literal in original code d332d98' {
        @(Get-BadHashtableKeys -Path $script:OriginalScriptPath).Count | Should -BeGreaterThan 0
    }

    It "P1-12: no 'break' outside a loop or switch (main script)" {
        Get-BreakOutsideLoop -Path $script:MainScriptPath | Should -BeNullOrEmpty
    }

    It "P1-12: the check finds the script-level 'break' in original code d332d98" {
        @(Get-BreakOutsideLoop -Path $script:OriginalScriptPath).Count | Should -BeGreaterThan 0
    }

    BeforeAll {
        function Get-BadHashtableKeys {
            param([string]$Path)
            $ast = Get-ScriptAst -Path $Path
            foreach ($ht in $ast.FindAll({ $args[0] -is [System.Management.Automation.Language.HashtableAst] }, $true)) {
                foreach ($kv in $ht.KeyValuePairs) {
                    if ($kv.Item1 -is [System.Management.Automation.Language.MemberExpressionAst]) { "line $($kv.Item1.Extent.StartLineNumber): $($kv.Item1.Extent.Text)" }
                }
            }
        }
        function Get-BreakOutsideLoop {
            param([string]$Path)
            $ast = Get-ScriptAst -Path $Path
            foreach ($br in $ast.FindAll({ $args[0] -is [System.Management.Automation.Language.BreakStatementAst] }, $true)) {
                $p = $br.Parent
                $inLoop = $false
                while ($p -and $p -isnot [System.Management.Automation.Language.ScriptBlockAst]) {
                    #(a 'break' in a trap is intended: it passes the error on)
                    if ($p -is [System.Management.Automation.Language.LoopStatementAst] -or $p -is [System.Management.Automation.Language.SwitchStatementAst] -or $p -is [System.Management.Automation.Language.TrapStatementAst]) { $inLoop = $true; break }
                    $p = $p.Parent
                }
                if (-not $inLoop) { "line $($br.Extent.StartLineNumber): $($br.Extent.Text)" }
            }
        }
    }
}
