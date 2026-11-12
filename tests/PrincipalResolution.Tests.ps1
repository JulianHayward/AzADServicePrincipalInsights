#Pester 5+ tests for the principal resolution (resolveObjectsById, 2026-11-07): beta getByIds with the types list as AzGovViz
#(issue Azure/Azure-Governance-Visualizer#66, foreign principals = directoryObjectPartnerReference) and the createdBy of Azure
#role assignments that cannot be resolved ('Unknown (deleted or not in this tenant)' instead of a bare GUID). No Graph calls.

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'resolveObjectsById' }, $true).Extent.Text))
    . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'getParallelBatches' }, $true).Extent.Text))

    function global:AzAPICall {
        param($AzAPICallConfiguration, $uri, $method, $body, $currentTask)
        $global:AzADSPITestRequests.Add(@{ uri = $uri; body = $body }) | Out-Null
        return $global:AzADSPITestResponse
    }
    function Invoke-Resolve([object[]]$Response, [string[]]$Ids) {
        $global:AzADSPITestRequests = [System.Collections.ArrayList]@()
        $global:AzADSPITestResponse = $Response
        $script:htPrincipalsResolved = @{}
        $azAPICallConf = @{ azAPIEndpointUrls = @{ MicrosoftGraph = 'https://graph.microsoft.com' } }
        resolveObjectsById -objects $Ids -targetHt 'htPrincipalsResolved' 6>$null
        $script:htPrincipalsResolved
    }
}

AfterAll {
    Remove-Item function:global:AzAPICall -ErrorAction SilentlyContinue
    Remove-Variable -Name AzADSPITestRequests, AzADSPITestResponse -Scope Global -ErrorAction SilentlyContinue
}

Describe 'resolveObjectsById' {
    It 'requests beta getByIds with the types list incl. foreign principals (as AzGovViz)' {
        $null = Invoke-Resolve -Response @() -Ids @('a')
        $request = $global:AzADSPITestRequests[0]
        $request.uri | Should -Match '/beta/directoryObjects/getByIds\?\$select=id,displayName,userType,objectType,externalPartnerTenantId$'
        ($request.body | ConvertFrom-Json).types | Should -Be @('user', 'group', 'servicePrincipal', 'application', 'device', 'directoryObjectPartnerReference')
        ($request.body | ConvertFrom-Json).ids | Should -Be @('a')
    }
    It 'labels <Case>' -ForEach @(
        @{ Case = 'a foreign group with its partner tenant'; Object = @{ '@odata.type' = '#microsoft.graph.directoryObjectPartnerReference'; id = 'f1'; displayName = 'Partner AdminAgents'; objectType = 'Group'; externalPartnerTenantId = 'pt1' }; TypeOnly = 'Foreign Group'; Full = 'Foreign Group, DisplayName: Partner AdminAgents, Id: f1, PartnerTenantId: pt1' }
        @{ Case = 'a foreign principal without objectType'; Object = @{ '@odata.type' = '#microsoft.graph.directoryObjectPartnerReference'; id = 'f2'; displayName = 'x' }; TypeOnly = 'Foreign principal'; Full = 'Foreign principal, DisplayName: x, Id: f2' }
        @{ Case = 'a member user'; Object = @{ '@odata.type' = '#microsoft.graph.user'; id = 'u1'; displayName = 'U'; userType = 'Member' }; TypeOnly = 'User (Member)'; Full = 'User (Member), DisplayName: U, Id: u1' }
        @{ Case = 'a user without userType'; Object = @{ '@odata.type' = '#microsoft.graph.user'; id = 'u2'; displayName = 'U2' }; TypeOnly = 'User (userType not set)'; Full = 'User (userType not set), DisplayName: U2, Id: u2' }
        @{ Case = 'a service principal'; Object = @{ '@odata.type' = '#microsoft.graph.servicePrincipal'; id = 's1'; displayName = 'S' }; TypeOnly = 'Serviceprincipal'; Full = 'Serviceprincipal, DisplayName: S, Id: s1' }
        @{ Case = 'a device'; Object = @{ '@odata.type' = '#microsoft.graph.device'; id = 'd1'; displayName = 'D' }; TypeOnly = 'Device'; Full = 'Device, DisplayName: D, Id: d1' }
    ) {
        $resolved = Invoke-Resolve -Response @([pscustomobject]$Object) -Ids @($Object.id)
        $resolved[$Object.id].typeOnly | Should -BeExactly $TypeOnly
        $resolved[$Object.id].full | Should -BeExactly $Full
    }
}

Describe 'createdBy of Azure role assignments' {
    It 'a GUID that cannot be resolved is labelled, not left as a bare GUID' {
        $script:Source | Should -Match "elseif \(\[guid\]::TryParse\(\`$assignmentCreatedBy, \`$\(\[ref\]\[guid\]::Empty\)\)\) \{\s+(#[^\r\n]*\s+)*\`$assignmentProperties\.createdBy = ""Unknown \(deleted or not in this tenant\), Id: \`$\(\`$assignmentCreatedBy\)"""
    }
}
