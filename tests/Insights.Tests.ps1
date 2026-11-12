#Pester 5+ tests for the insight helpers (themes 23/24): derived from the collected data, no API calls.

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    foreach ($name in 'getCredentialExpiryInfo', 'getFederatedCredentialTrust', 'setAzureDevOpsConvertedFinding', 'getRedirectUriFindings', 'getSPRowBase', 'getEffectivePrivileges', 'getCredentialRiskRows', 'getThirdPartyExposureRows', 'getCleanupCandidateRows', 'getFindingsOverviewRows', 'getAgentObjectRows', 'getRiskExplanation') {
        . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name }, $true).Extent.Text))
    }
    . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -eq '$script:escalationAppRoles' }, $true).Extent.Text))
    . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -eq '$script:spiRiskExplanations' }, $true).Extent.Text))
}

Describe 'getFederatedCredentialTrust' {
    It '<Issuer> / <Subject> -> <Type>' -ForEach @(
        @{ Issuer = 'https://token.actions.githubusercontent.com'; Subject = 'repo:contoso/app:ref:refs/heads/main'; Type = 'GitHub Actions'; Trust = 'repository contoso/app, branch main'; Finding = '' }
        @{ Issuer = 'https://token.actions.githubusercontent.com'; Subject = 'repo:contoso/app:environment:prod'; Type = 'GitHub Actions'; Trust = 'repository contoso/app, environment prod'; Finding = '' }
        @{ Issuer = 'https://token.actions.githubusercontent.com'; Subject = 'repo:contoso/app:pull_request'; Type = 'GitHub Actions'; Trust = 'repository contoso/app, any pull request'; Finding = 'broad*' }
        #GitHub's immutable subject: owner and repository with their IDs
        @{ Issuer = 'https://token.actions.githubusercontent.com'; Subject = 'repo:contoso@5544123/app@821093847:ref:refs/heads/main'; Type = 'GitHub Actions'; Trust = 'repository contoso/app (owner ID 5544123, repository ID 821093847), branch main'; Finding = '' }
        @{ Issuer = 'https://token.actions.githubusercontent.com'; Subject = 'repo:contoso@5544123/app@821093847:environment:prod'; Type = 'GitHub Actions'; Trust = 'repository contoso/app (owner ID 5544123, repository ID 821093847), environment prod'; Finding = '' }
        @{ Issuer = 'https://token.actions.githubusercontent.com'; Subject = 'repo:contoso@5544123/app@821093847:pull_request'; Type = 'GitHub Actions'; Trust = 'repository contoso/app (owner ID 5544123, repository ID 821093847), any pull request'; Finding = 'broad*' }
        @{ Issuer = 'https://vstoken.dev.azure.com/01ce4a3c'; Subject = 'sc://org/proj/conn'; Type = 'Azure DevOps'; Trust = 'organization org, project proj, service connection conn'; Finding = 'deprecated Azure DevOps issuer (retirement 2027-07-01)*' }
        @{ Issuer = 'https://westeurope.oic.prod-aks.azure.com/t/i/'; Subject = 'system:serviceaccount:ns1:sa1'; Type = 'Kubernetes (AKS)'; Trust = 'namespace ns1, service account sa1'; Finding = '' }
        @{ Issuer = 'https://login.microsoftonline.com/tenant1/v2.0'; Subject = 'mi-object-id'; Type = 'Microsoft Entra ID*'; Trust = 'mi-object-id'; Finding = '' }
        @{ Issuer = 'https://idp.example.com'; Subject = 'x'; Type = 'Other OIDC issuer'; Trust = 'x'; Finding = 'custom issuer*' }
        #review F-4: the host must match exactly; look-alike issuers keep the custom-issuer finding
        @{ Issuer = 'https://oidc.prod-aks.azure.com/abc/'; Subject = 'system:serviceaccount:ns1:sa1'; Type = 'Kubernetes (AKS)'; Trust = 'namespace ns1, service account sa1'; Finding = '' }
        @{ Issuer = 'https://gitlab.com'; Subject = 'project_path:g/p'; Type = 'GitLab'; Trust = 'project_path:g/p'; Finding = '' }
        @{ Issuer = 'https://token.actions.githubusercontent.com.attacker.example'; Subject = 'repo:victim/repo:ref:refs/heads/main'; Type = 'Other OIDC issuer'; Trust = 'repo:victim/repo:ref:refs/heads/main'; Finding = 'custom issuer*' }
        @{ Issuer = 'https://accounts.google.com.attacker.example'; Subject = 'x'; Type = 'Other OIDC issuer'; Trust = 'x'; Finding = 'custom issuer*' }
        @{ Issuer = 'https://app.terraform.io.attacker.example'; Subject = 'x'; Type = 'Other OIDC issuer'; Trust = 'x'; Finding = 'custom issuer*' }
        @{ Issuer = 'https://app.vstoken.visualstudio.com.attacker.example'; Subject = 'x'; Type = 'Other OIDC issuer'; Trust = 'x'; Finding = 'custom issuer*' }
        @{ Issuer = 'https://attacker.example/oidc.prod-aks.azure.com/'; Subject = 'x'; Type = 'Other OIDC issuer'; Trust = 'x'; Finding = 'custom issuer*' }
        @{ Issuer = 'https://login.microsoftonline.com/x/v2.0.attacker.example/'; Subject = 'x'; Type = 'Other OIDC issuer'; Trust = 'x'; Finding = 'custom issuer*' }
        @{ Issuer = 'https://gitlab.attacker.example'; Subject = 'x'; Type = 'Other OIDC issuer'; Trust = 'x'; Finding = 'custom issuer*' }
    ) {
        $r = getFederatedCredentialTrust -issuer $Issuer -subject $Subject
        $r.issuerType | Should -BeLike $Type
        $r.trust | Should -Be $Trust
        $r.finding | Should -BeLike $Finding
    }
}

#https://aka.ms/azdo-issuer: the Azure DevOps issuer is retired on 2027-07-01 for single-tenant apps and managed identities in the
#Azure public cloud; multitenant apps and other clouds keep it
Describe 'deprecated Azure DevOps issuer' {
    It '<Issuer> audience <Audience> cloud <Cloud> -> deprecated <Deprecated>' -ForEach @(
        @{ Issuer = 'https://vstoken.dev.azure.com/01ce4a3c'; Audience = 'AzureADMyOrg'; Cloud = 'AzureCloud'; Deprecated = $true }
        @{ Issuer = 'https://vstoken.dev.azure.com/01ce4a3c'; Audience = ''; Cloud = 'AzureCloud'; Deprecated = $true }
        @{ Issuer = 'https://vstoken.dev.azure.com/01ce4a3c'; Audience = ''; Cloud = ''; Deprecated = $true }
        @{ Issuer = 'https://app.vstoken.visualstudio.com/01ce4a3c'; Audience = 'AzureADMyOrg'; Cloud = 'AzureCloud'; Deprecated = $true }
        @{ Issuer = 'https://vstoken.dev.azure.com/01ce4a3c'; Audience = 'AzureADMultipleOrgs'; Cloud = 'AzureCloud'; Deprecated = $false }
        @{ Issuer = 'https://vstoken.dev.azure.com/01ce4a3c'; Audience = 'AzureADandPersonalMicrosoftAccount'; Cloud = 'AzureCloud'; Deprecated = $false }
        @{ Issuer = 'https://vstoken.dev.azure.com/01ce4a3c'; Audience = 'AzureADMyOrg'; Cloud = 'AzureUSGovernment'; Deprecated = $false }
        @{ Issuer = 'https://vstoken.dev.azure.com/01ce4a3c'; Audience = 'AzureADMyOrg'; Cloud = 'AzureChinaCloud'; Deprecated = $false }
        @{ Issuer = 'https://login.microsoftonline.com/tenant1/v2.0'; Audience = 'AzureADMyOrg'; Cloud = 'AzureCloud'; Deprecated = $false }
        @{ Issuer = 'https://vstoken.dev.azure.com.attacker.example/x'; Audience = 'AzureADMyOrg'; Cloud = 'AzureCloud'; Deprecated = $false }
    ) {
        $r = getFederatedCredentialTrust -issuer $Issuer -subject 'sc://org/proj/conn' -signInAudience $Audience -cloudEnvironment $Cloud
        ($r.finding -like 'deprecated Azure DevOps issuer*') | Should -Be $Deprecated
    }
    It 'the findings overview counts the credentials with the deprecated issuer' {
        $insightRows = [ordered]@{ EffectivePrivilege = @(); CredentialRisk = @(); SPCredentials = @(); ThirdParty = @(); AppConfiguration = @(); Cleanup = @()
            FederatedCredentialTrust = @(
                [PSCustomObject]@{ Finding = 'deprecated Azure DevOps issuer (retirement 2027-07-01): convert' }
                [PSCustomObject]@{ Finding = 'deprecated Azure DevOps issuer, converted: remove' }
                [PSCustomObject]@{ Finding = 'custom issuer: check who controls it' }
                [PSCustomObject]@{ Finding = '' })
        }
        $row = @(getFindingsOverviewRows -insightRows $insightRows).where({ $_.finding -eq 'Federated identity credentials with a finding' })[0]
        $row.count | Should -Be 3
        $row.detail | Should -Be '4 federated identity credentials; 2 with the deprecated Azure DevOps issuer (retirement 2027-07-01; 1 already converted)'
    }
}

#the conversion in Azure DevOps adds a credential with the Microsoft Entra issuer and keeps the old one (test tenant 2026-11-10)
Describe 'Azure DevOps service connection converted to the Microsoft Entra issuer' {
    BeforeAll {
        #/eid1/c/pub/t/<tenant base64url>/a/<Azure DevOps app id 499b84ac-1321-427f-aa17-267ca6975798 base64url>/sc/<organization id>/<service connection id>
        $script:EntraIssuer = 'https://login.microsoftonline.com/00000000-0000-0000-0000-000000000001/v2.0'
        $script:EntraSubject = '/eid1/c/pub/t/AQAAAAAAAAAAAAAAAAAAAA/a/rISbSSETf0KqFyZ8ppdXmA/sc/11111111-2222-3333-4444-555555555555/66666666-7777-8888-9999-000000000000'
    }
    It 'the new credential is classified as Azure DevOps with the organization and service connection ids' {
        $r = getFederatedCredentialTrust -issuer $script:EntraIssuer -subject $script:EntraSubject -signInAudience 'AzureADMyOrg' -cloudEnvironment 'AzureCloud'
        $r.issuerType | Should -BeExactly 'Azure DevOps (Microsoft Entra issuer)'
        $r.trust | Should -BeExactly 'organization id 11111111-2222-3333-4444-555555555555, service connection id 66666666-7777-8888-9999-000000000000'
        $r.finding | Should -Be ''
    }
    It 'another app in the subject stays a plain Microsoft Entra ID credential' {
        $r = getFederatedCredentialTrust -issuer $script:EntraIssuer -subject $script:EntraSubject.Replace('rISbSSETf0KqFyZ8ppdXmA', 'AAAAAAAAAAAAAAAAAAAAAA')
        $r.issuerType | Should -BeLike 'Microsoft Entra ID*'
        $r.trust | Should -Be $script:EntraSubject.Replace('rISbSSETf0KqFyZ8ppdXmA', 'AAAAAAAAAAAAAAAAAAAAAA')
    }
    It 'the same subject with another issuer is not Azure DevOps' {
        (getFederatedCredentialTrust -issuer 'https://idp.example.com' -subject $script:EntraSubject).issuerType | Should -Be 'Other OIDC issuer'
    }
    It 'the old credential of an identity that also has the new one gets the converted finding' {
        $trusts = @(
            getFederatedCredentialTrust -issuer 'https://vstoken.dev.azure.com/01ce4a3c' -subject 'sc://org/proj/conn' -signInAudience 'AzureADMyOrg' -cloudEnvironment 'AzureCloud'
            getFederatedCredentialTrust -issuer $script:EntraIssuer -subject $script:EntraSubject -signInAudience 'AzureADMyOrg' -cloudEnvironment 'AzureCloud'
            getFederatedCredentialTrust -issuer 'https://token.actions.githubusercontent.com' -subject 'repo:contoso/app:pull_request')
        setAzureDevOpsConvertedFinding $trusts
        $trusts[0].finding | Should -BeLike 'deprecated Azure DevOps issuer, converted: *remove this credential if no other service connection still uses it*'
        $trusts[1].finding | Should -Be ''
        $trusts[2].finding | Should -BeLike 'broad*'
    }
    It 'without a new credential the deprecated finding stays' {
        $trusts = @(getFederatedCredentialTrust -issuer 'https://vstoken.dev.azure.com/01ce4a3c' -subject 'sc://org/proj/conn')
        setAzureDevOpsConvertedFinding $trusts
        $trusts[0].finding | Should -BeLike 'deprecated Azure DevOps issuer (retirement 2027-07-01)*'
        setAzureDevOpsConvertedFinding @()
    }
}

Describe 'getRedirectUriFindings' {
    It '<Platform> <Uri> -> <Expected>' -ForEach @(
        @{ Platform = 'web'; Uri = 'https://app.contoso.com/signin'; Expected = '' }
        @{ Platform = 'web'; Uri = 'http://app.contoso.com/signin'; Expected = 'high:redirect URI without https' } # DevSkim: ignore DS137138,DS162092
        @{ Platform = 'spa'; Uri = 'https://*.contoso.com'; Expected = 'high:wildcard redirect URI' }
        @{ Platform = 'web'; Uri = 'https://myapp.azurewebsites.net/.auth/login/aad/callback'; Expected = 'medium:Azure-hosted domain*' }
        #review F-29: a query or fragment right after the host
        @{ Platform = 'web'; Uri = 'https://myapp.azurewebsites.net?x=1'; Expected = 'medium:Azure-hosted domain*' }
        @{ Platform = 'web'; Uri = 'https://myapp.azurewebsites.net#a'; Expected = 'medium:Azure-hosted domain*' }
        @{ Platform = 'web'; Uri = 'http://localhost:8080'; Expected = 'info:localhost redirect URI on the web platform' } # DevSkim: ignore DS137138,DS162092
        @{ Platform = 'publicClient'; Uri = 'http://localhost'; Expected = '' } # DevSkim: ignore DS137138,DS162092
    ) {
        $r = @(getRedirectUriFindings -platform $Platform -uri $Uri)
        (($r | ForEach-Object { "$($_.severity):$($_.finding)" }) -join '|') | Should -BeLike $Expected
    }
    It 'returns a flat list (not a list inside a list)' {
        $r = @(getRedirectUriFindings -platform 'web' -uri 'http://*.contoso.com') # DevSkim: ignore DS137138,DS162092
        $r.Count | Should -Be 2
        $r[0].severity | Should -Be 'high'
    }
}

Describe 'getCredentialExpiryInfo' {
    It '<Days> days -> <Expected>' -ForEach @(
        @{ Days = -1; Expected = 'expired' }
        @{ Days = 5; Expected = 'expires soon (less than grace period 14 days)' }
        @{ Days = 100; Expected = 'expires in 14 to 730 days' }
        @{ Days = 800; Expected = 'expires in more than 730 days' }
    ) {
        getCredentialExpiryInfo -endDateTime ([datetime]::UtcNow.AddDays($Days)) -warningDays 14 -maxDays 730 | Should -Be $Expected
    }
}

Describe 'effective privilege, credential risk, third-party apps, cleanup (theme 24)' {
    BeforeAll {
        function New-Entry([string]$Id, [string]$Type = 'SP APP INT', [hashtable]$Props = @{}, [hashtable]$SP = @{}) {
            $spObject = [ordered]@{ SPObjectId = $Id; SPAppId = "app-$Id"; SPDisplayName = $Id; SPAppOwnerOrganizationId = 'tenant1'; SPAccountEnabled = $true }
            foreach ($k in $SP.Keys) { $spObject[$k] = $SP[$k] }
            $entry = [ordered]@{ ObjectType = $Type; ObjectId = $Id; SP = @([pscustomobject]$spObject) }
            foreach ($k in $Props.Keys) { $entry[$k] = $Props[$k] }
            [pscustomobject]$entry
        }
        function New-Owner([string]$Id) { [ordered]@{ id = $Id; displayName = $Id; principalType = 'SP APP INT'; applicability = 'direct' } }
        $script:Entries = @(
            New-Entry 'graph' 'SP APP EXT' -SP @{ SPAppId = '00000003-0000-0000-c000-000000000000'; SPAppOwnerOrganizationId = 'f8cdef31-a31e-4b4a-93e4-5f571e91255a' }
            New-Entry 'A' -Props @{ SPAppRoleAssignments = @([ordered]@{ AppRoleAssignmentResourceId = 'graph'; AppRoleAssignmentResourceDisplayName = 'Microsoft Graph'; AppRolePermission = 'AppRoleAssignment.ReadWrite.All'; AppRolePermissionSensitivity = 'critical' }); SPOwners = @(New-Owner 'B'); APPPasswordCredentials = @([ordered]@{ keyId = 's1'; expiryInfo = 'expires in 14 to 730 days' }) }
            New-Entry 'B' -Props @{ SPOwners = @(New-Owner 'C') }
            #cycle: A owns C, C owns B, B owns A
            New-Entry 'C' -Props @{ APPAppOwners = @(New-Owner 'A') }
            New-Entry 'D' -Props @{ SPAzureRoleAssignments = @([ordered]@{ roleIsCritical = $true; roleName = 'Owner'; roleAssignmentAssignmentScope = 'RG'; roleAssignmentAssignmentResourceType = 'ResourceGroup'; roleAssignmentAssignmentScopeName = 'rg1'; roleAssignmentApplicability = 'direct' }); SPAADRoleAssignments = @([ordered]@{ roleIsCritical = $true; roleDefinitionName = 'User Administrator'; resourceScope = '/administrativeUnits/au1'; roleAssignmentApplicability = 'indirect'; roleAssignmentAppliesThrough = 'G1 (g1)' }) }
            New-Entry 'E' 'SP APP EXT' -SP @{ SPAppOwnerOrganizationId = 'other'; SPVerifiedPublisher = [pscustomobject]@{ displayName = $null } } -Props @{ SPAppRoleAssignments = @([ordered]@{ AppRoleAssignmentResourceId = 'graph'; AppRolePermission = 'Mail.Read'; AppRolePermissionSensitivity = 'critical' }) }
            New-Entry 'F' -SP @{ SPAccountEnabled = $false } -Props @{ SPAzureRoleAssignments = @([ordered]@{ roleIsCritical = $false; roleName = 'Reader' }); SPPasswordCredentials = @([ordered]@{ keyId = 'x'; expiryInfo = 'expired' }) }
            New-Entry 'M' 'SP MI System assigned' -Props @{ ManagedIdentity = @([ordered]@{ relict = $true }); SPAzureRoleAssignments = @([ordered]@{ roleIsCritical = $false; roleName = 'Reader' }) }
            [pscustomobject][ordered]@{ ObjectType = 'AppOnly'; ObjectId = 'app1'; APP = @([pscustomobject]@{ APPDisplayName = 'app only' }); APPPasswordCredentials = @([ordered]@{ keyId = 'k'; expiryInfo = 'expires in more than 730 days' }) }
        )
        $script:Priv = getEffectivePrivileges -entries $script:Entries
        foreach ($e in $script:Entries.where({ $_.SP })) { $e | Add-Member -NotePropertyName SPEffectivePrivilege -NotePropertyValue $script:Priv[@($e.SP)[0].SPObjectId] -Force }
    }
    It 'an app role that allows escalation makes the SP critical' {
        $script:Priv['A'].level | Should -Be 'critical'
        $script:Priv['A'].reasons[0].category | Should -Be 'App role (can escalate)'
    }
    It 'owners inherit the privileges of what they own, transitively, and cycles terminate' {
        $script:Priv['B'].level | Should -Be 'critical'
        @($script:Priv['B'].reasons.via) | Should -Contain "owner of 'A'"
        $script:Priv['C'].level | Should -Be 'critical'
        @($script:Priv['C'].reasons.via) | Should -Contain "owner of 'B' > owner of 'A'"
    }
    It 'scoped Entra role and Azure role below subscription: high; via group is shown' {
        $script:Priv['D'].level | Should -Be 'high'
        ($script:Priv['D'].reasons | Where-Object category -EQ 'Entra role').via | Should -Be 'group G1 (g1)'
    }
    It 'critical data access app role: high; nothing privileged: none' {
        $script:Priv['E'].level | Should -Be 'high'
        $script:Priv['F'].level | Should -Be 'none'
    }
    It 'credential risk: one active secret counts as one (not as the keys of the credential)' {
        $row = @(getCredentialRiskRows -entries $script:Entries) | Where-Object SPObjectId -EQ 'A'
        $row.Credentials | Should -BeLike 'app secrets 1, app certificates 0*'
        $row.Findings | Should -BeLike 'secret on a privileged identity*'
        $row.Findings | Should -Not -BeLike '*active secrets*'
    }
    It 'third-party apps: other organization, sensitive permission, unverified; Microsoft first-party not listed without permissions' {
        $rows = @(getThirdPartyExposureRows -entries $script:Entries -tenantId 'tenant1')
        @($rows.SPObjectId) | Should -Be @('E')
        $rows[0].Publisher | Should -Be 'not verified'
        $rows[0].MicrosoftFirstParty | Should -Be 'no'
        $rows[0].AppRoles | Should -Be 'Mail.Read (critical)'
    }
    It 'cleanup candidates' {
        $rows = @(getCleanupCandidateRows -entries $script:Entries)
        @(($rows | Where-Object SPObjectId -EQ 'F').Reason) | Should -Contain 'disabled, still holds permissions'
        @(($rows | Where-Object SPObjectId -EQ 'F').Reason) | Should -Contain 'expired credentials still present'
        ($rows | Where-Object SPObjectId -EQ 'M').Reason | Should -Be 'managed identity of a deleted resource, still holds permissions'
        ($rows | Where-Object SPDisplayName -EQ 'app only').Reason | Should -BeLike 'app registration without a service principal*'
        #B and C are critical through ownership, but have an owner
        @(($rows | Where-Object Reason -EQ 'privileged, no owner').SPObjectId) | Should -Not -Contain 'B'
    }
}

Describe 'R-12 / R-13 / R-14 / R-26' {
    BeforeAll {
        $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath
        function New-SimpleEntry([string]$Id, [hashtable]$Props) {
            $entry = [ordered]@{ ObjectType = 'SP APP INT'; ObjectId = $Id; SP = @([pscustomobject]@{ SPObjectId = $Id; SPAppId = "app-$Id"; SPDisplayName = $Id; SPAppOwnerOrganizationId = 'tenant1'; SPAccountEnabled = $true }) }
            foreach ($k in $Props.Keys) { $entry[$k] = $Props[$k] }
            [pscustomobject]$entry
        }
    }
    It 'F-7: a critical Entra role scoped to an administrative unit is high (directoryScopeId), tenant-wide is critical' {
        $entries = @(
            New-SimpleEntry 'au' @{ SPAADRoleAssignments = @([ordered]@{ roleIsCritical = $true; roleDefinitionName = 'User Administrator'; directoryScopeId = '/administrativeUnits/au1'; resourceScope = '/'; roleAssignmentApplicability = 'direct' }) }
            New-SimpleEntry 'ten' @{ SPAADRoleAssignments = @([ordered]@{ roleIsCritical = $true; roleDefinitionName = 'User Administrator'; directoryScopeId = '/'; resourceScope = '/'; roleAssignmentApplicability = 'direct' }) }
        )
        $result = getEffectivePrivileges -entries $entries
        $result['au'].level | Should -Be 'high'
        $result['au'].reasons[0].detail | Should -Be 'User Administrator (scope: /administrativeUnits/au1)'
        $result['ten'].level | Should -Be 'critical'
    }
    It 'R-12: a critical Azure role at the tenant root (/) is critical' {
        $entries = @(New-SimpleEntry 'root' @{ SPAzureRoleAssignments = @([ordered]@{ roleIsCritical = $true; roleName = 'Owner'; roleAssignmentAssignmentScope = 'Ten'; roleAssignmentAssignmentResourceType = 'Tenant'; roleAssignmentAssignmentScopeName = ''; roleAssignmentApplicability = 'direct' }) })
        (getEffectivePrivileges -entries $entries)['root'].level | Should -Be 'critical'
        getRiskExplanation -Table 'EffectivePrivilege' -Key 'Azure role|critical' | Should -BeLike '*tenant root*'
    }
    It 'R-13: lifetime = end - start; a long-lived credential close to its expiry is flagged, a short one is not' {
        $now = [datetime]::UtcNow
        $longOld = [ordered]@{ keyId = 'old'; expiryInfo = 'expires in 14 to 730 days'; startDateTime = $now.AddDays(-800); endDateTime = $now.AddDays(300) }
        $short = [ordered]@{ keyId = 'new'; expiryInfo = 'expires in 14 to 730 days'; startDateTime = $now.AddDays(-10); endDateTime = $now.AddDays(300) }
        $priv = [pscustomobject]@{ level = 'high'; reasons = @() }
        $rows = @(getCredentialRiskRows -entries @(
                (New-SimpleEntry 'longOld' @{ SPEffectivePrivilege = $priv; APPPasswordCredentials = @($longOld) })
                (New-SimpleEntry 'short' @{ SPEffectivePrivilege = $priv; APPPasswordCredentials = @($short) })
            ))
        ($rows | Where-Object SPObjectId -EQ 'longOld').Findings | Should -BeLike '*credential with a long lifetime*'
        ($rows | Where-Object SPObjectId -EQ 'short').Findings | Should -Not -BeLike '*long lifetime*'
    }
    It 'R-14: owners with any user type count for the high-privilege users table' {
        $script:Source | Should -Match "if \(@\(@\(\`$object\.SPOwners\)\.where\(\{ \`$_\.principalType -like 'User\*' \}\)\)\.Count -gt 0\) \{"
        $script:Source | Should -Not -Match "SPOwners\.principalType -contains 'User \(Member\)'"
    }
    It 'R-26: SP credentials without an end date do not throw' {
        $script:Source | Should -Match "expiryInfo = \`$\(if \(\`$credential\.endDateTime\) \{ getCredentialExpiryInfo"
        @([regex]::Matches($script:Source, "endDateTimeFormated = \`$credential\.endDateTime\.ToUniversalTime\(\)")).Count | Should -Be 0
    }
}

Describe 'R-1 delegated grants: consentType decides, not the type of the published scope' {
    BeforeAll {
        function New-Grant([string]$Permission, [string]$ScopeType, [string]$ConsentType, [string]$PrincipalId = $null, [string]$Sensitivity = 'critical') {
            [pscustomobject]@{ SPId = 'graph'; SPDisplayName = 'Microsoft Graph'; scope = $Permission; permission = $Permission; id = "id-$Permission"; type = $ScopeType; permissionSensitivity = $Sensitivity; consentType = $ConsentType; principalId = $PrincipalId; grantId = "g-$ConsentType-$PrincipalId" }
        }
        function New-GrantEntry([string]$Id, [object[]]$Grants, [string]$Owner = 'tenant1') {
            [pscustomobject][ordered]@{ ObjectType = 'SP APP EXT'; ObjectId = $Id; SP = @([pscustomobject]@{ SPObjectId = $Id; SPAppId = "app-$Id"; SPDisplayName = $Id; SPAppOwnerOrganizationId = $Owner; SPAccountEnabled = $true; SPVerifiedPublisher = [pscustomobject]@{ displayName = $null } }); SPOauth2PermissionGrants = $Grants }
        }
        $script:GrantEntries = @(
            #tenant-wide grant of a user-consentable critical scope: was missed (type 'User')
            New-GrantEntry 'tenantWideUserScope' @((New-Grant 'Mail.ReadWrite' 'User' 'AllPrincipals'), (New-Grant 'Files.Read.All' 'User' 'AllPrincipals'))
            #grant for one user of an admin-only scope: was 'admin consent for all users' (type 'Admin')
            New-GrantEntry 'singleUserAdminScope' @(New-Grant 'Directory.ReadWrite.All' 'Admin' 'Principal' 'u1')
            #third-party app with user consents of three users (four grant rows) and a tenant-wide grant
            New-GrantEntry 'thirdParty' @(
                New-Grant 'Files.Read.All' 'User' 'Principal' 'u1'
                New-Grant 'Files.Read.All' 'User' 'Principal' 'u2'
                New-Grant 'Files.Read.All' 'User' 'Principal' 'u3'
                New-Grant 'Mail.Read' 'User' 'Principal' 'u1'
                New-Grant 'User.ReadWrite.All' 'Admin' 'AllPrincipals' $null 'medium'
            ) -Owner 'other'
        )
        $script:GrantPriv = getEffectivePrivileges -entries $script:GrantEntries
    }
    It 'a tenant-wide grant of a critical scope that users may consent to raises the effective privilege' {
        $script:GrantPriv['tenantWideUserScope'].level | Should -Be 'high'
        $script:GrantPriv['tenantWideUserScope'].reasons[0].category | Should -Be 'Delegated permission (admin consent for all users)'
        #every critical tenant-wide grant is a reason (not only the first)
        @($script:GrantPriv['tenantWideUserScope'].reasons.detail) | Should -Be @('Microsoft Graph Mail.ReadWrite', 'Microsoft Graph Files.Read.All')
    }
    It 'a grant for a single user of an admin-only scope is not "admin consent for all users"' {
        $script:GrantPriv['singleUserAdminScope'].level | Should -Be 'none'
    }
    It 'third-party exposure: tenant-wide grants as admin consent, single-user grants counted by users' {
        foreach ($e in $script:GrantEntries) { $e | Add-Member -NotePropertyName SPEffectivePrivilege -NotePropertyValue $script:GrantPriv[$e.ObjectId] -Force }
        $row = @(getThirdPartyExposureRows -entries $script:GrantEntries -tenantId 'tenant1') | Where-Object SPObjectId -EQ 'thirdParty'
        $row.DelegatedAdminConsent | Should -Be 'User.ReadWrite.All (medium)'
        $row.DelegatedUserConsent | Should -Be 'Files.Read.All, Mail.Read (3 users)'
    }
}

Describe 'findings overview and agent objects (theme 25)' {
    It 'counts distinct SPs per level and marks not collected data' {
        $insightRows = [ordered]@{
            EffectivePrivilege = @([pscustomobject]@{ SPObjectId = 'a'; EffectiveLevel = 'critical' }, [pscustomobject]@{ SPObjectId = 'a'; EffectiveLevel = 'critical' }, [pscustomobject]@{ SPObjectId = 'b'; EffectiveLevel = 'high' })
            CredentialRisk = @([pscustomobject]@{ Findings = 'secret on a privileged identity (prefer ...)' })
            SPCredentials = @([pscustomobject]@{ Note = 'not visible in the app registration'; ExpiryInfo = 'expired' }, [pscustomobject]@{ Note = 'SAML single sign-on signing credential (expected)'; ExpiryInfo = 'expires in 14 to 730 days' })
            ThirdParty = @(); AppConfiguration = @([pscustomobject]@{ Severity = 'info' }); FederatedCredentialTrust = @(); Cleanup = @([pscustomobject]@{ Reason = 'x' }, [pscustomobject]@{ Reason = 'x' }); AgentObjects = @()
        }
        $rows = getFindingsOverviewRows -insightRows $insightRows -consentSettings $null -htEntraRecommendations $null -htAgentObjects @{}
        ($rows | Where-Object finding -Like '*critical effective*').count | Should -Be 1
        ($rows | Where-Object finding -Like '*high effective*').count | Should -Be 1
        ($rows | Where-Object finding -Like 'Privileged * active secret').count | Should -Be 1
        ($rows | Where-Object finding -Like 'Credentials on the service principal*').count | Should -Be 1 -Because 'SAML signing credentials are expected'
        ($rows | Where-Object finding -EQ 'Cleanup candidates').detail | Should -Be '2 x'
        ($rows | Where-Object finding -Like '*Entra recommendation*').count | Should -Be 'not collected'
        ($rows | Where-Object finding -Like 'Agent objects*').count | Should -Be 0
        ($rows | Where-Object finding -EQ 'Users can consent to apps').count | Should -Be 'not collected'
    }
    It 'agent objects: in the report or not, with the effective level' {
        $entries = @([pscustomobject]@{ ObjectType = 'SP APP INT'; SP = @([pscustomobject]@{ SPObjectId = 'agent1' }); SPEffectivePrivilege = [ordered]@{ level = 'high' } })
        $rows = @(getAgentObjectRows -entries $entries -htAgentObjects @{ agent1 = [ordered]@{ agentType = 'AgentIdentity'; displayName = 'A1' }; user1 = [ordered]@{ agentType = 'AgentUser'; displayName = 'U1' } })
        ($rows | Where-Object ObjectId -EQ 'agent1').InThisReport | Should -Be 'yes (SP APP INT)'
        ($rows | Where-Object ObjectId -EQ 'agent1').EffectiveLevel | Should -Be 'high'
        ($rows | Where-Object ObjectId -EQ 'user1').InThisReport | Should -Be 'no (user object)'
    }
}
