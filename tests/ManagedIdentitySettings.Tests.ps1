#Pester 5+ tests for the user-assigned managed identity settings (isolation scope, assignment restrictions (preview), 2026-11-06):
#one row per identity with what it is attached to and a suggestion that does not break an existing attachment (Azure refuses those).

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'getUserAssignedIdentitySettingsRows' }, $true).Extent.Text))

    function New-Mi([string]$Name, [string]$Location = 'westeurope', [string]$IsolationScope = 'None', [object]$Restrictions = @(), [bool]$Collected = $true) {
        [pscustomobject]@{
            id = "/subscriptions/s1/resourceGroups/rg1/providers/Microsoft.ManagedIdentity/userAssignedIdentities/$Name"; name = $Name; location = $Location
            subscriptionId = 's1'; subscriptionName = 'sub one'; resourceGroup = 'rg1'; principalId = "p-$Name"; isolationScope = $IsolationScope
            assignmentRestrictionsCollected = $Collected; assignmentRestrictions = @($Restrictions); apiVersion = $(if ($Collected) { '2025-05-31-preview' } else { '2024-11-30' })
        }
    }
    function New-Attachment([string]$MiName, [string]$Type, [string]$Location) {
        #the MI resource id as the resource returns it (other casing than the identity listing)
        [pscustomobject]@{ miResourceId = "/subscriptions/s1/resourcegroups/RG1/providers/Microsoft.ManagedIdentity/userAssignedIdentities/$MiName"; resourceType = $Type; resourceLocation = $Location }
    }
    function Get-Row([object[]]$Settings, [object[]]$Attachments, $Fic = @{}, [object[]]$PolicyAssignments = @()) {
        @(getUserAssignedIdentitySettingsRows -Settings $Settings -Attachments $Attachments -FicByMI $Fic -PolicyAssignments $PolicyAssignments -Delimiter ', ')
    }
}

Describe 'getUserAssignedIdentitySettingsRows' {
    It 'unrestricted, all attachments in its region: restrict to the attached types and set Regional' {
        $row = Get-Row -Settings (New-Mi 'mi1') -Attachments @((New-Attachment 'mi1' 'Microsoft.Web/sites' 'westeurope'), (New-Attachment 'mi1' 'Microsoft.Web/sites' 'westeurope'), (New-Attachment 'mi1' 'Microsoft.Web/serverFarms' 'westeurope'))
        $row.Count | Should -Be 1
        $row[0].AssignmentRestrictions | Should -BeExactly 'none (unrestricted)'
        $row[0].AttachedResourcesCount | Should -Be 3
        $row[0].AttachedResourceTypes | Should -BeExactly 'Microsoft.Web/serverFarms (1), Microsoft.Web/sites (2)'
        $row[0].Usage | Should -BeExactly 'attached to resources'
        $row[0].Suggestion | Should -BeExactly 'Restrict to Microsoft.Web/serverFarms, Microsoft.Web/sites. Set isolation scope Regional (all attached resources are in westeurope).'
    }
    It 'attachments in other regions: Regional is not suggested' {
        $row = Get-Row -Settings (New-Mi 'mi2' -Location 'northeurope') -Attachments @((New-Attachment 'mi2' 'Microsoft.Web/sites' 'westeurope'), (New-Attachment 'mi2' 'Microsoft.Automation/automationAccounts' 'westeurope'))
        $row[0].Suggestion | Should -Match 'Regional not possible without changes: 2 of 2 attached resources are in other regions \(westeurope\)\.'
        $row[0].Suggestion | Should -Not -Match 'Set isolation scope Regional'
        $row[0].AttachedRegions | Should -BeExactly 'westeurope'
    }
    It 'restricted and Regional: nothing to suggest; the restrictions are listed' {
        $row = Get-Row -Settings (New-Mi 'mi3' -IsolationScope 'Regional' -Restrictions @('Microsoft.Compute/diskEncryptionSets')) -Attachments @()
        $row[0].AssignmentRestrictions | Should -BeExactly 'Microsoft.Compute/diskEncryptionSets'
        $row[0].IsolationScope | Should -BeExactly 'Regional'
        $row[0].Usage | Should -BeExactly 'not attached (scanned scope)'
        $row[0].Suggestion | Should -BeExactly 'Restricted and Regional.'
    }
    It 'not attached: restrict before it is attached; federated identity credentials count as usage' {
        $mi = New-Mi 'mi4'
        $row = Get-Row -Settings $mi -Attachments @() -Fic @{ ($mi.id.ToUpper()) = @{ federatedCredentials = @('a', 'b') } }
        $row[0].FederatedIdentityCredentialsCount | Should -Be 2
        $row[0].Usage | Should -BeExactly 'federated identity credentials only'
        $row[0].Suggestion | Should -Match '^Used through federated identity credentials only \(restrictions do not apply to that use\); restrict it to the resource types it may be attached to\. Consider isolation scope Regional'
    }
    It 'restrictions not returned (stable api-version): not collected, no restriction suggestion' {
        $row = Get-Row -Settings (New-Mi 'mi5' -Collected $false) -Attachments @(New-Attachment 'mi5' 'Microsoft.Web/sites' 'westeurope')
        $row[0].AssignmentRestrictions | Should -BeExactly 'not collected (api-version 2024-11-30)'
        $row[0].Suggestion | Should -Not -Match 'Restrict'
        $row[0].Suggestion | Should -Match 'Set isolation scope Regional'
    }
    It 'missing isolation scope reads as None; location compare ignores case and spaces' {
        $row = Get-Row -Settings (New-Mi 'mi6' -IsolationScope '' -Location 'West Europe') -Attachments @(New-Attachment 'mi6' 'Microsoft.Web/sites' 'westeurope')
        $row[0].IsolationScope | Should -BeExactly 'None'
        $row[0].Suggestion | Should -Match 'Set isolation scope Regional'
    }
    It 'a policy assignment with the identity counts as an attachment; assignments without a user-assigned identity are ignored' {
        $mi = New-Mi 'mi7' -Location 'northeurope'
        $uai = [pscustomobject]@{}; $uai | Add-Member -NotePropertyName $mi.id.ToUpper() -NotePropertyValue ([pscustomobject]@{})
        $pa = @(
            [pscustomobject]@{ id = '/providers/Microsoft.Management/managementGroups/mg1/providers/Microsoft.Authorization/policyAssignments/pa1'; location = 'northeurope'; identity = [pscustomobject]@{ type = 'UserAssigned'; userAssignedIdentities = $uai } }
            [pscustomobject]@{ id = '/subscriptions/s1/providers/Microsoft.Authorization/policyAssignments/pa2'; location = 'westeurope'; identity = [pscustomobject]@{ type = 'SystemAssigned' } }
            [pscustomobject]@{ id = '/subscriptions/s1/providers/Microsoft.Authorization/policyAssignments/pa3'; identity = $null }
        )
        $row = Get-Row -Settings $mi -Attachments @() -PolicyAssignments $pa
        $row[0].AttachedResourcesCount | Should -Be 1
        $row[0].AttachedResourceTypes | Should -BeExactly 'Microsoft.Authorization/policyAssignments (1)'
        $row[0].Usage | Should -BeExactly 'attached to resources'
        $row[0].Suggestion | Should -Match 'Set isolation scope Regional \(all attached resources are in northeurope\)'
    }
    It 'sorted by name; attachments of other identities are not counted' {
        $rows = Get-Row -Settings @((New-Mi 'b'), (New-Mi 'a')) -Attachments @(New-Attachment 'b' 'Microsoft.Web/sites' 'westeurope')
        $rows.MIName | Should -Be @('a', 'b')
        $rows[0].AttachedResourcesCount | Should -Be 0
        $rows[1].AttachedResourcesCount | Should -Be 1
    }
}

Describe 'collection and report wiring' {
    It 'one call per subscription with user-assigned identities, preview api-version first, stable as fallback' {
        $script:Source | Should -Match "if \(\`$resource\.type -eq 'Microsoft\.ManagedIdentity/userAssignedIdentities'\) \{?
\s+\`$subscriptionHasUserAssignedIdentities = \`$true"
        $script:Source | Should -Match "if \(\`$subscriptionHasUserAssignedIdentities\) \{"
        $script:Source | Should -Match "foreach \(\`$apiVersion in '2025-05-31-preview', '2024-11-30'\)"
        $script:Source | Should -Match '/providers/Microsoft\.ManagedIdentity/userAssignedIdentities\?api-version=\$apiVersion'
    }
    It 'the section is written, also when NoAzureResourceSideRelations is set (not collected)' {
        @([regex]::Matches($script:Source, "addReportSection -Region 'ManagedIdentityUserAssignedAssignmentRestrictions'")).Count | Should -Be 2
        $script:Source | Should -Match "-NotCollectedReason ""parameter NoAzureResourceSideRelations"
    }
    It 'the JSON of a user-assigned identity gets isolationScope and assignmentRestrictions' {
        $script:Source | Should -Match '\$htOptInfo\.isolationScope = \$uamiSettings\.isolationScope'
        $script:Source | Should -Match '\$htOptInfo\.assignmentRestrictions = \[string\[\]\]@\(\$uamiSettings\.assignmentRestrictions\)'
    }
}
