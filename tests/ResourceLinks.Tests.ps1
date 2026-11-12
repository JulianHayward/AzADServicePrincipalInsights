#Pester 5+ tests for the links in report grid cells (2026-11-07): Azure resource types to AzAdvertizer
#(https://www.azadvertizer.net/azresourcetypes/<namespace>_<type>.html, checked: nested types work, a namespace alone has no
#page), ARM resource IDs to the Azure portal. The CSV keeps the text (htmlProperty is rendered in the report only).

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.TypeDefinitionAst] -and $n.Name -eq 'HtmlSafe' }, $true).Extent.Text))
    foreach ($name in 'getSpiResourceTypeLinkHtml', 'getSpiPortalResourceLinkHtml', 'getSpiResourceTypeListHtml', 'getSpiEntraObjectLinkHtml', 'getSpiApiPermissionLinkHtml', 'getSpiAzureRoleLinkHtml') {
        . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name }, $true).Extent.Text))
    }
    $azAPICallConf = @{ checkContext = @{ Tenant = @{ Id = 'tid' } } }
}

Describe 'resource type links (AzAdvertizer)' {
    It '<Type> -> <Page>' -ForEach @(
        @{ Type = 'Microsoft.Authorization/policyAssignments'; Page = 'microsoft.authorization_policyassignments' }
        @{ Type = 'Microsoft.Web/sites/slots'; Page = 'microsoft.web_sites_slots' }
        @{ Type = 'Microsoft.ManagedIdentity/userAssignedIdentities'; Page = 'microsoft.managedidentity_userassignedidentities' }
    ) {
        getSpiResourceTypeLinkHtml $Type | Should -BeExactly "<a class=`"externallink`" href=`"https://www.azadvertizer.net/azresourcetypes/$Page.html`" target=`"_blank`">$Type</a>"
    }
    It 'not linked: a namespace alone, other text; encoded' {
        getSpiResourceTypeLinkHtml 'Microsoft.Web' | Should -BeExactly 'Microsoft.Web'
        getSpiResourceTypeLinkHtml 'none (unrestricted)' | Should -BeExactly 'none (unrestricted)'
        getSpiResourceTypeLinkHtml 'a/<b>' | Should -BeExactly 'a/&lt;b&gt;'
        getSpiResourceTypeLinkHtml '' | Should -BeExactly ''
    }
    It 'list cell: every type linked, counts kept, separator kept' {
        $html = getSpiResourceTypeListHtml -Text 'Microsoft.Authorization/policyAssignments (1), Microsoft.Web/sites (2)' -Delimiter ', '
        $html | Should -BeExactly ('<a class="externallink" href="https://www.azadvertizer.net/azresourcetypes/microsoft.authorization_policyassignments.html" target="_blank">Microsoft.Authorization/policyAssignments</a> (1), ' +
            '<a class="externallink" href="https://www.azadvertizer.net/azresourcetypes/microsoft.web_sites.html" target="_blank">Microsoft.Web/sites</a> (2)')
        getSpiResourceTypeListHtml -Text 'Microsoft.Web; Microsoft.Compute/disks' -Delimiter '; ' | Should -Match '^Microsoft\.Web; <a [^>]+microsoft\.compute_disks\.html'
        getSpiResourceTypeListHtml -Text 'not collected (api-version 2024-11-30)' -Delimiter ', ' | Should -BeExactly 'not collected (api-version 2024-11-30)'
    }
}

Describe 'resource ID links (Azure portal)' {
    It 'subscription and management group scope: linked with the tenant' {
        getSpiPortalResourceLinkHtml '/subscriptions/s1/resourceGroups/rg 1/providers/Microsoft.Web/sites/a' |
            Should -BeExactly '<a class="externallink" href="https://portal.azure.com/#@tid/resource/subscriptions/s1/resourceGroups/rg%201/providers/Microsoft.Web/sites/a" target="_blank">/subscriptions/s1/resourceGroups/rg 1/providers/Microsoft.Web/sites/a</a>'
        getSpiPortalResourceLinkHtml '/providers/Microsoft.Management/managementGroups/mg1/providers/Microsoft.Authorization/policyAssignments/pa' | Should -Match '^<a class="externallink" href="https://portal\.azure\.com/#@tid/resource/providers/Microsoft\.Management/managementGroups/mg1/'
    }
    It 'not linked: other text; encoded' {
        getSpiPortalResourceLinkHtml 'isExplicit=False' | Should -BeExactly 'isExplicit=False'
        getSpiPortalResourceLinkHtml '' | Should -BeExactly ''
        getSpiPortalResourceLinkHtml '/subscriptions/s"><script>' | Should -Not -Match '<script>'
    }
}

Describe 'wiring' {
    It 'columns render the links: <Column>' -ForEach @(
        @{ Column = "@{ header = 'MI Resource type'; property = 'c16'; htmlProperty = 'c16Html'; filter = 'select' }" }
        @{ Column = "@{ header = 'MI ResourceId'; property = 'c5'; htmlProperty = 'c5Html' }" }
        @{ Column = "@{ header = 'Res Type'; property = 'c10'; htmlProperty = 'c10Html'; filter = 'select' }" }
        @{ Column = "@{ header = 'Res Id'; property = 'c15'; htmlProperty = 'c15Html'; snapshot = 'resource' }" }
        @{ Column = "@{ header = 'Resource Id (ARM)'; property = 'c3'; htmlProperty = 'c3Html' }" }
        @{ Column = "@{ Header = 'MI ResourceId'; Property = 'MIResourceId'; HtmlProperty = 'MIResourceIdHtml'; Snapshot = 'resource' }" }
        @{ Column = "@{ Header = 'Assignment restrictions'; Property = 'AssignmentRestrictions'; HtmlProperty = 'AssignmentRestrictionsHtml'; Select = `$true }" }
        @{ Column = "@{ Header = 'Attached resource types'; Property = 'AttachedResourceTypes'; HtmlProperty = 'AttachedResourceTypesHtml' }" }
    ) {
        $script:Source.Contains($Column) | Should -BeTrue
    }
    It 'addReportSection passes HtmlProperty to the grid; the CSV uses Property' {
        $script:Source | Should -Match 'htmlProperty = \$column\.HtmlProperty'
        $script:Source | Should -Match "Select-Object -Property @\(\`$Columns\.where\(\{ \`$_\.Csv -ne \`$false \}\)\.Property\)"
    }
    It 'the link prefixes are in the fragment table (appended, existing indexes unchanged)' {
        $script:Source | Should -Match "'</b>'\s+'<a class=""externallink"" href=""https://www\.azadvertizer\.net/azresourcetypes/'\s+'<a class=""externallink"" href=""https://portal\.azure\.com/#@'\s+'<a class=""externallink"" href=""https://portal\.azure\.com/#view/Microsoft_AAD_UsersAndTenants/UserProfileMenuBlade/~/overview/userId/'\s+'<a class=""externallink"" href=""https://portal\.azure\.com/#view/Microsoft_AAD_IAM/GroupDetailsMenuBlade/~/Overview/groupId/'\s+'<a class=""externallink"" href=""https://portal\.azure\.com/#view/Microsoft_AAD_IAM/ManagedAppMenuBlade/~/Overview/objectId/'\s+'<a class=""externallink"" href=""https://portal\.azure\.com/#view/Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/~/Overview/appId/'\s+'<a class=""externallink"" href=""https://www\.azadvertizer\.net/azentraidapipermissions/'\s+\)"
    }
}

Describe 'Service Principal and application links (Microsoft Entra pages in the Azure portal, built in the browser)' {
    It 'the renderer builds the links only from GUIDs (node)' {
        $node = Get-Command node -ErrorAction SilentlyContinue
        if (-not $node) { Set-ItResult -Skipped -Because 'node is not available'; return }
        $source = $script:Source
        #spiPortalHref (the links) and the renderer that uses it; the snapshot icon is tested in SpSnapshot.Tests.ps1
        $start = $source.IndexOf('    function spiPortalHref(')
        $end = $source.IndexOf("`n    }", $source.IndexOf('        };', $start)) + 6
        $js = $source.Substring($start, $end - $start) + @'

function spiSnapCellLink() { return ''; }
var highlighter = { text: function (v) { return String(v === undefined || v === null ? '' : v).replace(/&/g, '&amp;').replace(/</g, '&lt;'); } };
var encoded = { dictionaries: [['66666666-6666-4666-8666-666666666666', '<img>', ''], ['a3dfc3c6-2c7d-4f42-aeec-b2877f9bce97', 'x']] };
var column = { getColId: function () { return 'c0'; } };
var sp = spiColumnPortalLinkRenderer(encoded, 0, 1, 'servicePrincipal', highlighter);
var app = spiColumnPortalLinkRenderer(encoded, 0, 1, 'application', highlighter);
var self = spiColumnPortalLinkRenderer(encoded, 1, -1, 'application', highlighter);
var types = { dictionaries: [['44444444-4444-4444-8444-444444444444'], ['User (Member)', 'Group', 'Serviceprincipal']] };
var principal = spiColumnPortalLinkRenderer(types, 0, 1, 'principal', highlighter);
var user = spiColumnPortalLinkRenderer(types, 0, -1, 'user', highlighter);
console.log(JSON.stringify([
  sp({ data: [0, 0], column: column }), app({ data: [0, 0], column: column }), self({ data: [0, 0], column: column }),
  sp({ data: [1, 0], column: column }), sp({ data: [0, 1], column: column }), sp({ data: [2, 0], column: column }),
  principal({ data: [0, 0], column: column }), principal({ data: [0, 1], column: column }), principal({ data: [0, 2], column: column }), user({ data: [0], column: column })
]));
'@
        $file = Join-Path $TestDrive 'renderer.js'
        Set-Content -LiteralPath $file -Value $js
        $result = (& $node.Source $file) | ConvertFrom-Json
        $result[0] | Should -BeExactly '<a class="externallink" href="https://portal.azure.com/#view/Microsoft_AAD_IAM/ManagedAppMenuBlade/~/Overview/objectId/66666666-6666-4666-8666-666666666666/appId/a3dfc3c6-2c7d-4f42-aeec-b2877f9bce97" target="_blank" rel="noopener">66666666-6666-4666-8666-666666666666</a>'
        $result[1] | Should -BeExactly '<a class="externallink" href="https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/~/Overview/appId/a3dfc3c6-2c7d-4f42-aeec-b2877f9bce97" target="_blank" rel="noopener">66666666-6666-4666-8666-666666666666</a>'
        $result[2] | Should -Match 'ApplicationMenuBlade/~/Overview/appId/a3dfc3c6-2c7d-4f42-aeec-b2877f9bce97"'
        $result[3] | Should -BeExactly '&lt;img>' -Because 'not a GUID: text only'
        $result[4] | Should -BeExactly '66666666-6666-4666-8666-666666666666' -Because 'the application id is not a GUID'
        $result[5] | Should -BeExactly '' -Because 'empty cell'
        $result[6] | Should -Match 'href="https://portal\.azure\.com/#view/Microsoft_AAD_UsersAndTenants/UserProfileMenuBlade/~/overview/userId/44444444-4444-4444-8444-444444444444"'
        $result[7] | Should -Match 'href="https://portal\.azure\.com/#view/Microsoft_AAD_IAM/GroupDetailsMenuBlade/~/Overview/groupId/44444444-4444-4444-8444-444444444444/menuId/"'
        $result[8] | Should -BeExactly '44444444-4444-4444-8444-444444444444' -Because 'a service principal owner has no application id in the row'
        $result[9] | Should -Match 'UserProfileMenuBlade/~/overview/userId/44444444-4444-4444-8444-444444444444'
    }
    It 'object id columns are linked with their application id column' {
        @([regex]::Matches($script:Source, "@\{ header = 'SP object Id'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' \}")).Count | Should -Be 13
        @([regex]::Matches($script:Source, "@\{ header = 'App object Id'; property = 'c0'; portal = 'application'; appIdProperty = 'c1' \}")).Count | Should -Be 2
        @([regex]::Matches($script:Source, "@\{ header = 'Application ObjectId'; property = 'c4'; portal = 'application'; appIdProperty = 'c5' \}")).Count | Should -Be 3
        $script:Source | Should -Match "@\{ header = 'App object Id'; property = 'c8'; portal = 'application'; appIdProperty = 'c9' \}"
        $script:Source | Should -Match "@\{ header = 'MI SP object Id'; property = 'c6'; portal = 'servicePrincipal'; appIdProperty = 'c7' \}"
        $script:Source | Should -Match "@\{ header = 'SP \(MI\) ObjectId'; property = 'c0'; portal = 'servicePrincipal'; appIdProperty = 'c1' \}"
        $script:Source | Should -Match "@\{ header = 'SP objectId'; property = 'c6'; portal = 'servicePrincipal'; appIdProperty = 'c7' \}"
        $script:Source | Should -Match "@\{ Header = 'SP object Id'; Property = 'SPObjectId'; Portal = 'servicePrincipal'; AppIdProperty = 'SPAppId' \}"
    }
    It 'buildSpiGrid emits the renderer; addReportSection passes Portal and AppIdProperty' {
        $script:Source | Should -Match 'cellRenderer: spiColumnPortalLinkRenderer\(spiRowData, \$valueIndex, \$auxIndex'
        $script:Source | Should -Match 'portal = \$column\.Portal; appIdProperty = \$column\.AppIdProperty'
    }
}

Describe 'user and group names in list cells (server side)' {
    It 'users and groups are linked by id; other types and ids that are no GUID: text, encoded' {
        getSpiEntraObjectLinkHtml -Kind 'User (Member)' -Id '55555555-5555-4555-8555-555555555555' -Text 'u1 <x>' | Should -BeExactly '<a class="externallink" href="https://portal.azure.com/#view/Microsoft_AAD_UsersAndTenants/UserProfileMenuBlade/~/overview/userId/55555555-5555-4555-8555-555555555555" target="_blank">u1 &lt;x&gt;</a>'
        getSpiEntraObjectLinkHtml -Kind 'group' -Id '44444444-4444-4444-8444-444444444444' -Text 'g' | Should -BeExactly '<a class="externallink" href="https://portal.azure.com/#view/Microsoft_AAD_IAM/GroupDetailsMenuBlade/~/Overview/groupId/44444444-4444-4444-8444-444444444444/menuId/" target="_blank">g</a>'
        getSpiEntraObjectLinkHtml -Kind 'Serviceprincipal' -Id '44444444-4444-4444-8444-444444444444' -Text 'sp' | Should -BeExactly 'sp'
        getSpiEntraObjectLinkHtml -Kind 'User (Member)' -Id 'x"><script>' -Text 'u' | Should -BeExactly 'u'
    }
    It 'owners, assigned users and groups, group memberships use it; owner id and HiPo user id columns are linked' {
        $script:Source | Should -Match 'MainHtml = \(getSpiEntraObjectLinkHtml -Kind \$owner\.principalType -Id \$owner\.id -Text \$owner\.displayName\)'
        $script:Source | Should -Match 'MainHtml = \(getSpiEntraObjectLinkHtml -Kind \$_\.principalType -Id \$_\.principalId -Text \$_\.principalDisplayName\)'
        $script:Source | Should -Match "MainHtml = \(getSpiEntraObjectLinkHtml -Kind 'group' -Id \`$_\.ObjectId -Text \`$_\.DisplayName\)"
        @([regex]::Matches($script:Source, "@\{ header = 'Owner Id'; property = 'c7'; portal = 'principal'; typeProperty = 'c6' \}")).Count | Should -Be 2
        $script:Source | Should -Match "@\{ header = 'UserId'; property = 'c1'; portal = 'user'; typeProperty = 'c2' \}"
    }
}

Describe 'owned objects and Service Principal / application names (server side)' {
    AfterEach { $script:htSpiAppIdByObjectId = $null }
    It 'a Service Principal or application is linked with its application id (given or looked up by object id)' {
        $sp = '11111111-1111-4111-8111-111111111111'; $app = '22222222-2222-4222-8222-222222222222'; $appId = 'a3dfc3c6-2c7d-4f42-aeec-b2877f9bce97'
        getSpiEntraObjectLinkHtml -Kind 'Serviceprincipal' -Id $sp -Text 'sp' -AppId $appId | Should -BeExactly "<a class=`"externallink`" href=`"https://portal.azure.com/#view/Microsoft_AAD_IAM/ManagedAppMenuBlade/~/Overview/objectId/$sp/appId/$appId`" target=`"_blank`">sp</a>"
        #R-32: owner SPs carry their type label as principalType
        foreach ($ownerType in 'SP APP INT', 'SP MI System assigned', 'SP EXT') {
            getSpiEntraObjectLinkHtml -Kind $ownerType -Id $sp -Text 'sp' -AppId $appId | Should -BeExactly "<a class=`"externallink`" href=`"https://portal.azure.com/#view/Microsoft_AAD_IAM/ManagedAppMenuBlade/~/Overview/objectId/$sp/appId/$appId`" target=`"_blank`">sp</a>" -Because $ownerType
        }
        $script:htSpiAppIdByObjectId = @{ $app = $appId }
        getSpiEntraObjectLinkHtml -Kind 'Application' -Id $app -Text 'app <x>' | Should -BeExactly "<a class=`"externallink`" href=`"https://portal.azure.com/#view/Microsoft_AAD_RegisteredApps/ApplicationMenuBlade/~/Overview/appId/$appId`" target=`"_blank`">app &lt;x&gt;</a>"
        getSpiEntraObjectLinkHtml -Kind 'Serviceprincipal' -Id $sp -Text 'not in this run' | Should -BeExactly 'not in this run' -Because 'no application id: text'
    }
    It 'owned objects use it; the lookup is built from the objects of the run' {
        $script:Source | Should -Match 'MainHtml = \(getSpiEntraObjectLinkHtml -Kind \$ownedObject\.type -Id \$ownedObject\.objectId -Text \$ownedObject\.displayName\)'
        $script:Source | Should -Match '\$script:htSpiAppIdByObjectId\[\[string\]\$entrySP\.SPObjectId\] = \[string\]\$entrySP\.SPAppId'
        $script:Source | Should -Match '\$script:htSpiAppIdByObjectId\[\[string\]\$entryApp\.APPObjectId\] = \[string\]\$entryApp\.APPAppClientId'
    }
}

Describe 'API permission links (AzAdvertizer, High-privilege users)' {
    BeforeAll {
        $script:Graph = '77777777-7777-4777-8777-777777777777'; $script:AadGraph = 'aaaa0000-7777-4777-8777-777777777777'
        $htServicePrincipalsAppRoleIdsToAppId = @{ "$($script:Graph)_df021288-bdef-4463-88db-98f22de89214" = '00000003-0000-0000-c000-000000000000'; "$($script:AadGraph)_5778995a-e1bf-45b8-affa-663a9f3f4d04" = '00000002-0000-0000-c000-000000000000' }
        $htServicePrincipalsPublishedPermissionScopesIdsToAppId = @{ "$($script:AadGraph)_5778995a-e1bf-45b8-affa-663a9f3f4d04" = '00000002-0000-0000-c000-000000000000' }
    }
    It 'an id used only as application permission: the page without suffix' {
        getSpiApiPermissionLinkHtml -ResourceServicePrincipalId $script:Graph -PermissionId 'df021288-bdef-4463-88db-98f22de89214' -Type 'application' -Text 'User.Read.All' |
            Should -BeExactly '<a class="externallink" href="https://www.azadvertizer.net/azentraidapipermissions/00000003-0000-0000-c000-000000000000_df021288-bdef-4463-88db-98f22de89214.html" target="_blank">User.Read.All</a>'
    }
    It 'an id the API uses for both: the page of the type (as the AzAdvertizer example)' {
        getSpiApiPermissionLinkHtml -ResourceServicePrincipalId $script:AadGraph -PermissionId '5778995a-e1bf-45b8-affa-663a9f3f4d04' -Type 'application' -Text 'Directory.Read.All' |
            Should -Match 'href="https://www\.azadvertizer\.net/azentraidapipermissions/00000002-0000-0000-c000-000000000000_5778995a-e1bf-45b8-affa-663a9f3f4d04_application\.html"'
        getSpiApiPermissionLinkHtml -ResourceServicePrincipalId $script:AadGraph -PermissionId '5778995a-e1bf-45b8-affa-663a9f3f4d04' -Type 'delegated' -Text 'Directory.Read.All' |
            Should -Match '_5778995a-e1bf-45b8-affa-663a9f3f4d04_delegated\.html"'
    }
    It 'delegated without lookup entry: the API appId of the grant; no GUID: text, encoded' {
        getSpiApiPermissionLinkHtml -ResourceServicePrincipalId 'x' -PermissionId '5f8c59db-677d-491f-a6b8-5f174b11ec1d' -Type 'delegated' -Text 'Group.Read.All' -ResourceAppId '00000003-0000-0000-C000-000000000000' |
            Should -Match 'azentraidapipermissions/00000003-0000-0000-c000-000000000000_5f8c59db-677d-491f-a6b8-5f174b11ec1d\.html"'
        getSpiApiPermissionLinkHtml -ResourceServicePrincipalId 'x' -PermissionId 'not-a-guid' -Type 'application' -Text 'a <b>' | Should -BeExactly 'a &lt;b&gt;'
    }
    It 'HiPo permission cells of app role assignments and delegated grants use it' {
        $script:Source | Should -Match "getSpiApiPermissionLinkHtml -ResourceServicePrincipalId \`$SPAppRoleAssignment\.AppRoleAssignmentResourceId -PermissionId \`$SPAppRoleAssignment\.AppRoleId -Type 'application'"
        $script:Source | Should -Match "getSpiApiPermissionLinkHtml -ResourceServicePrincipalId \`$SPOauth2PermissionGrant\.SPId -PermissionId \`$SPOauth2PermissionGrant\.id -Type 'delegated'"
    }
}

Describe 'Azure role links (built-in: AzAdvertizer, custom: role definition in the Azure portal)' {
    BeforeAll {
        $script:Custom = 'c7db166c-f999-4d4e-b2b1-e462f97c3833'
        $htCacheDefinitionsRole = @{
            $script:Custom = @{ definition = @{ properties = @{ assignableScopes = @('/providers/Microsoft.Management/managementGroups/mg1', '/subscriptions/33333333-3333-4333-8333-333333333333') } } }
            'aaaaaaaa-f999-4d4e-b2b1-e462f97c3833' = @{ definition = @{ properties = @{ assignableScopes = @('/providers/Microsoft.Management/managementGroups/mg1') } } }
        }
    }
    It 'built-in role: AzAdvertizer' {
        getSpiAzureRoleLinkHtml -RoleId '8e3af657-a8ff-443c-a75c-2fe8c4bcb635' -RoleName 'Owner' -RoleType 'BuiltInRole' |
            Should -BeExactly '<a class="externallink" href="https://www.azadvertizer.net/azrolesadvertizer/8e3af657-a8ff-443c-a75c-2fe8c4bcb635.html" target="_blank">Owner</a>'
    }
    It 'custom role, subscription based roleDefinitionId: that id' {
        getSpiAzureRoleLinkHtml -RoleId $script:Custom -RoleName 'my <role>' -RoleType 'CustomRole' -RoleDefinitionId "/subscriptions/33333333-3333-4333-8333-333333333333/providers/Microsoft.Authorization/roleDefinitions/$($script:Custom)" |
            Should -BeExactly "<a class=`"externallink`" href=`"https://portal.azure.com/#@tid/resource/subscriptions/33333333-3333-4333-8333-333333333333/providers/Microsoft.Authorization/roleDefinitions/$($script:Custom)/overview`" target=`"_blank`">my &lt;role&gt;</a>"
    }
    It 'custom role, tenant level roleDefinitionId: a subscription of the assignable scopes first, else the management group' {
        getSpiAzureRoleLinkHtml -RoleId $script:Custom -RoleName 'r' -RoleType 'CustomRole' -RoleDefinitionId "/providers/Microsoft.Authorization/roleDefinitions/$($script:Custom)" |
            Should -Match "href=`"https://portal\.azure\.com/#@tid/resource/subscriptions/33333333-3333-4333-8333-333333333333/providers/Microsoft\.Authorization/roleDefinitions/$($script:Custom)/overview`""
        getSpiAzureRoleLinkHtml -RoleId 'aaaaaaaa-f999-4d4e-b2b1-e462f97c3833' -RoleName 'r' -RoleType 'CustomRole' -RoleDefinitionId '/providers/Microsoft.Authorization/roleDefinitions/aaaaaaaa-f999-4d4e-b2b1-e462f97c3833' |
            Should -Match 'resource/providers/Microsoft\.Management/managementGroups/mg1/providers/Microsoft\.Authorization/roleDefinitions/aaaaaaaa-f999-4d4e-b2b1-e462f97c3833/overview"'
    }
    It 'custom role without definition data, role id that is no GUID: text' {
        getSpiAzureRoleLinkHtml -RoleId 'bbbbbbbb-f999-4d4e-b2b1-e462f97c3833' -RoleName 'unknown' -RoleType 'CustomRole' -RoleDefinitionId '/providers/Microsoft.Authorization/roleDefinitions/bbbbbbbb-f999-4d4e-b2b1-e462f97c3833' | Should -BeExactly 'unknown'
        getSpiAzureRoleLinkHtml -RoleId 'x' -RoleName 'a<b' -RoleType 'BuiltInRole' | Should -BeExactly 'a&lt;b'
        #R-6: an orphaned assignment (definition deleted) has type 'unknown': no link
        getSpiAzureRoleLinkHtml -RoleId 'cccccccc-f999-4d4e-b2b1-e462f97c3833' -RoleName 'role definition not found (deleted?)' -RoleType 'unknown' -RoleDefinitionId '/subscriptions/33333333-3333-4333-8333-333333333333/providers/Microsoft.Authorization/roleDefinitions/cccccccc-f999-4d4e-b2b1-e462f97c3833' | Should -BeExactly 'role definition not found (deleted?)'
    }
    It 'Azure RBAC grid and HiPo users use it' {
        @([regex]::Matches($script:Source, '\$roleName = getSpiAzureRoleLinkHtml -RoleId')).Count | Should -Be 2
    }
}
