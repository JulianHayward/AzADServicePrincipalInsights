#pwsh/New-AzADSPIWebApp.ps1 and setup/azure-web-app.json: the web app that shows the report to signed-in members of a group.
#Real test 2026-11-11 (test tenant, F1): the template and the script (-SkipEntra) create the same web app (settings compared, one
#role assignment with the same name), Publish-AzADSPIWebApp.ps1 publishes to it with basic authentication off, anonymous requests
#are redirected to the sign-in. The Microsoft Entra part runs mocked here (the test identity cannot create app registrations).

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:SetupScript = Join-Path $script:RepoRoot 'pwsh/New-AzADSPIWebApp.ps1'
    $script:SetupSource = Get-Content -Raw -LiteralPath $script:SetupScript
    $script:SetupAst = Get-ScriptAst -Path $script:SetupScript
    foreach ($function in $script:SetupAst.FindAll({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] }, $false)) {
        . ([scriptblock]::Create($function.Extent.Text))
    }
    $script:Template = Get-Content -Raw -LiteralPath (Join-Path $script:RepoRoot 'setup/azure-web-app.json') | ConvertFrom-Json -AsHashtable
    function getTemplateResource([string]$Type, [string]$NameLike = '*') {
        @($script:Template.resources).where({ $_.type -eq $Type -and $_.name -like $NameLike })[0]
    }
    function getScriptVariable([string]$Name) {
        [regex]::Match($script:SetupSource, "(?m)^\`$$($Name) = '(?<v>[^']+)'").Groups['v'].Value
    }

    #mocked Azure context and REST calls: every call is recorded, the responses are what ARM / Microsoft Graph return. The mock
    #bodies do not see the script scope of this file: the shared state is one global hashtable (removed in AfterAll)
    $global:WebAppSetupTest = @{ Calls = $null; FoundApplications = @(); ExistingApplication = $null; DefaultHostName = ''; AppId = '55555555-5555-5555-5555-555555555555'; TenantId = '11111111-1111-1111-1111-111111111111' }
    $script:TenantId = '11111111-1111-1111-1111-111111111111'
    $script:SubscriptionId = '22222222-2222-2222-2222-222222222222'
    $script:GroupId = '33333333-3333-3333-3333-333333333333'
    $script:PublisherId = '44444444-4444-4444-4444-444444444444'
    $script:AppId = '55555555-5555-5555-5555-555555555555'
    function newContext([string]$Authority = 'https://login.microsoftonline.com/', [string]$GraphUrl = 'https://graph.microsoft.com/', [string]$Name = 'AzureCloud') {
        [pscustomobject]@{
            Tenant      = [pscustomobject]@{ Id = $global:WebAppSetupTest.TenantId }
            Account     = [pscustomobject]@{ Id = 'admin@contoso.example'; Type = 'User' }
            Environment = [pscustomobject]@{ Name = $Name; ActiveDirectoryAuthority = $Authority; ExtendedProperties = @{ MicrosoftGraphUrl = $GraphUrl } }
        }
    }
    function respond($Content, [int]$Status = 200) {
        [pscustomobject]@{ StatusCode = $Status; Content = $(if ($null -eq $Content) { '' } else { $Content | ConvertTo-Json -Depth 20 }) }
    }
    function invokeSetup([hashtable]$Parameters) {
        $global:WebAppSetupTest.Calls = [System.Collections.Generic.List[object]]::new()
        $base = @{ SubscriptionId = $script:SubscriptionId; ResourceGroupName = 'rg-azadspi'; WebAppName = 'azadspi-contoso'; AuthorizedGroupObjectId = $script:GroupId }
        foreach ($key in $Parameters.Keys) { $base[$key] = $Parameters[$key] }
        & $script:SetupScript @base
    }
    function getCalls([string]$Method, [string]$Like) {
        @($global:WebAppSetupTest.Calls.where({ $_.Method -eq $Method -and $_.Target -like $Like }))
    }
    function getCallIndex([string]$Method, [string]$Like) {
        for ($i = 0; $i -lt $global:WebAppSetupTest.Calls.Count; $i++) { if ($global:WebAppSetupTest.Calls[$i].Method -eq $Method -and $global:WebAppSetupTest.Calls[$i].Target -like $Like) { return $i } }
        return -1
    }
}

AfterAll {
    Remove-Variable -Name WebAppSetupTest -Scope Global -ErrorAction SilentlyContinue
}

Describe 'getArmGuid' {
    #the values of the ARM template function guid() for the same input (deployment outputs, 2026-11-11)
    It 'returns what guid(<Values>) returns in a template' -ForEach @(
        @{ Values = @('a'); Expected = '3703365d-5a9f-59b4-bca7-b9681389e4c1' }
        @{ Values = @('a', 'b', 'c'); Expected = 'a98fa280-e03f-5afa-8d8e-92d0a9c201e8' }
        @{ Values = @('/subscriptions/00000000-0000-0000-0000-000000000000/resourceGroups/rg/providers/Microsoft.Web/sites/azadspi-contoso', '11111111-1111-1111-1111-111111111111', 'de139f84-1756-47ae-9be6-808fbbe84772'); Expected = '699a50d3-081b-5e85-989b-b4772482618d' }
        @{ Values = @('Ä ü ß'); Expected = '841f47f8-a5fa-59b6-9f30-2ea46890a4ea' }
    ) {
        getArmGuid -Values $Values | Should -Be $Expected
    }
}

Describe 'getReportReference' {
    #the same rules as AzADServicePrincipalInsights.ps1 and Get-AzADSPIOutputPath.ps1 (the name of the published report)
    It '<Name>' -ForEach @(
        @{ Name = 'no management group: the tenant ID (Tenant Root Group)'; Ids = @(); Expected = 'tid' }
        @{ Name = 'one management group'; Ids = @('Contoso'); Expected = 'Contoso' }
        @{ Name = 'several: joined by _'; Ids = @('mg1', 'mg2'); Expected = 'mg1_mg2' }
        @{ Name = 'comma separated as in the variables file'; Ids = @('mg1, mg2'); Expected = 'mg1_mg2' }
        @{ Name = 'several with the tenant ID: the tenant ID'; Ids = @('mg1', 'tid'); Expected = 'tid' }
    ) {
        getReportReference -ManagementGroupId $Ids -TenantId 'tid' | Should -Be $Expected
    }
    It 'Get-AzADSPIOutputPath.ps1 still uses these rules' {
        $outputPaths = Get-Content -Raw -LiteralPath (Join-Path $script:RepoRoot 'pwsh/Get-AzADSPIOutputPath.ps1')
        $outputPaths | Should -Match ([regex]::Escape('$managementGroupIds.Count -gt 1 -and $managementGroupIds -contains $TenantId'))
        $outputPaths | Should -Match ([regex]::Escape("`$reference = `$managementGroupIds -join '_'"))
        $outputPaths | Should -Match ([regex]::Escape('"AzADServicePrincipalInsights_$($reference).html"'))
    }
}

Describe 'New-AzADSPIWebApp.ps1 (mocked REST calls)' {
    BeforeEach {
        Mock Write-Host {}
        Mock Start-Sleep {}
        Mock Get-AzContext { newContext }
        Mock Invoke-AzRestMethod {
            $target = $(if ($Path) { $Path } else { $Uri })
            $body = $(if ($Payload) { $Payload | ConvertFrom-Json -AsHashtable })
            $global:WebAppSetupTest.Calls.Add([pscustomobject]@{ Method = $Method; Target = $target; Body = $body; ResourceId = $ResourceId })
            switch -Wildcard ("$Method $target") {
                'GET /subscriptions/*/resourceGroups/rg-azadspi?*' { return respond @{ location = 'westeurope' } }
                'GET *applications(appId=*' { return respond $global:WebAppSetupTest.ExistingApplication }
                'GET *applications?*' { return respond @{ value = @($global:WebAppSetupTest.FoundApplications) } }
                'POST */applications' { return respond @{ id = 'app-object-id'; appId = $global:WebAppSetupTest.AppId; signInAudience = 'AzureADMyOrg'; web = @{ redirectUris = @() } } 201 }
                "GET *servicePrincipals?*appId eq 'pipeline-app-id'*" { return respond @{ value = @(@{ id = '66666666-6666-6666-6666-666666666666' }) } }
                "GET *servicePrincipals?*appId eq '00000003-0000-0000-c000-000000000000'*" {
                    return respond @{ value = @(@{ id = 'graph-sp-id'; oauth2PermissionScopes = @('openid', 'profile', 'email', 'offline_access', 'User.Read' | ForEach-Object { @{ value = $_; id = "scope-$_" } }) }) }
                }
                'GET *servicePrincipals?*' { return respond @{ value = @() } }
                'POST */servicePrincipals' { return respond @{ id = 'sp-object-id' } 201 }
                'GET *oauth2PermissionGrants?*' { return respond @{ value = @() } }
                'GET *directoryObjects/*' {
                    if ($null -eq $global:WebAppSetupTest.PublisherObject) { return [pscustomobject]@{ StatusCode = 404; Content = '{"error":{"code":"Request_ResourceNotFound"}}' } }
                    return respond $global:WebAppSetupTest.PublisherObject
                }
                'GET *appRoleAssignedTo' { return respond @{ value = @() } }
                'GET *federatedIdentityCredentials' { return respond @{ value = @() } }
                'PUT */userAssignedIdentities/*' { return respond @{ id = "$($target.Split('?')[0])"; properties = @{ clientId = 'mi-client-id'; principalId = 'mi-principal-id' } } }
                'PUT */serverfarms/*' { return respond @{ id = "$($target.Split('?')[0])" } }
                'PUT */providers/Microsoft.Web/sites/azadspi-contoso?*' { return respond @{ id = "$($target.Split('?')[0])"; properties = @{ defaultHostName = $global:WebAppSetupTest.DefaultHostName } } }
                default { return respond @{} }
            }
        }
        $global:WebAppSetupTest.FoundApplications = @()
        $global:WebAppSetupTest.ExistingApplication = $null
        $global:WebAppSetupTest.DefaultHostName = 'azadspi-contoso.azurewebsites.net'
        $global:WebAppSetupTest.PublisherObject = @{ '@odata.type' = '#microsoft.graph.servicePrincipal'; displayName = 'pipeline'; servicePrincipalType = 'Application' }
    }

    It 'Assignment: the access control before the authentication of the web app, the federated credential last' {
        invokeSetup @{ ManagementGroupId = 'Contoso'; PublisherPrincipalObjectId = $script:PublisherId }
        $assignmentRequired = getCallIndex 'PATCH' '*/servicePrincipals/sp-object-id'
        $groupAssigned = getCallIndex 'POST' '*/servicePrincipals/sp-object-id/appRoleAssignedTo'
        $consent = getCallIndex 'POST' '*/oauth2PermissionGrants'
        $site = getCallIndex 'PUT' '*/sites/azadspi-contoso?*'
        $authentication = getCallIndex 'PUT' '*/config/authsettingsV2?*'
        $federatedCredential = getCallIndex 'POST' '*/federatedIdentityCredentials'
        @($assignmentRequired, $groupAssigned, $consent, $site, $authentication, $federatedCredential) | Should -Not -Contain -1
        $assignmentRequired | Should -BeLessThan $authentication
        $groupAssigned | Should -BeLessThan $authentication
        $consent | Should -BeLessThan $authentication
        $site | Should -BeLessThan $authentication
        $authentication | Should -BeLessThan $federatedCredential
        (getCalls 'PATCH' '*/servicePrincipals/sp-object-id')[0].Body.appRoleAssignmentRequired | Should -BeTrue
        $group = (getCalls 'POST' '*/appRoleAssignedTo')[0].Body
        $group.principalId | Should -Be $script:GroupId
        $group.appRoleId | Should -Be '00000000-0000-0000-0000-000000000000'
    }

    It 'creates a single tenant app registration with the delegated sign-in permissions and their admin consent' {
        invokeSetup @{ ManagementGroupId = 'Contoso' }
        $application = (getCalls 'POST' '*/applications')[0].Body
        $application.displayName | Should -Be 'AzADSPI-WebApp-azadspi-contoso'
        $application.signInAudience | Should -Be 'AzureADMyOrg'
        $access = (getCalls 'PATCH' '*/applications/app-object-id').where({ $_.Body.requiredResourceAccess })[0].Body.requiredResourceAccess
        $graphAccess = @($access).where({ $_.resourceAppId -eq '00000003-0000-0000-c000-000000000000' })[0]
        @($graphAccess.resourceAccess.id) | Sort-Object | Should -Be (@('scope-email', 'scope-offline_access', 'scope-openid', 'scope-profile'))
        @($graphAccess.resourceAccess.type) | Select-Object -Unique | Should -Be 'Scope'
        $grant = (getCalls 'POST' '*/oauth2PermissionGrants')[0].Body
        $grant.consentType | Should -Be 'AllPrincipals'
        $grant.scope | Should -Be 'openid profile email offline_access'
        $grant.clientId | Should -Be 'sp-object-id'
        $grant.resourceId | Should -Be 'graph-sp-id'
    }

    It 'the web app: secure settings, the report of the pipeline as default document, the managed identity for the client assertion' {
        invokeSetup @{ ManagementGroupId = 'mg1,mg2' }
        $site = (getCalls 'PUT' '*/sites/azadspi-contoso?*')[0].Body
        $site.properties.httpsOnly | Should -BeTrue
        $site.properties.siteConfig.minTlsVersion | Should -Be '1.2'
        $site.properties.siteConfig.ftpsState | Should -Be 'Disabled'
        $site.properties.siteConfig.defaultDocuments | Should -Be @('AzADServicePrincipalInsights_mg1_mg2.html')
        @($site.properties.siteConfig.appSettings).where({ $_.name -eq 'OVERRIDE_USE_MI_FIC_ASSERTION_CLIENTID' })[0].value | Should -Be 'mi-client-id'
        @($site.properties.siteConfig.appSettings).where({ $_.name -eq 'WEBSITE_AUTH_AAD_ALLOWED_TENANTS' })[0].value | Should -Be $script:TenantId
        foreach ($policy in 'ftp', 'scm') { (getCalls 'PUT' "*/basicPublishingCredentialsPolicies/$($policy)?*")[0].Body.properties.allow | Should -BeFalse }
        $auth = (getCalls 'PUT' '*/config/authsettingsV2?*')[0].Body.properties
        $auth.platform.enabled | Should -BeTrue
        $auth.globalValidation.requireAuthentication | Should -BeTrue
        $auth.globalValidation.unauthenticatedClientAction | Should -Be 'RedirectToLoginPage'
        $auth.identityProviders.azureActiveDirectory.registration.clientId | Should -Be $script:AppId
        $auth.identityProviders.azureActiveDirectory.registration.openIdIssuer | Should -Be "https://login.microsoftonline.com/$($script:TenantId)/v2.0"
        $auth.identityProviders.azureActiveDirectory.registration.clientSecretSettingName | Should -Be 'OVERRIDE_USE_MI_FIC_ASSERTION_CLIENTID'
        $auth.identityProviders.azureActiveDirectory.validation.Count | Should -Be 0
    }

    It 'the redirect URI uses the host name Azure gave the web app; the federated credential trusts the managed identity' {
        $global:WebAppSetupTest.DefaultHostName = 'azadspi-contoso-abc123.westeurope-01.azurewebsites.net'
        invokeSetup @{ ManagementGroupId = 'Contoso' }
        $redirect = (getCalls 'PATCH' '*/applications/app-object-id').where({ $_.Body.web })[0]
        $redirect.Body.web.redirectUris | Should -Be @('https://azadspi-contoso-abc123.westeurope-01.azurewebsites.net/.auth/login/aad/callback')
        $redirect.Body.web.implicitGrantSettings.enableIdTokenIssuance | Should -BeFalse
        $credential = (getCalls 'POST' '*/federatedIdentityCredentials')[0].Body
        $credential.issuer | Should -Be "https://login.microsoftonline.com/$($script:TenantId)/v2.0"
        $credential.subject | Should -Be 'mi-principal-id'
        $credential.audiences | Should -Be @('api://AzureADTokenExchange')
    }

    It 'the role assignment of the pipeline identity has the name the template gives it' {
        invokeSetup @{ ManagementGroupId = 'Contoso'; PublisherPrincipalObjectId = $script:PublisherId }
        $siteId = "/subscriptions/$($script:SubscriptionId)/resourceGroups/rg-azadspi/providers/Microsoft.Web/sites/azadspi-contoso"
        $expectedName = getArmGuid -Values @($siteId, $script:PublisherId, 'de139f84-1756-47ae-9be6-808fbbe84772')
        $call = (getCalls 'PUT' '*/providers/Microsoft.Authorization/roleAssignments/*')[0]
        $call.Target | Should -BeLike "$siteId/providers/Microsoft.Authorization/roleAssignments/$($expectedName)?*"
        $call.Body.properties.principalId | Should -Be $script:PublisherId
        $call.Body.properties.roleDefinitionId | Should -BeLike '*/roleDefinitions/de139f84-1756-47ae-9be6-808fbbe84772'
    }

    It 'the object ID of an app registration as pipeline identity: its enterprise application gets the role (seen 2026-11-11: 400 PrincipalTypeNotSupported)' {
        $global:WebAppSetupTest.PublisherObject = @{ '@odata.type' = '#microsoft.graph.application'; displayName = 'pipeline app'; appId = 'pipeline-app-id' }
        invokeSetup @{ ManagementGroupId = 'Contoso'; PublisherPrincipalObjectId = $script:PublisherId }
        $call = (getCalls 'PUT' '*/providers/Microsoft.Authorization/roleAssignments/*')[0]
        $call.Body.properties.principalId | Should -Be '66666666-6666-6666-6666-666666666666'
    }

    It 'stops before creating anything when the pipeline identity does not exist or is no service principal' {
        $global:WebAppSetupTest.PublisherObject = $null
        { invokeSetup @{ ManagementGroupId = 'Contoso'; PublisherPrincipalObjectId = $script:PublisherId } } | Should -Throw '*does not exist in this tenant*'
        getCalls 'PUT' '*' | Should -BeNullOrEmpty
        getCalls 'POST' '*' | Should -BeNullOrEmpty
        $global:WebAppSetupTest.PublisherObject = @{ '@odata.type' = '#microsoft.graph.user'; displayName = 'someone' }
        { invokeSetup @{ ManagementGroupId = 'Contoso'; PublisherPrincipalObjectId = $script:PublisherId } } | Should -Throw '*is a user, not the pipeline identity*'
        getCalls 'PUT' '*' | Should -BeNullOrEmpty
    }

    It 'GroupClaim: no assignment, the groups claim and the group check of the web app' {
        invokeSetup @{ ManagementGroupId = 'Contoso'; AccessControl = 'GroupClaim' }
        getCalls 'PATCH' '*/servicePrincipals/sp-object-id' | Should -BeNullOrEmpty
        getCalls 'POST' '*/appRoleAssignedTo' | Should -BeNullOrEmpty
        (getCalls 'PATCH' '*/applications/app-object-id').where({ $_.Body.groupMembershipClaims })[0].Body.groupMembershipClaims | Should -Be 'SecurityGroup'
        $validation = (getCalls 'PUT' '*/config/authsettingsV2?*')[0].Body.properties.identityProviders.azureActiveDirectory.validation
        $validation.jwtClaimChecks.allowedGroups | Should -Be @($script:GroupId)
        $validation.defaultAuthorizationPolicy.allowedPrincipals.groups | Should -Be @($script:GroupId)
    }

    It 'reuses an existing app registration and its redirect URIs' {
        $global:WebAppSetupTest.FoundApplications = @(@{ id = 'app-object-id'; appId = $script:AppId; signInAudience = 'AzureADMyOrg'; web = @{ redirectUris = @('https://azadspi-contoso.azurewebsites.net/.auth/login/aad/callback') } })
        invokeSetup @{ ManagementGroupId = 'Contoso' }
        getCalls 'POST' '*/applications' | Should -BeNullOrEmpty
        (getCalls 'PATCH' '*/applications/app-object-id').where({ $_.Body.web }) | Should -BeNullOrEmpty -Because 'the redirect URI is there already'
    }

    It 'stops for an app registration that allows accounts of other tenants' {
        $global:WebAppSetupTest.ExistingApplication = @{ id = 'app-object-id'; appId = $script:AppId; signInAudience = 'AzureADMultipleOrgs' }
        { invokeSetup @{ ManagementGroupId = 'Contoso'; EntraClientId = $script:AppId } } | Should -Throw '*other tenants*'
        getCalls 'PUT' '*' | Should -BeNullOrEmpty
    }

    It '-SkipEntra: no Microsoft Graph call; requires -EntraClientId' {
        { invokeSetup @{ ManagementGroupId = 'Contoso'; SkipEntra = $true } } | Should -Throw '*-SkipEntra requires -EntraClientId*'
        invokeSetup @{ ManagementGroupId = 'Contoso'; SkipEntra = $true; EntraClientId = $script:AppId }
        @($global:WebAppSetupTest.Calls.where({ $_.Target -like 'https://*' })) | Should -BeNullOrEmpty
        (getCalls 'PUT' '*/config/authsettingsV2?*')[0].Body.properties.identityProviders.azureActiveDirectory.registration.clientId | Should -Be $script:AppId
    }

    It 'uses the endpoints of the cloud of the Azure context (Azure Government)' {
        Mock Get-AzContext { newContext -Authority 'https://login.microsoftonline.us/' -GraphUrl 'https://graph.microsoft.us/' -Name 'AzureUSGovernment' }
        invokeSetup @{ ManagementGroupId = 'Contoso' }
        $graphCalls = @($global:WebAppSetupTest.Calls.where({ $_.Target -like 'https://*' }))
        $graphCalls.Count | Should -BeGreaterThan 0
        @($graphCalls.where({ $_.Target -notlike 'https://graph.microsoft.us/v1.0/*' -or $_.ResourceId -ne 'https://graph.microsoft.us' })) | Should -BeNullOrEmpty
        (getCalls 'PUT' '*/config/authsettingsV2?*')[0].Body.properties.identityProviders.azureActiveDirectory.registration.openIdIssuer | Should -Be "https://login.microsoftonline.us/$($script:TenantId)/v2.0"
    }
}

Describe 'setup/azure-web-app.json and New-AzADSPIWebApp.ps1 create the same Azure resources' {
    It 'the same api versions' {
        foreach ($type in 'Microsoft.Web/serverfarms', 'Microsoft.Web/sites', 'Microsoft.Web/sites/config', 'Microsoft.Web/sites/basicPublishingCredentialsPolicies') {
            (getTemplateResource $type).apiVersion | Should -Be (getScriptVariable 'webApiVersion') -Because $type
        }
        (getTemplateResource 'Microsoft.ManagedIdentity/userAssignedIdentities').apiVersion | Should -Be (getScriptVariable 'managedIdentityApiVersion')
        (getTemplateResource 'Microsoft.Authorization/roleAssignments').apiVersion | Should -Be (getScriptVariable 'roleAssignmentApiVersion')
    }
    It 'the same SKUs and default' {
        $skuParameter = @($script:SetupAst.ParamBlock.Parameters).where({ $_.Name.VariablePath.UserPath -eq 'Sku' })[0]
        $validateSet = @($skuParameter.Attributes).where({ $_.TypeName.Name -eq 'ValidateSet' })[0]
        @($validateSet.PositionalArguments.Value) | Should -Be @($script:Template.parameters.sku.allowedValues)
        $skuParameter.DefaultValue.Value | Should -Be $script:Template.parameters.sku.defaultValue
    }
    It 'the same names, the same role and the same app setting' {
        $script:Template.variables.appServicePlanName | Should -Be "[format('asp-{0}', parameters('webAppName'))]"
        $script:SetupSource | Should -Match ([regex]::Escape('$planName = "asp-$($WebAppName)"'))
        $script:Template.variables.managedIdentityName | Should -Be "[format('id-{0}-auth', parameters('webAppName'))]"
        $script:SetupSource | Should -Match ([regex]::Escape('$managedIdentityName = "id-$($WebAppName)-auth"'))
        $script:Template.variables.websiteContributorRoleId | Should -Be (getScriptVariable 'websiteContributorRoleId')
        $script:Template.variables.assertionSettingName | Should -Be (getScriptVariable 'assertionSettingName')
        $script:Template.variables.defaultDocument | Should -Be "[format('AzADServicePrincipalInsights_{0}.html', variables('reportReference'))]"
    }
    It 'the same web app settings' {
        $site = getTemplateResource 'Microsoft.Web/sites'
        $site.properties.httpsOnly | Should -BeTrue
        $site.properties.siteConfig.minTlsVersion | Should -Be '1.2'
        $site.properties.siteConfig.scmMinTlsVersion | Should -Be '1.2'
        $site.properties.siteConfig.ftpsState | Should -Be 'Disabled'
        @($site.properties.siteConfig.appSettings.name) | Should -Be @("[variables('assertionSettingName')]", 'WEBSITE_AUTH_AAD_ALLOWED_TENANTS')
        foreach ($policy in 'ftp', 'scm') { (getTemplateResource 'Microsoft.Web/sites/basicPublishingCredentialsPolicies' "*'$policy')]").properties.allow | Should -BeFalse }
        $auth = (getTemplateResource 'Microsoft.Web/sites/config' "*'authsettingsV2')]").properties
        $auth.globalValidation.requireAuthentication | Should -BeTrue
        $auth.globalValidation.unauthenticatedClientAction | Should -Be 'RedirectToLoginPage'
        $auth.globalValidation.Keys | Should -Not -Contain 'excludedPaths' -Because 'Publish-AzADSPIWebApp.ps1 refuses excluded paths that cover the report'
        $auth.identityProviders.azureActiveDirectory.registration.clientSecretSettingName | Should -Be "[variables('assertionSettingName')]"
    }
    It 'the guide has the Deploy to Azure button of the template on the main branch' {
        $guide = Get-Content -Raw -LiteralPath (Join-Path $script:RepoRoot 'setup/azure-web-app.md')
        $m = [regex]::Match($guide, '\(https://portal\.azure\.com/#create/Microsoft\.Template/uri/(?<uri>[^)\s]+)\)')
        $m.Success | Should -BeTrue
        [uri]::UnescapeDataString($m.Groups['uri'].Value) | Should -Be 'https://raw.githubusercontent.com/JulianHayward/AzADServicePrincipalInsights/main/setup/azure-web-app.json'
    }
}
