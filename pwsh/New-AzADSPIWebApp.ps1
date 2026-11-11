<#
.SYNOPSIS
Creates the Azure Web App that shows the AzADServicePrincipalInsights HTML report to the members of a group only.

.DESCRIPTION
REST calls only (Invoke-AzRestMethod, module Az.Accounts); every step reuses what exists, the script can run again. It brings
the web app back to the settings below.
Microsoft Entra (skipped with -SkipEntra):
- app registration (single tenant) with the redirect URI of the web app and the delegated permissions for the sign-in
  (openid, profile, email, offline_access), its service principal and the admin consent for these permissions (with
  'Assignment required' users cannot consent)
- federated identity credential: the user-assigned managed identity of the web app authenticates the app registration,
  there is no client secret to store or renew
- access: 'Assignment' (default) - 'Assignment required' on the enterprise application and the group assigned to it;
  'GroupClaim' - the app registration emits the security groups and the web app accepts only tokens with the group
Azure:
- user-assigned managed identity, App Service plan (Windows), web app (HTTPS only, minimum TLS version 1.2, FTP off, basic authentication for
  FTP and SCM off, default document AzADServicePrincipalInsights_<reference>.html)
- App Service Authentication (Microsoft Entra, login required, client assertion by the managed identity)
- optional: role Website Contributor on the web app for the identity of the pipeline (Publish-AzADSPIWebApp.ps1)
The ARM template setup/azure-web-app.json (Deploy to Azure) creates the same Azure resources - keep both in sync
(tests/WebAppSetup.Tests.ps1 compares them).

Order: the Microsoft Entra access control (assignment or group claim) is set before the web app gets its authentication, and
the authentication before anybody can sign in (the federated credential comes last) - the report is never open to the whole
tenant.

Required permissions: Microsoft Entra 'Cloud Application Administrator' (or 'Application Administrator'), Azure 'Contributor'
on the resource group; for -PublisherPrincipalObjectId 'Owner', 'User Access Administrator' or 'Role Based Access Control
Administrator'.

.PARAMETER SubscriptionId
Subscription of the web app.

.PARAMETER ResourceGroupName
Existing resource group of the web app.

.PARAMETER WebAppName
Name of the web app (globally unique): https://<WebAppName>.azurewebsites.net

.PARAMETER ManagementGroupId
The management group ID(s) AzADServicePrincipalInsights reports on - the same value as the ManagementGroupId of the pipeline
(several: an array or comma separated). The report AzADServicePrincipalInsights_<reference>.html becomes the default document:
<reference> is the management group IDs joined by '_', or the tenant ID for the Tenant Root Group (empty).

.PARAMETER AuthorizedGroupObjectId
Object ID of the security group whose members may view the report.

.PARAMETER AccessControl
Assignment (default; group-based assignment requires Microsoft Entra ID P1 or P2, nested groups are not supported) or
GroupClaim (works without P1; users with more groups than fit into a token get no groups claim and are denied).

.PARAMETER Location
Location of the resources (default: location of the resource group).

.PARAMETER Sku
App Service plan SKU (default B1; F1 is free, without SLA).

.PARAMETER PublisherPrincipalObjectId
Object ID of the service principal or managed identity of the pipeline (the service connection / GitHub identity) - gets the
role Website Contributor on the web app.

.PARAMETER AppRegistrationDisplayName
Display name of the app registration (default AzADSPI-WebApp-<WebAppName>); an existing one with this name is reused.

.PARAMETER EntraClientId
Application (client) ID of an existing app registration to use (instead of the display name).

.PARAMETER SkipEntra
Only the Azure part (for example when an Entra administrator does the Entra part); requires -EntraClientId. The values for the
Entra steps are printed.

.EXAMPLE
./pwsh/New-AzADSPIWebApp.ps1 -SubscriptionId <sub> -ResourceGroupName rg-azadspi -WebAppName azadspi-contoso -ManagementGroupId Contoso -AuthorizedGroupObjectId <group object id> -PublisherPrincipalObjectId <object id of the pipeline identity>
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)][ValidatePattern('^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$')][string]$SubscriptionId,
    [Parameter(Mandatory)][string]$ResourceGroupName,
    [Parameter(Mandatory)][ValidatePattern('^[a-zA-Z0-9][a-zA-Z0-9-]{0,58}[a-zA-Z0-9]$')][string]$WebAppName,
    [string[]]$ManagementGroupId,
    [Parameter(Mandatory)][ValidatePattern('^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$')][string]$AuthorizedGroupObjectId,
    [ValidateSet('Assignment', 'GroupClaim')][string]$AccessControl = 'Assignment',
    [string]$Location,
    [ValidateSet('F1', 'B1', 'B2', 'S1', 'P0v3', 'P1v3')][string]$Sku = 'B1',
    [ValidatePattern('^$|^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$')][string]$PublisherPrincipalObjectId,
    [string]$AppRegistrationDisplayName,
    [ValidatePattern('^$|^[0-9a-fA-F]{8}-([0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}$')][string]$EntraClientId,
    [switch]$SkipEntra
)

$ErrorActionPreference = 'Stop'
$webApiVersion = '2024-04-01'
$managedIdentityApiVersion = '2023-01-31'
$roleAssignmentApiVersion = '2022-04-01'
$graphAppId = '00000003-0000-0000-c000-000000000000'
$signInScopes = @('openid', 'profile', 'email', 'offline_access')
$websiteContributorRoleId = 'de139f84-1756-47ae-9be6-808fbbe84772'
$defaultAppRoleId = '00000000-0000-0000-0000-000000000000'
#App Service Authentication: the app setting that names the managed identity whose token is the client assertion of the app registration
$assertionSettingName = 'OVERRIDE_USE_MI_FIC_ASSERTION_CLIENTID'

function invokeRest {
    #one REST call (ARM with -Path, Microsoft Graph with -Uri); retries 404/400 shortly after Microsoft Entra objects were created
    #(replication); throws on any other error
    param(
        [Parameter(Mandatory)][string]$Method,
        [string]$Path,
        [string]$Uri,
        $Body,
        [int[]]$AllowedStatus = @(),
        [int]$Retries = 0
    )
    $request = @{ Method = $Method }
    if ($Path) { $request.Path = $Path } else { $request.Uri = $Uri; $request.ResourceId = $script:graphUrl }
    if ($null -ne $Body) { $request.Payload = $Body | ConvertTo-Json -Depth 20 -Compress }
    for ($attempt = 0; ; $attempt++) {
        $response = Invoke-AzRestMethod @request
        $status = [int]$response.StatusCode
        if ($status -ge 200 -and $status -lt 300) {
            if ([string]::IsNullOrWhiteSpace($response.Content)) { return $null }
            return $response.Content | ConvertFrom-Json -Depth 20
        }
        if ($status -in $AllowedStatus) { return $null }
        if ($attempt -lt $Retries -and $status -in 400, 404) { Start-Sleep -Seconds 5; continue }
        throw "$Method $($Path)$($Uri) failed ($status): $($response.Content)"
    }
}

function getAll([string]$Uri) {
    #a Microsoft Graph list with all pages
    $next = $Uri
    while ($next) {
        $page = invokeRest -Method GET -Uri $next
        foreach ($item in @($page.value)) { if ($null -ne $item) { $item } }
        $next = $page.'@odata.nextLink'
    }
}

function getArmGuid([string[]]$Values) {
    #the ARM template function guid(): UUID v5 (SHA-1, namespace 11fb06fb-712d-4ddd-98c7-e71bbd588830) over the values joined
    #by '-' - the script and the template give the role assignment the same name
    $namespace = ([guid]'11fb06fb-712d-4ddd-98c7-e71bbd588830').ToByteArray()
    [array]::Reverse($namespace, 0, 4); [array]::Reverse($namespace, 4, 2); [array]::Reverse($namespace, 6, 2)
    #SHA-1 is required by the ARM template function guid() (UUID v5, RFC 4122) - no security use
    $sha1 = [System.Security.Cryptography.SHA1]::Create() # DevSkim: ignore DS126858
    try {
        $hash = $sha1.ComputeHash([byte[]]($namespace + [System.Text.Encoding]::UTF8.GetBytes($Values -join '-')))[0..15] # DevSkim: ignore DS126858
    }
    finally {
        $sha1.Dispose() # DevSkim: ignore DS126858
    }
    $hash[6] = ($hash[6] -band 0x0F) -bor 0x50
    $hash[8] = ($hash[8] -band 0x3F) -bor 0x80
    [array]::Reverse($hash, 0, 4); [array]::Reverse($hash, 4, 2); [array]::Reverse($hash, 6, 2)
    return [guid]::new([byte[]]$hash).ToString()
}

function getReportReference([string[]]$ManagementGroupId, [string]$TenantId) {
    #as AzADServicePrincipalInsights.ps1 / Get-AzADSPIOutputPath.ps1 name the report
    #an array or comma separated (as in the variables file of the pipeline)
    $ids = @($ManagementGroupId | ForEach-Object { $_ -split ',' } | ForEach-Object { $_.Trim() } | Where-Object { $_ })
    if ($ids.Count -eq 0) { return $TenantId }
    if ($ids.Count -gt 1 -and $ids -contains $TenantId) { return $TenantId }
    return $ids -join '_'
}

#region context
$context = Get-AzContext
if (-not $context) { throw 'No Azure context - Connect-AzAccount first' }
$tenantId = $context.Tenant.Id
Write-Host "Tenant: $tenantId; account: $($context.Account.Id) ($($context.Account.Type)); environment: $($context.Environment.Name)"
#the endpoints of the cloud of the context (Azure public cloud, Azure Government, Azure operated by 21Vianet)
$script:graphUrl = $(if ($context.Environment.ExtendedProperties -and $context.Environment.ExtendedProperties['MicrosoftGraphUrl']) { $context.Environment.ExtendedProperties['MicrosoftGraphUrl'].TrimEnd('/') } else { 'https://graph.microsoft.com' })
$graph = "$($script:graphUrl)/v1.0"
$issuer = "$($context.Environment.ActiveDirectoryAuthority.TrimEnd('/'))/$($tenantId)/v2.0"
if ($SkipEntra -and -not $EntraClientId) { throw '-SkipEntra requires -EntraClientId (the app registration created by the Entra administrator)' }
if (-not $AppRegistrationDisplayName) { $AppRegistrationDisplayName = "AzADSPI-WebApp-$($WebAppName)" }
$resourceGroupPath = "/subscriptions/$($SubscriptionId)/resourceGroups/$($ResourceGroupName)"
$resourceGroup = invokeRest -Method GET -Path "$($resourceGroupPath)?api-version=2021-04-01"
if (-not $Location) { $Location = $resourceGroup.location }
$reportReference = getReportReference -ManagementGroupId $ManagementGroupId -TenantId $tenantId
$defaultDocument = "AzADServicePrincipalInsights_$($reportReference).html"
Write-Host "Report: $($defaultDocument)"
#endregion context

#region Microsoft Entra: app registration, service principal, consent, access control
$application = $null
$servicePrincipal = $null
if (-not $SkipEntra) {
    Write-Host "Microsoft Entra: app registration '$($AppRegistrationDisplayName)'"
    if ($EntraClientId) {
        $application = invokeRest -Method GET -Uri "$($graph)/applications(appId='$($EntraClientId)')"
    }
    else {
        $found = @(getAll -Uri "$($graph)/applications?`$filter=displayName eq '$($AppRegistrationDisplayName -replace "'", "''")'")
        if ($found.Count -gt 1) { throw "$($found.Count) app registrations named '$($AppRegistrationDisplayName)' - use -EntraClientId" }
        $application = if ($found.Count -eq 1) { $found[0] }
        else {
            Write-Host ' creating the app registration (single tenant)'
            invokeRest -Method POST -Uri "$($graph)/applications" -Body @{ displayName = $AppRegistrationDisplayName; signInAudience = 'AzureADMyOrg' }
        }
    }
    if ($application.signInAudience -ne 'AzureADMyOrg') { throw "The app registration $($application.appId) allows accounts of other tenants ($($application.signInAudience)) - use a single tenant app registration" }
    $EntraClientId = $application.appId
    Write-Host " Application (client) ID: $($EntraClientId)"

    #sign-in permissions (delegated, Microsoft Graph) and - for GroupClaim - the security groups in the token; the redirect URI
    #follows when the host name of the web app is known
    $graphServicePrincipal = @(getAll -Uri "$($graph)/servicePrincipals?`$filter=appId eq '$($graphAppId)'&`$select=id,oauth2PermissionScopes")[0]
    $scopeIds = foreach ($scope in $signInScopes) { ($graphServicePrincipal.oauth2PermissionScopes | Where-Object value -EQ $scope).id }
    $requiredResourceAccess = [System.Collections.Generic.List[object]]::new()
    $graphAccess = $null
    foreach ($entry in @($application.requiredResourceAccess)) {
        if ($null -eq $entry) { continue }
        if ($entry.resourceAppId -eq $graphAppId) { $graphAccess = $entry } else { $requiredResourceAccess.Add($entry) }
    }
    $graphResourceAccess = [System.Collections.Generic.List[object]]::new()
    if ($graphAccess) { foreach ($access in @($graphAccess.resourceAccess)) { $graphResourceAccess.Add($access) } }
    foreach ($scopeId in $scopeIds) {
        if (-not ($graphResourceAccess | Where-Object { $_.id -eq $scopeId })) { $graphResourceAccess.Add([ordered]@{ id = $scopeId; type = 'Scope' }) }
    }
    $requiredResourceAccess.Add([ordered]@{ resourceAppId = $graphAppId; resourceAccess = $graphResourceAccess })
    $applicationPatch = @{ requiredResourceAccess = $requiredResourceAccess }
    if ($AccessControl -eq 'GroupClaim') { $applicationPatch.groupMembershipClaims = 'SecurityGroup' }
    $null = invokeRest -Method PATCH -Uri "$($graph)/applications/$($application.id)" -Body $applicationPatch -Retries 6
    Write-Host " sign-in permissions: $($signInScopes -join ', ')"

    $servicePrincipal = @(getAll -Uri "$($graph)/servicePrincipals?`$filter=appId eq '$($EntraClientId)'")[0]
    if (-not $servicePrincipal) {
        Write-Host ' creating the service principal (enterprise application)'
        $servicePrincipal = invokeRest -Method POST -Uri "$($graph)/servicePrincipals" -Body @{ appId = $EntraClientId } -Retries 6
    }

    #admin consent: with 'Assignment required' users cannot consent themselves
    $grant = @(getAll -Uri "$($graph)/oauth2PermissionGrants?`$filter=clientId eq '$($servicePrincipal.id)' and consentType eq 'AllPrincipals' and resourceId eq '$($graphServicePrincipal.id)'")[0]
    if ($grant) {
        $grantedScopes = @(([string]$grant.scope).Split(' ', [System.StringSplitOptions]::RemoveEmptyEntries))
        $missingScopes = @($signInScopes.where({ $_ -notin $grantedScopes }))
        if ($missingScopes.Count -gt 0) { $null = invokeRest -Method PATCH -Uri "$($graph)/oauth2PermissionGrants/$($grant.id)" -Body @{ scope = (@($grantedScopes) + $missingScopes) -join ' ' } }
    }
    else {
        Write-Host "Microsoft Entra: admin consent for $($signInScopes -join ', ')"
        $null = invokeRest -Method POST -Uri "$($graph)/oauth2PermissionGrants" -Body @{ clientId = $servicePrincipal.id; consentType = 'AllPrincipals'; resourceId = $graphServicePrincipal.id; scope = $signInScopes -join ' ' } -Retries 6
    }

    if ($AccessControl -eq 'Assignment') {
        Write-Host "Microsoft Entra: 'Assignment required' and the group $($AuthorizedGroupObjectId) assigned to the enterprise application"
        $null = invokeRest -Method PATCH -Uri "$($graph)/servicePrincipals/$($servicePrincipal.id)" -Body @{ appRoleAssignmentRequired = $true } -Retries 6
        $assignments = @(getAll -Uri "$($graph)/servicePrincipals/$($servicePrincipal.id)/appRoleAssignedTo")
        if (-not ($assignments | Where-Object { $_.principalId -eq $AuthorizedGroupObjectId })) {
            $null = invokeRest -Method POST -Uri "$($graph)/servicePrincipals/$($servicePrincipal.id)/appRoleAssignedTo" -Body @{ principalId = $AuthorizedGroupObjectId; resourceId = $servicePrincipal.id; appRoleId = $defaultAppRoleId } -Retries 6
        }
    }
}
#endregion Microsoft Entra: app registration, service principal, consent, access control

#region Azure: managed identity, plan, web app, authentication
$managedIdentityName = "id-$($WebAppName)-auth"
$managedIdentityPath = "$($resourceGroupPath)/providers/Microsoft.ManagedIdentity/userAssignedIdentities/$($managedIdentityName)"
Write-Host "Azure: managed identity '$($managedIdentityName)' (only for the sign-in of the web app - assign it to nothing else)"
$managedIdentity = invokeRest -Method PUT -Path "$($managedIdentityPath)?api-version=$($managedIdentityApiVersion)" -Body @{ location = $Location }

$planName = "asp-$($WebAppName)"
Write-Host "Azure: App Service plan '$($planName)' ($($Sku))"
$plan = invokeRest -Method PUT -Path "$($resourceGroupPath)/providers/Microsoft.Web/serverfarms/$($planName)?api-version=$($webApiVersion)" -Body @{ location = $Location; sku = @{ name = $Sku } }

$sitePath = "$($resourceGroupPath)/providers/Microsoft.Web/sites/$($WebAppName)"
Write-Host "Azure: web app '$($WebAppName)'"
$siteBody = @{
    location   = $Location
    identity   = @{ type = 'UserAssigned'; userAssignedIdentities = @{ $managedIdentity.id = @{} } }
    properties = @{
        serverFarmId        = $plan.id
        httpsOnly           = $true
        publicNetworkAccess = 'Enabled'
        siteConfig          = @{
            minTlsVersion    = '1.2'
            scmMinTlsVersion = '1.2'
            ftpsState        = 'Disabled'
            http20Enabled    = $true
            defaultDocuments = @($defaultDocument)
            appSettings      = @(
                @{ name = $assertionSettingName; value = $managedIdentity.properties.clientId }
                @{ name = 'WEBSITE_AUTH_AAD_ALLOWED_TENANTS'; value = $tenantId }
            )
        }
    }
}
$site = invokeRest -Method PUT -Path "$($sitePath)?api-version=$($webApiVersion)" -Body $siteBody
foreach ($policy in 'ftp', 'scm') {
    #Publish-AzADSPIWebApp.ps1 (Publish-AzWebApp) deploys with a Microsoft Entra token
    $null = invokeRest -Method PUT -Path "$($sitePath)/basicPublishingCredentialsPolicies/$($policy)?api-version=$($webApiVersion)" -Body @{ properties = @{ allow = $false } }
}
$null = invokeRest -Method PUT -Path "$($sitePath)/config/slotConfigNames?api-version=$($webApiVersion)" -Body @{ properties = @{ appSettingNames = @($assertionSettingName) } }

$validation = @{}
if ($AccessControl -eq 'GroupClaim') {
    $validation = @{
        jwtClaimChecks             = @{ allowedGroups = @($AuthorizedGroupObjectId) }
        defaultAuthorizationPolicy = @{ allowedPrincipals = @{ groups = @($AuthorizedGroupObjectId) } }
    }
}
Write-Host "Azure: App Service Authentication (Microsoft Entra, login required, access control: $($AccessControl))"
$authSettings = @{
    properties = @{
        platform          = @{ enabled = $true }
        globalValidation  = @{ requireAuthentication = $true; unauthenticatedClientAction = 'RedirectToLoginPage'; redirectToProvider = 'azureactivedirectory' }
        httpSettings      = @{ requireHttps = $true }
        login             = @{ tokenStore = @{ enabled = $true } }
        identityProviders = @{
            azureActiveDirectory = @{
                enabled      = $true
                registration = @{ openIdIssuer = $issuer; clientId = $EntraClientId; clientSecretSettingName = $assertionSettingName }
                validation   = $validation
            }
        }
    }
}
$null = invokeRest -Method PUT -Path "$($sitePath)/config/authsettingsV2?api-version=$($webApiVersion)" -Body $authSettings

if ($PublisherPrincipalObjectId) {
    Write-Host "Azure: role 'Website Contributor' on the web app for the pipeline identity $($PublisherPrincipalObjectId)"
    #the name the ARM template gives it; an existing assignment under another name is accepted (409)
    $roleAssignmentName = getArmGuid -Values @($site.id, $PublisherPrincipalObjectId, $websiteContributorRoleId)
    $null = invokeRest -Method PUT -Path "$($sitePath)/providers/Microsoft.Authorization/roleAssignments/$($roleAssignmentName)?api-version=$($roleAssignmentApiVersion)" -AllowedStatus 409 -Body @{
        properties = @{ roleDefinitionId = "/subscriptions/$($SubscriptionId)/providers/Microsoft.Authorization/roleDefinitions/$($websiteContributorRoleId)"; principalId = $PublisherPrincipalObjectId; principalType = 'ServicePrincipal' }
    }
}
#endregion Azure: managed identity, plan, web app, authentication

#region Microsoft Entra: redirect URI and federated credential (until they exist nobody can sign in)
#the host name of the web app as Azure set it (<name>.azurewebsites.net, or another domain in other clouds)
$hostName = $site.properties.defaultHostName
$redirectUri = "https://$($hostName)/.auth/login/aad/callback"
$federatedCredential = [ordered]@{ name = "AzADSPIWebApp-$($WebAppName)"; issuer = $issuer; subject = $managedIdentity.properties.principalId; audiences = @('api://AzureADTokenExchange') }
if ($SkipEntra) {
    Write-Host ''
    Write-Host "Microsoft Entra steps (not done, -SkipEntra) for the app registration $($EntraClientId) - in this order:" -ForegroundColor Yellow
    Write-Host ' 1. delegated Microsoft Graph permissions openid, profile, email, offline_access with admin consent'
    if ($AccessControl -eq 'Assignment') { Write-Host " 2. enterprise application: 'Assignment required?' Yes; assign the group $($AuthorizedGroupObjectId)" }
    else { Write-Host " 2. app registration: groupMembershipClaims 'SecurityGroup' (Token configuration > Add groups claim > Security groups)" }
    Write-Host " 3. redirect URI (platform Web): $($redirectUri)"
    Write-Host " 4. last - until then nobody can sign in: federated credential (scenario 'Managed identity' > $($managedIdentityName), or 'Other issuer'): issuer $($federatedCredential.issuer); subject $($federatedCredential.subject); audience api://AzureADTokenExchange"
}
else {
    $redirectUris = [System.Collections.Generic.List[string]]::new()
    foreach ($uri in @($application.web.redirectUris)) { if ($uri) { $redirectUris.Add($uri) } }
    if (-not $redirectUris.Contains($redirectUri)) {
        $redirectUris.Add($redirectUri)
        $null = invokeRest -Method PATCH -Uri "$($graph)/applications/$($application.id)" -Body @{ web = @{ redirectUris = $redirectUris; implicitGrantSettings = @{ enableIdTokenIssuance = $false; enableAccessTokenIssuance = $false } } } -Retries 6
    }
    Write-Host "Microsoft Entra: redirect URI $($redirectUri)"
    $credentials = @(getAll -Uri "$($graph)/applications/$($application.id)/federatedIdentityCredentials")
    if (-not ($credentials | Where-Object { $_.issuer -eq $federatedCredential.issuer -and $_.subject -eq $federatedCredential.subject })) {
        Write-Host 'Microsoft Entra: federated credential for the managed identity (no client secret)'
        $null = invokeRest -Method POST -Uri "$($graph)/applications/$($application.id)/federatedIdentityCredentials" -Body $federatedCredential -Retries 6
    }
}
#endregion Microsoft Entra: redirect URI and federated credential (until they exist nobody can sign in)

Write-Host ''
Write-Host "Web app: https://$($hostName) (the report appears after the first publish)" -ForegroundColor Green
Write-Host "Pipeline variables: WebAppPublish true, WebAppSubscriptionId '$($SubscriptionId)', WebAppResourceGroup '$($ResourceGroupName)', WebAppName '$($WebAppName)'"
if (-not $PublisherPrincipalObjectId) { Write-Host "The pipeline identity needs the role 'Website Contributor' on the web app (-PublisherPrincipalObjectId)" -ForegroundColor Yellow }
if ($AccessControl -eq 'Assignment') { Write-Host 'Note: assign users directly to the group - members of a group inside the group are not assigned.' }
