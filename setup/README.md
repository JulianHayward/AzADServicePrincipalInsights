# Set up AzADServicePrincipalInsights

**[Setup assistant](https://www.azadvertizer.net/azadserviceprincipalinsights/setup-assistant.html)** asks a few questions (where to run it, how to sign in, which optional features) and gives you the steps for your setup, with your values filled into the commands. It sends nothing and stores nothing. The same page is in this folder ([setup-assistant.html](setup-assistant.html)) - open it from your clone to use it offline.

The Microsoft Entra steps are described in the [Azure portal](https://portal.azure.com) (**Microsoft Entra ID**). The [Microsoft Entra admin center](https://entra.microsoft.com/) has the same pages, if you prefer it.

## Choose your guide

| You want | Guide |
|---|---|
| A report now, on your own computer or in a GitHub Codespace | [Run from the console](console.md) |
| Regular reports, saved in a private GitHub repository | [Run from GitHub](github.md) |
| Regular reports, saved in a private Azure DevOps repository | [Run from Azure DevOps](azure-devops.md) |

After GitHub or Azure DevOps, optionally:

| You want | Guide |
|---|---|
| Colleagues open the report on a website, after signing in | [Publish the report to an Azure Web App](azure-web-app.md) (a script creates it) |
| Query and alert on the data in Log Analytics or Microsoft Sentinel | [Send the output to Log Analytics](log-analytics.md) |

The output contains insights about your tenant: who has which permissions, credentials and owners. Keep the repository private. The GitHub workflows stop if the repository is public, the Azure DevOps pipeline if the project is public, and the Web App publishing stops if the Web App can be opened without signing in. Log Analytics: everybody who can read the workspace can read the table - see [Who can read the table](log-analytics.md#before-you-start).

The guides contain every step you need. The rest of this page is the reference: all permissions and how the parameters are set.

## Which ID is which?

An identity in Microsoft Entra ID has several IDs, and the setup needs the right one in each place:

| ID | Where you find it | Used for |
|---|---|---|
| **Application (client) ID** | **App registrations** > the app > **Overview** (also shown on the enterprise application) | signing in: `CLIENT_ID`, `CREDS`, the service connection, `-EntraClientId` |
| **Object ID of the enterprise application** (service principal) | **Enterprise applications** > the app > **Overview** > **Object ID** | roles and permissions: `-PublisherPrincipalObjectId`, `principalId`, the Microsoft Graph permission script |
| **Object (principal) ID of a managed identity** | **Managed Identities** > the identity > **Overview** | the same as the object ID of an enterprise application - a managed identity has no app registration |
| Object ID of the app registration | **App registrations** > the app > **Overview** > **Object ID** | **not used** in this setup - it is a different object than the enterprise application, and a role assignment to it fails (`PrincipalTypeNotSupported`) |

## Permissions

The identity that runs AzADServicePrincipalInsights only reads. Your user account runs it from the console; a pipeline or workflow uses an app registration or a managed identity.

### Azure

| Scope | Role |
|---|---|
| The management group you pass as `ManagementGroupId` (default: the Tenant Root Group) | '**Reader**' |

### Microsoft Graph

Application permissions with admin consent, for an app registration or a managed identity. A user doesn't need them: a member user reads most of the data with the default user permissions, and a '**Global Reader**' all of it.

| Permission | |
|---|---|
| `Application.Read.All` | required |
| `Group.Read.All` | required |
| `RoleManagement.Read.Directory` | required |
| `User.Read.All` | required |
| `DelegatedPermissionGrant.Read.All` | optional: reads the delegated permission grants once for the tenant instead of once per service principal (faster; the output is the same) |
| `DirectoryRecommendations.Read.All` | optional: Microsoft Entra recommendations for applications |
| `CustomSecAttributeAssignment.Read.All` | optional: custom security attributes of service principals. A user needs '**Attribute Assignment Reader**' |
| `Policy.Read.All` and `Policy.Read.PermissionGrant` | optional: tenant consent settings |

The script tests the optional permissions at the start. Without them, that data is not collected and the report says so.

A '**Privileged Role Administrator**' or '**Global Administrator**' grants them.

**App registration, in the portal**

1. [Azure portal](https://portal.azure.com) > **Microsoft Entra ID** > **App registrations** > **All applications** > your app registration
1. **API permissions** > **Add a permission** > **Microsoft Graph** > **Application permissions**
1. Select the permissions > **Add permissions**
1. **Grant admin consent for _&lt;tenant&gt;_** > **Yes** - the status changes to **Granted for _&lt;tenant&gt;_**

**Managed identity or app registration, with PowerShell**

A managed identity has no **API permissions** page. Use PowerShell (needs only the module `Az.Accounts`); remove the optional permissions you don't want from the list:

```powershell
Connect-AzAccount -TenantId <tenant ID>
$principalId = '<object ID of the enterprise application, or the object (principal) ID of the managed identity>'
$permissions = 'Application.Read.All', 'Group.Read.All', 'RoleManagement.Read.Directory', 'User.Read.All',
    'DelegatedPermissionGrant.Read.All', 'DirectoryRecommendations.Read.All', 'CustomSecAttributeAssignment.Read.All',
    'Policy.Read.All', 'Policy.Read.PermissionGrant'

$graphUrl = 'https://graph.microsoft.com' # Azure Government: https://graph.microsoft.us; Azure operated by 21Vianet: https://microsoftgraph.chinacloudapi.cn

$response = Invoke-AzRestMethod -Uri "$graphUrl/v1.0/servicePrincipals(appId='00000003-0000-0000-c000-000000000000')"
if ($response.StatusCode -ne 200) { throw "Microsoft Graph could not be read: $($response.Content)" }
$graph = $response.Content | ConvertFrom-Json
$response = Invoke-AzRestMethod -Uri "$graphUrl/v1.0/servicePrincipals/$principalId/appRoleAssignments"
if ($response.StatusCode -ne 200) { throw "The permissions of $principalId could not be read: $($response.Content)" }
$granted = @(($response.Content | ConvertFrom-Json).value.appRoleId)
foreach ($permission in $permissions) {
    $appRoleId = ($graph.appRoles | Where-Object { $_.value -eq $permission -and $_.allowedMemberTypes -contains 'Application' }).id
    if (-not $appRoleId) { "$permission - not found in Microsoft Graph (check the name)"; continue }
    if ($appRoleId -in $granted) { "$permission - already granted"; continue }
    $body = @{ principalId = $principalId; resourceId = $graph.id; appRoleId = $appRoleId } | ConvertTo-Json
    $result = Invoke-AzRestMethod -Method POST -Uri "$graphUrl/v1.0/servicePrincipals/$principalId/appRoleAssignments" -Payload $body
    "$permission - $(if ($result.StatusCode -eq 201) { 'granted' } else { $result.Content })"
}
```

Check: Azure portal > **Microsoft Entra ID** > **Enterprise applications** > your identity (a managed identity: filter **Application type** = **Managed Identities**) > **Permissions** lists the permissions.

### Optional features

| Feature | Role | Scope |
|---|---|---|
| [Web App publishing](azure-web-app.md) | '**Website Contributor**' | the Web App |
| [Log Analytics ingestion](log-analytics.md) | '**Monitoring Metrics Publisher**' and '**Monitoring Reader**' | the Data Collection Rule |

## Parameters

Every parameter of the script is described in the [README](../README.md#parameters). Pipelines and workflows set them in a variables file, one entry per parameter, with the same names on both platforms:

- GitHub: [.github/AzADServicePrincipalInsights.variables.yml](../.github/AzADServicePrincipalInsights.variables.yml)
- Azure DevOps: [.azuredevops/pipelines/AzADServicePrincipalInsights.variables.yml](../.azuredevops/pipelines/AzADServicePrincipalInsights.variables.yml)

An empty value means "not set" (the script default applies). A switch is `true`/`false`, a list is comma separated (`mgId0,mgId1`). Each entry in the file documents its type, default and an example.

Both files start with the settings of the pipeline/workflow (Azure DevOps: `ServiceConnection`, `ScriptDir`; GitHub: `ScriptDir`); from the section 'Required' on they have the same content. The optional features have their own sections, each with its switch first: 'Log Analytics ingestion' (`IngestToLogAnalytics`) and 'Web App publishing' (`WebAppPublish`). The schedule (and on GitHub the sign-in) is in the pipeline/workflow file.
