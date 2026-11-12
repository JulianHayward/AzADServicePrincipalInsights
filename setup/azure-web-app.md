# Publish the report to an Azure Web App

After every run, your pipeline or workflow can upload the report to a website in Azure. Your colleagues open a URL and sign in with their normal Microsoft account - **only the members of one group can see the report**, everybody else is turned away.

This guide takes you through it step by step. Plan about 30 minutes.

**Before this guide:** your pipeline or workflow runs successfully - [GitHub](github.md) or [Azure DevOps](azure-devops.md).

## What you will end up with

```mermaid
flowchart LR
    pipeline["Your pipeline or workflow<br/>(GitHub or Azure DevOps)"] -- "uploads the report<br/>after every run" --> webapp["Azure Web App<br/>https://&lt;name&gt;.azurewebsites.net"]
    member["Colleague in the<br/>viewer group"] -- "opens the URL,<br/>signs in" --> signin["Microsoft sign-in"]
    other["Anyone else"] -- "opens the URL,<br/>signs in" --> signin
    signin -- "allowed" --> webapp
    signin -. "denied" .-> other
```

| Component | What it is for |
|---|---|
| **Azure Web App** (with an App Service plan) | The website that shows the report. |
| **Sign-in for the web app** (an app registration in Microsoft Entra ID) | Makes everybody sign in before they see anything. |
| **Viewer group** (a security group in Microsoft Entra ID) | The people who may see the report. You manage access by adding or removing members. |
| **Managed identity** | Lets the sign-in work without a password. Nothing expires, nothing to renew. |
| **Your pipeline identity** | The account your pipeline or workflow already uses. It gets permission to upload to the web app. |

You don't have to create these one by one - a script does it for you (step 2).

The publish step of the pipeline ([pwsh/Publish-AzADSPIWebApp.ps1](../pwsh/Publish-AzADSPIWebApp.ps1)) checks the web app before every upload: it never publishes to a web app that can be opened without signing in.

## Before you start

The Microsoft Entra steps are described in the [Azure portal](https://portal.azure.com) (**Microsoft Entra ID**). The [Microsoft Entra admin center](https://entra.microsoft.com/) has the same pages, if you prefer it.

### Question 1: Does your tenant have Microsoft Entra ID P1 or P2?

Check it: [Azure portal](https://portal.azure.com) > **Microsoft Entra ID** > **Overview** > field **License**.

| License | Your setting | What happens |
|---|---|---|
| **P1 or P2** | `Assignment` | Microsoft Entra only lets the group members sign in to the web app. |
| **Free** (or you are not sure) | `GroupClaim` | Everybody in your tenant can sign in, but the web app only shows the report to the group members. |

Both keep the report away from people outside the group. Good to know:

- With `Assignment`: put the users **directly** into the viewer group - a group inside the group doesn't count.
- With `GroupClaim`: users who are members of a very large number of groups (more than about 200) can be turned away although they are in the viewer group.

### Question 2: Who does the setup?

| Situation | Your path |
|---|---|
| **One person** has both roles: '**Cloud Application Administrator**' (or '**Application Administrator**') in Microsoft Entra ID **and** '**Owner**' of the Azure resource group | **Path A** - run one script, done |
| **Two teams**: an Entra admin and an Azure admin | **Path B** - the Entra admin and the Azure admin each do their part ([appendix](#path-b---two-teams-entra-admin-and-azure-admin)) |

## Step 1 - Prepare

Collect these values. You need them in the next steps.

| Value | Example | Where to find it |
|---|---|---|
| Tenant ID | `11111111-...` | Azure portal > **Microsoft Entra ID** > **Overview** > **Tenant ID** |
| Subscription ID for the web app | `22222222-...` | Azure portal > **Subscriptions** |
| Resource group for the web app | `rg-azadspi` | Create one in the Azure portal if you don't have one yet |
| Web app name | `azadspi-contoso` | Your choice - it becomes the URL `https://azadspi-contoso.azurewebsites.net` and must be unique worldwide |
| Management group ID | `Contoso` | The same value your pipeline uses (`ManagementGroupId` in the variables file; several: comma separated; empty: the Tenant Root Group) |
| Object ID of the viewer group | `33333333-...` | Azure portal > **Microsoft Entra ID** > **Groups** > your group > **Object ID**. Create a **security** group if you don't have one |
| Object ID of the pipeline identity's enterprise application | `44444444-...` | Azure portal > **Microsoft Entra ID** > **Enterprise applications** (not **App registrations** - that object ID is a different one; see [Which ID is which?](README.md#which-id-is-which)) > search the name of the identity your pipeline uses (step 2 of the [GitHub](github.md) or [Azure DevOps](azure-devops.md) guide; a managed identity: filter **Application type** = **Managed Identities**) > **Object ID** |
| Your setting from question 1 | `Assignment` or `GroupClaim` | |

> **Checkpoint 1:** all values are filled in, and the viewer group has at least one test user in it.

## Step 2 - Create the web app (path A)

Open PowerShell 7 in the folder of this repository and run the following - replace the values with yours from step 1:

```powershell
Connect-AzAccount -TenantId <Tenant ID>

./pwsh/New-AzADSPIWebApp.ps1 `
  -SubscriptionId <Subscription ID> `
  -ResourceGroupName <Resource group> `
  -WebAppName <Web app name> `
  -ManagementGroupId <Management group ID> `
  -AuthorizedGroupObjectId <Object ID of the viewer group> `
  -PublisherPrincipalObjectId <Object ID of the pipeline identity's enterprise application> `
  -AccessControl <Assignment or GroupClaim>
```

Tenant Root Group (an empty `ManagementGroupId` in the pipeline): remove the line `-ManagementGroupId`.

The script needs only the module `Az.Accounts` and takes a few minutes. It first checks that the web app name is still free - a taken name stops it before anything is created. You can run it again at any time - it reuses what exists and sets the web app back to its secure settings: its app settings and authentication are written again (changes made by hand in the portal are overwritten); the App Service plan keeps its size and region unless you pass `-Sku` or `-Location`.

> **Checkpoint 2:** the script ends with a green line `Web app: https://<name>.azurewebsites.net (the report appears after the first publish)`. Open that URL in a private browser window:
> 1. you are sent to the **Microsoft sign-in page**;
> 2. sign in as a member of the viewer group: you see an error page - that's expected, the report isn't there yet;
> 3. sign in (another private window) as somebody who is **not** in the viewer group: they are turned away, as described in [step 4](#step-4---check-who-can-see-the-report). If they get the same error page as the member instead, stop here - the access control isn't set up (Path B: step 1, last point) and the report would be open to the whole tenant.

## Step 3 - Connect your pipeline

Tell the pipeline where to upload the report - in the variables file of your pipeline, section 'Web App publishing':

| Variable | Value |
|---|---|
| `WebAppPublish` | `true` |
| `WebAppSubscriptionId` | the subscription ID from step 1 |
| `WebAppResourceGroup` | the resource group from step 1 |
| `WebAppName` | the web app name from step 1 |

- **GitHub:** `.github/AzADServicePrincipalInsights.variables.yml`
- **Azure DevOps:** `.azuredevops/pipelines/AzADServicePrincipalInsights.variables.yml`

Then run the pipeline or workflow.

> **Checkpoint 3:** in the run, the step **Publish HTML to WebApp** shows `Published '.../AzADServicePrincipalInsights_<management group ID>.html'` (several management groups: `..._<ID1>_<ID2>.html`; the Tenant Root Group: `..._<tenant ID>.html`) and the URL of the report.

Right after step 2 the step can fail with `403` / `AuthorizationFailed`: a new role assignment can take up to about 30 minutes to work. Run it again later.

## Step 4 - Check who can see the report

1. A **member** of the viewer group opens the URL and signs in: they see the report.
1. Somebody who is **not** in the group opens the URL and signs in: they are turned away.
   - with `Assignment`: Microsoft shows a message that the user is not assigned to the application
   - with `GroupClaim`: the web app shows an "access denied" (403) page

The report loads styles and scripts from `www.azadvertizer.net`, `cdnjs.cloudflare.com` and `cdn.jsdelivr.net`, so the readers' browsers need access to these hosts.

> **Checkpoint 4:** both tests behave as described - you're done. From now on, every run updates the report, and you manage who can see it by adding or removing members of the viewer group.

## If something doesn't work

| What you see | Why | What to do |
|---|---|---|
| Pipeline: `HTML NOT published - the WebApp (...) has no Authentication configured` (or `allows unauthenticated requests`, or `excludes paths from the authentication`) | The web app doesn't require a sign-in for the report. The pipeline never publishes to an open website. | Run the script from step 2 again. |
| Pipeline: `The authentication settings of the WebApp (...) could not be read` (with `403` / `AuthorizationFailed` above) | The pipeline identity has no '**Website Contributor**' role on the web app yet, or the role assignment is new. | A new role assignment can take up to about 30 minutes - run it again later. Otherwise assign the role: the script of step 2 with `-PublisherPrincipalObjectId` (the object ID of the pipeline identity's enterprise application). |
| Pipeline: `HTML NOT published (...) - check: the WebApp (...) is running; its advanced tool site (SCM/Kudu) accepts requests from the pipeline agent ...` | The upload failed after the settings were read - the role works. Usually the web app is stopped, or access restrictions / a private endpoint of the advanced tool site (`<name>.scm.azurewebsites.net`) block the runner or agent. | Start the web app; allow the agent's network on the advanced tool site (**Networking** > **Access restriction** > **Advanced tool site**). The message above it names git's or Azure's reason. |
| Pipeline: `HTML NOT published - the authentication of the WebApp (...) is configured by the file '...'` | The web app reads its authentication settings from a file; the pipeline cannot check it. | Configure the authentication in the web app settings instead (run the script of step 2 again). |
| Script: `... failed (400): ... PrincipalTypeNotSupported` | An older version of the script got the object ID of the app registration instead of the enterprise application. The current script uses the enterprise application itself. | Run the current script again; it reuses what exists. |
| Step **Create arguments**: `Variable 'WebAppSubscriptionId' = '<YourWebAppSubscriptionId>' - provide the value ...` (or `Variable(s) not set: ...`) | `WebAppPublish` is `true`, but a `WebApp*` value is empty or still the placeholder. | Step 3. |
| Script: `The web app name '...' is not available` | Web app names are unique worldwide. | Choose another `-WebAppName`. Nothing was created. |
| Script: `... failed (409): ... MissingSubscriptionRegistration` | The subscription never hosted a web app or a managed identity: the resource providers `Microsoft.Web` and `Microsoft.ManagedIdentity` are not registered. | Somebody with the right on the subscription registers them (**Subscriptions** > **Resource providers** > **Register**); then run the script again. |
| Pipeline: `'...AzADServicePrincipalInsights_<ID>.html' not found` | The management group ID in the pipeline and the file name differ (also in upper/lower case). | Use exactly the same management group ID everywhere. |
| After signing in, the URL shows an error page instead of the report | The default document is a different report name: the script got another management group ID than the pipeline. | Run the script again with the `ManagementGroupId` of the pipeline. |
| A group member gets "not assigned to the application" (`Assignment`) | The user is only in a group *inside* the viewer group. | Add the user directly to the viewer group. |
| A group member gets "access denied" (`GroupClaim`) | The user is in very many groups. | Use `Assignment` if you have P1/P2, or ask your Entra admin about the user's group memberships. |
| Everybody gets an error right after signing in, code `AADSTS700054` (`response_type 'id_token' is not enabled`) | **ID tokens** are off in the app registration - the sign-in of the web app needs them. | Path A: run the current script again. Path B: the Entra admin ticks **ID tokens** (step 3 of Path B). |
| Everybody gets an error right after signing in, code `AADSTS50011`, `AADSTS70021`, `AADSTS70025` or `AADSTS700213` | The sign-in of the web app is not finished (redirect URI, or the managed identity isn't connected to the app registration yet). | Path A: run the script again. Path B: check the last step of the Entra admin. |

## Path B - two teams (Entra admin and Azure admin)

Do the steps in this order - then the web app is never open to the whole tenant.

**1. Entra admin - create the sign-in** ([Azure portal](https://portal.azure.com) > **Microsoft Entra ID**)

1. **App registrations** > **New registration**: name e.g. `AzADSPI-WebApp-<web app name>`, keep **Accounts in this organizational directory only** > **Register**
1. Note the **Application (client) ID** - the Azure admin needs it
1. **API permissions** > **Add a permission** > **Microsoft Graph** > **Delegated permissions**: `openid`, `profile`, `email`, `offline_access` > **Add permissions** > **Grant admin consent for _&lt;tenant&gt;_**
1. Access, depending on question 1:
   - `Assignment`: **Enterprise applications** > the same name > **Properties** > **Assignment required?** **Yes** > **Save**; then **Users and groups** > **Add user/group** > the viewer group > **Assign**. Don't skip it: the web app accepts every user of the tenant that Microsoft Entra lets sign in
   - `GroupClaim`: back in the app registration: **Token configuration** > **Add groups claim** > **Security groups** > **Add**

**2. Azure admin - create the web app**, either

- with the script (step 2 above) plus `-SkipEntra -EntraClientId <Application (client) ID>` - it prints the values the Entra admin needs next, or
- with this button: [![Deploy to Azure](https://aka.ms/deploytoazurebutton)](https://portal.azure.com/#create/Microsoft.Template/uri/https%3A%2F%2Fraw.githubusercontent.com%2FJulianHayward%2FAzADServicePrincipalInsights%2Fmain%2Fsetup%2Fazure-web-app.json) - fill in the values from step 1, **Entra Client Id** = the Application (client) ID, **Access Control** = the same setting as the Entra admin used. The deployment's **Outputs** show the values the Entra admin needs next.

Tell the Entra admin the **redirect URI** from the script output or the deployment outputs (usually `https://<web app name>.azurewebsites.net/.auth/login/aad/callback`) and the name of the managed identity: `id-<web app name>-auth`. If the Entra admin cannot see the Azure resource group, also give them the **issuer**, **subject** and **audience** of the federated credential (script output, or the outputs `federatedCredentialIssuer`, `federatedCredentialSubject`, `federatedCredentialAudience`).

**3. Entra admin - finish the sign-in**

1. App registration > **Authentication** > **Add a platform** > **Web** > the redirect URI > tick **ID tokens (used for implicit and hybrid flows)** > **Configure** (the sign-in of the web app needs ID tokens; access tokens stay off)
1. App registration > **Certificates & secrets** > **Federated credentials** > **Add credential**
1. Scenario **Managed identity** > select the managed identity `id-<web app name>-auth` > give it a name > **Add**. Can't see the managed identity? Scenario **Other issuer** with the issuer, subject and audience the Azure admin gave you.

Then continue with [checkpoint 2](#step-2---create-the-web-app-path-a).

<details>
<summary><b>Technical details</b> - what exactly gets created</summary>

- **Microsoft Entra ID:** app registration (single tenant, redirect URI `/.auth/login/aad/callback`, ID tokens on - App Service signs in with the hybrid flow `code id_token` -, access tokens off, delegated Microsoft Graph permissions `openid profile email offline_access` with admin consent) and its enterprise application. `Assignment`: `appRoleAssignmentRequired` and the group assigned (default app role). `GroupClaim`: `groupMembershipClaims: SecurityGroup`. Federated identity credential: issuer `https://login.microsoftonline.com/<tenant>/v2.0`, subject = principal ID of the managed identity, audience `api://AzureADTokenExchange` (Azure Government: `api://AzureADTokenExchangeUSGov`; Azure operated by 21Vianet: `api://AzureADTokenExchangeChina`).
- **Azure:** user-assigned managed identity `id-<name>-auth` (only for the sign-in - don't assign it to anything else), App Service plan `asp-<name>` (Windows, default SKU B1; `-Sku F1` is free but without SLA), web app `<name>`: HTTPS only, TLS 1.2, FTP disabled, basic authentication (FTP and SCM) disabled, default document `AzADServicePrincipalInsights_<management group ID>.html` (several management groups: joined by `_`; the Tenant Root Group: the tenant ID).
- **App Service Authentication (v2):** Microsoft Entra, login required (redirect to the sign-in page), client assertion by the managed identity (app setting `OVERRIDE_USE_MI_FIC_ASSERTION_CLIENTID`, no client secret), allowed tenant = your tenant. `GroupClaim`: `jwtClaimChecks.allowedGroups` and `defaultAuthorizationPolicy.allowedPrincipals.groups` = the viewer group.
- **Role:** '**Website Contributor**' on the web app for the pipeline identity. The publish step uploads with a Microsoft Entra token - basic authentication is not needed.
- **Script and template:** [pwsh/New-AzADSPIWebApp.ps1](../pwsh/New-AzADSPIWebApp.ps1) (REST calls, needs only the module `Az.Accounts`; the endpoints follow the cloud of your Azure context) and the ARM template [setup/azure-web-app.json](azure-web-app.json) create the same Azure resources.
- **Switching** from `Assignment` to `GroupClaim` later: run the script with the new setting and set **Assignment required?** of the enterprise application back to **No**.
- **An existing web app** that you set up yourself: the publish step works with any web app that requires a sign-in (App Service Authentication, **Require authentication**) and gives the pipeline identity '**Website Contributor**'.

</details>
