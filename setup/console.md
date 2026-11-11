# Run AzADServicePrincipalInsights from the console

The quickest way to get a report: run the script on your own computer (or in a GitHub Codespace) and sign in as yourself. Good for trying it out or a one-time report. For regular reports, use [GitHub](github.md) or [Azure DevOps](azure-devops.md) afterwards.

## Before you start

The Microsoft Entra steps are described in the [Azure portal](https://portal.azure.com) (**Microsoft Entra ID**). The [Microsoft Entra admin center](https://entra.microsoft.com/) has the same pages, if you prefer it.

**Tools** - install these on your computer:

- [PowerShell 7](https://learn.microsoft.com/powershell/scripting/install/installing-powershell) (7.0.3 or later; Windows PowerShell 5.1 is not supported)
- The PowerShell module `Az.Accounts`: `Install-Module -Name Az.Accounts`
- Only if you clone the repository (step 2): [Git](https://git-scm.com/downloads)

Nothing to install? Use a [GitHub Codespace](#run-in-a-github-codespace-instead) - it has all the tools.

**Your account** - which one are you?

| Your account | What you need |
|---|---|
| A normal user account (member) of the tenant | Only the '**Reader**' role (step 1). A member user reads most of Microsoft Entra ID with the default user permissions; with '**Global Reader**' the report is complete. This is the most common case. |
| A guest account (invited from another tenant) | The '**Reader**' role (step 1), plus the Microsoft Entra role '**Global Reader**' - ask the Microsoft Entra administrator of the tenant to [assign it to you](https://learn.microsoft.com/entra/identity/role-based-access-control/manage-roles-portal). Without it, a guest may not read applications: the script continues and marks the affected service principals as incomplete. |
| You want to run as a service principal instead of yourself | See [Run as a service principal](#run-as-a-service-principal-instead). |

## Step 1 - Check that you can read the management group

You need the '**Reader**' role (or more) on the management group you want to report on - usually the **Tenant Root Group**.

1. In the [Azure portal](https://portal.azure.com), open **Management groups** > your management group > **Access control (IAM)** > **Check access** > **View my access**.
1. If '**Reader**' (or '**Owner**', '**Contributor**', ...) is listed, continue with step 2.
1. If not: somebody with '**Owner**' or '**User Access Administrator**' on the management group [assigns you the role](https://learn.microsoft.com/azure/role-based-access-control/role-assignments-portal).

Note the **management group ID** (on the overview page of the management group, not the display name).

> **Checkpoint 1:** you have '**Reader**' (or more) on the management group, and you know its ID.

## Step 2 - Get the code

The script needs only two files in one folder: `AzADServicePrincipalInsights.ps1` and `permissionClassification.json`. In PowerShell 7:

```powershell
New-Item -ItemType Directory -Force -Path "$HOME/AzADSPI" | Out-Null
Set-Location "$HOME/AzADSPI"
foreach ($file in 'AzADServicePrincipalInsights.ps1', 'permissionClassification.json') {
    Invoke-WebRequest -Uri "https://raw.githubusercontent.com/JulianHayward/AzADServicePrincipalInsights/main/pwsh/$file" -OutFile $file
}
```

Downloaded with the browser instead (GitHub > the file > **Download raw file**)? Put both files in one folder; on Windows, run `Unblock-File ./AzADServicePrincipalInsights.ps1` once. For a new version, download both again.

Or clone the repository with Git - that also gets the setup files and the tests, and you update with `git pull`:

```powershell
git clone 'https://github.com/JulianHayward/AzADServicePrincipalInsights.git'
Set-Location 'AzADServicePrincipalInsights'
```

## Step 3 - Sign in and run

```powershell
Connect-AzAccount -TenantId <tenant ID> -UseDeviceAuthentication

New-Item -ItemType Directory -Force -Path "$HOME/AzADSPI-Output"
./AzADServicePrincipalInsights.ps1 -ManagementGroupId <management group ID> -OutputPath "$HOME/AzADSPI-Output"
```

- With the cloned repository (or in a Codespace) the script is in the folder `pwsh`: `./pwsh/AzADServicePrincipalInsights.ps1`.

- On the first run, the script asks to install the PowerShell module `AzAPICall` - answer `y`.
- The run takes a few minutes; large tenants take longer.
- An account without a subscription (e.g. '**Reader**' on a management group only) works too.
- You can change what the report contains with [parameters](../README.md#parameters) - the defaults are fine for a first run.

> **Checkpoint 2:** the script ends with `Completed successful`.

## Step 4 - Open the report

The report is the file `AzADServicePrincipalInsights_<version>_<date and time>_<management group ID>.html`. To open the newest one:

```powershell
Get-ChildItem "$HOME/AzADSPI-Output" -Filter 'AzADServicePrincipalInsights_*.html' | Sort-Object LastWriteTime | Select-Object -Last 1 | Invoke-Item
```

The report loads its styling from azadvertizer.net, so the browser needs internet access. In a Codespace, right-click the HTML file in the Explorer and select **Download**.

The folder also contains:

- `*.csv` - one file per table of the report
- `JSON_SP_<management group ID>_<date and time>` - one JSON file per service principal, application or managed identity
- `AzADServicePrincipalInsights_<management group ID>_ChangeHistory.jsonl` and `..._ChangeHistoryState.json` - the [change history](../README.md#change-history). Use the same output folder for the next run, and the next report shows what changed in between.

The output contains insights about your tenant. Store it like other sensitive data.

> **Checkpoint 3:** the report opens in your browser - you're done.

## If something doesn't work

| What you see | Why | What to do |
|---|---|---|
| `MG '<ID>' Reader permission check FAILED` | You don't have '**Reader**' on the management group, or you entered the display name instead of the ID. | Step 1. |
| `AAD ... Read permission check FAILED - the Microsoft Graph application permission ... is missing` (only as a service principal) | The message names the Microsoft Graph permission that is missing - not added, added as **Delegated** instead of **Application**, or without admin consent. | [Run as a service principal](#run-as-a-service-principal-instead), part 2. |
| `permissionClassification.json not found` | The script is alone in its folder. | Put `permissionClassification.json` next to it (step 2). |
| `Install it first: Install-Module -Name AzAPICall -RequiredVersion <version>` | The script runs unattended (e.g. `pwsh -NonInteractive`) and can't ask. | Run the command shown, then run the script again. |
| The report has no styling and shows all sections on one page | The browser can't reach azadvertizer.net. | Open the report on a computer with internet access. |

## Run in a GitHub Codespace instead

The repository includes a [Codespace](https://docs.github.com/codespaces/getting-started/quickstart) configuration ([.devcontainer/devcontainer.json](../.devcontainer/devcontainer.json)) with PowerShell 7, Git, `Az.Accounts` and `AzAPICall`. On the [repository page](https://github.com/JulianHayward/AzADServicePrincipalInsights), select **Code** > **Codespaces** > **Create codespace on main**, then continue with step 3 in the terminal of the Codespace. Use `./AzADSPI-Output` instead of `"$HOME/AzADSPI-Output"` there: then the Explorer of the Codespace shows the output folder. Download the output before you delete the Codespace, and don't commit it.

## Run as a service principal instead

A service principal can't read users, groups and applications in Microsoft Entra ID by default. Before the first run:

1. The service principal needs the '**Reader**' role on the management group - as in step 1, with the object ID of its **enterprise application** (**Enterprise applications** > the application > **Object ID** - not the object ID under **App registrations**; see [Which ID is which?](README.md#which-id-is-which)).
1. A '**Privileged Role Administrator**' or '**Global Administrator**' grants it four Microsoft Graph permissions:
   1. [Azure portal](https://portal.azure.com) > **Microsoft Entra ID** > **App registrations** > the application > **API permissions** > **Add a permission** > **Microsoft Graph** > **Application permissions**
   1. Select `Application.Read.All`, `Group.Read.All`, `RoleManagement.Read.Directory` and `User.Read.All` > **Add permissions**
   1. **Grant admin consent for _&lt;tenant&gt;_** > **Yes** - the status changes to **Granted for _&lt;tenant&gt;_**

   More data with [optional permissions](README.md#microsoft-graph) (e.g. `DelegatedPermissionGrant.Read.All` makes the run faster).

Then sign in as the service principal instead of `Connect-AzAccount -TenantId <tenant ID> -UseDeviceAuthentication` in step 3:

```powershell
$credential = Get-Credential   # User: the Application (client) ID; Password: a client secret of the app registration
Connect-AzAccount -ServicePrincipal -TenantId <tenant ID> -Credential $credential
```

## Next steps

- Regular reports: [Run from GitHub](github.md) or [Run from Azure DevOps](azure-devops.md).
- Share the report with a group: [Publish the report to an Azure Web App](azure-web-app.md) (needs a pipeline or workflow).
