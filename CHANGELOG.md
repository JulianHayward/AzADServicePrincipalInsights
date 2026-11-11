# Changelog

Versions of AzADServicePrincipalInsights, newest first. Setup and parameters: [README](README.md).

## v2_20261111_0

* azAPICallVersion 1.4.1 → 1.4.2
* ThrottleLimitGraph 30 → 25
* Change history: every run records what changed since the previous run (new parameters `-NoChangeHistory`, `-ChangeHistoryDays`) - see [Change history](README.md#change-history)
* Telemetry: the identifier is a hash of the tenant ID only (unique tenants; before: tenant and account mixed, for users including the user name) - unique-tenant counts start again with this version; `-StatsOptOut` sends only an anonymous run count; documented in [Telemetry](README.md#telemetry)
* **Setup change for Azure DevOps pipelines and GitHub Actions workflows:** the script parameters are set in a variables file (`.azuredevops/pipelines/AzADServicePrincipalInsights.variables.yml`, `.github/AzADServicePrincipalInsights.variables.yml`); both files have the same content (required, optional grouped, then the sections 'Log Analytics ingestion' and 'Web App publishing' with their switch first); the files start with the pipeline/workflow settings (Azure DevOps: `ServiceConnection`, `ScriptDir`; GitHub: `ScriptDir` - read by the first workflow step; the workflow `env` only keeps `VariablesFile`) - the Azure DevOps variables `Script`, `ScriptPrerequisites` and `IngestScript` are gone (the file names are written out, as on GitHub); move your values there - see [Azure DevOps and GitHub Actions](README.md#azure-devops-and-github-actions). The step 'Create arguments' checks them and the scripts get them as parameters (a value is never part of a script text). The Log Analytics ingestion uses the same file; Azure DevOps gets WebApp publishing
* Federated identity credentials of GitHub Actions: GitHub's immutable subject (`repo:<owner>@<owner ID>/<repository>@<repository ID>:...`, the default for repositories created after 2026-07-15) is shown as repository with its owner and repository ID; the GitHub setup guide creates the credential with both IDs and says how to find them
* Pipelines/workflows: both run on `ubuntu-latest` (Azure DevOps was `ubuntu-22.04`) with a shallow checkout and the same step names; Azure DevOps now checks `WebAppPublish` / `IngestToLogAnalytics` (true/false) and the Web App values (no placeholder) in the step 'Create arguments', as GitHub does, instead of failing later; GitHub checks the prerequisites before the Azure login
* Pipelines/workflows: the push step is one script for GitHub and Azure DevOps ([pwsh/Push-AzADSPIOutput.ps1](pwsh/Push-AzADSPIOutput.ps1), was inline bash/PowerShell in each pipeline file): commit only when the output changed, rebase on a concurrent run, clear messages for overlapping runs, a run that is not on a branch and a missing `SYSTEM_ACCESSTOKEN`; the token is passed per git command and never printed
* **Helper scripts renamed** to PowerShell Verb-Noun names (the pipeline and workflow files of this version use them): `prerequisites.ps1` → `Test-AzADSPIPrerequisite.ps1`, `AzADSPIDataCollectionIngest.ps1` → `Send-AzADSPILogAnalyticsData.ps1` - update your own automation if it calls them. New in this version: `New-AzADSPIArgumentFile.ps1`, `Read-AzADSPIArgumentFile.ps1`, `Get-AzADSPIOutputPath.ps1`, `Publish-AzADSPIWebApp.ps1`, `New-AzADSPIWebApp.ps1`. `AzADServicePrincipalInsights.ps1` keeps its name.
* Report: Azure portal look (pages, navigation, search, deep links, light/dark theme); tables with AG Grid (filters, column chooser, CSV download, pop-out); findings overview at the top; Azure resource types link to AzAdvertizer, resource IDs to the Azure portal, Service Principals, applications, users and groups to their Microsoft Entra pages in the Azure portal, API permissions of high-privilege users to AzAdvertizer, custom Azure roles to their role definition in the Azure portal. The report loads its CSS/JS and icons from azadvertizer.net
* New insights: effective privilege per Service Principal, credential risk of privileged Service Principals, Service Principal credentials, multi-tenant apps of other organizations, cleanup candidates, application configuration findings, federated identity credential trust, agent objects (Microsoft Entra Agent ID), user-assigned managed identities: assignment restrictions (preview) and isolation scope - each with report section and CSV
* Microsoft Entra roles: roles Microsoft marks as privileged (`isPrivileged`) count as critical in addition to `-CriticalAADRoles`; role assignments through role-assignable groups are included (`indirect`)
* New optional permissions (tested at the start, the run works without them): `DelegatedPermissionGrant.Read.All` (faster), `DirectoryRecommendations.Read.All`, `CustomSecAttributeAssignment.Read.All`, `Policy.Read.All` + `Policy.Read.PermissionGrant` - see [Permissions](README.md#permissions)
* Output changes (no existing column, property or file renamed or removed):
    * owner CSVs (`ServicePrincipalOwners`, `ApplicationOwners`): one row per owner (was: only the last owner); indirect owners also through app registration owners of owner Service Principals
    * JSON: lists are always arrays (`SPAppRoleAssignedTo`, `SPOauth2PermissionGrantedTo`, managed identity resources and federated identity credentials were an object or a nested array depending on the count)
    * credential end dates as `yyyy-MM-dd HH:mm:ss UTC`
    * CSV: all values quoted (`-CsvExportUseQuotesAsNeeded` for as-needed), no HTML markup, values starting with `= + - @` get a leading `'` (spreadsheet formula guard); classification per row (was influenced by the previous row)
    * HTML: all values are HTML encoded (a display name with markup was rendered as markup)
* Fixes: run stopped when a user or group was deleted during the run ("A null key is not allowed in a hash literal", issue #43) or an application was deleted; install prompt with side-by-side AzAPICall versions; indirect owners deeper than one level were missing; managed identity resource counts and names; chart filters and colours; owner/role principals that are not users were labelled '(MemberSynced)', a user without userType is now labelled '(userType not set)'; foreign principals (CSP/GDAP partner objects) are resolved and labelled 'Foreign <type>' (as AzGovViz, issue Azure/Azure-Governance-Visualizer#66), a creator of an Azure role assignment that cannot be resolved shows as 'Unknown (deleted or not in this tenant)' instead of a bare ID; delegated permission grants could be lost (parallel race); a failing item no longer stops the whole run
* Performance: tenant-wide prefetch of applications, directory role assignments and delegated permission grants (test tenant with 1,200 Service Principals: about 4 min / 11,600 API calls before, 2 min / 6,700 after)
* Pipelines/workflows: actions on Node 24 (`actions/checkout@v7`, `azure/login@v3`, `azure/powershell@v3`); push only when the output changed, rebase on a concurrent run; WebApp publishing only to a Web App that requires a login (App Service Authentication v1 or v2, unauthenticated requests not allowed); prerequisites check: stops for a public GitHub repository, checks the Azure DevOps 'Contribute' permission with clear messages; Log Analytics ingestion in batches below the 1 MB API limit; ARM api-versions updated to the latest stable
* CI for this repository: Pester tests, PSScriptAnalyzer, DevSkim, OpenSSF Scorecard, zizmor, Dependabot, dev container build (the Codespace has PowerShell 7.6, AzAPICall in the script's default version and Az.Accounts - `tests/Test-DevContainer.ps1`)
* Tenant consent settings: the row 'Group/team owners can consent to apps accessing their data' shows the Teams and chat resource-specific consent state (Microsoft-managed (default), all apps, pre-approved apps only, not allowed) instead of the policy IDs
* Setup guides ([setup](setup/README.md)): one guide per way to run it (console, GitHub, Azure DevOps) plus Web App publishing and Log Analytics; each guide lists who you need, has a checkpoint after every step and a troubleshooting table with the messages of the scripts; the permission check and the repository permission check link to the setup guides (the permission check linked to a heading that no longer existed)
* Setup assistant [setup/setup-assistant.html](setup/setup-assistant.html): asks where to run it, how to sign in and which optional features, and gives the steps of the setup guides for that setup, with the typed values filled into the commands (one page, sends and stores nothing)
* Web App publishing: setup script [pwsh/New-AzADSPIWebApp.ps1](pwsh/New-AzADSPIWebApp.ps1) and ARM template [setup/azure-web-app.json](setup/azure-web-app.json) (Deploy to Azure) - the web app shows the report only to the members of a group (`Assignment` or `GroupClaim`), the sign-in uses a managed identity instead of a client secret, basic authentication off, the pipeline identity gets Website Contributor; ported from Azure Governance Visualizer
* Log Analytics ingestion: ARM template [setup/log-analytics-dcr.json](setup/log-analytics-dcr.json) for the custom table and the Data Collection Rule, optionally the workspace too (`createWorkspace`) (kind `Direct`, all columns of the JSON output; optional Monitoring Metrics Publisher role) - a table created from one sample file missed the columns of other object types (e.g. `ManagedIdentity*` for applications) and dropped them without an error; guide [setup/log-analytics.md](setup/log-analytics.md); a failed ingestion names the signed-in identity, the Data Collection Rule and the needed role, and that a new role assignment can take up to about 30 minutes to work
* Review 2026-11-07 (logic, performance, failures):
    * delegated permissions: 'consent for all users' is the consent type of the grant (`AllPrincipals`), not the type of the permission (who may consent) - a tenant-wide grant of a critical permission that users may consent to now raises the effective privilege, a grant for a single user is not 'admin consent for all users'; the delegated permissions table shows 'all users' / 'n users'; JSON `SPOauth2PermissionGrants` gets `consentType`, `principalId`, `grantId`, CSV `_Oauth2PermissionGrants_` the columns `Oauth2PermissionGrantConsentType`, `Oauth2PermissionGrantPrincipalId`, `Oauth2PermissionGrantId` (added); the change history compares delegated permissions by consent kind (state format 2: one 'baseline reset' after the update)
    * application permissions: name, description and classification of an app role come from the API it belongs to (app role IDs are only unique per API; Microsoft reuses them, e.g. Exchange Online / Microsoft Graph)
    * Azure roles: a critical role at the tenant root (`/`) is critical; custom roles that the role listings do not return (assignable only to resource groups) are read by ID - an orphaned assignment of a deleted role is labelled 'role definition not found (deleted?)'; an inherited management group or root assignment is labelled with its scope (was 'Subscription')
    * credentials: 'long lifetime' is the lifetime (end - start), not the time left; JSON credentials get `startDateTime` (added)
    * high-privilege users: owners without a user type are included
    * `-OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes`: also Service Principals with Azure roles through a group; escalation permissions are found when Microsoft Graph is not processed; owners and createdBy of Service Principals that are not processed get type and name (JSON owner entries: `ownersNotCollected`, added)
    * collection errors: a phase in which every item failed stops the run (with up to 10 items it continued); the overview shows a warning with the failed items and pipelines get a warning annotation
    * the pinned `-azAPICallVersion` is used in the parallel runspaces too (they loaded the newest installed version)
    * change history: see the guards in [Change history](README.md#change-history); reading the log is fast for a long history (only the lines of the report period are parsed)
    * pipelines: GitHub workflows of a branch wait for each other (`concurrency`); a push conflict of overlapping runs ends with a clear message
    * Azure DevOps prerequisites step: uses `OutputPath` (the first run failed: the output folder was not created)
    * report: private use characters in names are shown as they are (were expanded into report markup)
* Performance (2026-11-09; output unchanged): per-object Graph requests only for objects that have entries (list passes with `$expand` tell which; test tenant 6,721 -> 708 API calls), faster report build, JSON export, enrichment and batching; getByIds batches in parallel (test tenant: run about 2 min -> 1 min)
* Collection: the tenant-wide Graph lists and the Azure collection run side by side (test tenant: SP collection 19 s -> 10 s)
* Review 2026-11-09:
    * runs no longer stop on: concurrent API statistics, a failing relationship pass or optional list, an application credential without end date, a JSON file that cannot be written; change history keeps its records when the log cannot be written, reads its previous state case-insensitively and shows read/update problems in the report
    * report: look-alike federated credential issuers keep the "custom issuer" finding; scoped Entra roles (administrative units) show their scope; AzAdvertizer links of Entra roles fixed; HiPo users one row per permission and consent kind; user consent policy `microsoft-user-default-recommended`; policy managed identities of subscriptions that were not collected are no longer "relict"; subscriptions out of scope listed on the overview and in `_SubscriptionsOutOfScope.csv` (new); more columns in the Azure role assignment, delegated grant and federated credential CSVs (scope id, assignment id, scope, claims expression)
    * report UI: case-insensitive sort, faster filter highlight in long list cells, portal search fixes, number filters not highlighted, select filters exact
    * Azure role conditions (2026-11-10): a role that can assign roles is critical only if a critical role is among the roles it may assign - conditions on the role definition (built-in roles such as Key Vault Data Access Administrator; role definitions are read with api-version 2023-07-01-preview, 2022-05-01-preview in AzureUSGovernment/AzureChinaCloud) and on the role assignment ('constrain roles' delegation). Shown in the Azure role assignment section ("limited by role definition condition: can assign only ...", "role assignment condition allows Owner"), the effective privilege (new reason "Azure role (limited by condition)", high), HiPo users, new CSV columns and JSON properties; the change history records changed conditions (state format 3: one baseline reset)
    * policy assignments: each management group below the requested ones reads only its own assignments (`atExactScope()`; test tenant: 1,408 -> 143 downloaded, same result)
    * federated identity credentials (2026-11-10): finding for the deprecated Azure DevOps issuer (`vstoken.dev.azure.com`, retirement 2027-07-01, [convert the service connection](https://aka.ms/azdo-issuer)) on single-tenant apps and managed identities in the Azure public cloud (multitenant apps and other clouds are out of scope); counted on the findings overview. A converted service connection's credential (Microsoft Entra issuer, subject `/eid1/.../sc/<organization id>/<service connection id>`) is shown as "Azure DevOps (Microsoft Entra issuer)"; the conversion keeps the old credential, which then gets the finding "converted: remove this credential if no other service connection still uses it"
    * pipelines: runs check out the current head of the branch; Azure DevOps ingests after the push and passes the build token for federated logins; tenant names cannot inject Azure DevOps logging commands; public-repository check on the run's GitHub server; ingestion stops on missing JSON
* Fixes 2026-11-09: a guest caller without access to applications no longer stops the run (the SPs are marked incomplete); flexible federated identity credentials (`claimsMatchingExpression`, added to the JSON) are shown with a finding; managed identity resource type of nested resources (e.g. web app slots); filter highlight in cells with `&`; runs without a subscription in the Az context; Graph permission check also for managed identities and workload identity federation; `permissionClassification.json` also found next to the script; owner data in the same order on every run; federated credentials of user-assigned identities read in parallel; less memory during the report build

## 20260803

* AzureRoleAssignmentMapping: using SP-id HashSet
* AAD Groups nested resolution: ForEach-Object -Parallel
* ServicePrincipalAppRoleAssignedTo: Sort-Object -Property { $_.id }.
* Added parameter `-PerfTracking` switch (default off)
* azAPICallVersion 1.2.5 → 1.4.1
* ThrottleLimitGraph 20 → 30.
* Casing normalizations: Param→param, Throw→throw.

## 20250121

* Fix issue #40 - Use [AzAPICall](https://aka.ms/AzAPICall) PowerShell module version 1.2.5
* Try to fix issue #41 - optimize hashtable processing
* Update ARM REST API versions to latest

## 20240730

* Fix SkipAzContextSubscriptionValidation check by using NoAzureResourceSideRelations (PR 38)

## 20240419

* Fix hardcoded ARM API Url using north europe (PR 31)
* Fix ContentLengthLimitExceeded error when using log ingestion API (PR 32)

## 20240212

* fix issue 27

## 20240208

* Contribution from @Cloud-Architekt to ingest data from the JSON files to an Azure Log Analytics workspace custom table using data collection rule / data collection endpoint. [Microsoft Entra Workload ID - Advanced Detections and Enrichment in Microsoft Sentinel](https://www.cloud-architekt.net/entra-workload-id-advanced-detection-enrichment/)
  * Ready for Azure DevOps and GitHub; configure in the pipeline/workflow YAML files
* Change PowerShell parallel handling / batches
* Optimize array handling / best practices
* Use [AzAPICall](https://aka.ms/AzAPICall) PowerShell module version 1.2.0 (support endpoint *.ingest.monitor.azure.com)

## 20231218 - thanks @kaiaschulz

* Fix scope of subscriptions to process. The ARM entities API may still return subscriptions that are meanwhile delted and therefore should not be processed in the data collection
* Use [AzAPICall](https://aka.ms/AzAPICall) PowerShell module version 1.1.86

## 20231217

* Fix for SP names that contain escapable characters
* Update GitHub workflows to support webApp publishing thanks @RS-MPersson
* Use [AzAPICall](https://aka.ms/AzAPICall) PowerShell module version 1.1.85

## 20231121 - thanks @cjtous1

* HTML updates
    * Added `SPTags` & `AppTags` to the following tables:
        * Service Principals
        * Service Principal AAD RoleAssignments
        * Service Principal App RoleAssignments
        * Service Principal App RoleAssignedTo
        * Service Principal Oauth Permission grants
        * Service Principal Azure RoleAssignments
    * Added `AppNotes` to the Service Principals table
* Added the following CSV file exports
    * Service Principals
    * Service Principal Owners
    * Application Owners
    * Service Principal Owned Objects
    * Service Principal AAD RoleAssignments
    * Service Principal AAD RoleAssignedOn
    * Service Principal App RoleAssignedTo
    * Service Principal App RoleAssignments
    * Service Principal Azure RoleAssignments
    * Service Principal Group memberships
* Fix: `NoCsvExport` is now working and preventing CSV files from being generated if true.
* Fix: `NoJsonExport` is now working and preventing JSON files from being generated if true.
* Use [AzAPICall](https://aka.ms/AzAPICall) PowerShell module version 1.1.84

## 20231001

* fix hardcoded delimiter for export-csv - thanks @cjtous1

## 20230316

* Use [AzAPICall](https://aka.ms/AzAPICall) PowerShell module version 1.1.70

## 20221017

* Use [AzAPICall](https://aka.ms/AzAPICall) PowerShell module version 1.1.40
    * Issue #10 - Handle error `404` User Assigned Managed Identity / ResourceGroup not found  

## 20221014

* Use [AzAPICall](https://aka.ms/AzAPICall) PowerShell module version 1.1.38 
    * Handle error `405` [Support for federated identity credentials not enabled](https://learn.microsoft.com/en-us/azure/active-directory/develop/workload-identity-federation-considerations#errors)

## 20221008

* New feature - Managed Identity User Assigned Federated Identity Credentials
* Rearrange JSON output for Managed Identity associated Azure Resources

## 20221007

* New feature - Managed Identity User Assigned associated Azure Resources
* Changed parameter name `NoAzureRoleAssignments` to `NoAzureResourceSideRelations`
    * Using `NoAzureResourceSideRelations`:
        * No (Azure Resource side) RBAC Role assignments collection
        * No (Azure Resource side) Policy assignments collection
        * No (Azure Resource side) Resources collection ('Managed Identity User Assigned associated Azure Resources' feature annul)
* Azure DevOps pipeline yml - update vmImage ~~ubuntu-20.04~~ ubuntu-22.04
* Minor fixes and optimizations
* Use [AzAPICall](https://aka.ms/AzAPICall) PowerShell module version 1.1.33 

## 20220717

* Removed identity governance state validation
* Use AzAPICall PowerShell module version 1.1.18  

## 20220630

* __Breaking Change__ on the Azure side: Instead of __RoleManagement.Read.All__ we require __RoleManagement.Read.Directory__

## 20220622_1

* Fix `/providers/Microsoft.Authorization/roleAssignmentScheduleInstances` AzAPICall errorhandling (error 400, 500)
* Optimize procedure to update the AzAPICall module
* Use AzAPICall PowerShell module version 1.1.17

## 20220613_1

* use AzAPICall module version 1.1.16
* enhance HiPo Users HTML output
* minor fixes

## 20220609_1

* add parameter `-CriticalAADRoles` (defaults: Global Administrator, Privileged Role Administrator, Privileged Authentication Administrator)
* add HiPo Users - A HiPo User has direct or indirect ownership on a ServicePrincipal(s) with classified permissions (AppRole, AAD Role, Azure Role, OAuthPermissionGrant)
* use AzAPICall module version 1.1.13
* minor fixes

## 20220505_1

* fix: `using:scriptPath` variable in foreach parallel (this is only relevant for Azure DevOps and GitHub if you have a non default folder structure in your repository) - thanks Matt :)

## 20220501_1

* parameter `-ManagementGroupId` accepts multiple Management Groups in form of an array e.g. `.\pwsh\AzADServicePrincipalInsights.ps1 -ManagementGroupId @('mgId0', 'mgId1')`
* new parameter `-OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes`. You may want to only report on Service Principals that have RBAC permissions on Azure resources at and below that Management Group scope(s) (Management Groups, Subscriptions, Resource Groups and Resources)
* Role assignments on Azure resources - mark those RBAC Role assignments which leverage a RBAC Role definition that can create role assignments as critical
* updated YAML workflow/pipeline files
* minor bug fixes
* performance optimization

## 20220425_2

* add parameter `-ManagementGroupId` (if undefined, then Tenant Root Management Group will be used)
* use AzAPICall module version 1.1.11

## 20220404_1 

* add FederatedIdentityCredentials
