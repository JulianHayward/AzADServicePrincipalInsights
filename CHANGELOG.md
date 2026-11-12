# Changelog

Versions of AzADServicePrincipalInsights, newest first. Setup and parameters: [README](README.md).

## v2_20261111_0

**Upgrading from v1:** read [Upgrading from v1](README.md#upgrading-from-v1) first - new pipeline/workflow files with a variables file, renamed helper scripts, removed parameters, a stricter Web App check, a new Log Analytics table schema.

**Breaking changes**
* Removed parameters: `-AzureDevOpsWikiAsCode` (the platform is detected), `-HtmlTableRowsLimit` (no row limit any more), `-AADGroupMembersLimit` (was not used)
* Stricter values: `-CsvDelimiter` `;` or `,`; throttle limits 1 to 1000 (`-ThrottleLimitMonitor` 1 to 100); PowerShell 7 required
* Azure DevOps pipelines and GitHub Actions workflows: new files; the script parameters are set in a variables file (`.azuredevops/pipelines/AzADServicePrincipalInsights.variables.yml`, `.github/AzADServicePrincipalInsights.variables.yml`) - see [Azure DevOps and GitHub Actions](README.md#azure-devops-and-github-actions)
* Helper scripts renamed: `prerequisites.ps1` → `Test-AzADSPIPrerequisite.ps1`, `AzADSPIDataCollectionIngest.ps1` → `Send-AzADSPILogAnalyticsData.ps1`
* Log Analytics ingestion: a new table schema, created by the ARM template [setup/log-analytics-dcr.json](setup/log-analytics-dcr.json)
* Web App publishing only to a Web App that requires a sign-in
* Telemetry: the identifier is a hash of the tenant ID only (unique-tenant counts start again); `-StatsOptOut` sends only an anonymous run count - see [Telemetry](README.md#telemetry)

**Report**
* New look: Azure portal style with pages, navigation, search, deep links and light/dark theme; tables with filters, column chooser, CSV download and pop-out; a findings overview; links to the Azure portal (Entra objects, resources) and to AzAdvertizer (roles, permissions, resource types). The report loads its CSS/JS and icons from azadvertizer.net
* Snapshot: everything about one service principal, application, user, group or Azure scope on one page - at a glance, who can control it, what it can reach, findings, permissions, roles, ownership, credentials, changes - with a graph of its relations; **Focus** filters the whole report to one service principal. Links `?sp=<ID>` and `?focus=<ID>` - see [Features](README.md#features)
* New insights, each with a report section and a CSV: effective privilege per service principal (incl. control through ownership), credential risk of privileged service principals, service principal credentials, multi-tenant apps of other organizations, cleanup candidates, application configuration findings, federated identity credential trust (who can sign in; the deprecated Azure DevOps issuer), agent objects (Microsoft Entra Agent ID), user-assigned managed identities (assignment restrictions, isolation scope), tenant consent settings
* Change history: what changed since the previous run (`-NoChangeHistory`, `-ChangeHistoryDays`) - see [Change history](README.md#change-history)
* Entra roles: roles Microsoft marks as privileged count as critical; roles through role-assignable groups and scoped roles (administrative units) are included
* Azure roles: role conditions are evaluated (a role that may assign only non-critical roles is not critical)
* Delegated permissions: the consent of each grant (all users or single users)

**Output changes** (no CSV column, JSON property or file was renamed or removed; columns and properties were added)
* Owner CSVs: one row per owner (was: only the last owner), incl. indirect owners
* JSON: lists are always arrays
* CSV: all values quoted (`-CsvExportUseQuotesAsNeeded` for as-needed), no HTML markup, values starting with `= + - @` get a leading `'`
* Credential end dates as `yyyy-MM-dd HH:mm:ss UTC`; HTML values are encoded

**Permissions:** new optional permissions, tested at the start (the run works without them): `DelegatedPermissionGrant.Read.All` (faster), `DirectoryRecommendations.Read.All`, `CustomSecAttributeAssignment.Read.All`, `Policy.Read.All` + `Policy.Read.PermissionGrant` - see [Permissions](README.md#permissions)

**Setup and pipelines**
* Setup guides for each way to run it (console, GitHub, Azure DevOps, Web App, Log Analytics) and the [setup assistant](setup/setup-assistant.html)
* Web App publishing: setup script [pwsh/New-AzADSPIWebApp.ps1](pwsh/New-AzADSPIWebApp.ps1) and ARM template [setup/azure-web-app.json](setup/azure-web-app.json): access for the members of a group, sign-in with a managed identity, no client secret
* Pipelines/workflows: `ubuntu-latest`, actions on Node 24, one push script for both platforms (commits only the output folder, only when it changed), prerequisite checks (public repository or project, permissions), overlapping runs wait for each other
* azAPICallVersion 1.4.1 → 1.4.2, ThrottleLimitGraph 30 → 25

**Performance and fixes**
* Faster: tenant-wide prefetch and requests only where there is data (test tenant with 1,200 service principals: about 4 min and 11,600 API calls before, about 1 min and 700 API calls after)
* Fixes: the run stopped when a user, group or application was deleted during the run (issue #43); indirect owners deeper than one level were missing; foreign principals (CSP/GDAP) are resolved (Azure/Azure-Governance-Visualizer#66); app role names per API (IDs are reused across APIs); delegated grants could be lost; a failing item or optional list no longer stops the whole run
* CI for this repository: Pester tests, PSScriptAnalyzer, DevSkim, OpenSSF Scorecard, zizmor, Dependabot, dev container

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
