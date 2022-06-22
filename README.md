__AzADServicePrincipalInsights__

Insights and change tracking on Azure Active Directory Service Principals (Enterprise Applications and Applications)

# Updates
* 20220622_1
    * Fix `/providers/Microsoft.Authorization/roleAssignmentScheduleInstances` AzAPICall errorhandling (error 400, 500)
    * Optimize procedure to update the AzAPICall module
    * Use AzAPICall PowerShell module version 1.1.17
* 20220613_1
    * use AzAPICall module version 1.1.16
    * enhance HiPo Users HTML output
    * minor fixes
* 20220609_1
    * add parameter `-CriticalAADRoles` (defaults: Global Administrator, Privileged Role Administrator, Privileged Authentication Administrator)
    * add HiPo Users - A HiPo User has direct or indirect ownership on a ServicePrincipal(s) with classified permissions (AppRole, AAD Role, Azure Role, OAuthPermissionGrant)
    * use AzAPICall module version 1.1.13
    * minor fixes
* 20220505_1
    * fix: `using:scriptPath` variable in foreach parallel (this is only relevant for Azure DevOps and GitHub if you have a non default folder structure in your repository) - thanks Matt :)
* 20220501_1
    * parameter `-ManagementGroupId` accepts multiple Management Groups in form of an array e.g. `.\pwsh\AzADServicePrincipalInsights.ps1 -ManagementGroupId @('mgId0', 'mgId1')`
    * new parameter `-OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes`. You may want to only report on Service Principals that have RBAC permissions on Azure resources at and below that Management Group scope(s) (Management Groups, Subscriptions, Resource Groups and Resources)
    * Role assignments on Azure resources - mark those RBAC Role assignments which leverage a RBAC Role definition that can create role assignments as critical
    * updated YAML workflow/pipeline files
    * minor bug fixes
    * performance optimization
* 20220425_2
    * add parameter `-ManagementGroupId` (if undefined, then Tenant Root Management Group will be used)
    * use AzAPICall module version 1.1.11
* 20220404_1 
    * add FederatedIdentityCredentials

# Features

* HTML export
* JSON export
* CSV export (wip)
  * AADRoleAssignments
  * AppRoleAssignments
  * Oauth2PermissionGrants
  * AppSecrets
  * AppCertificates
  * AppFederatedIdentityCredentials
* Customizable permission classification (permissionClassification.json)

# Data

* ServicePrincipals by type
* ServicePrincipal  owners
* Application owners
* ServicePrincipal owned objects
* ServicePrincipal  AAD Role assignments
* ServicePrincipal AAD Role assignedOn
* Application AAD Role assignedOn
* App Role assignments (API permissions Application)
* App Roles assignedTo (Users and Groups)
* Oauth permission grants (API permissions delegated)
* Azure Role assignments (Azure Resources; Management Groups, Subscriptions, Resource Groups, Resources)
* ServicePrincipal Group memberships
* Application Secrets
* Application Certificates
* Application Federated Identity Credentials

# Prerequisites

## Permissions

### Azure

Management Group (Tenant Root Management Group) RBAC: __Reader__

### Azure Active Directory

Microsoft Graph API | Application | __Application.Read.All__  
Microsoft Graph API | Application | __Group.Read.All__  
Microsoft Graph API | Application | __RoleManagement.Read.All__  
Microsoft Graph API | Application | __User.Read.All__

### Azure DevOps

The Build Service Account or Project Collection Build Service Account (which ever you use) requires __Contribute__ permissions on the repository (Project settings - Repos - Security)

## PowerShell
Requires PowerShell Version >= 7.0.3

Requires PowerShell Module 'AzAPICall'.  
Running in Azure DevOps or GitHub Actions the AzAPICall PowerShell module will be installed automatically.  
AzAPICall resources:

[![PowerShell Gallery Version (including pre-releases)](https://img.shields.io/powershellgallery/v/AzAPICall?include_prereleases&label=PowerShell%20Gallery)](https://www.powershellgallery.com/packages/AzAPICall)  
[GitHub Repository](https://aka.ms/AzAPICall)

# Execute as Service Principal / Application

#USER: 'Application (client) ID' of the App registration OR 'Application ID' of the Service Principal (Enterprise Application)  
#PASSWORD: Secret of the App registration  

```
$pscredential = Get-Credential
Connect-AzAccount -ServicePrincipal -TenantId <tenantId> -Credential $pscredential
```

# Preview

![previewHTML](img/preview.png)  
![previewHTML2](img/preview2.png)  
![previewJSON](img/previewJSON.png)

## AzAdvertizer

![alt text](img/azadvertizer70.png "example output")

Also check <https://www.azadvertizer.net> - AzAdvertizer helps you to keep up with the pace by providing overview and insights on new releases and changes/updates for Azure Governance capabilities such as Azure Policy's Policy definitions, initiatives (Set definitions), aliases and Azure RBAC's Role definitions and resource provider operations.

## AzGovViz

![alt text](img/AzGovVizConnectingDots_v4.2_h120.png "example output")

Also check <https://aka.ms/AzGovViz> - Azure Governance Visualizer is intended to help you to get a holistic overview on your technical Azure Governance implementation by connecting the dots.  
It is a PowerShell script that iterates your Azure Tenant's Management Group hierarchy down to Subscription level, it captures most relevant Azure governance capabilities such as Azure Policy, RBAC and Blueprints and a lot more..
* Listed as [tool](https://docs.microsoft.com/en-us/azure/cloud-adoption-framework/reference/tools-templates#govern) for the Govern discipline in the Microsoft Cloud Adoption Framework (CAF)  
* Listed as [security monitoring tool](https://docs.microsoft.com/en-us/azure/architecture/framework/security/monitor-tools) in the Microsoft Well Architected Framework (WAF)

## Closing Note

Please note that while being developed by a Microsoft employee, AzADServicePrincipalInsights is not a Microsoft service or product. AzADServicePrincipalInsights is a personal/community driven project, there are none implicit or explicit obligations related to this project, it is provided 'as is' with no warranties and confer no rights.