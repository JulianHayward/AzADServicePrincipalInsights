__AzADServicePrincipalInsights__

Insights and change tracking on Azure Active Directory Service Principals (Enterprise Applications and Applications)

# Updates
* 20220501_1
    * parameter `-ManagementGroupId` accepts multiple Management Groups in form of an array e.g. `.\pwsh\AzADServicePrincipalInsights.ps1 -ManagementGroupId @('mgId0', 'mgId1')`
    * new parameter `-OnlyProcessSPsThatHaveARoleAssignmentInTheRelevantMGScopes`. You may want to only report on Service Principals that have RBAC permissions on Azure resources at and below that Management Group scope(s) (Management Groups, Subscriptions, Resource Groups and Resources)
    * Role assignments on Azure resources - mark those RBAC Role assignments which leverage a RBAC Role definitions that can create role assignments as critical
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