# AzAdServicePrincipalInsights

![previewHTML](img/preview.png)

## Features

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

## Permission requirements:

### Azure

Management Group (Tenant Root Management Group) RBAC: __Reader__

### Azure Active Directory

Microsoft Graph API | Application | __Application.Read.All__  
Microsoft Graph API | Application | __Group.Read.All__  
Microsoft Graph API | Application | __RoleManagement.Read.All__  
Microsoft Graph API | Application | __User.Read.All__

### Azure DevOps

The Build Service Account or Project Collection Build Service Account (which ever you use) requires 'contribute' permissions on the repository
