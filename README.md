# AzAdServicePrincipalInsights

## Features

List all ServicePrincipals
* ServicePrincipal  owners
* Application owners
* ServicePrincipal owned objects
* AAD Role assignments
* App Role assignments
* App Roles assigned to
* Oauth permission grants
* Oauth permission granted to
* Azure Role assignments
* Group memberships
* Application Secrets
* Application Certificates
* ManagedIdentity Resource Type

## Permission requirements:

### Azure

Management Group (Tenant Root Management Group) RBAC: __Reader__

### Azure Active Directory

Microsoft Graph API | Application | __Directory.Read.All__  
Microsoft Graph API | Application | __RoleManagement.Read.All__
