#USER: 'Application (client) ID' of the App registration OR 'Application ID' of the Service Principal (Enterprise Application)
#PASSWORD:secret of the App registration
$pscredential = Get-Credential
Connect-AzAccount -ServicePrincipal -TenantId '<tenantId>' -Credential $pscredential