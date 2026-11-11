#Settings for New-AzADSPIArgumentFile.ps1 (pipeline/workflow arguments of AzADServicePrincipalInsights.ps1) - data only
@{
    #never taken from a variable: set by the pipeline/workflow (ScriptPath) or internal to the script
    Internal      = @(
        'Product'
        'ProductVersion'
        'AzAPICallModuleName'
        'azAPICallVersion'
        'GitHubRepository'
        'DirectorySeparatorChar'
        'ScriptPath'
    )
    #the output folder is pushed to the repository - it must be set (variables file: wiki)
    Required      = @(
        'OutputPath'
    )
    #no template placeholder '<...>', no spaces (ManagementGroupId: the ID, not the display name; empty = Tenant Root Group)
    NoPlaceholder = @(
        'ManagementGroupId'
        'SubscriptionId4AzContext'
    )
}
