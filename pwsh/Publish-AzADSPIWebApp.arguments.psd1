#Settings for New-AzADSPIArgumentFile.ps1 (workflow arguments of Publish-AzADSPIWebApp.ps1) - data only
@{
    #set by the workflow: the arguments file of the run, the folders
    Internal      = @(
        'ArgumentsFile'
        'BasePath'
        'ZipDirectory'
    )
    Required      = @(
        'WebAppSubscriptionId'
        'WebAppResourceGroup'
        'WebAppName'
    )
    NoPlaceholder = @(
        'WebAppSubscriptionId'
        'WebAppResourceGroup'
        'WebAppName'
    )
}
