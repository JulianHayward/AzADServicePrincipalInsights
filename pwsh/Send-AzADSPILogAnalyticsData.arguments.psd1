#Settings for New-AzADSPIArgumentFile.ps1 (pipeline/workflow arguments of Send-AzADSPILogAnalyticsData.ps1) - data only
@{
    #set by the pipeline/workflow: the JSON_SP_* folder of the run
    Internal      = @(
        'ImportPath'
    )
    NoPlaceholder = @(
        'DataCollectionRuleSubscriptionId'
        'DataCollectionRuleResourceGroup'
        'DataCollectionRuleName'
        'LogAnalyticsCustomLogTableName'
    )
}
