#Pester 5+ tests: ARM api-versions = latest stable (maintainer decision, theme 20; checked 2026-11-06 against ARM provider metadata
#and azure-rest-api-specs, old/new responses compared in the test tenant). Update this list together with the code.

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    #operation (path suffix before '?') -> api-version
    $script:Expected = [ordered]@{
        'Microsoft.Authorization/policyAssignments'               = '2026-07-01'
        #Azure role conditions (maintainer decision 2026-11-10): the role definitions with their conditions - preview api-version in a
        #variable (2023-07-01-preview; 2022-05-01-preview in AzureUSGovernment / AzureChinaCloud; fallback 2022-04-01), see the test below
        'Microsoft.Authorization/roleDefinitions'                 = '$($roleDefinitionsApiVersion)'
        'Microsoft.Authorization/roleAssignments'                 = '2022-04-01'
        'Microsoft.Authorization/roleAssignmentScheduleInstances' = '2020-10-01'   #only stable version
        '/resources'                                              = '2025-04-01'
        '/federatedIdentityCredentials'                           = '2024-11-30'
        'Microsoft.Management/getEntities'                        = '2023-04-01'
        'Microsoft.Management/managementGroups/'                  = '2023-04-01'
        '/subscriptions'                                          = '2022-12-01'
        '/authsettings/list'                                      = '2026-08-01'
        '/authsettingsV2/list'                                    = '2026-08-01'
        'Microsoft.Insights/dataCollectionRules/'                 = '2024-03-11'   #newest stable in azure-rest-api-specs
        'dataCollectionEndpointId)'                               = '2024-03-11'
    }
    $files = @('pwsh/AzADServicePrincipalInsights.ps1', 'pwsh/Send-AzADSPILogAnalyticsData.ps1', '.github/workflows/AzADServicePrincipalInsights.yml', '.github/workflows/AzADServicePrincipalInsights_OIDC.yml', 'pwsh/Publish-AzADSPIWebApp.ps1')
    $script:Uses = foreach ($f in $files) {
        foreach ($line in (Get-Content -LiteralPath (Join-Path $script:RepoRoot $f))) {
            if ($line -match '^\s*#') { continue }
            foreach ($m in [regex]::Matches($line, '"[^"]*?(?<path>[\w./{}()$:\[\]''-]+)\?[^"]*?api-version=(?<v>[\w-]+|\$\(\$\w+\))')) {
                #ARM only (AzAPICall endpoint ARM, or a path built from an ARM resource id)
                if ($line -notmatch "azAPIEndpointUrls'?\]?\.ARM|uriConfig|dataCollectionEndpointId") { continue }
                [pscustomobject]@{ File = $f; Uri = $m.Value; Version = $m.Groups['v'].Value }
            }
        }
    }
}

Describe 'ARM api-versions' {
    It 'finds the ARM calls' {
        @($script:Uses).Count | Should -BeGreaterOrEqual 18
    }
    It 'every ARM call uses the latest stable api-version of its operation' {
        foreach ($u in $script:Uses) {
            #the operation is the end of the path (…/subscriptions/{id}/resources is 'resources'); otherwise the longest contained key
            $path = $u.Uri.Split('?')[0]
            $key = @($script:Expected.Keys | Where-Object { $path.EndsWith($_) }) + @($script:Expected.Keys | Where-Object { $path -like "*$_*" } | Sort-Object Length -Descending) | Select-Object -First 1
            $key | Should -Not -BeNullOrEmpty -Because "$($u.File): $($u.Uri) is not in the list"
            #the built-in listing tries the api-versions in turn ($roleDefinitionsApiVersionTry)
            $expected = $(if ($key -eq 'Microsoft.Authorization/roleDefinitions') { @('$($roleDefinitionsApiVersion)', '$($roleDefinitionsApiVersionTry)') } else { @($script:Expected[$key]) })
            $u.Version | Should -BeIn $expected -Because "$($u.File): $($u.Uri)"
            $u.Version | Should -Not -Match 'preview'
        }
    }
    It 'role definitions: the preview api-version with conditions, per cloud, with the stable fallback' {
        $source = Get-Content -Raw -LiteralPath (Join-Path $script:RepoRoot 'pwsh/AzADServicePrincipalInsights.ps1')
        $source | Should -Match ([regex]::Escape("`$roleDefinitionsApiVersion = `$(if (`$azAPICallConf['checkContext'].Environment.Name -in 'AzureUSGovernment', 'AzureChinaCloud') { '2022-05-01-preview' } else { '2023-07-01-preview' })"))
        $source | Should -Match ([regex]::Escape("foreach (`$roleDefinitionsApiVersionTry in @(`$roleDefinitionsApiVersion, '2022-04-01'))"))
        ([regex]::Matches($source, [regex]::Escape('$roleDefinitionsApiVersion = $using:roleDefinitionsApiVersion'))).Count | Should -Be 2
    }
}
