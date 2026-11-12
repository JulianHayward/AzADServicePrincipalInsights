#Pester 5+ tests for the usage statistics (README: Telemetry; maintainer decision 2026-11-07). Nothing is sent: the identifier
#function is run and the event bodies of the stats region are checked.
#Identifier = SHA-256 of 'AzADServicePrincipalInsights|<tenant ID>': one value per tenant (unique-tenant count), no account or
#user name. -StatsOptOut sends only a random ID (run count).

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'getTelemetryIdentifier' }, $true).Extent.Text))
    $script:StatsIf = $ast.Find({ param($n) $n -is [System.Management.Automation.Language.IfStatementAst] -and $n.Clauses[0].Item1.Extent.Text -eq '-not $StatsOptOut' }, $true)
    $script:Tenant = '11111111-2222-4333-8444-555555555555'
}

Describe 'telemetry identifier' {
    It 'is the SHA-256 of the product name and the tenant ID' {
        $expected = [System.BitConverter]::ToString([System.Security.Cryptography.SHA256]::Create().ComputeHash([System.Text.Encoding]::UTF8.GetBytes("AzADServicePrincipalInsights|$($script:Tenant)"))) -replace '-'
        getTelemetryIdentifier -TenantId $script:Tenant | Should -BeExactly $expected
        getTelemetryIdentifier -TenantId $script:Tenant | Should -Match '^[0-9A-F]{64}$'
    }
    It 'is one value per tenant: the casing and spaces of the tenant ID do not matter' {
        getTelemetryIdentifier -TenantId " $($script:Tenant.ToUpperInvariant()) " | Should -BeExactly (getTelemetryIdentifier -TenantId $script:Tenant)
    }
    It 'differs between tenants' {
        getTelemetryIdentifier -TenantId '11111111-2222-4333-8444-555555555556' | Should -Not -Be (getTelemetryIdentifier -TenantId $script:Tenant)
    }
    It 'is computed from the tenant ID only (no account, no user name, no repository ID)' {
        $branch = $script:StatsIf.Clauses[0].Item2.Extent.Text
        $branch | Should -Match "\`$identifier = getTelemetryIdentifier -TenantId \(\[string\]\`$azAPICallConf\['checkContext'\]\.Tenant\.Id\)"
        $branch | Should -Not -Match 'Account\.Id'
        $branch | Should -Not -Match 'BUILD_REPOSITORY_ID'
        $function = (Get-ScriptAst -Path $script:MainScriptPath).Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'getTelemetryIdentifier' }, $true).Extent.Text
        $function | Should -Not -Match 'Account'
    }
}

Describe 'telemetry events' {
    It 'default: no tenant data, only the identifier, versions, platform, account type, size buckets and the error count' {
        $branch = $script:StatsIf.Clauses[0].Item2.Extent.Text
        $properties = @([regex]::Matches($branch, '"(\w+)": "\$\(') | ForEach-Object { $_.Groups[1].Value }) | Sort-Object
        $properties | Should -Be @('accType', 'azCloud', 'identifier', 'name', 'platform', 'productVersion', 'psAzAccountsVersion', 'psVersion', 'statsCountErrors', 'statsCountSPs', 'statsCountSubscriptions', 'statsTry', 'time')
    }
    It '-StatsOptOut: only a random ID and the try counter' {
        $optOut = $script:StatsIf.ElseClause.Extent.Text
        $optOut | Should -Match '\$identifier = \(New-Guid\)\.Guid'
        $properties = @([regex]::Matches($optOut, '"(\w+)": "\$\(') | ForEach-Object { $_.Groups[1].Value }) | Sort-Object
        $properties | Should -Be @('identifier', 'name', 'statsTry', 'time')
        $optOut | Should -Not -Match 'checkContext|Tenant|Account'
    }
}
