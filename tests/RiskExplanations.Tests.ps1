#Pester 5+ tests for the explanations of the Risk page (theme 28): every reason, finding, category and setting the report can
#show has an explanation ('Why it is risky' column) or the section has a note; the column is not exported to the CSV.

BeforeDiscovery {
    #the texts the row builders write (literals in their source); -ForEach data is needed at discovery time
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $discoveryAst = Get-ScriptAst -Path $script:MainScriptPath
    $discoverySource = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $functionText = { param([string]$name) $discoveryAst.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name }, $true).Extent.Text }
    $script:AppFindings = @([regex]::Matches((& $functionText 'getRedirectUriFindings') + (& $functionText 'getAppConfigurationFindingRows'), "finding = [`"']([^`"']+)[`"']") | ForEach-Object { $_.Groups[1].Value } | Sort-Object -Unique)
    $script:CleanupReasons = @([regex]::Matches((& $functionText 'getCleanupCandidateRows'), "\`$reasons\.Add\(@\('([^']+)'") | ForEach-Object { $_.Groups[1].Value })
    $script:PrivilegeReasons = @([regex]::Matches((& $functionText 'getEffectivePrivileges'), "level = (?:'(\w+)'|\`$\(if [^)]*\) \{ '(\w+)' \} else \{ '(\w+)' \}\)); category = '([^']+)'") | ForEach-Object {
            $category = $_.Groups[4].Value
            foreach ($level in @($_.Groups[1].Value, $_.Groups[2].Value, $_.Groups[3].Value).where({ $_ })) { "$category|$level" }
        })
    $script:CredentialFindings = @([regex]::Matches((& $functionText 'getCredentialRiskRows'), "\`$findings\.Add\([`"']([^`"']+)[`"']\)") | ForEach-Object { $_.Groups[1].Value -replace '\$secrets', '2' })
    $script:ConsentSettings = @([regex]::Matches($discoverySource, "tenantConsentSettings\.Add\(\[PSCustomObject\]\[ordered\]@\{ Setting = '([^']+)'") | ForEach-Object { $_.Groups[1].Value })
}

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'getRiskExplanation' }, $true).Extent.Text))
    foreach ($variable in '$script:spiRiskExplanations', '$script:spiRiskSectionNotes') {
        . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -eq $variable }, $true).Extent.Text))
    }
}

Describe 'Risk page explanations' {
    It 'the source lists were found (the test reads the row builders)' -ForEach @(@{ Counts = @($AppFindings.Count, $CleanupReasons.Count, $PrivilegeReasons.Count, $CredentialFindings.Count, $ConsentSettings.Count) }) {
        $Counts[0] | Should -BeGreaterOrEqual 7 -Because 'application configuration findings'
        $Counts[1] | Should -Be 5 -Because 'cleanup reasons'
        $Counts[2] | Should -Be 9 -Because 'effective privilege category|level combinations (+1 Azure role conditions)'
        $Counts[3] | Should -Be 4 -Because 'credential risk findings'
        $Counts[4] | Should -Be 5 -Because 'consent settings'
    }
    It 'application configuration finding: <_>' -ForEach $AppFindings {
        getRiskExplanation -Table 'AppConfiguration' -Key $_ | Should -Not -BeNullOrEmpty
    }
    It 'cleanup reason: <_>' -ForEach $CleanupReasons {
        getRiskExplanation -Table 'Cleanup' -Key $_ | Should -Not -BeNullOrEmpty
    }
    It 'effective privilege reason: <_>' -ForEach $PrivilegeReasons {
        getRiskExplanation -Table 'EffectivePrivilege' -Key $_ | Should -Not -BeNullOrEmpty
    }
    It 'credential risk finding: <_>' -ForEach $CredentialFindings {
        getRiskExplanation -Table 'CredentialRisk' -Key $_ | Should -Not -BeNullOrEmpty
    }
    It 'consent setting: <_>' -ForEach $ConsentSettings {
        getRiskExplanation -Table 'ConsentSettings' -Key $_ | Should -Not -BeNullOrEmpty
    }
    It 'scope makes the difference for Entra and Azure roles' {
        getRiskExplanation -Table 'EffectivePrivilege' -Key 'Entra role|critical' | Should -BeLike '*whole tenant*'
        getRiskExplanation -Table 'EffectivePrivilege' -Key 'Entra role|high' | Should -BeLike '*scope*'
        getRiskExplanation -Table 'EffectivePrivilege' -Key 'Azure role|critical' | Should -BeLike '*tenant root, a management group or a subscription*'
        getRiskExplanation -Table 'EffectivePrivilege' -Key 'App role|high' | Should -BeLike '*OwnedBy*'
        getRiskExplanation -Table 'EffectivePrivilege' -Key 'unknown|high' | Should -BeExactly ''
    }
    It 'every Risk section has a column or a note' {
        foreach ($region in 'ApplicationConfigurationFindings', 'ServicePrincipalEffectivePrivilege', 'ServicePrincipalCredentialRisk', 'CleanupCandidates', 'TenantConsentSettings') {
            $call = [regex]::Match($script:Source, "(?s)addReportSection -Region '$region'.*?\r?\n\r?\n").Value
            $call | Should -Match "Property = 'Why'; Csv = \`$false" -Because "$region explains each row"
        }
        $script:Source | Should -Match "addReportSection -Region 'ThirdPartyAppExposure' [^\r\n]*-Description \`$script:spiRiskSectionNotes\.ThirdPartyAppExposure"
        $script:spiRiskSectionNotes.ThirdPartyAppExposure | Should -BeLike 'Why it is risky:*'
    }
    It 'the explanation column is report only (CSV columns unchanged)' {
        $script:Source | Should -Match '\$Rows \| Select-Object -Property @\(\$Columns\.where\(\{ \$_\.Csv -ne \$false \}\)\.Property\) \| protectCsvFormula'
    }
}
