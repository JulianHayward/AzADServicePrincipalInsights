# PSScriptAnalyzer settings of this repository - one source for the CI workflow (.github/workflows/psScriptAnalyzer.yml),
# the editor (.vscode/settings.json 'powershell.scriptAnalysis.settingsPath') and the local call in CONTRIBUTING.md:
#   Invoke-ScriptAnalyzer -Path ./pwsh -Recurse -Settings ./PSScriptAnalyzerSettings.psd1
@{
    ExcludeRules = @(
        # Write-Host is the console output convention of the scripts
        'PSAvoidUsingWriteHost'
        # false positives for this code base (e.g. variables read by dot-sourced code regions in the Pester tests)
        'PSUseDeclaredVarsMoreThanAssignments'
        'PSReviewUnusedParameter'
        'PSUseOutputTypeCorrectly'
    )
}
