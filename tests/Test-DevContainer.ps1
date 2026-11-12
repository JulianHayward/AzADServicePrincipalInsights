#Runs inside the dev container (workflow .github/workflows/devcontainer.yml, devcontainers/ci): the container has what
#.devcontainer/devcontainer.json promises and what the script needs, so a Codespace can run AzADServicePrincipalInsights
#without installing anything:
#- PowerShell in the version of the PowerShell feature
#- AzAPICall in the version of the script's -azAPICallVersion default (the script imports exactly that version; otherwise it
#  stops at the install question), and Az.Accounts
#- git
#Not a Pester file (no *.Tests.ps1): it checks the environment it runs in, not the code.

$ErrorActionPreference = 'Stop'
$repoRoot = Split-Path -Path $PSScriptRoot -Parent
$devContainer = Get-Content -Raw -LiteralPath (Join-Path $repoRoot '.devcontainer/devcontainer.json')
$mainScript = Get-Content -Raw -LiteralPath (Join-Path $repoRoot 'pwsh/AzADServicePrincipalInsights.ps1')

#expected values (regex: devcontainer.json has comments)
$expectedPowerShell = [regex]::Match($devContainer, '"version":\s*"(?<v>[^"]+)"').Groups['v'].Value
$expectedAzAPICall = [regex]::Match($mainScript, "\[string\]\`$azAPICallVersion = '(?<v>[^']+)'").Groups['v'].Value
$devContainerAzAPICall = [regex]::Match($devContainer, 'AzAPICall==(?<v>[\d.]+)').Groups['v'].Value

$results = [System.Collections.Generic.List[object]]::new()
function check([string]$Name, [bool]$Passed, [string]$Detail) {
    $results.Add([PSCustomObject]@{ Check = $Name; Result = $(if ($Passed) { 'PASSED' } else { 'FAILED' }); Detail = $Detail })
}

check 'expected values found' ($expectedPowerShell -and $expectedAzAPICall -and $devContainerAzAPICall) "PowerShell '$expectedPowerShell', AzAPICall script default '$expectedAzAPICall', devcontainer.json '$devContainerAzAPICall'"
check 'devcontainer.json AzAPICall = script default' ($devContainerAzAPICall -eq $expectedAzAPICall) "$devContainerAzAPICall / $expectedAzAPICall"

$psVersion = $PSVersionTable.PSVersion.ToString()
check "PowerShell $expectedPowerShell" ($psVersion -eq $expectedPowerShell -or $psVersion.StartsWith("$expectedPowerShell.")) $psVersion

$azAPICall = @(Get-Module -ListAvailable -Name AzAPICall)
check "AzAPICall $expectedAzAPICall installed" ([bool]($azAPICall.where({ $_.Version.ToString() -eq $expectedAzAPICall }))) "installed: $(@($azAPICall.Version | ForEach-Object { $_.ToString() }) -join ', ')"
try {
    Import-Module -Name AzAPICall -RequiredVersion $expectedAzAPICall -Force
    check "AzAPICall $expectedAzAPICall imports" $true ''
}
catch {
    check "AzAPICall $expectedAzAPICall imports" $false $_.Exception.Message
}

try {
    Import-Module -Name Az.Accounts -Force
    check 'Az.Accounts imports' $true "$((Get-Module -Name Az.Accounts).Version)"
}
catch {
    check 'Az.Accounts imports' $false $_.Exception.Message
}

$gitVersion = try { (& git --version) 2>&1 } catch { $null }
check 'git' ([bool]$gitVersion) "$gitVersion"

$results | Format-Table -AutoSize -Wrap | Out-String -Width 200 | Write-Host
$failed = @($results.where({ $_.Result -eq 'FAILED' }))
if ($failed.Count -gt 0) {
    throw "$($failed.Count) dev container check(s) failed: $(($failed.Check) -join '; ')"
}
Write-Host 'Dev container checks passed'
