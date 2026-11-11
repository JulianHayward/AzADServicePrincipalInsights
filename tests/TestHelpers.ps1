#Helpers to run code blocks of the main script in isolation, without a tenant.

$script:RepoRoot = Split-Path -Parent $PSScriptRoot
$script:MainScriptPath = Join-Path $script:RepoRoot 'pwsh/AzADServicePrincipalInsights.ps1'

function Get-ScriptAst {
    param([Parameter(Mandatory)][string]$Path)
    $tokens = $null; $errors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseFile($Path, [ref]$tokens, [ref]$errors)
    if ($errors.Count) { throw "Parse errors in '$Path': $($errors[0].Message)" }
    $ast
}

#Writes the main script as it was at a given commit to a temp file (to prove a test catches the original bug).
function Get-MainScriptAtCommit {
    param([Parameter(Mandatory)][string]$Commit)
    $out = Join-Path ([IO.Path]::GetTempPath()) "AzADSPI_$($Commit).ps1"
    if (-not (Test-Path -LiteralPath $out)) {
        $content = git -C $script:RepoRoot show "$($Commit):pwsh/AzADServicePrincipalInsights.ps1"
        if ($LASTEXITCODE -ne 0) { throw "git show $Commit failed" }
        $content | Set-Content -LiteralPath $out -Encoding utf8
    }
    $out
}

#Returns the body of the 'ForEach-Object -Parallel { ... }' block that contains $Marker,
#with '$using:x' rewritten to '$x' so it can run in-process against variables of the caller.
#'$script:x' is rewritten to '$x' as well: inside a -Parallel runspace the block's top-level scope is also its
#script scope, so both refer to the same variable at runtime. In-process they would not.
function Get-ParallelBlock {
    param(
        [Parameter(Mandatory)][string]$Path,
        [Parameter(Mandatory)][string]$Marker
    )
    $ast = Get-ScriptAst -Path $Path
    $commands = $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.CommandAst] -and $n.GetCommandName() -eq 'ForEach-Object' }, $true)
    $found = foreach ($command in $commands) {
        $elements = $command.CommandElements
        for ($i = 0; $i -lt $elements.Count - 1; $i++) {
            if ($elements[$i] -is [System.Management.Automation.Language.CommandParameterAst] -and $elements[$i].ParameterName -eq 'Parallel' -and
                $elements[$i + 1] -is [System.Management.Automation.Language.ScriptBlockExpressionAst] -and $elements[$i + 1].Extent.Text.Contains($Marker)) {
                $elements[$i + 1].ScriptBlock
            }
        }
    }
    $found = @($found)
    if ($found.Count -ne 1) { throw "Expected exactly 1 parallel block containing '$Marker', found $($found.Count)" }
    $text = $found[0].Extent.Text
    $text = $text.Substring(1, $text.Length - 2) #strip outer braces
    #the AzAPICall import of each runspace (review R-7) is module plumbing: in-process the module (or its stand-in) is already there
    $text = $text -replace '(?m)^\s*if \(-not \(Get-Module -Name \$using:AzAPICallModuleName\)\) \{ Import-Module -Name \$using:azAPICallModuleImportPath -ErrorAction Stop \}\s*$', ''
    $usingNames = @([regex]::Matches($text, '(?i)\$using:(\w+)') | ForEach-Object { $_.Groups[1].Value } | Sort-Object -Unique)
    [pscustomobject]@{
        ScriptBlock = [scriptblock]::Create(($text -replace '(?i)\$using:', '$' -replace '(?i)\$script:', '$'))
        UsingNames = $usingNames
    }
}
