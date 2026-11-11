<#
.SYNOPSIS
Creates the arguments file of a PowerShell script in Azure DevOps pipelines and GitHub Actions workflows.

.DESCRIPTION
Every parameter of the script (its param block, read with the PowerShell parser - the script is not executed) is looked up
- Azure DevOps (no -VariablesFile): in the environment - Azure DevOps sets the non-secret pipeline variables (variables
  template, variable groups, pipeline UI variables) as environment variables (upper case); secret variables are not visible
- GitHub Actions (-VariablesFile): in the variables file, same layout as the Azure DevOps variables template:
  'variables:' followed by '- name:' / 'value:' pairs
converted to the type of the parameter (switch: true/false, integer, comma separated list, hashtable as @{..} literal or JSON,
string), checked against the ValidateSet / ValidateRange of the parameter and written as JSON object to -OutputFile.
Read-AzADSPIArgumentFile.ps1 reads the file and the run step passes the parameters by splatting: a value never becomes part of a
script text (no code injection through ';', '$( )' etc., spaces and JSON values arrive unchanged).

Product specific rules are read from the settings file (default: <script name>.arguments.psd1 next to the script, data only):
- Internal: parameters that are never taken from a variable (set by the pipeline/workflow or internal to the script)
- Required: parameters that must have a value (in addition to the Mandatory parameters of the script)
- NoPlaceholder: parameters whose value (each item of a list) may not start with '<' (template placeholder) or contain spaces
- Dependencies: parameter = parameter it needs; a note when it is set without the other one (the value is passed anyway)

Any error fails the step (Azure DevOps '##vso[task.logissue]' / GitHub Actions '::error::' plus throw). The output file is
deleted first and only written when every check passed.

.PARAMETER ScriptFile
Path of the script the arguments are created for.

.PARAMETER OutputFile
Path of the arguments file (JSON). Use the temp directory of the agent/runner, not the output folder of the script - the
output folder is pushed to the repository.

.PARAMETER VariablesFile
GitHub Actions: path of the variables file. Without it the values are read from the environment (Azure DevOps).

.PARAMETER KnownScriptFile
Other scripts whose parameters are also set in the variables file (e.g. a second script of the same workflow); their
variables are not reported as unknown.

.PARAMETER SettingsFile
Path of the settings file. Default: <ScriptFile without .ps1>.arguments.psd1, if it exists.

.PARAMETER ScriptPath
Value for the -ScriptPath parameter of the script (the script directory, e.g. 'pwsh'), if the script has that parameter.

.EXAMPLE
Azure DevOps:
./pwsh/New-AzADSPIArgumentFile.ps1 -ScriptFile ./pwsh/AzADServicePrincipalInsights.ps1 -OutputFile "$($env:AGENT_TEMPDIRECTORY)/AzADSPIArguments.json" -ScriptPath pwsh

.EXAMPLE
GitHub Actions:
./pwsh/New-AzADSPIArgumentFile.ps1 -ScriptFile ./pwsh/AzADServicePrincipalInsights.ps1 -OutputFile "$($env:RUNNER_TEMP)/AzADSPIArguments.json" -ScriptPath pwsh -VariablesFile ./.github/AzADServicePrincipalInsights.variables.yml
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [string]
    $ScriptFile,

    [Parameter(Mandatory)]
    [string]
    $OutputFile,

    [string]
    $VariablesFile,

    [string[]]
    $KnownScriptFile,

    [string]
    $SettingsFile,

    [string]
    $ScriptPath
)

function writeMessage {
    param(
        [ValidateSet('error', 'warning')]
        [string]
        $Type,

        [string]
        $Message
    )
    if ($env:TF_BUILD -eq 'True') { Write-Host "##vso[task.logissue type=$Type]$Message" }
    elseif ($env:GITHUB_ACTIONS -eq 'true') { Write-Host "::$($Type)::$Message" }
    else { Write-Host "$($Type.ToUpperInvariant()): $Message" }
}

function stopWithError {
    param(
        [string]
        $Message
    )
    writeMessage -Type 'error' -Message $Message
    #throw, not exit: exit would only end this script when it is called from another script
    throw $Message
}

function getScriptAst {
    param(
        [string]
        $Path
    )
    if (-not (Test-Path -LiteralPath $Path -PathType Leaf)) {
        stopWithError "Script file '$($Path)' not found"
    }
    $tokens = $null
    $parseErrors = $null
    $ast = [System.Management.Automation.Language.Parser]::ParseFile((Resolve-Path -LiteralPath $Path).ProviderPath, [ref]$tokens, [ref]$parseErrors)
    if ($parseErrors.Count -gt 0) {
        stopWithError "Script file '$($Path)' cannot be parsed: $($parseErrors[0].Message) (line $($parseErrors[0].Extent.StartLineNumber))"
    }
    if (-not $ast.ParamBlock -or $ast.ParamBlock.Parameters.Count -eq 0) {
        stopWithError "Script file '$($Path)' has no param block"
    }
    return $ast
}

function convertFromYamlScalar {
    #one YAML scalar of the variables file: '' (empty), 'single quoted', "double quoted" or plain (an inline comment ' #...' is removed)
    param(
        [string]
        $Text,

        [string]
        $Name,

        [int]
        $LineNumber
    )
    $text = $Text.Trim()
    if ($text -eq '' -or $text.StartsWith('#')) {
        return ''
    }
    if ($text.StartsWith("'")) {
        $match = [regex]::Match($text, "^'((?:[^']|'')*)'\s*(#.*)?$")
        if (-not $match.Success) {
            stopWithError "Variables file line $($LineNumber) ($($Name)): the single quoted value is not closed"
        }
        return $match.Groups[1].Value.Replace("''", "'")
    }
    if ($text.StartsWith('"')) {
        $match = [regex]::Match($text, '^"((?:[^"\\]|\\.)*)"\s*(#.*)?$')
        if (-not $match.Success) {
            stopWithError "Variables file line $($LineNumber) ($($Name)): the double quoted value is not closed"
        }
        $escaped = $match.Groups[1].Value
        $unknownEscape = [regex]::Match($escaped, '\\[^\\"/nt]')
        if ($unknownEscape.Success) {
            stopWithError "Variables file line $($LineNumber) ($($Name)): unsupported escape sequence '$($unknownEscape.Value)' in the double quoted value (supported: \\ \"" \/ \n \t) - use a single quoted value"
        }
        return [regex]::Replace($escaped, '\\([\\"/nt])', {
                param($m)
                switch ($m.Groups[1].Value) {
                    'n' { "`n" }
                    't' { "`t" }
                    default { $m.Groups[1].Value }
                }
            })
    }
    if ($text -match '^[\[\{\|>&\*!%@`]') {
        stopWithError "Variables file line $($LineNumber) ($($Name)): a value starting with '$($text[0])' must be quoted, e.g. value: '$($text)'"
    }
    if ($text.Contains('${{')) {
        stopWithError "Variables file line $($LineNumber) ($($Name)): expressions ('`${{ }}') are not supported in the variables file - provide the value itself (lists comma separated)"
    }
    $commentIndex = $text.IndexOf(' #')
    if ($commentIndex -ge 0) {
        $text = $text.Substring(0, $commentIndex)
    }
    $text = $text.Trim()
    if ($text -in '~', 'null', 'Null', 'NULL') {
        #YAML null - same as an empty value
        return ''
    }
    return $text
}

function readVariablesFile {
    #strict reader for the layout of the variables file: 'variables:' followed by '- name: <name>' / 'value: <scalar>' pairs
    param(
        [string]
        $Path
    )
    $variables = [System.Collections.Generic.Dictionary[string, string]]::new([System.StringComparer]::OrdinalIgnoreCase)
    $lines = Get-Content -LiteralPath $Path
    $inVariables = $false
    $currentName = $null
    $currentNameLine = 0
    for ($i = 0; $i -lt $lines.Count; $i++) {
        $lineNumber = $i + 1
        $line = $lines[$i]
        $trimmed = $line.Trim()
        if ($trimmed -eq '' -or $trimmed.StartsWith('#')) {
            continue
        }
        if ($line -match '^variables:\s*(#.*)?$') {
            $inVariables = $true
            continue
        }
        if (-not $inVariables) {
            stopWithError "Variables file line $($lineNumber): expected 'variables:' before '$($trimmed)'"
        }
        if ($line -match '^\S') {
            stopWithError "Variables file line $($lineNumber): only the 'variables:' section is supported, found '$($trimmed)'"
        }
        $nameMatch = [regex]::Match($trimmed, '^-\s+name:\s*(.*)$')
        if ($nameMatch.Success) {
            if ($null -ne $currentName) {
                stopWithError "Variables file line $($currentNameLine): variable '$($currentName)' has no 'value:' line"
            }
            $name = convertFromYamlScalar -Text $nameMatch.Groups[1].Value -Name 'name' -LineNumber $lineNumber
            if ($name -notmatch '^[A-Za-z][A-Za-z0-9_]*$') {
                stopWithError "Variables file line $($lineNumber): invalid variable name '$($name)'"
            }
            if ($variables.ContainsKey($name)) {
                stopWithError "Variables file line $($lineNumber): variable '$($name)' is defined more than once"
            }
            $currentName = $name
            $currentNameLine = $lineNumber
            continue
        }
        $valueMatch = [regex]::Match($trimmed, '^value:(\s.*)?$')
        if ($valueMatch.Success) {
            if ($null -eq $currentName) {
                stopWithError "Variables file line $($lineNumber): 'value:' without a preceding '- name:'"
            }
            $variables[$currentName] = convertFromYamlScalar -Text $valueMatch.Groups[1].Value -Name $currentName -LineNumber $lineNumber
            $currentName = $null
            continue
        }
        stopWithError "Variables file line $($lineNumber): unexpected line '$($trimmed)' (supported: '- name: <name>' followed by 'value: <value>', comments)"
    }
    if ($null -ne $currentName) {
        stopWithError "Variables file line $($currentNameLine): variable '$($currentName)' has no 'value:' line"
    }
    return $variables
}

function getEnvironmentValue {
    #Azure DevOps sets the variables upper case; exact name first, then upper case
    param(
        [string]
        $Name
    )
    $value = [Environment]::GetEnvironmentVariable($Name)
    if ($null -eq $value) {
        $value = [Environment]::GetEnvironmentVariable($Name.ToUpperInvariant())
    }
    return $value
}

function getAttributeName {
    #'ValidateSet', 'System.Management.Automation.ValidateSetAttribute' -> 'ValidateSet'
    param(
        [System.Management.Automation.Language.AttributeBaseAst]
        $Attribute
    )
    return ($Attribute.TypeName.Name -replace '^System\.Management\.Automation\.', '' -replace 'Attribute$', '')
}

#region prepare
if (Test-Path -LiteralPath $OutputFile) {
    Remove-Item -LiteralPath $OutputFile -Force -ErrorAction Stop
}

$ast = getScriptAst -Path $ScriptFile
$scriptParameterNames = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
foreach ($parameterAst in $ast.ParamBlock.Parameters) {
    $null = $scriptParameterNames.Add($parameterAst.Name.VariablePath.UserPath)
}
$knownVariableNames = [System.Collections.Generic.HashSet[string]]::new([System.StringComparer]::OrdinalIgnoreCase)
foreach ($knownScript in $KnownScriptFile) {
    foreach ($parameterAst in (getScriptAst -Path $knownScript).ParamBlock.Parameters) {
        $null = $knownVariableNames.Add($parameterAst.Name.VariablePath.UserPath)
    }
}

#settings: data only (Import-PowerShellDataFile does not run code)
$settings = @{}
if (-not $SettingsFile) {
    $defaultSettingsFile = [System.IO.Path]::ChangeExtension($ScriptFile, '.arguments.psd1')
    if (Test-Path -LiteralPath $defaultSettingsFile -PathType Leaf) {
        $SettingsFile = $defaultSettingsFile
    }
}
if ($SettingsFile) {
    if (-not (Test-Path -LiteralPath $SettingsFile -PathType Leaf)) {
        stopWithError "Settings file '$($SettingsFile)' not found"
    }
    try {
        $settings = Import-PowerShellDataFile -LiteralPath $SettingsFile -ErrorAction Stop
    }
    catch {
        stopWithError "Settings file '$($SettingsFile)' cannot be read: $($_.Exception.Message)"
    }
    foreach ($key in $settings.Keys) {
        if ($key -notin 'Internal', 'Required', 'NoPlaceholder', 'Dependencies') {
            stopWithError "Settings file '$($SettingsFile)': unknown key '$($key)' (supported: Internal, Required, NoPlaceholder, Dependencies)"
        }
    }
    #a parameter name in the settings that the script does not have is a typo or a removed parameter
    $settingsNames = @($settings['Internal']) + @($settings['Required']) + @($settings['NoPlaceholder'])
    if ($settings['Dependencies']) {
        $settingsNames += @($settings['Dependencies'].Keys) + @($settings['Dependencies'].Values)
    }
    foreach ($settingsName in ($settingsNames | Where-Object { $_ })) {
        if (-not $scriptParameterNames.Contains($settingsName)) {
            stopWithError "Settings file '$($SettingsFile)': '$($settingsName)' is not a parameter of '$($ScriptFile)'"
        }
    }
}
$internalParameters = @($settings['Internal'] | Where-Object { $_ })
$requiredParameters = @($settings['Required'] | Where-Object { $_ })
$noPlaceholderParameters = @($settings['NoPlaceholder'] | Where-Object { $_ })
$dependencies = if ($settings['Dependencies']) { $settings['Dependencies'] } else { @{} }

if ($VariablesFile) {
    if (-not (Test-Path -LiteralPath $VariablesFile -PathType Leaf)) {
        stopWithError "Variables file '$($VariablesFile)' not found"
    }
    $fileVariables = readVariablesFile -Path $VariablesFile
    $source = "variables file '$($VariablesFile)'"
}
else {
    $source = 'environment (pipeline variables)'
}
Write-Host "Creating the arguments for '$($ScriptFile)' from the $($source)"
#endregion prepare

#region parameters
$params = @{}
foreach ($parameterAst in $ast.ParamBlock.Parameters) {
    $name = $parameterAst.Name.VariablePath.UserPath

    if ($VariablesFile) {
        $value = $null
        $null = $fileVariables.TryGetValue($name, [ref]$value)
    }
    else {
        $value = getEnvironmentValue -Name $name
    }
    if ([string]::IsNullOrWhiteSpace($value)) {
        continue
    }
    if ($name -in $internalParameters) {
        if ($VariablesFile) {
            writeMessage -Type 'warning' -Message "Variable '$($name)' ignored - the parameter is set by the pipeline/workflow or internal to the script"
        }
        continue
    }
    $value = $value.Trim()

    switch ($parameterAst.StaticType.Name) {
        'SwitchParameter' {
            if ($value -eq 'true') {
                $params[$name] = $true
            }
            elseif ($value -ne 'false') {
                #an error, not a warning: a run with a feature silently missing is harder to notice than a failed step
                stopWithError "Variable '$($name)' = '$($value)' - switch: true or false (or empty)"
            }
            break
        }
        'Int32' {
            $intValue = 0
            if (-not [int]::TryParse($value, [System.Globalization.NumberStyles]::Integer, [cultureinfo]::InvariantCulture, [ref]$intValue)) {
                stopWithError "Variable '$($name)' = '$($value)' - integer expected"
            }
            $params[$name] = $intValue
            break
        }
        { $_ -in 'Array', 'Object[]', 'String[]' } {
            $listValues = @($value -split ',' | ForEach-Object { $_.Trim() } | Where-Object { $_ })
            #review F-60: only separators (' , ') gave an empty list - -ManagementGroupId @() ran against the Tenant Root Group,
            #-SubscriptionQuotaIdWhitelist @() excluded every subscription
            if ($listValues.Count -eq 0) {
                stopWithError "Variable '$($name)' = '$($value)' - a comma separated list without values (leave it empty to use the default)"
            }
            $params[$name] = $listValues
            break
        }
        'Hashtable' {
            if ($value.StartsWith('{')) {
                try {
                    $hashtable = $value | ConvertFrom-Json -AsHashtable -NoEnumerate -ErrorAction Stop
                }
                catch {
                    stopWithError "Variable '$($name)' is not valid JSON: $($_.Exception.Message)"
                }
                if ($hashtable -isnot [System.Collections.IDictionary]) {
                    stopWithError "Variable '$($name)' must be a JSON object"
                }
                $params[$name] = $hashtable
            }
            else {
                #@{ "root" = "alz-root"; ... } - literal keys and values only: SafeGetValue() refuses variables, commands and subexpressions
                $htTokens = $null
                $htErrors = $null
                $htAst = [System.Management.Automation.Language.Parser]::ParseInput($value, [ref]$htTokens, [ref]$htErrors)
                $hashtableAst = $htAst.Find({ $args[0] -is [System.Management.Automation.Language.HashtableAst] }, $false)
                if ($htErrors.Count -gt 0 -or -not $hashtableAst -or $hashtableAst.Extent.Text -ne $value) {
                    stopWithError "Variable '$($name)' is not a hashtable - expected @{ key = 'value'; ... } or a JSON object"
                }
                try {
                    $params[$name] = $hashtableAst.SafeGetValue()
                }
                catch {
                    stopWithError "Variable '$($name)' may only contain literal keys and values"
                }
            }
            break
        }
        default {
            #string - also a parameter without a type ([object]): a comma separated value stays one string; give list
            #parameters a list type ([string[]]) so that they are split
            $params[$name] = $value
        }
    }

    #ValidateSet / ValidateRange of the parameter - checked here, not only at the parameter binding after the Azure login
    if ($params.ContainsKey($name)) {
        foreach ($attribute in $parameterAst.Attributes) {
            if ($attribute -isnot [System.Management.Automation.Language.AttributeAst]) {
                continue
            }
            $attributeName = getAttributeName -Attribute $attribute
            if ($attributeName -notin 'ValidateSet', 'ValidateRange') {
                continue
            }
            try {
                $allowed = @($attribute.PositionalArguments | ForEach-Object { $_.SafeGetValue() })
            }
            catch {
                #e.g. [ValidateRange(10KB, 1MB)] - not a constant for SafeGetValue: a number with an optional KB/MB/GB suffix
                $allowed = @(foreach ($argumentAst in $attribute.PositionalArguments) {
                        $numberMatch = [regex]::Match($argumentAst.Extent.Text, '^(\d+)(KB|MB|GB)?$', 'IgnoreCase')
                        if (-not $numberMatch.Success) {
                            stopWithError "Parameter '$($name)': the $($attributeName) argument '$($argumentAst.Extent.Text)' cannot be evaluated"
                        }
                        $multiplier = switch ($numberMatch.Groups[2].Value) { 'KB' { 1KB } 'MB' { 1MB } 'GB' { 1GB } default { 1 } }
                        [long]$numberMatch.Groups[1].Value * $multiplier
                    })
            }
            foreach ($item in @($params[$name])) {
                if ($attributeName -eq 'ValidateSet') {
                    #case-insensitive, as PowerShell's ValidateSet (IgnoreCase defaults to true)
                    if (-not ($allowed | Where-Object { [string]$_ -eq [string]$item })) {
                        stopWithError "Variable '$($name)' = '$($item)' - allowed values: $(($allowed | ForEach-Object { "'$($_)'" }) -join ', ')"
                    }
                }
                elseif ($allowed.Count -eq 2) {
                    if ([double]$item -lt [double]$allowed[0] -or [double]$item -gt [double]$allowed[1]) {
                        stopWithError "Variable '$($name)' = '$($item)' - allowed range: $($allowed[0]) to $($allowed[1])"
                    }
                }
            }
        }
    }
}

if ($ScriptPath -and $scriptParameterNames.Contains('ScriptPath')) {
    $params['ScriptPath'] = $ScriptPath
}

if ($VariablesFile) {
    foreach ($variableName in $fileVariables.Keys) {
        if (-not $scriptParameterNames.Contains($variableName) -and -not $knownVariableNames.Contains($variableName)) {
            writeMessage -Type 'warning' -Message "Variable '$($variableName)' ignored - '$($ScriptFile)' has no such parameter"
        }
    }
}
#endregion parameters

#region checks
#Mandatory parameters of the script (except the ones the pipeline/workflow sets) and the Required ones of the settings
$mandatoryParameters = foreach ($parameterAst in $ast.ParamBlock.Parameters) {
    $isMandatory = $false
    foreach ($attribute in $parameterAst.Attributes) {
        if ($attribute -is [System.Management.Automation.Language.AttributeAst] -and (getAttributeName -Attribute $attribute) -eq 'Parameter') {
            foreach ($namedArgument in $attribute.NamedArguments) {
                if ($namedArgument.ArgumentName -eq 'Mandatory' -and ($namedArgument.ExpressionOmitted -or $namedArgument.Argument.SafeGetValue() -eq $true)) {
                    $isMandatory = $true
                }
            }
        }
    }
    if ($isMandatory) {
        $parameterAst.Name.VariablePath.UserPath
    }
}
$missingNames = @(@($mandatoryParameters) + $requiredParameters | Where-Object { $_ -and $_ -notin $internalParameters -and -not $params.ContainsKey($_) } | Sort-Object -Unique)
if ($missingNames.Count -gt 0) {
    stopWithError "Variable(s) not set: $($missingNames -join ', ') - '$($ScriptFile)' requires them"
}

foreach ($noPlaceholderName in $noPlaceholderParameters) {
    foreach ($item in @($params[$noPlaceholderName] | Where-Object { $_ })) {
        if ([string]$item -match '^<|\s') {
            stopWithError "Variable '$($noPlaceholderName)' = '$($item)' - provide the value (no spaces, not the template placeholder '<...>')"
        }
    }
}

#parameters that have no effect without another one - a note, the value is passed anyway
foreach ($dependency in $dependencies.GetEnumerator()) {
    if ($params.ContainsKey($dependency.Key) -and -not $params.ContainsKey($dependency.Value)) {
        Write-Host "Note: $($dependency.Key) has no effect without $($dependency.Value)"
    }
}
#endregion checks

#region write
$sortedParams = [ordered]@{}
foreach ($key in ($params.Keys | Sort-Object)) {
    $sortedParams[$key] = $params[$key]
}

Write-Host 'Arguments:'
foreach ($entry in $sortedParams.GetEnumerator()) {
    Write-Host "  -$($entry.Key) $(ConvertTo-Json -InputObject $entry.Value -Compress -Depth 10)"
}

$outputDirectory = Split-Path -Path $OutputFile -Parent
if ($outputDirectory -and -not (Test-Path -LiteralPath $outputDirectory)) {
    $null = New-Item -ItemType Directory -Path $outputDirectory -Force -ErrorAction Stop
}
ConvertTo-Json -InputObject $sortedParams -Depth 10 | Set-Content -LiteralPath $OutputFile -Encoding utf8NoBOM -ErrorAction Stop
Write-Host "Arguments file: '$($OutputFile)'"
#endregion write
