#Pester 5+ tests for the pipeline/workflow arguments (theme 30): New-AzADSPIArgumentFile.ps1 / Read-AzADSPIArgumentFile.ps1 (shared,
#product settings in <script>.arguments.psd1), the variables files of both platforms, Get-AzADSPIOutputPath.ps1,
#Publish-AzADSPIWebApp.ps1 and the wiring in the Azure DevOps pipeline and the GitHub workflows. No Azure access.

BeforeDiscovery {
    function yamlVariable([string]$Name, [string]$Value) { "  - name: $Name`n    value: $Value`n" }
}

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:Pwsh = Join-Path $script:RepoRoot 'pwsh'
    $script:Create = Join-Path $script:Pwsh 'New-AzADSPIArgumentFile.ps1'
    $script:Read = Join-Path $script:Pwsh 'Read-AzADSPIArgumentFile.ps1'
    $script:OutputPaths = Join-Path $script:Pwsh 'Get-AzADSPIOutputPath.ps1'
    $script:Publish = Join-Path $script:Pwsh 'Publish-AzADSPIWebApp.ps1'
    $script:Main = Join-Path $script:Pwsh 'AzADServicePrincipalInsights.ps1'
    $script:Ingest = Join-Path $script:Pwsh 'Send-AzADSPILogAnalyticsData.ps1'
    $script:GhVariables = Join-Path $script:RepoRoot '.github/AzADServicePrincipalInsights.variables.yml'
    $script:AdoVariables = Join-Path $script:RepoRoot '.azuredevops/pipelines/AzADServicePrincipalInsights.variables.yml'
    $script:AdoPipeline = Join-Path $script:RepoRoot '.azuredevops/pipelines/AzADServicePrincipalInsights.yml'
    $script:Workflows = @('AzADServicePrincipalInsights.yml', 'AzADServicePrincipalInsights_OIDC.yml') | ForEach-Object { Join-Path $script:RepoRoot ".github/workflows/$_" }

    #the reader of the create script, for the tests of the variables files
    $createAst = Get-ScriptAst -Path $script:Create
    foreach ($functionName in 'writeMessage', 'stopWithError', 'convertFromYamlScalar', 'readVariablesFile') {
        . ([scriptblock]::Create($createAst.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $functionName }, $true).Extent.Text))
    }

    #the lines of the YAML block scalars ('key: |') - the indent of the first content line ends the block
    function Get-YamlBlockScalarLines([string[]]$Lines, [string]$KeyPattern) {
        $blockIndent = -1; $inBlock = $false
        foreach ($line in $Lines) {
            $indent = $line.Length - $line.TrimStart().Length
            if ($line -match "^\s*$($KeyPattern): \|\s*$") { $inBlock = $true; $blockIndent = -1; continue }
            if (-not $inBlock -or -not $line.Trim()) { continue }
            if ($blockIndent -lt 0) { $blockIndent = $indent }
            if ($indent -lt $blockIndent) { $inBlock = $false; continue }
            $line
        }
    }

    function Get-ParameterName([string]$Path) {
        @((Get-ScriptAst -Path $Path).ParamBlock.Parameters | ForEach-Object { $_.Name.VariablePath.UserPath })
    }

    #runs the create script; returns @{ Params; Output; Error; FileWritten }
    function Invoke-Create {
        param([string]$ScriptFile, [string]$VariablesFile, [string[]]$KnownScriptFile, [string]$SettingsFile, [string]$ScriptPath)
        $outputFile = Join-Path $TestDrive 'arguments.json'
        Set-Content -LiteralPath $outputFile -Value '{"stale":true}'
        $splat = @{ ScriptFile = $ScriptFile; OutputFile = $outputFile }
        foreach ($key in 'VariablesFile', 'KnownScriptFile', 'SettingsFile', 'ScriptPath') {
            if ($PSBoundParameters.ContainsKey($key)) { $splat[$key] = $PSBoundParameters[$key] }
        }
        $result = @{ Params = $null; Output = ''; Error = $null; FileWritten = $false }
        try {
            $result.Output = & $script:Create @splat 6>&1 | Out-String
        }
        catch {
            $result.Error = $_.Exception.Message
        }
        if (Test-Path -LiteralPath $outputFile) {
            $result.FileWritten = $true
            $result.Params = Get-Content -LiteralPath $outputFile -Raw | ConvertFrom-Json -AsHashtable
        }
        $result
    }

    function New-VariablesFile([string]$Body, [string]$Name = 'variables.yml') {
        $path = Join-Path $TestDrive $Name
        Set-Content -LiteralPath $path -Value "variables:`n$Body"
        $path
    }
    function yamlVariable([string]$Name, [string]$Value) {
        "  - name: $Name`n    value: $Value`n"
    }

    #a target script with every supported type, validation attributes, a Mandatory parameter and a settings file
    $script:Stub = Join-Path $TestDrive 'Stub.ps1'
    Set-Content -LiteralPath $script:Stub -Value @'
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true)][string]$Id,
    [switch]$Flag,
    [int]$Count = 1,
    [ValidateRange(1, 30)][int]$Days = 7,
    [ValidateRange(10KB, 1MB)][int]$Bytes = 900KB,
    [ValidateSet(';', ',')][string]$Delimiter = ';',
    [string[]]$List,
    [array]$Array = @('undefined'),
    [hashtable]$Map,
    [string]$Text,
    $Untyped,
    [string]$Version = '1',
    [string]$ScriptPath = 'pwsh'
)
'@
    Set-Content -LiteralPath (Join-Path $TestDrive 'Stub.arguments.psd1') -Value "@{ Internal = @('Version', 'ScriptPath'); NoPlaceholder = @('Id', 'List'); Dependencies = @{ Days = 'Flag' } }"
}

Describe 'New-AzADSPIArgumentFile.ps1: conversion and checks (stub script)' {
    It 'converts every type and writes a JSON object' {
        $vars = New-VariablesFile ((yamlVariable Id 'abc') + (yamlVariable Flag 'True') + (yamlVariable Count '5') + (yamlVariable List "'a, `$(whoami) ,b,'") + (yamlVariable Array 'x') + (yamlVariable Map "'@{ `"root`" = `"r`"; `"corp`" = `"c`" }'") + (yamlVariable Text "'two words; `$(x)'") + (yamlVariable Untyped 'a,b'))
        $r = Invoke-Create -ScriptFile $script:Stub -VariablesFile $vars -ScriptPath 'pwsh'
        $r.Error | Should -BeNullOrEmpty
        $r.Params.Id | Should -BeExactly 'abc'
        $r.Params.Flag | Should -BeTrue
        $r.Params.Count | Should -Be 5
        $r.Params.List | Should -Be @('a', '$(whoami)', 'b')
        @($r.Params.Array) | Should -Be @('x')
        $r.Params.Map.root | Should -BeExactly 'r'
        $r.Params.Text | Should -BeExactly 'two words; $(x)'
        $r.Params.Untyped | Should -BeExactly 'a,b' -Because 'a parameter without a type stays one string'
        $r.Params.ScriptPath | Should -BeExactly 'pwsh'
    }
    It 'false / empty / null / ~ leave the parameter out' {
        $vars = New-VariablesFile ((yamlVariable Id 'abc') + (yamlVariable Flag 'false') + (yamlVariable Count '') + (yamlVariable Text 'null') + (yamlVariable List '~'))
        $r = Invoke-Create -ScriptFile $script:Stub -VariablesFile $vars
        $r.Params.Keys | Should -Be @('Id')
    }
    It 'a JSON object for a hashtable parameter' {
        $r = Invoke-Create -ScriptFile $script:Stub -VariablesFile (New-VariablesFile ((yamlVariable Id 'abc') + (yamlVariable Map "'{`"a`": {`"b`": 1}}'")))
        $r.Params.Map.a.b | Should -Be 1
    }
    It 'refuses <Case> and writes no file' -ForEach @(
        @{ Case = 'a switch typo'; Body = (yamlVariable Flag 'ture'); Message = "Variable 'Flag' = 'ture' - switch" }
        @{ Case = 'an invalid integer'; Body = (yamlVariable Count '1,5'); Message = 'integer expected' }
        @{ Case = 'a value outside the ValidateSet'; Body = (yamlVariable Delimiter "'|'"); Message = "allowed values: ';', ','" }
        @{ Case = 'a value outside the ValidateRange'; Body = (yamlVariable Days '31'); Message = 'allowed range: 1 to 30' }
        @{ Case = 'a value below a KB ValidateRange'; Body = (yamlVariable Bytes '100'); Message = 'allowed range: 10240 to 1048576' }
        @{ Case = 'a hashtable with a subexpression'; Body = (yamlVariable Map "'@{`"a`"=`"`$(whoami)`"}'"); Message = 'may only contain literal keys and values' }
        @{ Case = 'a hashtable followed by a command'; Body = (yamlVariable Map "'@{`"a`"=`"b`"}; whoami'"); Message = 'is not a hashtable' }
        @{ Case = 'a JSON array for a hashtable'; Body = (yamlVariable Map "'[{`"a`":1}]'"); Message = 'is not a hashtable' }
        @{ Case = 'the template placeholder'; Body = ''; Id = "'<YourId>'"; Message = "Variable 'Id' = '<YourId>'" }
        @{ Case = 'a placeholder in a list'; Body = (yamlVariable List "'a,<x>'"); Message = "Variable 'List' = '<x>'" }
        #review F-60: separators only gave an empty list (-ManagementGroupId @() ran against the Tenant Root Group)
        @{ Case = 'a list of separators only'; Body = (yamlVariable List "' , '"); Message = 'a comma separated list without values' }
    ) {
        $idValue = if ($Id) { $Id } else { 'abc' }
        $r = Invoke-Create -ScriptFile $script:Stub -VariablesFile (New-VariablesFile ((yamlVariable Id $idValue) + $Body))
        $r.Error | Should -Match ([regex]::Escape($Message))
        $r.FileWritten | Should -BeFalse -Because 'a failed run leaves no (stale) arguments file'
    }
    It 'accepts the ValidateSet in other case and the range edges' {
        $r = Invoke-Create -ScriptFile $script:Stub -VariablesFile (New-VariablesFile ((yamlVariable Id 'abc') + (yamlVariable Delimiter "','") + (yamlVariable Days '30') + (yamlVariable Bytes '1048576')))
        $r.Error | Should -BeNullOrEmpty
        $r.Params.Days | Should -Be 30
    }
    It 'a Mandatory parameter without value is an error' {
        $r = Invoke-Create -ScriptFile $script:Stub -VariablesFile (New-VariablesFile (yamlVariable Text 'x'))
        $r.Error | Should -Match 'Variable\(s\) not set: Id'
    }
    It 'warns for an internal parameter, an unknown variable; notes a dependency' {
        $other = Join-Path $TestDrive 'Other.ps1'; Set-Content -LiteralPath $other -Value 'param([string]$OtherParam)'
        $r = Invoke-Create -ScriptFile $script:Stub -KnownScriptFile $other -VariablesFile (New-VariablesFile ((yamlVariable Id 'abc') + (yamlVariable Version '2') + (yamlVariable Typo 'x') + (yamlVariable OtherParam 'y') + (yamlVariable Days '3')))
        $r.Error | Should -BeNullOrEmpty
        $r.Params.ContainsKey('Version') | Should -BeFalse
        $r.Output | Should -Match "Variable 'Version' ignored - the parameter is set by the pipeline/workflow"
        $r.Output | Should -Match "Variable 'Typo' ignored"
        $r.Output | Should -Not -Match "Variable 'OtherParam' ignored" -Because 'a parameter of a -KnownScriptFile is known'
        $r.Output | Should -Match 'Note: Days has no effect without Flag'
    }
    It 'refuses a settings file with <Case>' -ForEach @(
        @{ Case = 'an unknown key'; Settings = "@{ Interal = @('Version') }"; Message = "unknown key 'Interal'" }
        @{ Case = 'a name that is no parameter'; Settings = "@{ Internal = @('Versoin') }"; Message = "'Versoin' is not a parameter" }
    ) {
        $settingsPath = Join-Path $TestDrive 'bad.psd1'; Set-Content -LiteralPath $settingsPath -Value $Settings
        $r = Invoke-Create -ScriptFile $script:Stub -SettingsFile $settingsPath -VariablesFile (New-VariablesFile (yamlVariable Id 'abc'))
        $r.Error | Should -Match ([regex]::Escape($Message))
    }
    It 'refuses a variables file with <Case>' -ForEach @(
        @{ Case = 'a duplicate name'; Body = (yamlVariable Id 'a') + (yamlVariable id 'b'); Message = 'defined more than once' }
        @{ Case = 'a name without value'; Body = "  - name: Id`n  - name: Text`n    value: x`n"; Message = "variable 'Id' has no 'value:' line" }
        @{ Case = 'an unquoted {'; Body = (yamlVariable Id '{a}'); Message = "a value starting with '{' must be quoted" }
        @{ Case = 'an unquoted @'; Body = (yamlVariable Id '@{a=1}'); Message = "a value starting with '@' must be quoted" }
        @{ Case = 'an expression'; Body = (yamlVariable Id "x`${{ parameters.y }}"); Message = 'expressions' }
        @{ Case = 'a block scalar'; Body = (yamlVariable Id '|'); Message = "a value starting with '|' must be quoted" }
    ) {
        $r = Invoke-Create -ScriptFile $script:Stub -VariablesFile (New-VariablesFile $Body)
        $r.Error | Should -Match ([regex]::Escape($Message))
    }
    It 'refuses a second top-level key' {
        $path = Join-Path $TestDrive 'toplevel.yml'; Set-Content -LiteralPath $path -Value "variables:`n  - name: Id`n    value: a`nparameters:`n"
        (Invoke-Create -ScriptFile $script:Stub -VariablesFile $path).Error | Should -Match "only the 'variables:' section is supported"
    }
    It 'reads the environment without -VariablesFile (Azure DevOps: upper case, True/False)' {
        $env:ID = 'fromEnv'; $env:FLAG = 'True'; $env:LIST = 'a,b'
        try {
            $r = Invoke-Create -ScriptFile $script:Stub
        }
        finally {
            Remove-Item env:ID, env:FLAG, env:LIST -ErrorAction SilentlyContinue
        }
        $r.Error | Should -BeNullOrEmpty
        $r.Params.Id | Should -BeExactly 'fromEnv'
        $r.Params.Flag | Should -BeTrue
        $r.Params.List | Should -Be @('a', 'b')
    }
}

Describe 'Read-AzADSPIArgumentFile.ps1' {
    It 'returns the object as hashtable' {
        $file = Join-Path $TestDrive 'ok.json'; Set-Content -LiteralPath $file -Value '{"ManagementGroupId":["a","b"],"ThrottleLimitARM":5,"DoTranscript":true}'
        $params = & $script:Read -ArgumentsFile $file
        $params | Should -BeOfType [System.Collections.IDictionary]
        $params.ManagementGroupId | Should -Be @('a', 'b')
        $params.ThrottleLimitARM | Should -Be 5
    }
    It 'fails for <Case>' -ForEach @(
        @{ Case = 'a missing file'; Content = $null; Message = 'not found' }
        @{ Case = 'an empty file'; Content = ''; Message = 'is empty' }
        @{ Case = 'invalid JSON'; Content = '{"a":'; Message = 'is not valid JSON' }
        @{ Case = 'an array with one object'; Content = '[{"OutputPath":"wiki"}]'; Message = 'must contain a JSON object' }
        @{ Case = 'an array'; Content = '[1,2]'; Message = 'must contain a JSON object' }
        @{ Case = 'null'; Content = 'null'; Message = 'must contain a JSON object' }
        @{ Case = 'a missing required key'; Content = '{"a":1}'; Message = 'has no OutputPath'; Required = @('OutputPath') }
    ) {
        $file = Join-Path $TestDrive "read_$([guid]::NewGuid().ToString('n')).json"
        if ($null -ne $Content) { Set-Content -LiteralPath $file -Value $Content -NoNewline }
        $splat = @{ ArgumentsFile = $file }
        if ($Required) { $splat.RequiredParameters = $Required }
        { & $script:Read @splat 6>$null } | Should -Throw "*$Message*"
    }
}

Describe 'AzADServicePrincipalInsights variables files and settings' {
    BeforeAll {
        $script:MainNames = Get-ParameterName $script:Main
        $script:IngestNames = Get-ParameterName $script:Ingest
        $script:MainSettings = Import-PowerShellDataFile -LiteralPath (Join-Path $script:Pwsh 'AzADServicePrincipalInsights.arguments.psd1')
        $script:IngestSettings = Import-PowerShellDataFile -LiteralPath (Join-Path $script:Pwsh 'Send-AzADSPILogAnalyticsData.arguments.psd1')
        $script:Gh = readVariablesFile -Path $script:GhVariables
        $script:Ado = readVariablesFile -Path $script:AdoVariables
        $script:AdoOnly = @('ServiceConnection', 'ScriptDir', 'Script', 'ScriptPrerequisites', 'IngestScript', 'IngestToLogAnalytics', 'WebAppPublish', 'WebAppSubscriptionId', 'WebAppResourceGroup', 'WebAppName')
    }
    It 'the GitHub file has every parameter users set (main and ingest script), and nothing else' {
        $expected = @($script:MainNames | Where-Object { $_ -notin $script:MainSettings.Internal }) + @($script:IngestNames | Where-Object { $_ -notin $script:IngestSettings.Internal })
        @($script:Gh.Keys | Sort-Object) | Should -Be @($expected | Sort-Object)
    }
    It 'the Azure DevOps file has the same variables (same order, same values) plus its pipeline variables' {
        @($script:Ado.Keys | Where-Object { $_ -notin $script:AdoOnly }) | Should -Be @($script:Gh.Keys)
        foreach ($name in $script:Gh.Keys) { $script:Ado[$name] | Should -BeExactly $script:Gh[$name] -Because $name }
        foreach ($name in $script:AdoOnly) { $script:Ado.ContainsKey($name) | Should -BeTrue -Because $name; $name | Should -Not -BeIn ($script:MainNames + $script:IngestNames) }
    }
    It 'every entry documents its type' {
        foreach ($path in $script:GhVariables, $script:AdoVariables) {
            $lines = Get-Content -LiteralPath $path
            for ($i = 0; $i -lt $lines.Count; $i++) {
                if ($lines[$i] -match '^\s+- name: (\S+)') { $lines[$i + 1] | Should -Match '^\s+# (String|Switch|Integer|List)' -Because "$path $($Matches[1])" }
            }
        }
    }
    It 'documented defaults match the script defaults' {
        $defaults = @{}
        foreach ($p in (Get-ScriptAst -Path $script:Main).ParamBlock.Parameters + (Get-ScriptAst -Path $script:Ingest).ParamBlock.Parameters) {
            if ($p.DefaultValue) { try { $defaults[$p.Name.VariablePath.UserPath] = $p.DefaultValue.SafeGetValue() } catch { $null = 'dynamic default (e.g. [IO.Path]::DirectorySeparatorChar)' } }
        }
        $lines = Get-Content -LiteralPath $script:GhVariables
        $checked = 0
        for ($i = 0; $i -lt $lines.Count; $i++) {
            if ($lines[$i] -match '^\s+- name: (\S+)' -and $lines[$i + 1] -match 'default = ([^|]+?)\s*(\||$)') {
                $name = ([regex]::Match($lines[$i], '- name: (\S+)')).Groups[1].Value
                $documented = ([regex]::Match($lines[$i + 1], 'default = ([^|(]+?)\s*(\(|\||$)')).Groups[1].Value -replace "'", ''
                (@($defaults[$name]) -join ',') | Should -BeExactly $documented -Because $name
                $checked++
            }
        }
        $checked | Should -BeGreaterOrEqual 12
    }
    It 'the GitHub file as shipped fails on the ManagementGroupId placeholder' {
        $r = Invoke-Create -ScriptFile $script:Main -VariablesFile $script:GhVariables -KnownScriptFile $script:Ingest -ScriptPath 'pwsh'
        $r.Error | Should -Match "Variable 'ManagementGroupId' = '<YourManagementGroupId>'"
        $r.FileWritten | Should -BeFalse
        $r.Output | Should -Not -Match 'ignored' -Because 'every variable of the file is a parameter of one of the two scripts'
    }
    It 'with management groups set: the expected arguments, no warnings' {
        $vars = Join-Path $TestDrive 'gh.yml'
        (Get-Content -LiteralPath $script:GhVariables -Raw) -replace "'<YourManagementGroupId>'", 'mg0, mg1' | Set-Content -LiteralPath $vars -NoNewline
        $r = Invoke-Create -ScriptFile $script:Main -VariablesFile $vars -KnownScriptFile $script:Ingest -ScriptPath 'pwsh'
        $r.Error | Should -BeNullOrEmpty
        @($r.Params.Keys | Sort-Object) | Should -Be @('ManagementGroupId', 'OutputPath', 'ScriptPath')
        $r.Params.ManagementGroupId | Should -Be @('mg0', 'mg1')
        $r.Params.OutputPath | Should -BeExactly 'wiki'
        $r.Output | Should -Not -Match 'WARNING'
    }
    It 'an empty ManagementGroupId is allowed (Tenant Root Group); an empty OutputPath is not' {
        $vars = Join-Path $TestDrive 'gh_root.yml'
        (Get-Content -LiteralPath $script:GhVariables -Raw) -replace "'<YourManagementGroupId>'", '' | Set-Content -LiteralPath $vars -NoNewline
        $r = Invoke-Create -ScriptFile $script:Main -VariablesFile $vars -KnownScriptFile $script:Ingest
        $r.Error | Should -BeNullOrEmpty
        $r.Params.ContainsKey('ManagementGroupId') | Should -BeFalse
        (Get-Content -LiteralPath $vars -Raw) -replace 'value: wiki', 'value:' | Set-Content -LiteralPath $vars -NoNewline
        (Invoke-Create -ScriptFile $script:Main -VariablesFile $vars -KnownScriptFile $script:Ingest).Error | Should -Match 'not set: OutputPath'
    }
    It 'ingest: the four Data Collection Rule values are required; ImportPath is set by the pipeline' {
        $r = Invoke-Create -ScriptFile $script:Ingest -VariablesFile $script:GhVariables -KnownScriptFile $script:Main
        $r.Error | Should -Match 'not set: DataCollectionRuleName, DataCollectionRuleResourceGroup, DataCollectionRuleSubscriptionId, LogAnalyticsCustomLogTableName'
        $r.Error | Should -Not -Match 'ImportPath'
        $r.Output | Should -Not -Match 'ignored'
    }
    It 'Azure DevOps (simulated): the variables template as environment variables gives the same arguments' {
        $saved = @{}
        foreach ($name in $script:Ado.Keys) {
            $envName = $name.ToUpperInvariant()
            $saved[$envName] = [Environment]::GetEnvironmentVariable($envName)
            $value = $script:Ado[$name]
            if ($value -in 'true', 'false') { $value = (Get-Culture).TextInfo.ToTitleCase($value) }   #Azure DevOps renders YAML booleans as True/False
            if ($name -eq 'ManagementGroupId') { $value = 'mg0,mg1' }
            [Environment]::SetEnvironmentVariable($envName, $value)
        }
        try {
            $r = Invoke-Create -ScriptFile $script:Main -ScriptPath 'pwsh'
        }
        finally {
            foreach ($envName in $saved.Keys) { [Environment]::SetEnvironmentVariable($envName, $saved[$envName]) }
        }
        $r.Error | Should -BeNullOrEmpty
        @($r.Params.Keys | Sort-Object) | Should -Be @('ManagementGroupId', 'OutputPath', 'ScriptPath')
        $r.Params.ManagementGroupId | Should -Be @('mg0', 'mg1')
    }
    It 'the reader gives the same values as PyYAML (when available)' {
        $python = Get-Command python, python3 -ErrorAction SilentlyContinue | Select-Object -First 1
        $hasYaml = $false
        if ($python) {
            $null = & $python.Source -c 'import yaml' 2>$null
            $hasYaml = $LASTEXITCODE -eq 0
        }
        if (-not $hasYaml) {
            Set-ItResult -Skipped -Because 'python with PyYAML is not available'
            return
        }
        foreach ($path in $script:GhVariables, $script:AdoVariables) {
            $json = & $python.Source -I -c "import yaml,json,sys; d=yaml.safe_load(open(sys.argv[1],encoding='utf-8')); print(json.dumps({v['name']: ('' if v['value'] is None else str(v['value'])) for v in d['variables']}))" $path
            $expected = $json | ConvertFrom-Json -AsHashtable
            $actual = readVariablesFile -Path $path
            $actual.Count | Should -Be $expected.Count
            foreach ($name in $expected.Keys) {
                #YAML booleans: PyYAML True/False, the reader keeps the text (compared without regard to case later)
                $actual[$name] | Should -Be $expected[$name] -Because "$path $name"
            }
        }
    }
    It 'old and new arguments bind to the same values (stub with the param block of the script)' {
        $stub = Join-Path $TestDrive 'MainStub.ps1'
        $ast = Get-ScriptAst -Path $script:Main
        Set-Content -LiteralPath $stub -Value ("[CmdletBinding()]`n" + $ast.ParamBlock.Extent.Text + "`n" + '$out = [ordered]@{}; foreach ($p in $MyInvocation.MyCommand.Parameters.Keys) { if ($p -notin [System.Management.Automation.PSCmdlet]::CommonParameters) { $out[$p] = (Get-Variable -Name $p -ValueOnly) } }; $out | ConvertTo-Json -Depth 5 -Compress')
        #before theme 30, Azure DevOps (AzurePowerShell@5 scriptArguments, 2 management groups) and the GitHub workflow
        $old = @{
            ado    = & ([scriptblock]::Create("& '$stub' -OutputPath wiki -ScriptPath pwsh -ManagementGroupId mg0,mg1 -CsvDelimiter "";"" -SubscriptionQuotaIdWhitelist undefined"))
            github = & ([scriptblock]::Create("`$ManagementGroupId = ('mg0,mg1').split(','); & '$stub' -ScriptPath pwsh -OutputPath wiki -ManagementGroupId `$ManagementGroupId"))
        }
        $vars = Join-Path $TestDrive 'gh_bind.yml'
        (Get-Content -LiteralPath $script:GhVariables -Raw) -replace "'<YourManagementGroupId>'", 'mg0,mg1' | Set-Content -LiteralPath $vars -NoNewline
        $params = (Invoke-Create -ScriptFile $script:Main -VariablesFile $vars -KnownScriptFile $script:Ingest -ScriptPath 'pwsh').Params
        $new = & $stub @params
        $new | Should -BeExactly $old.ado
        $new | Should -BeExactly $old.github
    }
}

Describe 'Get-AzADSPIOutputPath.ps1: the output names of AzADServicePrincipalInsights.ps1' {
    BeforeAll {
        #the naming code of the main script, run with a fake context (tenant 'tid')
        $source = Get-Content -LiteralPath $script:Main -Raw
        $start = $source.IndexOf('if (-not $ManagementGroupId) {')
        $end = $source.IndexOf('$fileNameMGRef = $ManagementGroupID -join ''_''')
        $script:MainNaming = $source.Substring($start, $end - $start) + '$ManagementGroupID -join ''_'''
        function Get-MainReference([string[]]$ManagementGroupId) {
            $azAPICallConf = @{ checkContext = @{ Tenant = @{ Id = 'tid' } } }
            & ([scriptblock]::Create("param(`$ManagementGroupId, `$azAPICallConf) function Write-Host {}; $script:MainNaming")) $ManagementGroupId $azAPICallConf
        }
    }
    It 'management groups <Mg>: the same reference as the script' -ForEach @(
        @{ Mg = @() }
        @{ Mg = @('mg0') }
        @{ Mg = @('tid') }
        @{ Mg = @('mg0', 'mg1') }
        @{ Mg = @('mg0', 'tid') }
    ) {
        $file = Join-Path $TestDrive 'out.json'
        $params = @{ OutputPath = 'wiki' }; if ($Mg.Count) { $params.ManagementGroupId = $Mg }
        ConvertTo-Json -InputObject $params | Set-Content -LiteralPath $file
        $output = & $script:OutputPaths -ArgumentsFile $file -TenantId 'tid' -BasePath $TestDrive
        $expected = Get-MainReference -ManagementGroupId $(if ($Mg.Count) { $Mg } else { $null })
        $output.Reference | Should -BeExactly $expected
        $output.HtmlPath | Should -BeExactly (Join-Path (Join-Path $TestDrive 'wiki') "AzADServicePrincipalInsights_$expected.html")
        $output.JsonPath | Should -BeExactly (Join-Path (Join-Path $TestDrive 'wiki') "JSON_SP_$expected")
    }
    It 'an absolute OutputPath is used as is' {
        $file = Join-Path $TestDrive 'abs.json'
        ConvertTo-Json -InputObject @{ OutputPath = (Join-Path $TestDrive 'abs'); ManagementGroupId = @('mg0') } | Set-Content -LiteralPath $file
        (& $script:OutputPaths -ArgumentsFile $file -BasePath 'X:\other').OutputPath | Should -BeExactly (Join-Path $TestDrive 'abs')
    }
}

Describe 'Publish-AzADSPIWebApp.ps1 (mocked Azure calls)' {
    BeforeAll {
        function global:initAzAPICall { param($DebugAzAPICall) @{ azAPIEndpointUrls = @{ ARM = 'https://arm' } } }
        function global:AzAPICall {
            param($AzAPICallConfiguration, $uri, $method, $currentTask, $listenOn)
            $null = $global:AzADSPITestCalls.Add("$method $(($uri -split '/config/')[1])")
            if ($uri -match '/authsettings/list') { if ($global:AzADSPITestV1 -eq 'throw') { throw 'HTTP 403' }; return $global:AzADSPITestV1 }
            return $global:AzADSPITestV2
        }
        function global:Select-AzSubscription { param($SubscriptionId) 'ok' }
        function global:Publish-AzWebApp { param($ResourceGroupName, $Name, $ArchivePath, [switch]$Force) $global:AzADSPITestPublished = $ArchivePath; 'app' }
        $script:Base = Join-Path $TestDrive 'repo'
        $null = New-Item -ItemType Directory -Path (Join-Path $script:Base 'wiki') -Force
        Set-Content -LiteralPath (Join-Path $script:Base 'wiki/AzADServicePrincipalInsights_mg0.html') -Value '<html></html>'
        $script:PublishArgs = Join-Path $TestDrive 'publish.json'
        Set-Content -LiteralPath $script:PublishArgs -Value '{"OutputPath":"wiki","ManagementGroupId":["mg0"]}'
        $script:ZipDir = Join-Path $TestDrive 'zip'
        $null = New-Item -ItemType Directory -Path $script:ZipDir -Force
        function Invoke-Publish([string]$SubscriptionId = 'sub', [string]$Base = $script:Base) {
            $global:AzADSPITestCalls = [System.Collections.ArrayList]@(); $global:AzADSPITestPublished = $null
            try { & $script:Publish -ArgumentsFile $script:PublishArgs -WebAppSubscriptionId $SubscriptionId -WebAppResourceGroup 'rg' -WebAppName 'app' -BasePath $Base -ZipDirectory $script:ZipDir *>$null; $null }
            catch { $_.Exception.Message }
        }
    }
    AfterAll {
        Remove-Item function:global:initAzAPICall, function:global:AzAPICall, function:global:Select-AzSubscription, function:global:Publish-AzWebApp -ErrorAction SilentlyContinue
        Remove-Variable -Name AzADSPITestCalls, AzADSPITestPublished, AzADSPITestV1, AzADSPITestV2 -Scope Global -ErrorAction SilentlyContinue
    }
    It '<Case>: published <Published>, calls <Calls>' -ForEach @(
        @{ Case = 'v1 login required'; V1 = @{ properties = @{ enabled = $true; unauthenticatedClientAction = 'RedirectToLoginPage' } }; V2 = $null; Published = $true; Calls = 1; Message = $null }
        @{ Case = 'v1 AllowAnonymous'; V1 = @{ properties = @{ enabled = $true; unauthenticatedClientAction = 'AllowAnonymous' } }; V2 = $null; Published = $false; Calls = 1; Message = "unauthenticatedClientAction 'AllowAnonymous'" }
        @{ Case = 'v2 login required'; V1 = @{ properties = @{ enabled = $false } }; V2 = @{ properties = @{ platform = @{ enabled = $true }; globalValidation = @{ requireAuthentication = $true } } }; Published = $true; Calls = 2; Message = $null }
        @{ Case = 'v2 requireAuthentication false'; V1 = @{ properties = @{ enabled = $false } }; V2 = @{ properties = @{ platform = @{ enabled = $true }; globalValidation = @{ requireAuthentication = $false } } }; Published = $false; Calls = 2; Message = 'globalValidation.requireAuthentication false' }
        @{ Case = 'no authentication'; V1 = @{ properties = @{ enabled = $false } }; V2 = @{ properties = @{ platform = @{ enabled = $false } } }; Published = $false; Calls = 2; Message = 'has no Authentication configured' }
        @{ Case = 'the settings cannot be read'; V1 = 'throw'; V2 = $null; Published = $false; Calls = 1; Message = 'could not be read (HTTP 403)' }
    ) {
        $global:AzADSPITestV1 = $V1; $global:AzADSPITestV2 = $V2
        $message = Invoke-Publish
        [bool]$global:AzADSPITestPublished | Should -Be $Published
        $global:AzADSPITestCalls.Count | Should -Be $Calls
        @($global:AzADSPITestCalls) -join ' ' | Should -Match 'authsettings/list\?api-version=2026-08-01'
        if ($Message) { $message | Should -Match ([regex]::Escape($Message)) } else { $message | Should -BeNullOrEmpty }
        if ($Published) { $global:AzADSPITestPublished | Should -BeExactly (Join-Path $script:ZipDir 'AzADServicePrincipalInsights_mg0.zip') -Because 'the zip is not written to the pushed output folder' }
    }
    It 'refuses a placeholder and a missing report before any Azure call' {
        $global:AzADSPITestV1 = @{ properties = @{ enabled = $true; unauthenticatedClientAction = 'RedirectToLoginPage' } }
        Invoke-Publish -SubscriptionId '<subscriptionId>' | Should -Match 'provide the Web App details'
        $global:AzADSPITestCalls.Count | Should -Be 0
        Invoke-Publish -Base $TestDrive | Should -Match 'AzADServicePrincipalInsights_mg0.html'' not found'
        $global:AzADSPITestCalls.Count | Should -Be 0
    }
}

Describe 'pipeline and workflow wiring' {
    It 'no Azure DevOps macro $(...) inside a script of the pipeline (only in task inputs and env mappings)' {
        $lines = Get-Content -LiteralPath $script:AdoPipeline
        $scriptLines = Get-YamlBlockScalarLines -Lines $lines -KeyPattern '(- pwsh|Inline)'
        $scriptLines.Count | Should -BeGreaterThan 20
        foreach ($line in $scriptLines) { $line | Should -Not -Match '\$\([A-Za-z_][\w.]*\)' }
    }
    It 'no ${{ }} expression inside a script of the workflows (secrets reach the scripts as environment variables)' {
        foreach ($workflow in $script:Workflows) {
            $lines = Get-Content -LiteralPath $workflow
            $scriptLines = Get-YamlBlockScalarLines -Lines $lines -KeyPattern '(run|inlineScript)'
            $scriptLines.Count | Should -BeGreaterThan 20
            foreach ($line in $scriptLines) { $line | Should -Not -Match '\$\{\{' -Because $workflow }
        }
    }
    It 'the run steps splat the arguments file; the arguments are created before the Azure login (GitHub)' {
        $ado = Get-Content -LiteralPath $script:AdoPipeline -Raw
        $ado | Should -Match "template: 'AzADServicePrincipalInsights.variables.yml'"
        $ado | Should -Match 'New-AzADSPIArgumentFile\.ps1'
        $ado | Should -Match '\$\(\$env:SCRIPT\)" @params'
        $ado | Should -Match '\$\(\$env:INGESTSCRIPT\)" @params'
        $ado | Should -Match 'SYSTEM_ACCESSTOKEN: \$\(System.AccessToken\)'
        $ado | Should -Match 'HEAD:\$branch'
        foreach ($workflow in $script:Workflows) {
            $text = Get-Content -LiteralPath $workflow -Raw
            $text | Should -Match 'VariablesFile: \.github/AzADServicePrincipalInsights\.variables\.yml'
            $text.IndexOf('name: Create arguments') | Should -BeLessThan $text.IndexOf('uses: azure/login')
            $text | Should -Match '\$\(\$env:ScriptFile\)" @params'
            $text | Should -Match '\$\(\$env:IngestScriptFile\)" @params'
            $text | Should -Match 'Publish-AzADSPIWebApp\.ps1'
        }
        (Get-Content -LiteralPath $script:Workflows[1] -Raw) | Should -Match "\`$params\['SubscriptionId4AzContext'\] = \`$env:SUBSCRIPTION_ID"
    }
    It 'R-15: the workflows share one concurrency group without cancelling; a rebase conflict ends with a clear message' {
        foreach ($workflow in $script:Workflows) {
            $text = Get-Content -LiteralPath $workflow -Raw
            $text | Should -Match '(?m)^concurrency:\r?\n  group: AzADServicePrincipalInsights-\$\{\{ github\.ref \}\}\r?\n  cancel-in-progress: false' -Because $workflow
            $text | Should -Match 'if ! git pull --rebase; then' -Because $workflow
            $text | Should -Match 'git rebase --abort' -Because $workflow
        }
        $ado = Get-Content -LiteralPath $script:AdoPipeline -Raw
        $ado | Should -Match 'another run changed the same output files at the same time'
        $ado | Should -Match 'git rebase --abort'
    }
}
