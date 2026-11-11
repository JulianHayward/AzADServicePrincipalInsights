#Documentation (README.md, CHANGELOG.md, setup/*.md) and the dev container: links resolve, versions match the script

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:SetupDir = Join-Path $script:RepoRoot 'setup'
    #GitHub anchor of a markdown heading: lower case, punctuation removed, spaces to '-'
    function getAnchors([string]$Path) {
        @(Get-Content -LiteralPath $Path | Where-Object { $_ -match '^#{1,6} ' } | ForEach-Object {
                (($_ -replace '^#{1,6} ', '').ToLowerInvariant() -replace '[^\p{L}\p{Nd}\s_-]', '' -replace ' ', '-')
            })
    }
}

Describe 'setup guides' {
    It 'exist' {
        foreach ($name in 'README.md', 'console.md', 'github.md', 'azure-devops.md', 'azure-web-app.md', 'log-analytics.md') {
            Join-Path $script:SetupDir $name | Should -Exist
        }
    }
    It 'relative links of <_> point to existing files and headings' -ForEach @('setup/README.md', 'setup/console.md', 'setup/github.md', 'setup/azure-devops.md', 'setup/azure-web-app.md', 'setup/log-analytics.md', 'README.md', 'CHANGELOG.md', 'SECURITY.md', 'CONTRIBUTING.md') {
        $path = Join-Path $script:RepoRoot $_
        $baseDir = Split-Path -Path $path -Parent
        $broken = foreach ($m in [regex]::Matches((Get-Content -Raw -LiteralPath $path), '\]\((?!https?://|mailto:)(?<target>[^)\s]+)\)')) {
            $target, $anchor = $m.Groups['target'].Value -split '#', 2
            $targetPath = $(if ($target) { [System.IO.Path]::GetFullPath((Join-Path $baseDir $target)) } else { $path })
            if (-not (Test-Path -LiteralPath $targetPath)) { "missing file: $($m.Groups['target'].Value)"; continue }
            if ($anchor -and $targetPath -like '*.md' -and $anchor -notin (getAnchors $targetPath)) { "missing heading: $($m.Groups['target'].Value)" }
        }
        @($broken) | Should -BeNullOrEmpty
    }
    It 'the README links to the setup guides' {
        Get-Content -Raw -LiteralPath (Join-Path $script:RepoRoot 'README.md') | Should -Match '\(setup/README\.md\)'
    }
    It 'every guide has numbered steps, each with a checkpoint: <_>' -ForEach @('setup/console.md', 'setup/github.md', 'setup/azure-devops.md', 'setup/azure-web-app.md', 'setup/log-analytics.md') {
        $text = Get-Content -Raw -LiteralPath (Join-Path $script:RepoRoot $_)
        $steps = @([regex]::Matches($text, '(?m)^## Step (\d+) - ') | ForEach-Object { [int]$_.Groups[1].Value })
        $steps.Count | Should -BeGreaterThan 0
        $steps | Should -Be (1..$steps.Count) -Because 'the steps are numbered 1, 2, 3, ...'
        $text | Should -Match '(?m)^## If something doesn.t work' -Because 'every guide has a troubleshooting table'
    }
}

#setup/setup-assistant.html: the questions-and-plan page. Its texts are condensed from the guides; these checks keep the parts
#that can drift (links, permissions, the template URL) and its privacy promise (nothing leaves the page) in place.
#The behaviour (answer paths, plans, XSS, CSP, layout) was tested in headless Chromium on 2026-11-11.
Describe 'setup assistant' {
    BeforeAll {
        $script:AssistantPath = Join-Path $script:SetupDir 'setup-assistant.html'
        $script:Assistant = Get-Content -Raw -LiteralPath $script:AssistantPath
        $script:SetupReadme = Get-Content -Raw -LiteralPath (Join-Path $script:SetupDir 'README.md')
    }
    It 'links to existing guide sections' {
        $links = @([regex]::Matches($script:Assistant, "link\('(?<file>[\w.-]+)', '(?<anchor>[\w-]*)'") | ForEach-Object { [PSCustomObject]@{ File = $_.Groups['file'].Value; Anchor = $_.Groups['anchor'].Value } })
        $links += @([regex]::Matches($script:Assistant, "GUIDE \+ '(?<file>[\w.-]+)#(?<anchor>[\w-]+)'") | ForEach-Object { [PSCustomObject]@{ File = $_.Groups['file'].Value; Anchor = $_.Groups['anchor'].Value } })
        $links.Count | Should -BeGreaterThan 20
        $broken = foreach ($link in $links) {
            $target = Join-Path $script:SetupDir $link.File
            if (-not (Test-Path -LiteralPath $target)) { "missing file: $($link.File)"; continue }
            if ($link.Anchor -and $link.Anchor -notin (getAnchors $target)) { "missing heading: $($link.File)#$($link.Anchor)" }
        }
        @($broken) | Should -BeNullOrEmpty
        $script:Assistant | Should -Match ([regex]::Escape("var GUIDE = 'https://github.com/JulianHayward/AzADServicePrincipalInsights/blob/main/setup/';"))
    }
    It 'lists the Microsoft Graph permissions of the permission reference (setup/README.md)' {
        $readmeRows = @([regex]::Matches($script:SetupReadme, '(?m)^\| `(?<first>[\w.]+)`(?: and `(?<second>[\w.]+)`)? \| (?<kind>required|optional)'))
        $required = @($readmeRows.where({ $_.Groups['kind'].Value -eq 'required' }) | ForEach-Object { $_.Groups['first'].Value })
        $optional = @($readmeRows.where({ $_.Groups['kind'].Value -eq 'optional' }) | ForEach-Object { $_.Groups['first'].Value; if ($_.Groups['second'].Success) { $_.Groups['second'].Value } })
        $required.Count | Should -Be 4
        $assistantRequired = [regex]::Match($script:Assistant, 'var REQUIRED_PERMISSIONS = \[(?<list>[^\]]+)\]').Groups['list'].Value -replace "[' ]" -split ','
        $assistantRequired | Should -Be $required
        foreach ($permission in $optional) { $script:Assistant | Should -Match ([regex]::Escape($permission)) -Because 'every optional permission of the reference can be chosen' }
    }
    It 'uses the Deploy to Azure template of the Log Analytics guide' {
        $script:Assistant | Should -Match ([regex]::Escape("encodeURIComponent('https://raw.githubusercontent.com/JulianHayward/AzADServicePrincipalInsights/main/setup/log-analytics-dcr.json')"))
    }
    It 'sends nothing and stores nothing: CSP without network access, no request or storage APIs' {
        $csp = [regex]::Match($script:Assistant, '<meta http-equiv="Content-Security-Policy" content="(?<csp>[^"]+)"').Groups['csp'].Value
        $csp | Should -Match "default-src 'none'"
        $csp | Should -Not -Match 'connect-src'
        $script:Assistant | Should -Not -Match '\b(fetch|XMLHttpRequest|sendBeacon|localStorage|sessionStorage|indexedDB|document\.cookie|WebSocket)\b'
        $script:Assistant | Should -Not -Match '<(script|link|img|iframe)[^>]+(src|href)="https?:'
    }
    It 'is linked from the setup guides and the README: the hosted page, and the file for offline use' {
        $hosted = [regex]::Escape('(https://www.azadvertizer.net/azadserviceprincipalinsights/setup-assistant.html)')
        $script:SetupReadme | Should -Match $hosted
        $script:SetupReadme | Should -Match '\(setup-assistant\.html\)'
        Get-Content -Raw -LiteralPath (Join-Path $script:RepoRoot 'README.md') | Should -Match $hosted
        #the hosted page lies next to the report assets: same folder as the script uses
        Get-Content -Raw -LiteralPath $script:MainScriptPath | Should -Match ([regex]::Escape('https://www.azadvertizer.net/azadserviceprincipalinsights'))
    }
}

#links to the documentation in the scripts and pipelines (console output, comments): the file and the heading exist on the main branch
Describe 'documentation links in scripts and pipelines' {
    It 'point to existing files and headings' {
        $files = @(Get-ChildItem -LiteralPath (Join-Path $script:RepoRoot 'pwsh') -Filter '*.ps1') +
            @(Get-ChildItem -LiteralPath (Join-Path $script:RepoRoot '.github') -Recurse -Filter '*.yml') +
            @(Get-ChildItem -LiteralPath (Join-Path $script:RepoRoot '.azuredevops') -Recurse -Filter '*.yml')
        $links = foreach ($file in $files) {
            foreach ($m in [regex]::Matches((Get-Content -Raw -LiteralPath $file.FullName), 'github\.com/JulianHayward/AzADServicePrincipalInsights/blob/main/(?<path>[\w./-]+)(#(?<anchor>[\w-]+))?')) {
                [PSCustomObject]@{ File = $file.Name; Path = $m.Groups['path'].Value; Anchor = $m.Groups['anchor'].Value }
            }
        }
        @($links).Count | Should -BeGreaterThan 0
        $broken = foreach ($link in $links) {
            $targetPath = Join-Path $script:RepoRoot $link.Path
            if (-not (Test-Path -LiteralPath $targetPath)) { "$($link.File): missing file $($link.Path)"; continue }
            if ($link.Anchor -and $link.Anchor -notin (getAnchors $targetPath)) { "$($link.File): missing heading $($link.Path)#$($link.Anchor)" }
        }
        @($broken) | Should -BeNullOrEmpty
    }
    It 'the permission check links to the setup guide, not to a heading appended to the GitHubRepository (aka.ms) link' {
        Get-Content -Raw -LiteralPath $script:MainScriptPath | Should -Not -Match 'gitHubRepository\)#'
    }
}

#setup/log-analytics-dcr.json: the table and the stream must have a column for every top-level property of the JSON output
#(a property without a column is dropped by the Data Collection Rule without an error)
Describe 'Log Analytics template' {
    BeforeAll {
        $script:Template = Get-Content -Raw -LiteralPath (Join-Path $script:RepoRoot 'setup/log-analytics-dcr.json') | ConvertFrom-Json -AsHashtable
        $script:Columns = @($script:Template.variables.columns)
        $source = Get-Content -Raw -LiteralPath $script:MainScriptPath
        $ast = Get-ScriptAst -Path $script:MainScriptPath
        $names = [System.Collections.Generic.HashSet[string]]::new()
        #the objects of the JSON output ($cu): hashtable literals with ObjectType = $object.objectTypeConcatinated
        foreach ($hashtable in $ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.HashtableAst] }, $true)) {
            if (@($hashtable.KeyValuePairs).where({ $_.Item1.Extent.Text -eq 'ObjectType' -and $_.Item2.Extent.Text -match '\$object\.objectTypeConcatinated' })) {
                foreach ($pair in $hashtable.KeyValuePairs) { $null = $names.Add($pair.Item1.Extent.Text) }
            }
        }
        #properties added later to these objects
        foreach ($m in [regex]::Matches($source, '\$(entry|customObjectMi) \| Add-Member -NotePropertyName (?<name>\w+)')) { $null = $names.Add($m.Groups['name'].Value) }
        foreach ($m in [regex]::Matches($source, "\`$entry\.PSObject\.Properties\.Add\(\[psnoteproperty\]::new\('(?<name>\w+)'")) { $null = $names.Add($m.Groups['name'].Value) }
        #added by the ingestion
        $ingest = Get-Content -Raw -LiteralPath (Join-Path $script:RepoRoot 'pwsh/Send-AzADSPILogAnalyticsData.ps1')
        foreach ($m in [regex]::Matches($ingest, '\$jsonObject \| Add-Member -NotePropertyName (?<name>\w+)')) { $null = $names.Add($m.Groups['name'].Value) }
        $script:OutputProperties = @($names)
    }
    It 'finds the properties of the JSON output in the code' {
        foreach ($name in 'ObjectId', 'ObjectType', 'SP', 'APP', 'ManagedIdentity', 'ManagedIdentityFederatedIdentityCredentials', 'SPEffectivePrivilege', 'AgentType', 'TimeGenerated', 'RunId') {
            $script:OutputProperties | Should -Contain $name
        }
    }
    It 'has exactly one column per property of the JSON output' {
        $columnNames = @($script:Columns | ForEach-Object { $_.name })
        @($script:OutputProperties | Where-Object { $_ -notin $columnNames }) | Should -BeNullOrEmpty -Because 'a property without a column is not stored'
        @($columnNames | Where-Object { $_ -notin $script:OutputProperties }) | Should -BeNullOrEmpty -Because 'a column without a property stays empty'
        @($columnNames | Group-Object | Where-Object Count -gt 1) | Should -BeNullOrEmpty
    }
    It 'column types: TimeGenerated datetime, ids and types string, the rest dynamic' {
        foreach ($column in $script:Columns) {
            $expected = $(if ($column.name -eq 'TimeGenerated') { 'datetime' } elseif ($column.name -in 'RunId', 'ObjectId', 'ObjectType', 'AgentType') { 'string' } else { 'dynamic' })
            "$($column.name):$($column.type)" | Should -Be "$($column.name):$expected"
        }
    }
    It 'the table and the stream of the Data Collection Rule use the same columns; the stream is Custom- plus the table name' {
        $table = @($script:Template.resources).where({ $_.type -eq 'Microsoft.OperationalInsights/workspaces/tables' })[0]
        $dcr = @($script:Template.resources).where({ $_.type -eq 'Microsoft.Insights/dataCollectionRules' })[0]
        $table.properties.schema.columns | Should -Be "[variables('columns')]"
        $dcr.properties.streamDeclarations | Should -Be "[createObject(variables('streamName'), createObject('columns', variables('columns')))]"
        $script:Template.variables.streamName | Should -Be "[format('Custom-{0}', parameters('tableName'))]"
        $dcr.kind | Should -Be 'Direct' -Because 'Send-AzADSPILogAnalyticsData.ps1 uses the own logs ingestion endpoint of the rule'
    }
    It 'creates the workspace only with createWorkspace (default false); the table waits for it' {
        #real test 2026-11-11: createWorkspace true created workspace, table (30 columns) and rule; false left the existing workspace unchanged
        $script:Template.parameters.createWorkspace.type | Should -Be 'bool'
        $script:Template.parameters.createWorkspace.defaultValue | Should -BeFalse -Because 'true would overwrite the settings of an existing workspace'
        $workspace = @($script:Template.resources).where({ $_.type -eq 'Microsoft.OperationalInsights/workspaces' })[0]
        $workspace.condition | Should -Be "[parameters('createWorkspace')]"
        $workspace.name | Should -Be "[parameters('workspaceName')]"
        $table = @($script:Template.resources).where({ $_.type -eq 'Microsoft.OperationalInsights/workspaces/tables' })[0]
        $table.dependsOn | Should -Contain "[resourceId('Microsoft.OperationalInsights/workspaces', parameters('workspaceName'))]"
    }
    It 'the Deploy to Azure button of the guide points to the template on the main branch' {
        $guide = Get-Content -Raw -LiteralPath (Join-Path $script:RepoRoot 'setup/log-analytics.md')
        $m = [regex]::Match($guide, '\(https://portal\.azure\.com/#create/Microsoft\.Template/uri/(?<uri>[^)\s]+)\)')
        $m.Success | Should -BeTrue
        $uri = [uri]::UnescapeDataString($m.Groups['uri'].Value)
        $uri | Should -Be 'https://raw.githubusercontent.com/JulianHayward/AzADServicePrincipalInsights/main/setup/log-analytics-dcr.json'
        Join-Path $script:RepoRoot ($uri -replace '^https://raw\.githubusercontent\.com/JulianHayward/AzADServicePrincipalInsights/main/', '') | Should -Exist
    }
}

Describe 'permission check messages' {
    It 'every failed Microsoft Graph check names a required permission of the permission reference (setup/README.md)' {
        $readme = Get-Content -Raw -LiteralPath (Join-Path $script:SetupDir 'README.md')
        $required = @([regex]::Matches($readme, '(?m)^\| `(?<p>[\w.]+)` \| required') | ForEach-Object { $_.Groups['p'].Value })
        $required.Count | Should -Be 4
        $failed = @([regex]::Matches((Get-Content -Raw -LiteralPath $script:MainScriptPath), "'AAD \w+ Read permission check FAILED - the Microsoft Graph application permission (?<p>[\w.]+) \(with admin consent\) is missing'") | ForEach-Object { $_.Groups['p'].Value })
        @($failed | Sort-Object) | Should -Be @($required | Sort-Object)
    }
}

Describe 'VS Code extensions' {
    It 'the dev container installs the extensions that .vscode/extensions.json recommends' {
        #both files are JSON with comments: whole-line // comments are removed before parsing
        function readJsonc([string]$Path) { (Get-Content -LiteralPath $Path | Where-Object { $_ -notmatch '^\s*//' }) -join "`n" | ConvertFrom-Json }
        $recommended = @((readJsonc (Join-Path $script:RepoRoot '.vscode/extensions.json')).recommendations)
        $installed = @((readJsonc (Join-Path $script:RepoRoot '.devcontainer/devcontainer.json')).customizations.vscode.extensions)
        $recommended.Count | Should -BeGreaterThan 0
        @($recommended | Sort-Object) | Should -Be @($installed | Sort-Object)
    }
}

Describe 'dev container' {
    It 'installs the AzAPICall version the script asks for' {
        $default = [regex]::Match((Get-Content -Raw -LiteralPath $script:MainScriptPath), "\[string\]\`$azAPICallVersion = '(?<v>[^']+)'").Groups['v'].Value
        $default | Should -Not -BeNullOrEmpty
        Get-Content -Raw -LiteralPath (Join-Path $script:RepoRoot '.devcontainer/devcontainer.json') | Should -Match ([regex]::Escape("AzAPICall==$default"))
    }
}
