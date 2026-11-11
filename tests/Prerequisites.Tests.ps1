#pwsh/Test-AzADSPIPrerequisite.ps1: output folder from -OutputPath (review R-2: Azure DevOps used the removed WIKIDIR variable)
BeforeAll {
    $script:PrereqScript = Join-Path $PSScriptRoot '..' 'pwsh' 'Test-AzADSPIPrerequisite.ps1'
    $script:EnvNames = @('SYSTEM_TEAMPROJECTID', 'BUILD_REPOSITORY_ID', 'SYSTEM_DEFAULTWORKINGDIRECTORY', 'SYSTEM_COLLECTIONURI', 'SYSTEM_TEAMPROJECT', 'SYSTEM_ACCESSTOKEN', 'GITHUB_ACTIONS', 'GITHUB_WORKSPACE', 'GITHUB_REPOSITORY', 'GITHUB_SERVER_URL', 'GITHUB_API_URL', 'GITHUB_TOKEN', 'WIKIDIR', 'BUILD_SOURCEBRANCH', 'GITHUB_REF', 'AGENT_TEMPDIRECTORY')
}

Describe 'Test-AzADSPIPrerequisite.ps1 on Azure DevOps' {
    BeforeEach {
        $script:SavedEnv = @{}
        foreach ($name in $EnvNames) { $SavedEnv[$name] = [Environment]::GetEnvironmentVariable($name); [Environment]::SetEnvironmentVariable($name, $null) }
        $script:WorkDir = Join-Path ([System.IO.Path]::GetTempPath()) "azadspi-prereq-$([guid]::NewGuid())"
        $null = New-Item -ItemType Directory -Path $WorkDir
        $env:SYSTEM_TEAMPROJECTID = '11111111-1111-1111-1111-111111111111'
        $env:BUILD_REPOSITORY_ID = '22222222-2222-2222-2222-222222222222'
        $env:SYSTEM_DEFAULTWORKINGDIRECTORY = $WorkDir
        #not dev.azure.com / visualstudio.com: the identity lookup is skipped after the repository list
        $env:SYSTEM_COLLECTIONURI = 'https://tfs.example.invalid/tfs/DefaultCollection/'
        $env:SYSTEM_TEAMPROJECT = 'proj'
        $env:SYSTEM_ACCESSTOKEN = 'dummy'
        $env:BUILD_SOURCEBRANCH = 'refs/heads/main'
        Mock Invoke-RestMethod { [pscustomobject]@{ value = @() } }
        Mock Invoke-RestMethod { [pscustomobject]@{ visibility = 'private' } } -ParameterFilter { $Uri -like '*/_apis/projects/*' }
        Mock Start-Sleep {}
        Mock Write-Host {}
    }
    AfterEach {
        foreach ($name in $EnvNames) { [Environment]::SetEnvironmentVariable($name, $SavedEnv[$name]) }
        Remove-Item -LiteralPath $WorkDir -Recurse -Force -ErrorAction SilentlyContinue
    }

    It 'creates the -OutputPath folder below the working directory on the first run and runs the repository check' {
        $null = & $PrereqScript -OutputPath 'wiki'
        Test-Path -LiteralPath (Join-Path $WorkDir 'wiki') | Should -BeTrue
        Should -Invoke Invoke-RestMethod -Times 1 -Exactly -ParameterFilter { $Uri -like '*/_apis/git/repositories*' }
    }

    It 'skips creation and the repository check when the folder exists' {
        $null = New-Item -ItemType Directory -Path (Join-Path $WorkDir 'out')
        $null = & $PrereqScript -OutputPath 'out'
        Should -Invoke Invoke-RestMethod -Times 0 -Exactly -ParameterFilter { $Uri -like '*/_apis/git/repositories*' }
        Should -Invoke Invoke-RestMethod -Times 1 -Exactly -ParameterFilter { $Uri -eq 'https://tfs.example.invalid/tfs/DefaultCollection/_apis/projects/11111111-1111-1111-1111-111111111111?api-version=7.1' }
    }

    It 'SR-16: a public project stops the run before the output folder is created' {
        Mock Invoke-RestMethod { [pscustomobject]@{ visibility = 'public' } } -ParameterFilter { $Uri -like '*/_apis/projects/*' }
        { & $PrereqScript -OutputPath 'wiki' } | Should -Throw "*Project 'proj' is public*"
        Test-Path -LiteralPath (Join-Path $WorkDir 'wiki') | Should -BeFalse
    }

    It 'SR-16: a visibility that cannot be determined stops the run after 3 tries' {
        Mock Invoke-RestMethod { throw 'offline' } -ParameterFilter { $Uri -like '*/_apis/projects/*' }
        { & $PrereqScript -OutputPath 'wiki' } | Should -Throw "*visibility of project 'proj' could not be determined*"
        Should -Invoke Invoke-RestMethod -Times 3 -Exactly -ParameterFilter { $Uri -like '*/_apis/projects/*' }
    }

    It 'SR-39: a run that is not on a branch stops before the collection' {
        $env:BUILD_SOURCEBRANCH = 'refs/pull/7/merge'
        { & $PrereqScript -OutputPath 'wiki' } | Should -Throw "*The run is not on a branch ('refs/pull/7/merge')*"
    }

    It 'SR-57: an output folder outside the repository or ignored by git stops the run' {
        { & $PrereqScript -OutputPath '../outside' } | Should -Throw '*is not inside the repository*'
        $null = git -C $WorkDir init --quiet 2>&1
        Set-Content -LiteralPath (Join-Path $WorkDir '.gitignore') -Value 'AzADSPI-Output/'
        { & $PrereqScript -OutputPath 'AzADSPI-Output' } | Should -Throw '*is ignored by git*'
        $null = & $PrereqScript -OutputPath 'wiki'
        Test-Path -LiteralPath (Join-Path $WorkDir 'wiki') | Should -BeTrue
    }

    It 'keeps a rooted -OutputPath' {
        $rooted = Join-Path $WorkDir 'rooted' 'deeper'
        $null = & $PrereqScript -OutputPath $rooted
        Test-Path -LiteralPath $rooted | Should -BeTrue
    }

    It 'ignores a leftover WIKIDIR variable' {
        $env:WIKIDIR = 'legacy'
        $null = & $PrereqScript -OutputPath 'wiki'
        Test-Path -LiteralPath (Join-Path $WorkDir 'wiki') | Should -BeTrue
        Test-Path -LiteralPath (Join-Path $WorkDir 'legacy') | Should -BeFalse
    }
}

Describe 'Test-AzADSPIPrerequisite.ps1 on GitHub Actions' {
    BeforeEach {
        $script:SavedEnv = @{}
        foreach ($name in $EnvNames) { $SavedEnv[$name] = [Environment]::GetEnvironmentVariable($name); [Environment]::SetEnvironmentVariable($name, $null) }
        $script:WorkDir = Join-Path ([System.IO.Path]::GetTempPath()) "azadspi-prereq-$([guid]::NewGuid())"
        $null = New-Item -ItemType Directory -Path $WorkDir
        $env:GITHUB_ACTIONS = 'true'
        $env:GITHUB_WORKSPACE = $WorkDir
        $env:GITHUB_REPOSITORY = 'owner/repo'
        $env:GITHUB_REF = 'refs/heads/main'
        #review F-59: anonymous, the repository API answers 404 for a repository that is not public
        Mock Invoke-WebRequest { throw [Microsoft.PowerShell.Commands.HttpResponseException]::new('not found', [System.Net.Http.HttpResponseMessage]::new([System.Net.HttpStatusCode]::NotFound)) }
        Mock Start-Sleep {}
        Mock Write-Host {}
    }
    AfterEach {
        foreach ($name in $EnvNames) { [Environment]::SetEnvironmentVariable($name, $SavedEnv[$name]) }
        Remove-Item -LiteralPath $WorkDir -Recurse -Force -ErrorAction SilentlyContinue
    }

    It 'creates the -OutputPath folder below the workspace' {
        $null = & $PrereqScript -OutputPath 'wiki'
        Test-Path -LiteralPath (Join-Path $WorkDir 'wiki') | Should -BeTrue
        Should -Invoke Invoke-WebRequest -Times 1 -Exactly -ParameterFilter { $Uri -eq 'https://api.github.com/repos/owner/repo' }
    }
    It 'F-59: a public repository stops the run' {
        Mock Invoke-WebRequest { [pscustomobject]@{ StatusCode = 200; Content = '{"private":false,"visibility":"public"}' } }
        { & $PrereqScript -OutputPath 'wiki' } | Should -Throw '*accessible from the public*'
    }
    It 'F-59: a private repository read with the workflow token proceeds; the server of the run is used' {
        $env:GITHUB_TOKEN = 'token'
        $env:GITHUB_API_URL = 'https://ghe.example.invalid/api/v3'
        Mock Invoke-WebRequest { [pscustomobject]@{ StatusCode = 200; Content = '{"private":true,"visibility":"private"}' } }
        $null = & $PrereqScript -OutputPath 'wiki'
        Test-Path -LiteralPath (Join-Path $WorkDir 'wiki') | Should -BeTrue
        Should -Invoke Invoke-WebRequest -Times 1 -Exactly -ParameterFilter { $Uri -eq 'https://ghe.example.invalid/api/v3/repos/owner/repo' -and $Headers['Authorization'] -eq 'Bearer token' }
    }
    It 'F-59: a visibility that cannot be determined (network, rate limit) stops the run after 3 tries' {
        Mock Invoke-WebRequest { throw 'offline' }
        { & $PrereqScript -OutputPath 'wiki' } | Should -Throw '*could not be determined*'
        Should -Invoke Invoke-WebRequest -Times 3 -Exactly
    }
}

Describe 'no script uses the removed WIKIDIR variable' {
    It '<_>' -ForEach @(Get-ChildItem (Join-Path $PSScriptRoot '..' 'pwsh') -Filter '*.ps1' | ForEach-Object Name) {
        $code = Get-Content -Raw (Join-Path $PSScriptRoot '..' 'pwsh' $_)
        $codeWithoutComments = ($code -split "`r?`n" | Where-Object { $_ -notmatch '^\s*#' }) -join "`n"
        $codeWithoutComments | Should -Not -Match '\$env:WIKIDIR'
    }
}
