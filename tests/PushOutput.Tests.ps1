#Pester 5+ tests for pwsh/Push-AzADSPIOutput.ps1 (the push step of the GitHub workflows and the Azure DevOps pipeline).
#Each test runs the script in a child pwsh against a local bare repository ('origin'). git's global and system config are
#isolated (GIT_CONFIG_GLOBAL = a temp file, GIT_CONFIG_NOSYSTEM): the user's config is not used and not changed, and the tests
#can check that the script never writes the global config (review F-64).

BeforeAll {
    $script:PushScript = Join-Path (Split-Path -Parent $PSScriptRoot) 'pwsh/Push-AzADSPIOutput.ps1'
    $script:Root = Join-Path ([IO.Path]::GetTempPath()) "azadspi-push-$([guid]::NewGuid().ToString('n'))"
    $null = New-Item -ItemType Directory -Path $script:Root
    $script:SavedEnv = @{}
    foreach ($name in 'GIT_CONFIG_GLOBAL', 'GIT_CONFIG_NOSYSTEM', 'GITHUB_ACTIONS', 'TF_BUILD', 'GITHUB_REF', 'BUILD_SOURCEBRANCH', 'SYSTEM_ACCESSTOKEN') {
        $script:SavedEnv[$name] = [Environment]::GetEnvironmentVariable($name)
    }
    $script:GlobalConfig = Join-Path $script:Root 'gitconfig-global'
    Set-Content -LiteralPath $script:GlobalConfig -Value "[user]`n`tname = seed`n`temail = seed@example.com`n[commit]`n`tgpgsign = false`n[init]`n`tdefaultBranch = main" -NoNewline
    $env:GIT_CONFIG_GLOBAL = $script:GlobalConfig
    $env:GIT_CONFIG_NOSYSTEM = '1'
    foreach ($name in 'GITHUB_ACTIONS', 'TF_BUILD', 'GITHUB_REF', 'BUILD_SOURCEBRANCH', 'SYSTEM_ACCESSTOKEN') { [Environment]::SetEnvironmentVariable($name, $null) }

    #a bare 'origin' with one commit (the output of an earlier run) and a clone = the checkout of the run
    function New-TestRepo([string]$Name) {
        $base = Join-Path $script:Root $Name
        $remote = Join-Path $base 'origin.git'
        $seed = Join-Path $base 'seed'
        $run = Join-Path $base 'run'
        $null = git init --quiet --bare $remote 2>&1
        $null = git clone --quiet $remote $seed 2>&1
        Set-Content -LiteralPath (Join-Path $seed 'report.html') -Value 'run 1'
        Set-Content -LiteralPath (Join-Path $seed 'history.jsonl') -Value 'line 1'
        $null = git -C $seed add --all
        $null = git -C $seed commit --quiet -m 'run 1'
        $null = git -C $seed push --quiet origin HEAD:refs/heads/main 2>&1
        $null = git clone --quiet $remote $run 2>&1
        [PSCustomObject]@{ Remote = $remote; Seed = $seed; Run = $run }
    }

    #another run pushes in the meantime (from the seed clone)
    function Push-Other([object]$Repo, [string]$File, [string]$Content) {
        Set-Content -LiteralPath (Join-Path $Repo.Seed $File) -Value $Content
        $null = git -C $Repo.Seed add --all
        $null = git -C $Repo.Seed commit --quiet -m 'other run'
        $null = git -C $Repo.Seed push --quiet origin HEAD:refs/heads/main 2>&1
    }

    function Invoke-Push([object]$Repo, [hashtable]$Environment, [string[]]$Arguments = @()) {
        foreach ($key in $Environment.Keys) { [Environment]::SetEnvironmentVariable($key, $Environment[$key]) }
        try {
            $output = & pwsh -NoProfile -NonInteractive -File $script:PushScript -RepositoryPath $Repo.Run @Arguments 2>&1
            [PSCustomObject]@{ ExitCode = $LASTEXITCODE; Output = ($output | Out-String) }
        }
        finally {
            foreach ($key in $Environment.Keys) { [Environment]::SetEnvironmentVariable($key, $null) }
        }
    }

    function Get-RemoteLog([object]$Repo) { @(git -C $Repo.Remote log --format='%an|%ae|%s' refs/heads/main) }

    $script:GitHub = @{ GITHUB_ACTIONS = 'true'; GITHUB_REF = 'refs/heads/main'; GITHUB_ACTOR = 'octocat'; GITHUB_WORKFLOW = 'AzADServicePrincipalInsights'; GITHUB_JOB = 'AzADServicePrincipalInsights' }
    $script:Token = 'ado-token-' + [guid]::NewGuid().ToString('n')
    $script:Ado = @{ TF_BUILD = 'True'; BUILD_SOURCEBRANCH = 'refs/heads/main'; SYSTEM_ACCESSTOKEN = $script:Token; BUILD_DEFINITIONNAME = 'AzADSPI'; BUILD_BUILDNUMBER = '1'; SYSTEM_TEAMPROJECT = 'proj'; BUILD_REPOSITORY_NAME = 'repo'; BUILD_SOURCEBRANCHNAME = 'main'; BUILD_SOURCEVERSION = 'abc' }
}

AfterAll {
    foreach ($name in $script:SavedEnv.Keys) { [Environment]::SetEnvironmentVariable($name, $script:SavedEnv[$name]) }
    Remove-Item -LiteralPath $script:Root -Recurse -Force -ErrorAction SilentlyContinue
}

Describe 'Push-AzADSPIOutput.ps1' {
    It 'nothing changed: no commit, success' {
        $repo = New-TestRepo 'unchanged'
        $result = Invoke-Push $repo $script:GitHub
        $result.ExitCode | Should -Be 0
        $result.Output | Should -Match 'No changes in the output - nothing to commit'
        @(Get-RemoteLog $repo).Count | Should -Be 1
    }

    It 'GitHub: commits with the workflow identity and pushes; the global git config stays unchanged (F-64)' {
        $repo = New-TestRepo 'github'
        $globalBefore = Get-Content -LiteralPath $script:GlobalConfig -Raw
        Set-Content -LiteralPath (Join-Path $repo.Run 'report.html') -Value 'run 2'
        $result = Invoke-Push $repo $script:GitHub
        $result.ExitCode | Should -Be 0 -Because $result.Output
        (Get-RemoteLog $repo)[0] | Should -Be 'octocat|AzADServicePrincipalInsightsGHActions@ghActions.com|AzADServicePrincipalInsights AzADServicePrincipalInsights'
        git -C $repo.Remote show refs/heads/main:report.html | Should -Be 'run 2'
        Get-Content -LiteralPath $script:GlobalConfig -Raw | Should -Be $globalBefore
        git -C $repo.Run config --local user.email | Should -Be 'AzADServicePrincipalInsightsGHActions@ghActions.com'
    }

    It 'another run pushed in the meantime (other files): rebased on top of it and pushed' {
        $repo = New-TestRepo 'rebase'
        Push-Other $repo 'other.csv' 'from the other run'
        Set-Content -LiteralPath (Join-Path $repo.Run 'report.html') -Value 'run 2'
        $result = Invoke-Push $repo $script:GitHub
        $result.ExitCode | Should -Be 0 -Because $result.Output
        $log = Get-RemoteLog $repo
        $log.Count | Should -Be 3
        $log[1] | Should -Match '\|other run$'
        git -C $repo.Remote show refs/heads/main:other.csv | Should -Be 'from the other run'
        git -C $repo.Remote show refs/heads/main:report.html | Should -Be 'run 2'
    }

    It 'overlapping runs (same file): GitHub error annotation, rebase aborted, origin unchanged' {
        $repo = New-TestRepo 'conflict-gh'
        Push-Other $repo 'history.jsonl' 'line 1 other'
        $remoteHead = git -C $repo.Remote rev-parse refs/heads/main
        Set-Content -LiteralPath (Join-Path $repo.Run 'history.jsonl') -Value 'line 1 this'
        $result = Invoke-Push $repo $script:GitHub
        $result.ExitCode | Should -Be 1
        $result.Output | Should -Match "(?m)^::error::git pull --rebase from 'refs/heads/main' failed: another run changed the same output files at the same time\. Runs of this workflow \(concurrency group\) must not overlap"
        Test-Path -LiteralPath (Join-Path $repo.Run '.git/rebase-merge') | Should -BeFalse
        Test-Path -LiteralPath (Join-Path $repo.Run '.git/rebase-apply') | Should -BeFalse
        git -C $repo.Remote rev-parse refs/heads/main | Should -Be $remoteHead
    }

    It 'Azure DevOps (detached HEAD like the pipeline checkout): pushes to the branch with the pipeline identity' {
        $repo = New-TestRepo 'ado'
        $null = git -C $repo.Run checkout --quiet --detach 2>&1
        Set-Content -LiteralPath (Join-Path $repo.Run 'report.html') -Value 'run 2'
        $result = Invoke-Push $repo $script:Ado
        $result.ExitCode | Should -Be 0 -Because $result.Output
        (Get-RemoteLog $repo)[0] | Should -Match "^Pipeline: 'AzADSPI' 'rev 1' \(Project: proj; Repository: repo; Branch: main Commit: abc\)\|AzADServicePrincipalInsightsPipeline@azdo\.com\|wiki "
        $result.Output | Should -Not -Match ([regex]::Escape($script:Token))
        @(git -C $repo.Run config --get-all http.extraheader).Count | Should -Be 0 -Because 'the token is passed per command, not stored'
    }

    It 'Azure DevOps overlapping runs: ##vso error, the token is not printed' {
        $repo = New-TestRepo 'conflict-ado'
        Push-Other $repo 'history.jsonl' 'line 1 other'
        Set-Content -LiteralPath (Join-Path $repo.Run 'history.jsonl') -Value 'line 1 this'
        $result = Invoke-Push $repo $script:Ado
        $result.ExitCode | Should -Be 1
        $result.Output | Should -Match '(?m)^##vso\[task\.logissue type=error\]git pull --rebase from .refs/heads/main. failed: another run changed the same output files at the same time\. Runs of this pipeline \(schedule, manual runs; optionally an exclusive lock\) must not overlap'
        $result.Output | Should -Not -Match ([regex]::Escape($script:Token))
    }

    It 'Azure DevOps without SYSTEM_ACCESSTOKEN: fails before committing' {
        $repo = New-TestRepo 'ado-notoken'
        Set-Content -LiteralPath (Join-Path $repo.Run 'report.html') -Value 'run 2'
        $environment = $script:Ado.Clone(); $environment['SYSTEM_ACCESSTOKEN'] = ''
        $result = Invoke-Push $repo $environment
        $result.ExitCode | Should -Be 1
        $result.Output | Should -Match 'SYSTEM_ACCESSTOKEN is empty'
        @(git -C $repo.Run log --format=%s).Count | Should -Be 1
    }

    It 'a run that is not on a branch (pull request ref) fails with a clear message' {
        $repo = New-TestRepo 'pr'
        $environment = $script:GitHub.Clone(); $environment['GITHUB_REF'] = 'refs/pull/1/merge'
        $result = Invoke-Push $repo $environment
        $result.ExitCode | Should -Be 1
        $result.Output | Should -Match "::error::The run is not on a branch \('refs/pull/1/merge'\)"
    }

    It 'outside a pipeline/workflow and without -Platform: fails with a clear message' {
        $repo = New-TestRepo 'noplatform'
        $result = Invoke-Push $repo @{}
        $result.ExitCode | Should -Be 1
        $result.Output | Should -Match 'runs in a GitHub Actions workflow or an Azure DevOps pipeline'
    }

    It 'the pipelines call the script in their push step and keep the token in the env of the step' {
        $repoRoot = Split-Path -Parent $PSScriptRoot
        foreach ($workflow in 'AzADServicePrincipalInsights_OIDC.yml', 'AzADServicePrincipalInsights.yml') {
            $text = Get-Content -Raw -LiteralPath (Join-Path $repoRoot ".github/workflows/$workflow")
            $text | Should -Match '(?m)^      - name: Push AzADServicePrincipalInsights output to repository\r?\n        shell: pwsh\r?\n        run: \|\r?\n          & "\./\$\(\$env:ScriptDir\)/Push-AzADSPIOutput\.ps1" -RepositoryPath \$env:GITHUB_WORKSPACE\r?\n' -Because $workflow
            ($text -split '\r?\n' | Where-Object { $_ -notmatch '^\s*#' }) -join "`n" | Should -Not -Match 'git (push|commit|pull)' -Because "$workflow - the git logic is in the script"
        }
        $ado = Get-Content -Raw -LiteralPath (Join-Path $repoRoot '.azuredevops/pipelines/AzADServicePrincipalInsights.yml')
        $ado | Should -Match '(?m)^      & "\$\(\$env:SYSTEM_DEFAULTWORKINGDIRECTORY\)/\$\(\$env:SCRIPTDIR\)/Push-AzADSPIOutput\.ps1" -RepositoryPath \$env:SYSTEM_DEFAULTWORKINGDIRECTORY\r?\n    displayName: ''Push AzADServicePrincipalInsights output to repository''\r?\n    env:\r?\n      SYSTEM_ACCESSTOKEN: \$\(System\.AccessToken\)'
        ($ado -split '\r?\n' | Where-Object { $_ -notmatch '^\s*#' }) -join "`n" | Should -Not -Match 'git (push|commit|pull)'
    }
}
