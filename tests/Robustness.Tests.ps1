#Pester 5+ tests for unattended runs and setup helpers (review P2 items of theme 11).

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:Ast = Get-ScriptAst -Path $script:MainScriptPath
    function Get-FunctionText([string]$Name) {
        $fn = $script:Ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $Name }, $true)
        if (-not $fn) { throw "$Name not found in main script" }
        $fn.Extent.Text
    }
}

Describe 'testInteractiveSession' {
    It 'is false in pwsh -NonInteractive with redirected input (pipelines, Automation, scheduled tasks)' {
        $file = Join-Path ([IO.Path]::GetTempPath()) "azadspi-interactive-$([guid]::NewGuid().ToString('n')).ps1"
        Set-Content -LiteralPath $file -Value ((Get-FunctionText 'testInteractiveSession') + "`ntestInteractiveSession")
        try { $result = pwsh -NoProfile -NonInteractive -File $file } finally { Remove-Item -LiteralPath $file -ErrorAction SilentlyContinue }
        $result | Should -Be 'False'
    }
}

Describe 'testPowerShellVersion' {
    It 'accepts the running PowerShell (7.x) and would accept a higher major version' {
        $code = Get-FunctionText 'testPowerShellVersion'
        { . ([scriptblock]::Create($code)); testPowerShellVersion 6>$null } | Should -Not -Throw
        #the comparison itself: [version] compare instead of 'Major -eq 7'
        ([version]'8.0.0' -ge [version]'7.0.3') | Should -BeTrue
        ([version]'7.0.2' -ge [version]'7.0.3') | Should -BeFalse
    }
    It 'original code d332d98 compared Major with -eq (rejects PowerShell 8)' {
        Get-Content -LiteralPath (Get-MainScriptAtCommit -Commit 'd332d98') -Raw | Should -Match '\$thisPSVersionMajor -eq \$splitRequiredPSVersionMajor'
    }
}

Describe 'setOutput timestamp' {
    BeforeAll {
        $script:SetOutput = Get-FunctionText 'setOutput'
        $script:TempDir = Join-Path ([IO.Path]::GetTempPath()) "azadspi-test-$([guid]::NewGuid().ToString('n'))"
        $null = New-Item -ItemType Directory -Path $script:TempDir
    }
    AfterAll { Remove-Item -LiteralPath $script:TempDir -Recurse -Force -ErrorAction SilentlyContinue }
    #literal '/' and ':' (unquoted, '/' and ':' are the culture's date/time separators, e.g. '.' for the date in de-DE)
    It 'falls back to the default format if the format produces characters invalid in file names (<Format>)' -ForEach @(@{ Format = "yyyy'/'MM'/'dd" }, @{ Format = "HH':'mm" }) {
        $outputPath = $script:TempDir; $FileTimeStampFormat = $Format
        . ([scriptblock]::Create($script:SetOutput)); setOutput 6>$null
        $fileTimestamp | Should -Match '^\d{8}_\d{6}$'
    }
    It 'keeps a valid custom format' {
        $outputPath = $script:TempDir; $FileTimeStampFormat = 'yyyy-MM-dd_HHmm'
        . ([scriptblock]::Create($script:SetOutput)); setOutput 6>$null
        $fileTimestamp | Should -Match '^\d{4}-\d{2}-\d{2}_\d{4}$'
    }
}

Describe 'static checks' {
    It 'the transcript is stopped by a trap when the script fails' {
        $traps = $script:Ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.TrapStatementAst] }, $false)
        @($traps | Where-Object { $_.Body.Extent.Text -match 'Stop-Transcript' -and $_.Body.Extent.Text -match '\bbreak\b' }).Count | Should -Be 1
    }
    It 'no output timestamp is generated after setOutput (all files share the start timestamp)' {
        $assignments = $script:Ast.FindAll({ param($n) $n -is [System.Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -match '^\$(script:)?fileTimestamp$' }, $true)
        @($assignments | Where-Object { $_.Extent.StartLineNumber -gt (Select-String -LiteralPath $script:MainScriptPath -Pattern '^setOutput$').LineNumber }).Count | Should -Be 0
    }
    It 'output files are written with -LiteralPath' {
        Get-Content -LiteralPath $script:MainScriptPath -Raw | Should -Not -Match '(Export-Csv|Set-Content|Start-Transcript) -Path "\$\('
    }
}
