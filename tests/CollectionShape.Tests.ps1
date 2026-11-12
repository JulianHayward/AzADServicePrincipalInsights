#Pester 5+ static checks for collection-shape bugs (review items P1-6, P1-14).

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:OriginalScriptPath = Get-MainScriptAtCommit -Commit 'd332d98'

    #'$x = ($x | Sort-Object ...)' returns a scalar for a single item; it must be '$x = @($x | Sort-Object ...)'
    function Get-UnwrappingSelfSort {
        param([string]$Path)
        $ast = Get-ScriptAst -Path $Path
        foreach ($a in $ast.FindAll({ $args[0] -is [System.Management.Automation.Language.AssignmentStatementAst] }, $true)) {
            $right = $a.Right.Extent.Text
            if ($right -match '^\(\s*\$(\w+)\s*\|\s*Sort-Object' -and $a.Left.Extent.Text -match "^\`$(script:)?$($Matches[1])$") {
                "line $($a.Extent.StartLineNumber): $($a.Extent.Text)"
            }
        }
    }
}

Describe 'P1-6 Sort-Object must not unwrap single-item collections' {
    It 'main script has no "$x = ($x | Sort-Object ...)"' {
        Get-UnwrappingSelfSort -Path $script:MainScriptPath | Should -BeNullOrEmpty
    }
    It 'the check finds the pattern in original code d332d98' {
        @(Get-UnwrappingSelfSort -Path $script:OriginalScriptPath).Count | Should -BeGreaterThan 0
    }
}

Describe 'P1-14 MI records are added as one object' {
    It 'main script does not add an ArrayList-wrapped MI record ($customObjectMi) to $cu' {
        $text = Get-Content -LiteralPath $script:MainScriptPath -Raw
        $text | Should -Not -Match '\$customObjectMi\s*=\s*\[System\.Collections\.ArrayList\]'
    }
    It 'the check finds the wrapper in original code d332d98' {
        Get-Content -LiteralPath $script:OriginalScriptPath -Raw | Should -Match '\$customObjectMi\s*=\s*\[System\.Collections\.ArrayList\]'
    }
}
