#Pester 5+ tests for the user type label of owners and role principals (review P1-9, DECISION part done 2026-11-07):
#a user with an empty userType is labelled 'User (userType not set)' - an empty userType says nothing about on-premises sync.

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath
}

Describe 'user type label' {
    It 'no user is labelled MemberSynced' {
        $script:Source | Should -Not -Match "principalUserType = 'MemberSynced'"
    }
    It 'principal resolution, SP and App role assignments: an empty userType is labelled as not set (3 places)' {
        $pattern = "if \(\[string\]::IsNullOrEmpty\(\`$[\w.]+userType\)\) \{\s+\`$principalUserType = 'userType not set'"
        @([regex]::Matches($script:Source, $pattern)).Count | Should -Be 3
    }
}
