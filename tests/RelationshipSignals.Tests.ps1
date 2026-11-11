#Review P-1: list passes with $expand tell which objects have owners, owned objects, app role assignments, group memberships,
#federated credentials; only those get the per-object request (which still delivers the data)

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    $assignment = $ast.Find({ param($n) $n -is [System.Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -eq '$hasNoEntries' }, $true)
    . ([scriptblock]::Create($assignment.Extent.Text))
}

Describe 'P-1 relationship signals' {
    It 'skips only when the signal of that relationship exists, returned the object and it has no entries' {
        $htRelationshipSignals = @{ spOwners = @{
                withEntries = [System.Collections.Generic.HashSet[string]]::new([string[]]@('a'), [System.StringComparer]::OrdinalIgnoreCase)
                seen = [System.Collections.Generic.HashSet[string]]::new([string[]]@('a', 'b'), [System.StringComparer]::OrdinalIgnoreCase)
            }
        }
        & $hasNoEntries 'spOwners' 'b' | Should -BeTrue
        & $hasNoEntries 'spOwners' 'B' | Should -BeTrue
        & $hasNoEntries 'spOwners' 'A' | Should -BeFalse
        #review F-20: not returned by the pass (created while the lists were read, paging): the request is made
        & $hasNoEntries 'spOwners' 'c' | Should -BeFalse
        #no signal for this relationship (pass failed): the request is made
        & $hasNoEntries 'spOwnedObjects' 'b' | Should -BeFalse
        #no signals at all (e.g. -OnlyProcessSPs... with few SPs): the request is made
        $htRelationshipSignals = $null
        & $hasNoEntries 'spOwners' 'b' | Should -BeFalse
    }
    It 'all seven per-object requests use their signal' {
        foreach ($pair in @(
                @('getSPOwnedObjects', 'spOwnedObjects'), @('getSPAppRoleAssignments', 'spAppRoleAssignments'), @('getSPAppRoleAssignedTo', 'spAppRoleAssignedTo'),
                @('getSPGroupMemberships', 'spMemberOf'), @('getSPOwner', 'spOwners'), @('getAppOwner', 'appOwners'), @('getFederatedIdentityCredentials', 'appFederatedIdentityCredentials'))) {
            $script:Source | Should -Match ("\`$$($pair[0]) = \`$\(if \(& \`$hasNoEntries '$($pair[1])' ") -Because $pair[0]
            $script:Source | Should -Match ("@\{ name = '$($pair[1])'; property = '\w+'; uri = ") -Because $pair[1]
        }
    }
    It 'the passes use one $expand each (Graph allows only one per request)' {
        $passes = [regex]::Matches($script:Source, "@\{ name = '\w+'; property = '\w+'; uri = `"\`$\(\`$graphUrl\)/(?:v1\.0|beta)/[^`"]+`"")
        $passes.Count | Should -Be 7
        foreach ($m in $passes) {
            ([regex]::Matches($m.Value, 'expand=')).Count | Should -Be 1 -Because $m.Value
        }
    }
}
