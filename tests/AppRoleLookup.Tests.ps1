#Review R-3: app role ids are only unique per application; the enrichment resolves an assignment's app role through its resource SP

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    $assignment = $ast.Find({ param($n) $n -is [System.Management.Automation.Language.AssignmentStatementAst] -and $n.Left.Extent.Text -eq '$getResourceAppRole' }, $true)
    if (-not $assignment) { throw '$getResourceAppRole not found in main script' }
    . ([scriptblock]::Create($assignment.Extent.Text))
    $script:Source = Get-Content -Raw -LiteralPath $script:MainScriptPath

    #the same app role id published by two APIs (as Microsoft does for Exchange Online / Microsoft Graph)
    $sharedId = '11111111-1111-1111-1111-111111111111'
    $graphRole = [pscustomobject]@{ id = $sharedId; value = 'Calendars.ReadWrite'; displayName = 'Graph role' }
    $exoRole = [pscustomobject]@{ id = $sharedId; value = 'Calendars.ReadWrite.All'; displayName = 'EXO role' }
    $htServicePrincipalsAppRoles = @{
        'graph' = @{ spDetails = $null; appRoles = @{ $sharedId = $graphRole } }
        'exo' = @{ spDetails = $null; appRoles = @{ $sharedId = $exoRole } }
    }
    #id-only lookup: the first SP wins
    $htAppRoles = @{ $sharedId = $graphRole; '22222222-2222-2222-2222-222222222222' = [pscustomobject]@{ value = 'Fallback.Role' } }
}

Describe 'R-3 app role of an assignment' {
    It 'is taken from the resource SP of the assignment' {
        (& $getResourceAppRole 'exo' $sharedId).value | Should -Be 'Calendars.ReadWrite.All'
        (& $getResourceAppRole 'graph' $sharedId).value | Should -Be 'Calendars.ReadWrite'
    }
    It 'falls back to the id-only lookup when the resource SP has no such role (or is unknown)' {
        (& $getResourceAppRole 'graph' '22222222-2222-2222-2222-222222222222').value | Should -Be 'Fallback.Role'
        (& $getResourceAppRole 'unknown' $sharedId).value | Should -Be 'Calendars.ReadWrite'
        (& $getResourceAppRole '' $sharedId).value | Should -Be 'Calendars.ReadWrite'
    }
    It 'is used for app role assignments and app role assigned to (no id-only lookup left in the enrichment)' {
        @([regex]::Matches($script:Source, '& \$getResourceAppRole \$servicePrincipalAppRole')).Count | Should -Be 3
        @([regex]::Matches($script:Source, '\$hlper = \$htAppRoles\[')).Count | Should -Be 0
    }
}
