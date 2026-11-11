#Pester 5+ tests for the owner resolution (direct and indirect owners of Service Principals, review P1-10 and the follow-up of
#2026-11-06): the walk follows SP owners and the app registration owners of owner SPs, never lists the object itself or a
#direct owner as indirect, and keeps the path ('via'); the 'Owner OwnedBy' cell.

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    $source = Get-Content -Raw -LiteralPath $script:MainScriptPath
    $ast = Get-ScriptAst -Path $script:MainScriptPath
    . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.TypeDefinitionAst] -and $n.Name -eq 'HtmlSafe' }, $true).Extent.Text))
    foreach ($name in 'getOwner', 'getSPControllers', 'getOwnerOwnedByCell', 'getServicePrincipalTypeLabel', 'getServicePrincipalTypeLabelById') {
        . ([scriptblock]::Create($ast.Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq $name }, $true).Extent.Text))
    }
    #the walk: script-level code from '$htSPOwnersTmp = @{}' to the end of its foreach over the objects
    $start = $source.IndexOf('$htSPOwnersTmp = @{}')
    $loop = $ast.Find({ param($n) $n -is [System.Management.Automation.Language.ForEachStatementAst] -and $n.Extent.StartOffset -gt $start -and $n.Condition.Extent.Text -eq '$htServicePrincipalsAndAppsOnlyEnriched.Keys' }, $true)
    $script:Walk = $source.Substring($start, $loop.Extent.EndOffset - $start)

    function New-Principal([string]$id, [string]$type) { [pscustomobject]@{ id = $id; displayName = "name-$id"; '@odata.type' = "#microsoft.graph.$type" } }
    function Invoke-Walk {
        #ownership graph (SP owners), app registrations and their owners:
        #  X  owned by user u1 and SP O;  O owned by SP P (SP owner) and user u1 (also a direct owner of X);
        #  P  not owned, but its app registration is owned by user u2;  O and X own each other (cycle)
        $script:htSPOwners = @{
            X = @((New-Principal 'u1' 'user'), (New-Principal 'O' 'servicePrincipal'))
            O = @((New-Principal 'P' 'servicePrincipal'), (New-Principal 'u1' 'user'), (New-Principal 'X' 'servicePrincipal'))
        }
        $script:htOwnedByEnriched = @{}
        foreach ($k in $script:htSPOwners.Keys) {
            $script:htOwnedByEnriched[$k] = @{ ownedBy = @($script:htSPOwners[$k] | ForEach-Object { [ordered]@{ id = $_.id; displayName = $_.displayName; '@odata.type' = $_.'@odata.type'; principalType = 'p'; applicability = 'direct' } }) }
        }
        $script:htServicePrincipalsAndAppsOnlyEnriched = @{ X = @{}; O = @{}; P = @{ Application = @{ ApplicationDetails = @{ id = 'appP' } } } }
        $script:htAppOwners = @{ appP = @((New-Principal 'u2' 'user')) }
        $script:htSpLookup = @{ X = @{ objectTypeConcatinated = 'SP APP INT' }; O = @{ objectTypeConcatinated = 'SP APP INT' }; P = @{ objectTypeConcatinated = 'SP APP INT' } }
        $script:htPrincipalsResolved = @{ u1 = @{ typeOnly = 'User (Member)' }; u2 = @{ typeOnly = 'User (Member)' } }
        $htSPOwners = $script:htSPOwners; $htOwnedByEnriched = $script:htOwnedByEnriched; $htServicePrincipalsAndAppsOnlyEnriched = $script:htServicePrincipalsAndAppsOnlyEnriched
        $htAppOwners = $script:htAppOwners; $htSpLookup = $script:htSpLookup; $htPrincipalsResolved = $script:htPrincipalsResolved
        . ([scriptblock]::Create($script:Walk))
        $htSPOwnersFinal
    }
}

Describe 'indirect owners' {
    BeforeAll { $script:Final = Invoke-Walk }
    It 'direct owners are the owners of the SP' {
        @($script:Final.X.where({ $_.applicability -eq 'direct' }).id | Sort-Object) | Should -Be @('O', 'u1')
    }
    It 'indirect owners: through SP owners and through the app registration owner of an owner SP' {
        @($script:Final.X.where({ $_.applicability -eq 'indirect' }).id | Sort-Object) | Should -Be @('P', 'u2')
    }
    It 'the object is never its own indirect owner (cycle: X and O own each other), a direct owner is not repeated as indirect' {
        $script:Final.X.where({ $_.applicability -eq 'indirect' }).id | Should -Not -Contain 'X'
        $script:Final.X.where({ $_.applicability -eq 'indirect' }).id | Should -Not -Contain 'u1'
        $script:Final.O.where({ $_.applicability -eq 'indirect' }).id | Should -Not -Contain 'O'
    }
    It 'the path of an indirect owner' {
        ($script:Final.X.where({ $_.id -eq 'P' })).via | Should -BeExactly "owner of 'name-O'"
        ($script:Final.X.where({ $_.id -eq 'u2' })).via | Should -BeExactly "app registration owner of 'name-P', which controls 'name-O'"
    }
}

Describe 'Owner OwnedBy cell' {
    BeforeAll {
        $CsvDelimiterOpposite = ','
        $script:Cell = getOwnerOwnedByCell -OwnerEntries @(
            [ordered]@{ id = '1'; displayName = 'alice <b>'; principalType = 'User (Member)'; applicability = 'direct' }
            [ordered]@{ id = '2'; displayName = 'app2'; principalType = 'SP APP INT'; applicability = 'direct' }
            [ordered]@{ id = '3'; displayName = 'bob'; principalType = 'User (Member)'; applicability = 'indirect'; via = "app registration owner of 'app2'" }
        )
    }
    It 'CSV keeps the former text format' {
        $script:Cell.Csv | Should -BeExactly 'direct 2 [alice <b> User (Member), app2 SP APP INT], indirect 1 [bob User (Member)]'
    }
    It 'HTML: labelled lists, names encoded, type and path dimmed' {
        $script:Cell.Html | Should -BeExactly ('<b>direct (2)</b><ul class="spiList"><li>alice &lt;b&gt; <span class="spiDim">User (Member)</span></li><li>app2 <span class="spiDim">SP APP INT</span></li></ul>' +
            '<b>indirect (1)</b><ul class="spiList"><li>bob <span class="spiDim">User (Member) &middot; app registration owner of &#39;app2&#39;</span></li></ul>')
    }
    It 'the own Service Principal of the row is marked (ownership cycle)' {
        $cell = getOwnerOwnedByCell -OwnerEntries @([ordered]@{ id = 'X'; displayName = 'x'; principalType = 'SP APP INT'; applicability = 'direct' }) -SelfId 'X'
        $cell.Html | Should -BeLike '*this Service Principal (ownership cycle)*'
        $cell.Csv | Should -BeExactly 'direct 1 [x SP APP INT]'
    }
    It 'no owners: empty' {
        (getOwnerOwnedByCell -OwnerEntries @()).Html | Should -BeExactly ''
    }
}
