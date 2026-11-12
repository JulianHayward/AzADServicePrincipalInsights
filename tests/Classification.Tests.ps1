#Pester 5+ tests for getClassification (review P2, theme 14).

BeforeAll {
    . (Join-Path $PSScriptRoot 'TestHelpers.ps1')
    foreach ($spec in @(@{ Path = $script:MainScriptPath; Name = 'getClassification' }, @{ Path = (Get-MainScriptAtCommit -Commit 'd332d98'); Name = 'getClassificationOld' })) {
        $fn = (Get-ScriptAst -Path $spec.Path).Find({ param($n) $n -is [System.Management.Automation.Language.FunctionDefinitionAst] -and $n.Name -eq 'getClassification' }, $true)
        . ([scriptblock]::Create(($fn.Extent.Text -replace '^function getClassification', "function $($spec.Name)")))
    }
    #classification set as ConvertFrom-Json -AsHashtable would deliver it, in the given key order
    function New-Classifications([object[]]$Order) {
        $classes = [ordered]@{}
        foreach ($c in $Order) { $classes[$c.Name] = @{ includes = $c.Includes; excludes = $c.Excludes } }
        @{ permissions = @{ appRolePermissions = @{ classifications = $classes } } }
    }
    $script:Critical = @{ Name = 'critical'; Includes = @('*.ReadWrite.All', 'Mail.*'); Excludes = @('Mail.ReadBasic*', 'Notes.ReadWrite.All') }
    $script:Medium = @{ Name = 'medium'; Includes = @('Notes.ReadWrite.All', 'Directory.Read.All', 'Sites.ReadWrite.All'); Excludes = @() }
}

Describe 'getClassification' {
    It 'shipped permissionClassification.json: <Permission> -> <Expected>' -ForEach @(
        @{ Permission = 'Mail.Send'; Expected = 'critical' }
        @{ Permission = 'Mail.ReadBasic.All'; Expected = 'unclassified' }
        @{ Permission = 'Directory.Read.All'; Expected = 'medium' }
        @{ Permission = 'User.Read'; Expected = 'unclassified' }
        @{ Permission = 'Application.ReadWrite.All'; Expected = 'critical' }
    ) {
        $getClassifications = Get-Content -Raw -LiteralPath (Join-Path $script:RepoRoot 'pwsh/permissionClassification.json') | ConvertFrom-Json -AsHashtable
        getClassification -permission $Permission -permissionType 'appRolePermissions' | Should -Be $Expected
    }
    It 'an exclude of one classification does not cancel a match of another one (key order <Label>)' -ForEach @(@{ Label = 'critical, medium'; Swap = $false }, @{ Label = 'medium, critical'; Swap = $true }) {
        $order = if ($Swap) { @($script:Medium, $script:Critical) } else { @($script:Critical, $script:Medium) }
        $getClassifications = New-Classifications $order
        #Notes.ReadWrite.All: critical via *.ReadWrite.All but excluded there, explicitly medium
        getClassification -permission 'Notes.ReadWrite.All' -permissionType 'appRolePermissions' | Should -Be 'medium'
    }
    It 'overlapping classifications: critical wins, independent of the key order (<Label>)' -ForEach @(@{ Label = 'critical, medium'; Swap = $false }, @{ Label = 'medium, critical'; Swap = $true }) {
        $order = if ($Swap) { @($script:Medium, $script:Critical) } else { @($script:Critical, $script:Medium) }
        $getClassifications = New-Classifications $order
        getClassification -permission 'Sites.ReadWrite.All' -permissionType 'appRolePermissions' | Should -Be 'critical'
    }
    It 'original code d332d98: an exclude in a classification checked later reset the match of the earlier one' {
        $getClassifications = New-Classifications @($script:Medium, $script:Critical)
        getClassificationOld -permission 'Notes.ReadWrite.All' -permissionType 'appRolePermissions' | Should -Be 'unclassified'
    }
    It 'original code d332d98: the result depended on the key order' {
        $getClassifications = New-Classifications @($script:Critical, $script:Medium)
        $a = getClassificationOld -permission 'Sites.ReadWrite.All' -permissionType 'appRolePermissions'
        $getClassifications = New-Classifications @($script:Medium, $script:Critical)
        $b = getClassificationOld -permission 'Sites.ReadWrite.All' -permissionType 'appRolePermissions'
        $a | Should -Not -Be $b
    }
    It 'unknown permission type: unclassified' {
        $getClassifications = New-Classifications @($script:Critical)
        getClassification -permission 'Mail.Send' -permissionType 'doesNotExist' | Should -Be 'unclassified'
    }
}
