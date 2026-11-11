#This script should be run in Azure DevOps Pipelines and GitHub Actions only

param(
    [string]
    $OutputPath = 'wiki'
)

if ($env:SYSTEM_TEAMPROJECTID -and $env:BUILD_REPOSITORY_ID) {
    $codeRunPlatform = 'AzureDevOps'
}
elseif ($env:GITHUB_ACTIONS) {
    $codeRunPlatform = 'GitHubActions'
}
else {
    $codeRunPlatform = "not 'AzureDevOps', not 'GitHubActions'"
}

Write-Host 'CodeRunPlatform:' $codeRunPlatform

#an error annotation in the run summary (not only 'exit code 1'), then stop
function stopWithError([string]$Message) {
    switch ($codeRunPlatform) {
        'GitHubActions' { Write-Host "::error::$Message" }
        'AzureDevOps' { Write-Host "##vso[task.logissue type=error]$Message" }
    }
    throw $Message
}

#the output path as the main script gets it (-OutputPath): relative to the working directory of the pipeline/workflow
#(Azure DevOps: SYSTEM_DEFAULTWORKINGDIRECTORY, GitHub: GITHUB_WORKSPACE), a rooted path as is
function resolveOutputPath([string]$Path, [string]$BasePath) {
    if ([System.IO.Path]::IsPathRooted($Path)) { return $Path }
    if (-not $BasePath) { $BasePath = (Get-Location).Path }
    return (Join-Path -Path $BasePath -ChildPath $Path)
}

#the push step commits the output folder: it must be inside the checkout and not ignored by git - else the run would end green
#without any output in the repository
function testOutputPathInRepository([string]$Resolved, [string]$Repository) {
    if (-not $Repository) { return }
    $fullPath = [System.IO.Path]::GetFullPath($Resolved).TrimEnd('/', '\')
    $root = [System.IO.Path]::GetFullPath($Repository).TrimEnd('/', '\')
    if (-not $fullPath.StartsWith($root + [System.IO.Path]::DirectorySeparatorChar, [System.StringComparison]::OrdinalIgnoreCase)) {
        stopWithError "OutputPath '$($OutputPath)' ($($fullPath)) is not inside the repository ($($root)) - the output is pushed to the repository; use a folder inside it, e.g. wiki"
    }
    #trailing '/': a pattern for a folder ('out/') matches a folder that does not exist yet
    $null = git -C $root check-ignore -q -- "$($fullPath)/" 2>&1
    if ($LASTEXITCODE -eq 0) {
        stopWithError "OutputPath '$($OutputPath)' is ignored by git (.gitignore) - the output would not be pushed; use another folder, e.g. wiki"
    }
}

#the push step pushes to the branch of the run - a tag or a pull request ref would fail only after the whole collection
$runRef = $(if ($codeRunPlatform -eq 'AzureDevOps') { $env:BUILD_SOURCEBRANCH } elseif ($codeRunPlatform -eq 'GitHubActions') { $env:GITHUB_REF })
if ($runRef -and $runRef -notlike 'refs/heads/*') {
    stopWithError "The run is not on a branch ('$($runRef)') - the output is pushed to the branch of the run; run it on a branch"
}

if ($codeRunPlatform -eq 'GitHubActions') {

    #review F-59: the server of the run (GitHub Enterprise Server, ghe.com: GITHUB_SERVER_URL / GITHUB_API_URL; was always github.com)
    #and the visibility from the repository API (with the workflow token when the step provides GITHUB_TOKEN). A result that does not
    #tell (rate limit, server error, network) stops the run - it was 'skipping this test' and the tenant report was pushed.
    $serverUrl = $(if ($env:GITHUB_SERVER_URL) { $env:GITHUB_SERVER_URL.TrimEnd('/') } else { 'https://github.com' })
    $apiUrl = $(if ($env:GITHUB_API_URL) { $env:GITHUB_API_URL.TrimEnd('/') } else { 'https://api.github.com' })
    $repoUri = "$($serverUrl)/$($env:GITHUB_REPOSITORY)"
    $repoApiUri = "$($apiUrl)/repos/$($env:GITHUB_REPOSITORY)"
    Write-Host "Testing if repository '$($repoUri)' is accessible from the public"
    $headers = @{ Accept = 'application/vnd.github+json' }
    if ($env:GITHUB_TOKEN) { $headers['Authorization'] = "Bearer $($env:GITHUB_TOKEN)" }
    $visibility = $null
    $statusCode = $null
    for ($try = 1; $try -le 3 -and -not $visibility; $try++) {
        try {
            $res = Invoke-WebRequest -Uri $repoApiUri -Headers $headers -ErrorAction Stop
            $statusCode = $res.StatusCode
            $repository = $res.Content | ConvertFrom-Json
            $visibility = $(if ($repository.visibility) { [string]$repository.visibility } elseif ($repository.private -eq $false) { 'public' } elseif ($repository.private -eq $true) { 'private' })
        }
        catch {
            $statusCode = $_.Exception.Response.StatusCode.value__
            #anonymous: a repository that is not public is 'not found'
            if ($statusCode -eq 404 -and -not $env:GITHUB_TOKEN) { $visibility = 'not public (not found anonymously)' }
        }
        if (-not $visibility -and $try -lt 3) { Start-Sleep -Seconds (5 * $try) }
    }

    if ($visibility -eq 'public') {
        Write-Host "Test returned visibility '$visibility' - '$($repoUri)' is accessible from the public!"
        stopWithError "Repository '$($repoUri)' is accessible from the public - assuming and insisting that you do not want to publish your tenant insights to the public"
    }
    elseif ($visibility) {
        Write-Host "Test returned visibility '$visibility' - '$($repoUri)' is not accessible from the public - proceed"
    }
    else {
        stopWithError "The visibility of repository '$($repoUri)' could not be determined (statusCode: '$statusCode') - stopping, the tenant insights must not be pushed to a public repository; run it again"
    }

    $outputPathResolved = resolveOutputPath -Path $OutputPath -BasePath $env:GITHUB_WORKSPACE
    Write-Host "outputpath is '$OutputPath' ($outputPathResolved)"
    if (-not (Test-Path -LiteralPath $outputPathResolved)) {
        #Assuming this is the initial run

        #Create the outputpath dir
        Write-Host "Creating directory '$outputPathResolved'"
        New-Item -ItemType Directory -Force -Path $outputPathResolved

        Get-ChildItem
    }
    else {
        Write-Host "outputpath dir '$OutputPath' already exists"
    }
    testOutputPathInRepository -Resolved $outputPathResolved -Repository $env:GITHUB_WORKSPACE
}

if ($codeRunPlatform -eq 'AzureDevOps') {
    $collectionUri = "$($env:SYSTEM_COLLECTIONURI)".TrimEnd('/') #$(System.CollectionUri)
    $project = $env:SYSTEM_TEAMPROJECT #$(System.TeamProject)
    if (-not $env:SYSTEM_ACCESSTOKEN) {
        stopWithError "SYSTEM_ACCESSTOKEN is empty - the step 'Check prerequisites' needs 'env: SYSTEM_ACCESSTOKEN: `$(System.AccessToken)' (project visibility check)"
    }
    #createHeader
    $pat = $env:SYSTEM_ACCESSTOKEN #$(System.AccessToken)
    $token = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes(":$($pat)"))
    $header = @{authorization = "Basic $token" }

    #region project visibility - the same rule as the GitHub repository: public stops, an unclear answer stops too
    Write-Host "Testing if project '$($project)' is public"
    $projectVisibility = $null
    $projectError = $null
    for ($try = 1; $try -le 3 -and -not $projectVisibility; $try++) {
        try {
            $projectInfo = Invoke-RestMethod -Uri "$($collectionUri)/_apis/projects/$($env:SYSTEM_TEAMPROJECTID)?api-version=7.1" -Method 'GET' -Headers $header -ContentType 'application/json' -ErrorAction Stop
            $projectVisibility = [string]$projectInfo.visibility
            if (-not $projectVisibility) { $projectError = 'no visibility in the answer' }
        }
        catch {
            $projectError = $(if ($_.Exception.Response) { "status code $([int]$_.Exception.Response.StatusCode)" } else { $_.Exception.Message })
        }
        if (-not $projectVisibility -and $try -lt 3) { Start-Sleep -Seconds (5 * $try) }
    }
    if ($projectVisibility -eq 'public') {
        stopWithError "Project '$($project)' is public - the repository with the output and the run logs are readable by anyone. Assuming and insisting that you do not want to publish your tenant insights to the public - make the project private"
    }
    elseif ($projectVisibility) {
        Write-Host "Project '$($project)' visibility: $($projectVisibility) - proceed"
    }
    else {
        stopWithError "The visibility of project '$($project)' could not be determined ($($projectError)) - stopping, the tenant insights must not be pushed to a public project; run it again"
    }
    #endregion project visibility

    #was: $env:WIKIDIR (no longer set since the pipeline variables file; -OutputPath was ignored and the folder never created)
    $outputPathResolved = resolveOutputPath -Path $OutputPath -BasePath $env:SYSTEM_DEFAULTWORKINGDIRECTORY
    Write-Host "outputpath is '$OutputPath' ($outputPathResolved)"
    testOutputPathInRepository -Resolved $outputPathResolved -Repository $env:SYSTEM_DEFAULTWORKINGDIRECTORY
    if (-not (Test-Path -LiteralPath $outputPathResolved)) {
        #Assuming this is the initial run

        #Create the outputpath dir
        Write-Host "Creating directory '$outputPathResolved'"
        New-Item -ItemType Directory -Force -Path $outputPathResolved

        #Repository permission check
        Write-Host 'Repository access check'

        #region listRepos
        $uri = "$($collectionUri)/$($project)/_apis/git/repositories?api-version=7.1"
        $repos = Invoke-RestMethod -Uri $uri -Method 'get' -Headers $header -ContentType 'application/json'
        $htRepos = @{}
        foreach ($repo in $repos.value) {
            $htRepos[$repo.id] = @{}
            $htRepos[$repo.id].name = $repo.name
        }
        #endregion listRepos

        #region gettingSubjectDescriptor
        #https://dev.azure.com/{organization}/ or the legacy https://{organization}.visualstudio.com/
        $skipCheck = $false
        $collectionUriObject = [uri]$collectionUri
        if ($collectionUriObject.Host -eq 'dev.azure.com') {
            $organization = $collectionUriObject.Segments[1].Trim('/')
        }
        elseif ($collectionUriObject.Host -like '*.visualstudio.com') {
            #was: the host name, so vssps.dev.azure.com was asked for organization '{organization}.visualstudio.com'
            $organization = $collectionUriObject.Host.Split('.')[0]
        }
        else {
            Write-Host "Collection URI '$($collectionUri)' is neither dev.azure.com nor *.visualstudio.com (Azure DevOps Server?); skipping check"
            $skipCheck = $true
        }
        $buildServiceAccountId = $env:SYSTEM_TEAMPROJECTID #$(System.TeamProjectId)

        if (-not $skipCheck) {
            #either 'Project Collection Build Service ($($organization))' OR '$($project) Build Service ($($organization))'
            $buildAccount = "Project Collection Build Service ($($organization))"
            Write-Host "Checking: $buildAccount"
            $uri = "https://vssps.dev.azure.com/$($organization)/_apis/identities?searchFilter=General&filterValue=$([uri]::EscapeDataString($buildAccount))&queryMembership=None&api-version=7.1"
            try {
                $res = Invoke-RestMethod -Uri $uri -Method 'GET' -Headers $header -ContentType 'application/json'
            }
            catch {
                Write-Host "Checking: $buildAccount failed:"
                $_
                Write-Host "Checking: $buildAccount failed; skipping check"
                $skipCheck = $true
            }
        }

        if (-not $skipCheck) {
            $providerDisplayName = $res.value.providerDisplayName
            $providerDisplayNameProjectCollectionBuildService = $providerDisplayName
            $subjectDescriptor = $res.value.subjectDescriptor

            if ($providerDisplayName -ne $buildServiceAccountId) {
                $buildAccount = "$($project) Build Service ($($organization))"
                Write-Host "Checking: $buildAccount"
                $uri = "https://vssps.dev.azure.com/$($organization)/_apis/identities?searchFilter=General&filterValue=$([uri]::EscapeDataString($buildAccount))&queryMembership=None&api-version=7.1"
                $res = Invoke-RestMethod -Uri $uri -Method 'GET' -Headers $header -ContentType 'application/json'
                $providerDisplayName = $res.value.providerDisplayName
                $providerDisplayNameProjectBuildService = $providerDisplayName
                $subjectDescriptor = $res.value.subjectDescriptor
            }

            #endregion gettingSubjectDescriptor

            if ($providerDisplayName -ne $buildServiceAccountId) {
                Write-Host "Neither 'Project Collection Build Service ($($organization))' ($($providerDisplayNameProjectCollectionBuildService)) nor '$($project) Build Service ($($organization))' ($($providerDisplayNameProjectBuildService)) matching Id: $($buildServiceAccountId)"
            }
            else {
                Write-Host "subjectDescriptor for '$($buildAccount)':" $subjectDescriptor

                #region gettingPermissions
                $repositoryId = $env:BUILD_REPOSITORY_ID #$(Build.Repository.ID)
                $permissionSetId = '2e9eb7ed-3c0a-47d4-87c1-0ffdd275fd87' #Git Repositories

                #Contribution/HierarchyQuery is not a documented API; its api-version is kept
                $uri = "$($collectionUri)/_apis/Contribution/HierarchyQuery?api-version=5.0-preview.1"
                $body = @"
{
    "contributionIds": [
        "ms.vss-admin-web.security-view-permissions-data-provider"
    ],
    "dataProviderContext": {
        "properties": {
            "subjectDescriptor": "$($subjectDescriptor)",
            "permissionSetId": "$($permissionSetId)",
            "permissionSetToken": "repoV2/$($buildServiceAccountId)/$($repositoryId)"
        }
    }
}
"@

                $permissions = Invoke-RestMethod -Uri $uri -Method 'post' -Body $body -Headers $header -ContentType 'application/json'
                $htPermissionsRef = @{}
                $htPermissionsRef['1'] = 'allow'
                $htPermissionsRef['2'] = 'deny'
                $htPermissionsRef['3'] = 'allow(inherited)'
                $repositoryName = $htRepos[$repositoryId].name
                Write-Host "'$($buildAccount)' ($buildServiceAccountId) permissions for Repository: $($repositoryName) ($repositoryId)"
                $contributePermissionState = ($permissions.dataProviders.'ms.vss-admin-web.security-view-permissions-data-provider'.subjectPermissions).where( { $_.displayName -eq 'Contribute' } ).effectivePermissionValue
                if ($contributePermissionState -eq 1 -or $contributePermissionState -eq 3) {
                    Write-Host " Contribute: $($htPermissionsRef["$contributePermissionState"]) ($($contributePermissionState))"
                }
                else {
                    if ($contributePermissionState) {
                        Write-Host " Contribute: $($htPermissionsRef["$contributePermissionState"]) ($($contributePermissionState))"
                    }
                    else {
                        Write-Host ' Contribute: not set'
                    }
                    $testResult = 'FAILED'
                }
                Write-Host 'All permissions:'
                foreach ($permission in $permissions.dataProviders.'ms.vss-admin-web.security-view-permissions-data-provider'.subjectPermissions) {
                    if ($permission.effectivePermissionValue) {
                        Write-Host " $($permission.displayName): $($htPermissionsRef["$($permission.effectivePermissionValue)"]) ($($permission.effectivePermissionValue))"
                    }
                    else {
                        Write-Host " $($permission.displayName): not set"
                    }
                }

                if ($testResult -eq 'FAILED') {
                    Write-Host ''
                    Write-Host '- - - - - - - - - - - - - - - - -'
                    Write-Host 'Repository permission test failed'
                    Write-Host "You must grant the Account '$($buildAccount)' ($buildServiceAccountId) with 'Contribute' permissions on the Repository '$($repositoryName)' ($repositoryId)"
                    Write-Host 'Instructions: https://github.com/JulianHayward/AzADServicePrincipalInsights/blob/main/setup/azure-devops.md#step-4---allow-the-pipeline-to-save-the-output-in-the-repository'
                    Write-Error "Repository permission test failed: '$($buildAccount)' requires 'Contribute' permissions on the Repository '$($repositoryName)'"
                }
                else {
                    $timestamp = Get-Date -Format 'yyyy-MM-dd_HH:mm:ss'
                    $fileContent = @"
        Repository access check result:
        Date: $($timestamp)
        Build Account: '$($buildAccount)' ($buildServiceAccountId)
        Permission 'Contribute' for Repository '$($repositoryName) ($repositoryId)': $($contributePermissionState)
"@
                    #the agent temp folder, not the checkout (the push step would commit it)
                    $fileContent | Set-Content -Path "$($env:AGENT_TEMPDIRECTORY)/AzADServicePrincipalInsights_RepositoryPermissionCheck.log" -Encoding utf8 -Force
                }
                #endregion gettingPermissions
            }
        }
    }
    else {
        Write-Host "outputpath dir '$outputPathResolved' already exists"
    }
}
