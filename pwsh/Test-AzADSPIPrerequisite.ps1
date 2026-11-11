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

#the output path as the main script gets it (-OutputPath): relative to the working directory of the pipeline/workflow
#(Azure DevOps: SYSTEM_DEFAULTWORKINGDIRECTORY, GitHub: GITHUB_WORKSPACE), a rooted path as is
function resolveOutputPath([string]$Path, [string]$BasePath) {
    if ([System.IO.Path]::IsPathRooted($Path)) { return $Path }
    if (-not $BasePath) { $BasePath = (Get-Location).Path }
    return (Join-Path -Path $BasePath -ChildPath $Path)
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
        throw "Repository '$($repoUri)' is accessible from the public - assuming and insisting that you do not want to publish your tenant insights to the public"
    }
    elseif ($visibility) {
        Write-Host "Test returned visibility '$visibility' - '$($repoUri)' is not accessible from the public - proceed"
    }
    else {
        throw "The visibility of repository '$($repoUri)' could not be determined (statusCode: '$statusCode') - stopping, the tenant insights must not be pushed to a public repository; run it again"
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
}

if ($codeRunPlatform -eq 'AzureDevOps') {
    #was: $env:WIKIDIR (no longer set since the pipeline variables file; -OutputPath was ignored and the folder never created)
    $outputPathResolved = resolveOutputPath -Path $OutputPath -BasePath $env:SYSTEM_DEFAULTWORKINGDIRECTORY
    Write-Host "outputpath is '$OutputPath' ($outputPathResolved)"
    if (-not (Test-Path -LiteralPath $outputPathResolved)) {
        #Assuming this is the initial run

        #Create the outputpath dir
        Write-Host "Creating directory '$outputPathResolved'"
        New-Item -ItemType Directory -Force -Path $outputPathResolved

        #Repository permission check
        Write-Host 'Repository access check'

        #createHeader
        $pat = $env:SYSTEM_ACCESSTOKEN #$(System.AccessToken)
        $token = [System.Convert]::ToBase64String([System.Text.Encoding]::ASCII.GetBytes(":$($pat)"))
        $header = @{authorization = "Basic $token" }

        $collectionUri = $env:SYSTEM_COLLECTIONURI.TrimEnd('/') #$(System.CollectionUri)
        $project = $env:SYSTEM_TEAMPROJECT #$(System.TeamProject)

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
                    $fileContent | Set-Content -Path "$($env:SYSTEM_DEFAULTWORKINGDIRECTORY)/AzADServicePrincipalInsights_RepositoryPermissionCheck.log" -Encoding utf8 -Force
                }
                #endregion gettingPermissions
            }
        }
    }
    else {
        Write-Host "outputpath dir '$outputPathResolved' already exists"
    }
}
