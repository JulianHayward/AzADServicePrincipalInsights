# Contributing to AzADServicePrincipalInsights

Thank you for helping. Bug reports, fixes and new insights are welcome.

- **Bug or question:** open an [issue](https://github.com/JulianHayward/AzADServicePrincipalInsights/issues/new/choose) - the template asks for the version, where it runs and the error.
- **Security vulnerability:** not in an issue - see [SECURITY.md](SECURITY.md).
- **Larger change** (a new insight, a new output file, a change to the pipelines): open an issue first, so we can agree on the approach before you spend time on it.

**Never share data of your tenant** - not in issues, pull requests, tests or screenshots: no tenant, subscription or object IDs, no names of users, groups, applications or service principals, no report, CSV or JSON files. Use made-up values such as `11111111-1111-1111-1111-111111111111`.

## What is where

| Path | What it is |
|---|---|
| `pwsh/AzADServicePrincipalInsights.ps1` | The script users run. One file - there is no build step, edit it directly. |
| `pwsh/*.ps1` (other) | Helpers of the pipelines and workflows: arguments, prerequisites, Web App setup and publishing, Log Analytics ingestion. |
| `setup/` | The setup guides and the ARM templates. |
| `.azuredevops/`, `.github/` | The Azure DevOps pipeline, the GitHub workflows (users' runs and this repository's CI), the variables files. |
| `tests/` | Pester tests. They need no Azure access. |

## Set up your environment

- PowerShell 7.0.3 or later (the script's minimum). For development the current PowerShell 7 is fine.
- The modules `Az.Accounts` and `AzAPICall` (the version of the script's `-azAPICallVersion` default) - for a real run. The tests need `Az.Accounts` installed too (Pester can only mock a command that exists); CI uses the one of the runner image.
- A full clone (`git clone`, not a ZIP download or `--depth 1`): some tests compare the script with an earlier commit (`d332d98`).
- [Pester](https://pester.dev) **6.2.0** (as in CI) and [PSScriptAnalyzer](https://github.com/PowerShell/PSScriptAnalyzer):

  ```powershell
  Install-Module -Name Pester -RequiredVersion 6.2.0 -Scope CurrentUser -SkipPublisherCheck
  Install-Module -Name PSScriptAnalyzer -Scope CurrentUser
  ```

- VS Code with the [PowerShell extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode.PowerShell): the formatting rules are in `.vscode/settings.json` and applied on save, the analyzer shows the same findings as the CI, and VS Code suggests the extensions of `.vscode/extensions.json`. Search and the file watcher skip `zzzzz_Output/` (local run output with tenant data). Or use a GitHub Codespace: the repository's dev container has PowerShell, `Az.Accounts` and `AzAPICall`.

## Make a change

1. Fork the repository and create a branch from `main`.
1. Make the change.
1. Add or update a test in `tests/` - every fix gets a test that fails without it (see [Tests](#tests)).
1. Run the tests and the analyzer from the repository root:

   ```powershell
   $result = Invoke-Pester -Path ./tests -PassThru -Output Normal
   $result.FailedCount; $result.FailedContainersCount   # both must be 0

   Invoke-ScriptAnalyzer -Path ./pwsh -Recurse -Settings ./PSScriptAnalyzerSettings.psd1
   ```

   [PSScriptAnalyzerSettings.psd1](PSScriptAnalyzerSettings.psd1) is used by the CI workflow and by VS Code too: no new findings, please.
1. If you can: run the changed script against a tenant where you have '**Reader**' on a management group ([console guide](setup/console.md)) and check the report. Run it twice, before and after your change, into the same output folder: the [change history](README.md#change-history) of the second run shows what your change did to the output. [tests/Invoke-LocalTestRun.ps1](tests/Invoke-LocalTestRun.ps1) runs it into `zzzzz_Output` (gitignored), and [tests/Compare-AzADSPIRuns.ps1](tests/Compare-AzADSPIRuns.ps1) compares the two newest runs there file by file. **Don't commit the output** - it contains data of your tenant.
1. Add a line for your change at the top of [CHANGELOG.md](CHANGELOG.md), under the newest version. Leave `ProductVersion` and `version.txt` alone - the maintainer sets the version when releasing.
1. Open a pull request against `main`. Describe what changed and how you tested it (the Pester result; for a real run: the size of the tenant, e.g. "1,200 service principals", not its name).

The CI runs Pester, PSScriptAnalyzer, DevSkim, zizmor, OpenSSF Scorecard and a dev container build - only in this repository, not in your fork. That's why step 4 runs the tests and the analyzer locally.

## Rules

### Output

- Don't rename or remove CSV columns, JSON properties, file names or report sections: users' queries, dashboards and the Log Analytics table depend on them. Add new ones.
- A new top-level JSON property needs a column in the Log Analytics template [setup/log-analytics-dcr.json](setup/log-analytics-dcr.json) - a test checks it.
- Values from the tenant are data, never code: HTML-encode them in the report, and keep the CSV formula guard. Display names can contain `<`, `"`, `'`, `&`.
- New permissions must be optional: test them at the start, and without them the run continues and the report says the data was not collected.

### Code

- Hashtable lookups with a variable key: `$ht[$key]`, not `$ht.($key)`.
- Console output with `Write-Host`. No prompts (`Read-Host`) in code that also runs unattended - check the session first, as the module installation does.
- Keep the encoding and the line endings of the file you edit (UTF-8 without BOM).
- Pipelines and workflows: a value from a variables file or from the tenant never becomes part of a script text - pass it as a parameter or an environment variable. Pin actions to a commit SHA (zizmor checks it).

### Report assets

- The report loads its stylesheet, shell script and icons (`azadspi-portal_<version>.css/.js`) from azadvertizer.net. They are maintained there, not in this repository - for a change of the report's look or navigation, open an issue. A published file is never changed in place: a change gets a new file name, and the script's `$script:spiPortalAssetVersion` points to it. Otherwise reports that are already out there would change their look.
- Some tests check these files; they run only where the maintainer's local copy (`_local/html/`) exists and are skipped otherwise.

## Tests

- The tests load code regions or functions of the script and run them against stand-in data - no Azure access. [tests/TestHelpers.ps1](tests/TestHelpers.ps1) has the helpers; look at a test file of a similar area before you write a new one.
- Fixtures and test data use made-up IDs and names only.
- `<name>` in a test name is a Pester template placeholder: `It 'reads <value>'` is expanded. A test file that fails while Pester discovers it is not in the pass/fail counts - that's why step 4 checks `FailedContainersCount`.
- To prove that a test catches a bug, change the code, run the test, and restore the code with git (`git checkout -- <file>`) or from a copy. Don't use a variable that lives across `Invoke-Pester` (for example `$p` or `$d`) as the path to write to: Pester 6 overwrites such variables in the calling scope.

## Documentation

- The setup assistant ([setup/setup-assistant.html](setup/setup-assistant.html)) is a condensed version of the guides: when you change a guide, check whether the assistant says the same. It is also hosted on azadvertizer.net - the maintainer uploads it after a change.
- The [setup guides](setup/README.md) have a fixed shape: "What you will end up with", "Before you start", numbered steps that each end with a checkpoint, and a troubleshooting table with the messages of the scripts. A test checks the shape and every relative link and heading.
- A new parameter goes into the parameter table of the [README](README.md#parameters) and into both variables files (`.azuredevops/pipelines/AzADServicePrincipalInsights.variables.yml`, `.github/AzADServicePrincipalInsights.variables.yml`) with its type, default and an example.

## License

By contributing you agree that your contribution is licensed under the [MIT License](LICENSE) of this repository.
