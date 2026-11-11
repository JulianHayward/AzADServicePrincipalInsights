# Security policy

## Reporting a vulnerability

Please do **not** report security vulnerabilities in a public issue.

Report them privately through GitHub: [Report a vulnerability](https://github.com/JulianHayward/AzADServicePrincipalInsights/security/advisories/new) (tab **Security** > **Report a vulnerability**).

Please include:
- what is affected (the script, the report, a pipeline/workflow, a helper script) and the version (`ProductVersion` in the script or in the report's run parameters dialog)
- how to reproduce it
- what an attacker could achieve

Do not include data of your tenant (names, IDs, report files). Use made-up values.

AzADServicePrincipalInsights is a personal, community-driven project. Reports are handled on a best-effort basis: you get an answer, a fix is released as a new version, and the advisory is published once the fix is available.

## How fixes are published

- A confirmed vulnerability gets a [GitHub Security Advisory](https://github.com/JulianHayward/AzADServicePrincipalInsights/security/advisories) in this repository, published together with the fixed version - with a CVE when the severity warrants it, and credit to the reporter.
- Every version is a [GitHub Release](https://github.com/JulianHayward/AzADServicePrincipalInsights/releases); the release of a security fix and its [changelog](CHANGELOG.md) entry (starting with **Security**) link the advisory.
- To be notified: **Watch** > **Custom** > **Releases**. The report and the console also show when a newer version exists.
- AzADServicePrincipalInsights is not distributed through a package manager, so Dependabot cannot alert you about it - watch the repository.

## Supported versions

Only the latest version (the `main` branch) receives fixes.

## Scope

In scope:
- **The PowerShell scripts in `pwsh/`.** Example: a tenant value that ends up as code or markup in the report, in the CSV files or in a pipeline step.
- **The report shell and stylesheet** (`azadspi-portal_<version>.css/.js`, loaded by the report from azadvertizer.net).
- **The Azure DevOps pipeline and the GitHub Actions workflows.** Examples: secrets or tokens exposed, output published to the public.

Not in scope, please report to the owner:
- **Microsoft Graph, Azure Resource Manager, Microsoft Entra ID:** [Microsoft Security Response Center](https://msrc.microsoft.com/report).
- **The AzAPICall module:** [AzAPICall](https://github.com/JulianHayward/AzAPICall/security).
- **Third-party libraries in the report** (e.g. AG Grid): their maintainers.

## Handling of the output

The output (HTML, CSV, JSON, change history) describes the permissions, owners and credentials of the service principals in your tenant. It does not contain secrets. Keep it as restricted as that information: a private repository, a Web App that requires a login, restricted Log Analytics access. See [README: Security](README.md#security).
