---
name: Bug report
about: Something does not work as expected
title: ''
labels: ''
assignees: ''

---

<!--
Please do not paste data of your tenant: no tenant, subscription or object IDs, no names of users, groups, applications or
service principals, no report, CSV or JSON files. Replace them with made-up values (e.g. 11111111-1111-1111-1111-111111111111, 'app1').
A security vulnerability does not belong here: see SECURITY.md (Security > Report a vulnerability).
-->

**AzADServicePrincipalInsights version**
The `ProductVersion` from the start of the script output (`Start AzADServicePrincipalInsights ... (#v2_...)`) or from the report's run parameters. Please check first that the issue still happens with the [latest version](https://github.com/JulianHayward/AzADServicePrincipalInsights/blob/main/version.txt).

**Where does it run?**
e.g. console (Windows/Linux/macOS), GitHub Codespace, Azure DevOps pipeline, GitHub Actions workflow (OIDC or client secret), Azure Automation. PowerShell version (`$PSVersionTable.PSVersion`) and AzAPICall version (in the script output) if you know them.

**Which part?**
e.g. the run itself, the HTML report (which page / section), a CSV or JSON file, the change history, Web App publishing (`Publish-AzADSPIWebApp.ps1`, `New-AzADSPIWebApp.ps1`), Log Analytics ingestion (`Send-AzADSPILogAnalyticsData.ps1`).

**Describe the bug**
What happened, and what did you expect? Paste the error from the script output as text: the full message, with the lines before it (pipeline/workflow: from the log of the failed step; with `-DoTranscript` the output folder has the whole output in `AzADServicePrincipalInsights_..._Log.txt`).

**Parameters**
The parameters you set (from the command line, or the values you changed in the variables file of the pipeline/workflow). Leave out IDs.

**Steps to reproduce**
If it happens only with certain objects: describe the object (e.g. 'a managed identity with a federated credential', 'an app registration with 20 owners'), not its name.

**Screenshots**
Only as an addition - please paste errors as text.
