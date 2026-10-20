<#
  Imports WWI-01-bronze-to-silver.ipynb into LAB-Platform-Gold-DEV as notebook
  "nb_wwi_bronze_to_silver", with lh_bronze as the default lakehouse and lh_silver attached.
  Runs as the fabric-lab-automation service principal, so there is no sign-in pop-up.

  Usage (from the folder this script is in):
    .\Import-WwiNotebook.ps1 -TenantId <sandbox tenant id> -AppId <Application (client) ID> -Secret <client secret Value>
  Re-running updates the existing notebook instead of creating a duplicate.
#>
param(
  [Parameter(Mandatory)] [string] $AppId,
  [Parameter(Mandatory)] [string] $Secret,
  [Parameter(Mandatory)] [string] $TenantId,
  [string] $Workspace     = 'LAB-Platform-Gold-DEV',
  [string] $NotebookName  = 'nb_wwi_bronze_to_silver',
  [string] $IpynbPath     = (Join-Path $PSScriptRoot 'WWI-01-bronze-to-silver.ipynb')
)
$ErrorActionPreference = 'Stop'
$Base = 'https://api.fabric.microsoft.com/v1'

# ---- token (client credentials)
$tok = (Invoke-RestMethod -Method Post -Uri "https://login.microsoftonline.com/$TenantId/oauth2/v2.0/token" -Body @{
  grant_type = 'client_credentials'; client_id = $AppId; client_secret = $Secret
  scope = 'https://api.fabric.microsoft.com/.default' }).access_token
$H = @{ Authorization = "Bearer $tok"; 'Content-Type' = 'application/json' }
function F($m, $p, $b) {
  $a = @{ Method = $m; Uri = "$Base/$p"; Headers = $H }
  if ($b) { $a.Body = ($b | ConvertTo-Json -Depth 20 -Compress) }
  Invoke-RestMethod @a
}

# ---- look up workspace and lakehouses
$ws = (F GET 'workspaces').value | Where-Object displayName -eq $Workspace
if (-not $ws) { throw "Workspace '$Workspace' not visible to the app. Add fabric-lab-automation as Admin in Manage access." }
$items  = (F GET "workspaces/$($ws.id)/items").value
$bronze = $items | Where-Object { $_.type -eq 'Lakehouse' -and $_.displayName -eq 'lh_bronze' }
$silver = $items | Where-Object { $_.type -eq 'Lakehouse' -and $_.displayName -eq 'lh_silver' }
if (-not $bronze -or -not $silver) { throw "lh_bronze and lh_silver must both exist in $Workspace." }
Write-Host "Workspace  $Workspace ($($ws.id))"
Write-Host "lh_bronze  $($bronze.id)"
Write-Host "lh_silver  $($silver.id)"

# ---- attach lakehouses in the notebook metadata
$nb = Get-Content $IpynbPath -Raw | ConvertFrom-Json
$deps = [ordered]@{ lakehouse = [ordered]@{
  default_lakehouse              = $bronze.id
  default_lakehouse_name         = 'lh_bronze'
  default_lakehouse_workspace_id = $ws.id
  known_lakehouses               = @(@{ id = $bronze.id }, @{ id = $silver.id })
} }
$nb.metadata | Add-Member -NotePropertyName dependencies -NotePropertyValue $deps -Force
$json    = $nb | ConvertTo-Json -Depth 50
$payload = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($json))
$definition = @{ format = 'ipynb'; parts = @(@{ path = 'notebook-content.ipynb'; payload = $payload; payloadType = 'InlineBase64' }) }

# ---- create or update
$existing = $items | Where-Object { $_.type -eq 'Notebook' -and $_.displayName -eq $NotebookName }
if ($existing) {
  F POST "workspaces/$($ws.id)/notebooks/$($existing.id)/updateDefinition" @{ definition = $definition } | Out-Null
  Write-Host "updated  $NotebookName" -ForegroundColor Green
} else {
  F POST "workspaces/$($ws.id)/notebooks" @{ displayName = $NotebookName; definition = $definition } | Out-Null
  Write-Host "created  $NotebookName (may take ~30s to appear)" -ForegroundColor Green
}
