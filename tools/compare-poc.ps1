param(
  [string]$Ref = "",
  [Parameter(Mandatory = $true)][string]$Out,
  [string]$ObjectList = "",
  [string]$Source = ""
)

$ErrorActionPreference = "Stop"

$helperPath = Join-Path $PSScriptRoot "local-fixture-sources.ps1"
. $helperPath

$hasExplicitRef = -not [string]::IsNullOrWhiteSpace($Ref)
$hasExplicitObjectList = -not [string]::IsNullOrWhiteSpace($ObjectList)
if (-not $hasExplicitRef -or -not $hasExplicitObjectList) {
  $resolvedSource = Resolve-LocalFixtureSource -Source $Source
  if (-not $hasExplicitRef) {
    $Ref = $resolvedSource.ReferencePath
  }
  if (-not $hasExplicitObjectList) {
    $ObjectList = $resolvedSource.ObjectListPath
  }
} elseif (-not [string]::IsNullOrWhiteSpace($Source)) {
  Write-Warning "-Source is ignored because -Ref and -ObjectList were provided."
}

if ([string]::IsNullOrWhiteSpace($Ref) -or [string]::IsNullOrWhiteSpace($ObjectList)) {
  throw "Ref and ObjectList are required."
}

function Normalize-SqlText {
  param([string]$Content)
  if ($null -eq $Content) { return "" }
  $normalized = [regex]::Replace(
    $Content,
    "\bCREATE\s+(OR\s+ALTER\s+)?(PROCEDURE|FUNCTION)\b",
    "CREATE $2",
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
  )
  return $normalized.TrimEnd("`r", "`n")
}

function Get-NormalizedHash {
  param([string]$Path)
  $text = [System.IO.File]::ReadAllText($Path, [System.Text.Encoding]::UTF8)
  $normalized = Normalize-SqlText -Content $text
  $bytes = [System.Text.Encoding]::UTF8.GetBytes($normalized)
  $sha = [System.Security.Cryptography.SHA256]::Create()
  try {
    $hash = $sha.ComputeHash($bytes)
    return ([System.BitConverter]::ToString($hash) -replace "-", "")
  } finally {
    $sha.Dispose()
  }
}

function Parse-ObjectList {
  param([string]$Path)
  if (-not (Test-Path $Path)) { throw "Object list not found: $Path" }
  $items = @()
  foreach ($line in [System.IO.File]::ReadAllLines($Path, [System.Text.Encoding]::UTF8)) {
    $trim = $line.Trim()
    if ($trim -eq "" -or $trim.StartsWith("#")) { continue }
    if ($trim -match "\.sql$") {
      $entry = $trim.TrimStart("-").Trim()
      if ($entry -match ".+/.*\.sql$") { $items += $entry }
    }
  }
  return $items
}

$items = Parse-ObjectList -Path $ObjectList
if ($items.Count -eq 0) { throw "No objects found in $ObjectList" }

$results = @()
foreach ($rel in $items) {
  $left = Join-Path $Ref $rel
  $right = Join-Path $Out $rel
  if (-not (Test-Path $left)) {
    $results += [pscustomobject]@{ File = $rel; Status = "missing_ref" }
    continue
  }
  if (-not (Test-Path $right)) {
    $results += [pscustomobject]@{ File = $rel; Status = "missing_out" }
    continue
  }
  $hashLeft = Get-NormalizedHash -Path $left
  $hashRight = Get-NormalizedHash -Path $right
  $status = if ($hashLeft -eq $hashRight) { "match" } else { "diff" }
  $results += [pscustomobject]@{ File = $rel; Status = $status }
}

$results | Group-Object Status | Sort-Object Name | ForEach-Object { "{0}: {1}" -f $_.Name, $_.Count }
if ($results | Where-Object { $_.Status -eq "diff" }) {
  "`nDiff files:"
  $results | Where-Object { $_.Status -eq "diff" } | ForEach-Object { $_.File }
}
