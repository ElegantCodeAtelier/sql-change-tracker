param(
  [string]$Ref = "",
  [string]$Out = "",
  [string]$SampleList = "",
  [string]$ReportPath = "",
  [string]$Source = "",
  [switch]$LocalFixtures = $false
)

$ErrorActionPreference = "Stop"

$helperPath = Join-Path $PSScriptRoot "local-fixture-sources.ps1"
. $helperPath

function Get-RelativePaths {
  param([string]$Root)
  $root = (Resolve-Path -Path $Root).Path
  $items = Get-ChildItem -Recurse -File -Path $root
  return $items | ForEach-Object { $_.FullName.Substring($root.Length + 1) }
}

function Get-FilteredPaths {
  param([string]$Root, [string]$SampleList)
  if ([string]::IsNullOrWhiteSpace($SampleList)) {
    return Get-RelativePaths -Root $Root
  }
  if (-not (Test-Path $SampleList)) { throw "Sample list not found: $SampleList" }
  $items = @()
  foreach ($line in Get-Content -Path $SampleList) {
    $trim = $line.Trim()
    if ($trim -eq "" -or $trim.StartsWith("#")) { continue }
    if ($trim -match "\.sql$") {
      $items += $trim.TrimStart("-").Trim()
    }
  }
  return $items
}

function Compare-Bytes {
  param([string]$Left, [string]$Right)
  $leftBytes = [System.IO.File]::ReadAllBytes($Left)
  $rightBytes = [System.IO.File]::ReadAllBytes($Right)
  if ($leftBytes.Length -ne $rightBytes.Length) { return $false }
  for ($i = 0; $i -lt $leftBytes.Length; $i++) {
    if ($leftBytes[$i] -ne $rightBytes[$i]) { return $false }
  }
  return $true
}

if ([string]::IsNullOrWhiteSpace($Ref)) {
  $resolvedSource = Resolve-LocalFixtureSource -Source $Source
  $Ref = $resolvedSource.ReferencePath
} elseif (-not [string]::IsNullOrWhiteSpace($Source)) {
  Write-Warning "-Source is ignored because -Ref was provided."
}

if ([string]::IsNullOrWhiteSpace($Ref)) {
  throw "Ref is required."
}

if ([string]::IsNullOrWhiteSpace($Out)) {
  throw "Out is required."
}

$refRoot = (Resolve-Path -Path $Ref).Path
$outRoot = (Resolve-Path -Path $Out).Path

$refFiles = Get-FilteredPaths -Root $refRoot -SampleList $SampleList
$outFiles = Get-RelativePaths -Root $outRoot

$results = New-Object System.Collections.Generic.List[object]

foreach ($rel in $refFiles) {
  $left = Join-Path $refRoot $rel
  $right = Join-Path $outRoot $rel
  if (-not (Test-Path $left)) {
    $results.Add([pscustomobject]@{ File = $rel; Status = "missing_ref" })
    continue
  }
  if (-not (Test-Path $right)) {
    $results.Add([pscustomobject]@{ File = $rel; Status = "missing_out" })
    continue
  }
  $match = Compare-Bytes -Left $left -Right $right
  $results.Add([pscustomobject]@{ File = $rel; Status = $(if ($match) { "match" } else { "diff" }) })
}

$extra = $outFiles | Where-Object { $refFiles -notcontains $_ }
foreach ($rel in $extra) {
  $results.Add([pscustomobject]@{ File = $rel; Status = "extra_out" })
}

$summary = $results | Group-Object Status | Sort-Object Name | ForEach-Object { "{0}: {1}" -f $_.Name, $_.Count }
$summaryText = $summary -join "`n"

Write-Output $summaryText
if ($results | Where-Object { $_.Status -eq "diff" }) {
  Write-Output "`nDiff files:"
  $results | Where-Object { $_.Status -eq "diff" } | ForEach-Object { $_.File }
}

if (-not [string]::IsNullOrWhiteSpace($ReportPath)) {
  $dir = Split-Path -Parent $ReportPath
  if (-not (Test-Path $dir)) {
    New-Item -ItemType Directory -Path $dir -Force | Out-Null
  }
  $report = [pscustomobject]@{
    refPath = $refRoot
    outPath = $outRoot
    summary = $summary
    results = $results
  } | ConvertTo-Json -Depth 4
  Set-Content -Path $ReportPath -Value $report -Encoding UTF8
}
