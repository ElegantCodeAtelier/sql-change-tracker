$script:LocalFixtureRepoRoot = (Resolve-Path -Path (Join-Path $PSScriptRoot "..")).Path

function Get-LocalFixtureConfigPath {
  param([string]$ConfigPath = "")

  if ([string]::IsNullOrWhiteSpace($ConfigPath)) {
    return (Join-Path $script:LocalFixtureRepoRoot "local/fixtures.local.json")
  }

  if ([System.IO.Path]::IsPathRooted($ConfigPath)) {
    return [System.IO.Path]::GetFullPath($ConfigPath)
  }

  return [System.IO.Path]::GetFullPath((Join-Path $script:LocalFixtureRepoRoot $ConfigPath))
}

function Resolve-RelativeFixturePath {
  param(
    [string]$BasePath,
    [string]$PathValue
  )

  if ([System.IO.Path]::IsPathRooted($PathValue)) {
    return [System.IO.Path]::GetFullPath($PathValue)
  }

  return [System.IO.Path]::GetFullPath((Join-Path $BasePath $PathValue))
}

function Resolve-LocalFixtureSource {
  param(
    [string]$Source = "",
    [string]$ConfigPath = ""
  )

  $resolvedConfigPath = Get-LocalFixtureConfigPath -ConfigPath $ConfigPath
  if (-not (Test-Path -LiteralPath $resolvedConfigPath)) {
    throw "Local fixture config not found: $resolvedConfigPath"
  }

  $json = Get-Content -Raw -LiteralPath $resolvedConfigPath
  if ([string]::IsNullOrWhiteSpace($json)) {
    throw "Local fixture config is empty: $resolvedConfigPath"
  }

  $data = $json | ConvertFrom-Json

  if ($null -ne $data.compatExportPath) {
    throw "Legacy key 'compatExportPath' is not supported. Migrate local/fixtures.local.json to the source-map schema."
  }

  if ([string]::IsNullOrWhiteSpace($data.defaultSource)) {
    throw "Missing required key 'defaultSource' in $resolvedConfigPath"
  }

  if ($null -eq $data.sources) {
    throw "Missing required object 'sources' in $resolvedConfigPath"
  }

  $selectedSource = if ([string]::IsNullOrWhiteSpace($Source)) { [string]$data.defaultSource } else { $Source }
  $sourceProperty = $data.sources.PSObject.Properties[$selectedSource]
  if ($null -eq $sourceProperty) {
    $available = $data.sources.PSObject.Properties.Name
    if ($available.Count -eq 0) {
      throw "No sources are defined in $resolvedConfigPath"
    }

    throw "Unknown source '$selectedSource'. Available sources: $($available -join ', ')"
  }

  $sourceConfig = $sourceProperty.Value
  if ([string]::IsNullOrWhiteSpace($sourceConfig.referencePath)) {
    throw "Missing required key 'sources.$selectedSource.referencePath' in $resolvedConfigPath"
  }

  if ([string]::IsNullOrWhiteSpace($sourceConfig.objectListPath)) {
    throw "Missing required key 'sources.$selectedSource.objectListPath' in $resolvedConfigPath"
  }

  $repoRoot = (Resolve-Path -Path (Join-Path (Split-Path -Parent $resolvedConfigPath) "..")).Path
  return [pscustomobject]@{
    Name = $selectedSource
    ReferencePath = Resolve-RelativeFixturePath -BasePath $repoRoot -PathValue ([string]$sourceConfig.referencePath)
    ObjectListPath = Resolve-RelativeFixturePath -BasePath $repoRoot -PathValue ([string]$sourceConfig.objectListPath)
    ConfigPath = $resolvedConfigPath
  }
}
