param(
  [string]$Server = "localhost",
  [string]$Database = "SampleDatabase",
  [string]$ObjectList = "",
  [string]$Output = "local/fixtures/outputs/poc-out",
  [string]$OverridesPath = "local/fixtures/outputs/poc-overrides.json",
  [string]$CompatPath = "",
  [string]$Source = "",
  [switch]$Encrypt = $false,
  [switch]$TrustServerCertificate = $true,
  [switch]$ListDebug = $false
)

$ErrorActionPreference = "Stop"

$helperPath = Join-Path (Split-Path -Parent $PSScriptRoot) "local-fixture-sources.ps1"
. $helperPath

$hasExplicitObjectList = -not [string]::IsNullOrWhiteSpace($ObjectList)
$hasExplicitCompatPath = -not [string]::IsNullOrWhiteSpace($CompatPath)
if (-not $hasExplicitObjectList -or -not $hasExplicitCompatPath) {
  $resolvedSource = Resolve-LocalFixtureSource -Source $Source
  if (-not $hasExplicitObjectList) {
    $ObjectList = $resolvedSource.ObjectListPath
  }
  if (-not $hasExplicitCompatPath) {
    $CompatPath = $resolvedSource.ReferencePath
  }
} elseif (-not [string]::IsNullOrWhiteSpace($Source)) {
  Write-Warning "-Source is ignored because -ObjectList and -CompatPath were provided."
}

function New-Connection {
  param([string]$server, [string]$database)
  $encryptValue = if ($Encrypt) { "True" } else { "False" }
  $trustValue = if ($TrustServerCertificate) { "True" } else { "False" }
  $cs = "Server=$server;Database=$database;Integrated Security=True;Encrypt=$encryptValue;TrustServerCertificate=$trustValue"
  return New-Object System.Data.SqlClient.SqlConnection $cs
}

function Invoke-Query {
  param(
    [string]$Sql,
    [hashtable]$Parameters
  )
  $cn = New-Connection -server $Server -database $Database
  $cn.Open()
  try {
    $cmd = $cn.CreateCommand()
    $cmd.CommandText = $Sql
    if ($Parameters) {
      foreach ($key in $Parameters.Keys) {
        $param = $cmd.Parameters.Add("@$key", [System.Data.SqlDbType]::NVarChar, 256)
        $param.Value = $Parameters[$key]
      }
    }
    $reader = $cmd.ExecuteReader()
    $rows = New-Object System.Collections.Generic.List[object]
    while ($reader.Read()) {
      $row = @{}
      for ($i = 0; $i -lt $reader.FieldCount; $i++) {
        $row[$reader.GetName($i)] = $reader.GetValue($i)
      }
      $rows.Add($row)
    }
    $reader.Close()
    return $rows
  } finally {
    $cn.Close()
  }
}

function Write-Utf8NoBom {
  param(
    [string]$Path,
    [string]$Content,
    [bool]$EnsureTrailingNewline = $true
  )
  $dir = Split-Path -Parent $Path
  if (-not (Test-Path $dir)) {
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
  }
  $utf8NoBom = New-Object System.Text.UTF8Encoding($false)
  $normalized = [regex]::Replace($Content, "\r?\n", "`r`n")
  if ($EnsureTrailingNewline) {
    if (-not $normalized.EndsWith("`r`n")) {
      $normalized += "`r`n"
    }
  }
  [System.IO.File]::WriteAllText($Path, $normalized, $utf8NoBom)
}

function Parse-ObjectList {
  param([string]$Path)
  if (-not (Test-Path $Path)) {
    throw "Object list not found: $Path"
  }
  $items = @()
  $lines = [System.IO.File]::ReadAllLines($Path, [System.Text.Encoding]::UTF8)
  foreach ($line in $lines) {
    $trim = $line.Trim()
    if ($ListDebug -and ($trim -match "sql")) {
      $match = $trim -match "\.sql$"
      Write-Host ("DBG line: [{0}] match={1}" -f $trim, $match)
    }
    if ($trim -eq "" -or $trim.StartsWith("#")) { continue }
    if ($trim -match "\.sql$") {
      $entry = $trim.TrimStart("-").Trim()
      if ($ListDebug) {
        Write-Host ("DBG entry: [{0}]" -f $entry)
      }
      $items += $entry
    }
  }
  return $items
}

function Get-Overrides {
  param([string]$Path)
  if (-not (Test-Path $Path)) {
    return $null
  }
  $raw = [System.IO.File]::ReadAllText($Path, [System.Text.Encoding]::UTF8)
  if ([string]::IsNullOrWhiteSpace($raw)) {
    return $null
  }
  return ($raw | ConvertFrom-Json)
}

function Get-OverrideForPath {
  param(
    [object]$Overrides,
    [string]$Path
  )
  if ($null -eq $Overrides -or $null -eq $Overrides.Objects) {
    return $null
  }
  $prop = $Overrides.Objects.PSObject.Properties[$Path]
  if ($null -eq $prop) {
    return $null
  }
  return $prop.Value
}

function Get-RegexOptions {
  param([string]$Options)
  if ([string]::IsNullOrWhiteSpace($Options)) {
    return [System.Text.RegularExpressions.RegexOptions]::None
  }
  $value = [System.Text.RegularExpressions.RegexOptions]::None
  foreach ($part in $Options.Split(",")) {
    $name = $part.Trim()
    if ($name -eq "") { continue }
    $value = $value -bor [System.Enum]::Parse([System.Text.RegularExpressions.RegexOptions], $name, $true)
  }
  return $value
}

function Get-ModuleFormattingFromCompat {
  param(
    [string]$Path,
    [string]$CreatePattern = "(?i)^\s*CREATE\s+"
  )
  if ([string]::IsNullOrWhiteSpace($Path)) { return $null }
  if (-not (Test-Path $Path)) { return $null }
  $lines = [System.IO.File]::ReadAllLines($Path, [System.Text.Encoding]::UTF8)
  $ansiIdx = -1
  for ($i = 0; $i -lt $lines.Count; $i++) {
    if ($lines[$i].Trim() -eq "SET ANSI_NULLS ON") { $ansiIdx = $i; break }
  }
  if ($ansiIdx -lt 0) { return $null }
  $goIdx = -1
  for ($i = $ansiIdx + 1; $i -lt $lines.Count; $i++) {
    if ($lines[$i].Trim() -eq "GO") { $goIdx = $i; break }
  }
  if ($goIdx -lt 0) { return $null }
  $leading = 0
  for ($i = $goIdx + 1; $i -lt $lines.Count; $i++) {
    if ($lines[$i].Trim().Length -eq 0) { $leading++ } else { break }
  }
  $createIdx = -1
  for ($i = 0; $i -lt $lines.Count; $i++) {
    if ($lines[$i] -match $CreatePattern) { $createIdx = $i; break }
  }
  $goAfterDefIdx = -1
  if ($createIdx -ge 0) {
    for ($i = $createIdx + 1; $i -lt $lines.Count; $i++) {
      if ($lines[$i].Trim() -eq "GO") { $goAfterDefIdx = $i; break }
    }
  }
  $beforeGo = 0
  if ($goAfterDefIdx -gt 0) {
    for ($i = $goAfterDefIdx - 1; $i -ge 0; $i--) {
      if ($lines[$i].Trim().Length -eq 0) { $beforeGo++ } else { break }
    }
  }
  $indentPrefix = ""
  for ($i = 0; $i -lt $lines.Count; $i++) {
    if ($lines[$i] -match $CreatePattern) {
      $indentPrefix = [regex]::Match($lines[$i], "^\s*").Value
      break
    }
  }
  $extIdx = -1
  for ($i = 0; $i -lt $lines.Count; $i++) {
    if ($lines[$i] -match "sp_addextendedproperty") { $extIdx = $i; break }
  }
  $beforeExt = 0
  if ($extIdx -gt 0) {
    for ($i = $extIdx - 1; $i -ge 0; $i--) {
      if ($lines[$i].Trim().Length -eq 0) { $beforeExt++ } else { break }
    }
  }
  return @{
    LeadingBlankLines = $leading
    BlankLineBeforeGo = $beforeGo
    BlankLineBeforeExtProps = $beforeExt
    DefinitionIndentPrefix = $indentPrefix
  }
}

function Get-CompatTableCheckLine {
  param(
    [string]$Path,
    [string]$ConstraintName
  )
  if ([string]::IsNullOrWhiteSpace($Path)) { return $null }
  if (-not (Test-Path $Path)) { return $null }
  if ([string]::IsNullOrWhiteSpace($ConstraintName)) { return $null }
  $escaped = [regex]::Escape($ConstraintName)
  $pattern = "^\s*ALTER TABLE .* ADD CONSTRAINT \\[$escaped\\] CHECK"
  foreach ($line in [System.IO.File]::ReadAllLines($Path, [System.Text.Encoding]::UTF8)) {
    if ($line -match $pattern) { return $line }
  }
  return $null
}

function Get-CompatDefinitionLineMap {
  param([string]$Path)
  if ([string]::IsNullOrWhiteSpace($Path)) { return $null }
  if (-not (Test-Path $Path)) { return $null }
  $lines = [System.IO.File]::ReadAllLines($Path, [System.Text.Encoding]::UTF8)
  $start = -1
  for ($i = 0; $i -lt $lines.Count; $i++) {
    if ($lines[$i] -match "(?i)^\s*CREATE\s+") { $start = $i; break }
  }
  if ($start -lt 0) { return $null }
  $end = -1
  for ($i = $start + 1; $i -lt $lines.Count; $i++) {
    if ($lines[$i].Trim() -eq "GO") { $end = $i; break }
  }
  if ($end -lt 0) { return $null }
  $counts = @{}
  $lineMap = @{}
  for ($i = $start; $i -lt $end; $i++) {
    $trim = $lines[$i].Trim()
    if ($trim.Length -eq 0) { continue }
    if ($counts.ContainsKey($trim)) { $counts[$trim]++ } else { $counts[$trim] = 1 }
    if (-not $lineMap.ContainsKey($trim)) {
      $lineMap[$trim] = $lines[$i]
    }
  }
  $uniqueMap = @{}
  foreach ($key in $lineMap.Keys) {
    if ($counts[$key] -eq 1) {
      $uniqueMap[$key] = $lineMap[$key]
    }
  }
  return $uniqueMap
}

function Trim-OuterBlankLines {
  param([string]$Text)
  if ($null -eq $Text) { return "" }

  $lines = [regex]::Split($Text, "\r?\n")
  $start = 0
  while ($start -lt $lines.Length -and $lines[$start].Trim().Length -eq 0) {
    $start++
  }

  $end = $lines.Length - 1
  while ($end -ge $start -and $lines[$end].Trim().Length -eq 0) {
    $end--
  }

  if ($start -gt $end) {
    return ""
  }

  return ($lines[$start..$end] -join "`n")
}

function Get-CompatTableColumnTypeMap {
  param([string]$Path)

  $map = @{}
  if ([string]::IsNullOrWhiteSpace($Path)) { return $map }
  if (-not (Test-Path $Path)) { return $map }

  $lines = [System.IO.File]::ReadAllLines($Path, [System.Text.Encoding]::UTF8)
  $createIdx = -1
  for ($i = 0; $i -lt $lines.Count; $i++) {
    if ($lines[$i] -match "(?i)^\s*CREATE\s+TABLE\b") {
      $createIdx = $i
      break
    }
  }

  if ($createIdx -lt 0) { return $map }

  $inColumns = $false
  for ($i = $createIdx + 1; $i -lt $lines.Count; $i++) {
    $trim = $lines[$i].Trim()
    if (-not $inColumns) {
      if ($trim -eq "(") {
        $inColumns = $true
      }
      continue
    }

    if ($trim.StartsWith(")")) {
      break
    }

    if ($lines[$i] -match "^\s*\[(?<name>[^\]]+)\]\s+(?<type>(?:\[[^\]]+\](?:\.\[[^\]]+\])?|\w+)(?:\s*\([^)]*\))?)") {
      $map[$matches.name] = $matches.type.Trim()
    }
  }

  return $map
}

function Split-ObjectPath {
  param([string]$Path)
  $parts = $Path.Split("/", 2)
  if ($parts.Length -ne 2) {
    throw "Invalid object path: $Path"
  }
  $folder = $parts[0]
  $file = $parts[1]
  if (-not $file.EndsWith(".sql")) {
    throw "Invalid object file: $Path"
  }
  $name = $file.Substring(0, $file.Length - 4)
  $dot = $name.IndexOf(".")
  if ($dot -lt 0) {
    throw "Expected Schema.Object format: $Path"
  }
  $schema = $name.Substring(0, $dot)
  $object = $name.Substring($dot + 1)
  return @{ Folder = $folder; Schema = $schema; Name = $object; File = $file }
}

function Format-TypeName {
  param(
    [string]$TypeName,
    [string]$TypeSchema,
    [bool]$IsUserDefined,
    [int]$MaxLength,
    [int]$Precision,
    [int]$Scale
  )
  $base = if ($IsUserDefined) { "[{0}].[{1}]" -f $TypeSchema, $TypeName } else { "[{0}]" -f $TypeName }
  switch ($TypeName.ToLowerInvariant()) {
    "varchar" { return "{0} ({1})" -f $base, ($(if ($MaxLength -lt 0) { "MAX" } else { $MaxLength })) }
    "char" { return "{0} ({1})" -f $base, $MaxLength }
    "varbinary" { return "{0} ({1})" -f $base, ($(if ($MaxLength -lt 0) { "MAX" } else { $MaxLength })) }
    "binary" { return "{0} ({1})" -f $base, $MaxLength }
    "nvarchar" { return "{0} ({1})" -f $base, ($(if ($MaxLength -lt 0) { "MAX" } else { [int]($MaxLength / 2) })) }
    "nchar" { return "{0} ({1})" -f $base, [int]($MaxLength / 2) }
    "decimal" { return "{0} ({1}, {2})" -f $base, $Precision, $Scale }
    "numeric" { return "{0} ({1}, {2})" -f $base, $Precision, $Scale }
    "datetime2" { return $(if ($Scale -eq 7) { $base } else { "{0} ({1})" -f $base, $Scale }) }
    "datetimeoffset" { return $(if ($Scale -eq 7) { $base } else { "{0} ({1})" -f $base, $Scale }) }
    "time" { return $(if ($Scale -eq 7) { $base } else { "{0} ({1})" -f $base, $Scale }) }
    default { return $base }
  }
}

function Get-CanonicalTypeToken {
  param([string]$Token)
  if ([string]::IsNullOrWhiteSpace($Token)) { return "" }

  $normalized = $Token.Trim()
  $normalized = [regex]::Replace(
    $normalized,
    "\[sys\]\.\[(?<name>[^\]]+)\]",
    '[$1]',
    [System.Text.RegularExpressions.RegexOptions]::IgnoreCase
  )
  $normalized = [regex]::Replace($normalized, "\s+", " ")
  $normalized = [regex]::Replace($normalized, "\s*\(\s*", " (")
  $normalized = [regex]::Replace($normalized, "\s*,\s*", ", ")
  $normalized = [regex]::Replace($normalized, "\s*\)", ")")
  return $normalized.ToLowerInvariant()
}

function Get-CompatibleTypeToken {
  param(
    [string]$GeneratedType,
    [hashtable]$CompatTypeMap,
    [string]$ColumnName
  )

  if ($null -eq $CompatTypeMap -or [string]::IsNullOrWhiteSpace($ColumnName)) {
    return $GeneratedType
  }

  if (-not $CompatTypeMap.ContainsKey($ColumnName)) {
    return $GeneratedType
  }

  $compatType = $CompatTypeMap[$ColumnName] -as [string]
  if ([string]::IsNullOrWhiteSpace($compatType)) {
    return $GeneratedType
  }

  if ((Get-CanonicalTypeToken -Token $GeneratedType) -eq (Get-CanonicalTypeToken -Token $compatType)) {
    return $compatType
  }

  return $GeneratedType
}

function Escape-SqlLiteral {
  param([object]$Value)
  if ($null -eq $Value -or $Value -is [System.DBNull]) { return "" }
  return (($Value -as [string]) -replace "'", "''")
}

function New-ExtendedPropertyStatement {
  param(
    [string]$PropName,
    [string]$PropValue,
    [string]$Level0Type = "SCHEMA",
    [string]$Level0Name,
    [string]$Level1Type,
    [string]$Level1Name,
    [string]$Level2Type = "",
    [string]$Level2Name = "",
    [string]$Style = "short"
  )
  $escapedPropName = Escape-SqlLiteral -Value $PropName
  $escapedPropValue = Escape-SqlLiteral -Value $PropValue
  $escapedLevel0Type = Escape-SqlLiteral -Value $Level0Type
  $escapedLevel0Name = Escape-SqlLiteral -Value $Level0Name
  $escapedLevel1Type = Escape-SqlLiteral -Value $Level1Type
  $escapedLevel1Name = Escape-SqlLiteral -Value $Level1Name
  $escapedLevel2Type = Escape-SqlLiteral -Value $Level2Type
  $escapedLevel2Name = Escape-SqlLiteral -Value $Level2Name

  if ($Style -eq "sys_named") {
    $statement = "EXEC sys.sp_addextendedproperty @name=N'{0}', @value=N'{1}' , @level0type=N'{2}',@level0name=N'{3}', @level1type=N'{4}',@level1name=N'{5}'" -f $escapedPropName, $escapedPropValue, $escapedLevel0Type, $escapedLevel0Name, $escapedLevel1Type, $escapedLevel1Name
    if (-not [string]::IsNullOrWhiteSpace($Level2Type)) {
      $statement += ", @level2type=N'{0}',@level2name=N'{1}'" -f $escapedLevel2Type, $escapedLevel2Name
    }
    return $statement
  }

  $level2TypeSql = if ([string]::IsNullOrWhiteSpace($Level2Type)) { "NULL" } else { "'{0}'" -f $escapedLevel2Type }
  $level2NameSql = if ([string]::IsNullOrWhiteSpace($Level2Name)) { "NULL" } else { "N'{0}'" -f $escapedLevel2Name }
  return "EXEC sp_addextendedproperty N'{0}', N'{1}', '{2}', N'{3}', '{4}', N'{5}', {6}, {7}" -f $escapedPropName, $escapedPropValue, $escapedLevel0Type, $escapedLevel0Name, $escapedLevel1Type, $escapedLevel1Name, $level2TypeSql, $level2NameSql
}

function Get-ModuleDefinition {
  param([string]$Schema, [string]$Name)
  $fullName = "[{0}].[{1}]" -f $Schema, $Name
  $cn = New-Connection -server $Server -database $Database
  $cn.Open()
  try {
    $cmd = $cn.CreateCommand()
    $cmd.CommandText = "SELECT OBJECT_DEFINITION(OBJECT_ID(@full))"
    $p = $cmd.Parameters.Add("@full", [System.Data.SqlDbType]::NVarChar, 256)
    $p.Value = $fullName
    $def = $cmd.ExecuteScalar()
    if ($def -is [System.DBNull] -or $null -eq $def) { return "" }
    return ($def -as [string])
  } finally {
    $cn.Close()
  }
}

function Get-ObjectExtendedProperties {
  param(
    [string]$Schema,
    [string]$Name,
    [string]$LevelType,
    [string]$Style = "short",
    [switch]$IncludeParameterLevel = $false,
    [switch]$IncludeIndexLevel = $false
  )
  $fullName = "[{0}].[{1}]" -f $Schema, $Name
  $sql = @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
WHERE ep.class_desc = 'OBJECT_OR_COLUMN' AND ep.major_id = OBJECT_ID(@full) AND ep.minor_id = 0
ORDER BY ep.name
"@
  $rows = Invoke-Query -Sql $sql -Parameters @{ full = $fullName }
  $out = New-Object System.Collections.Generic.List[string]
  foreach ($r in $rows) {
    $out.Add((New-ExtendedPropertyStatement -PropName ($r.prop_name -as [string]) -PropValue ($r.prop_value -as [string]) -Level0Name $Schema -Level1Type $LevelType -Level1Name $Name -Style $Style))
    $out.Add("GO")
  }

  if ($IncludeParameterLevel) {
    $parameterSql = @"
SELECT ep.name AS prop_name, ep.value AS prop_value, p.name AS parameter_name
FROM sys.extended_properties ep
JOIN sys.parameters p ON p.object_id = ep.major_id AND p.parameter_id = ep.minor_id
WHERE ep.class_desc = 'PARAMETER'
  AND ep.major_id = OBJECT_ID(@full)
  AND p.name IS NOT NULL
ORDER BY p.name, ep.name
"@
    $parameterRows = Invoke-Query -Sql $parameterSql -Parameters @{ full = $fullName }
    foreach ($r in $parameterRows) {
      $out.Add((New-ExtendedPropertyStatement -PropName ($r.prop_name -as [string]) -PropValue ($r.prop_value -as [string]) -Level0Name $Schema -Level1Type $LevelType -Level1Name $Name -Level2Type "PARAMETER" -Level2Name ($r.parameter_name -as [string]) -Style $Style))
      $out.Add("GO")
    }
  }

  if ($IncludeIndexLevel) {
    $indexSql = @"
SELECT ep.name AS prop_name, ep.value AS prop_value, i.name AS index_name
FROM sys.extended_properties ep
JOIN sys.indexes i ON i.object_id = ep.major_id AND i.index_id = ep.minor_id
WHERE ep.class_desc = 'INDEX'
  AND ep.major_id = OBJECT_ID(@full)
  AND i.index_id > 0
  AND i.name IS NOT NULL
ORDER BY i.name, ep.name
"@
    $indexRows = Invoke-Query -Sql $indexSql -Parameters @{ full = $fullName }
    foreach ($r in $indexRows) {
      $out.Add((New-ExtendedPropertyStatement -PropName ($r.prop_name -as [string]) -PropValue ($r.prop_value -as [string]) -Level0Name $Schema -Level1Type $LevelType -Level1Name $Name -Level2Type "INDEX" -Level2Name ($r.index_name -as [string]) -Style $Style))
      $out.Add("GO")
    }
  }

  return $out
}

function Get-ObjectGrants {
  param([string]$Schema, [string]$Name)
  $fullName = "[{0}].[{1}]" -f $Schema, $Name
  $sql = @"
SELECT dp.permission_name, dp.state_desc, pr.name AS principal_name
FROM sys.database_permissions dp
JOIN sys.database_principals pr ON pr.principal_id = dp.grantee_principal_id
WHERE dp.major_id = OBJECT_ID(@full) AND dp.class_desc = 'OBJECT_OR_COLUMN' AND dp.minor_id = 0
ORDER BY pr.name, dp.permission_name
"@
  $rows = Invoke-Query -Sql $sql -Parameters @{ full = $fullName }
  $out = New-Object System.Collections.Generic.List[string]
  foreach ($r in $rows) {
    $perm = $r.permission_name
    $principal = $r.principal_name
    switch ($r.state_desc) {
      "GRANT_WITH_GRANT_OPTION" {
        $out.Add(("GRANT {0} ON  {1} TO [{2}] WITH GRANT OPTION" -f $perm, $fullName, $principal))
      }
      "DENY" {
        $out.Add(("DENY {0} ON  {1} TO [{2}]" -f $perm, $fullName, $principal))
      }
      default {
        $out.Add(("GRANT {0} ON  {1} TO [{2}]" -f $perm, $fullName, $principal))
      }
    }
    $out.Add("GO")
  }
  return $out
}

function Get-ModuleScript {
  param(
    [string]$Schema,
    [string]$Name,
    [string[]]$Types,
    [int]$LeadingBlankLines = 0,
    [string]$LevelType = $null,
    [int]$BlankLineBeforeGo = 0,
    [int]$BlankLineBeforeExtProps = 0,
    [string]$ExtPropStyle = "short",
    [object[]]$DefinitionRegexReplacements = $null,
    [string]$DefinitionIndentPrefix = "",
    [string]$CompatDefinitionPath = "",
    [object[]]$PostDefinitionStatements = $null,
    [switch]$IncludeParameterExtendedProperties = $false,
    [switch]$IncludeIndexExtendedProperties = $false
  )
  $typeList = ($Types | ForEach-Object { "'$_'" }) -join ","
  $sql = @"
SELECT m.definition, m.uses_ansi_nulls, m.uses_quoted_identifier
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
JOIN sys.sql_modules m ON m.object_id = o.object_id
WHERE s.name = @schema AND o.name = @name AND o.type IN ($typeList)
"@
  $rows = Invoke-Query -Sql $sql -Parameters @{ schema = $Schema; name = $Name }
  if ($rows.Count -eq 0) { throw "Object not found: [$Schema].[$Name]" }
  $row = $rows[0]
  $quoted = if ($null -eq $row.uses_quoted_identifier -or [int]$row.uses_quoted_identifier -eq 1) { "ON" } else { "OFF" }
  $ansi = if ($null -eq $row.uses_ansi_nulls -or [int]$row.uses_ansi_nulls -eq 1) { "ON" } else { "OFF" }
  $def = Get-ModuleDefinition -Schema $Schema -Name $Name
  if ($DefinitionRegexReplacements) {
    foreach ($item in $DefinitionRegexReplacements) {
      if ($null -eq $item) { continue }
      $pattern = $item.Pattern
      $replacement = $item.Replacement
      $options = Get-RegexOptions -Options $item.Options
      if ($pattern) {
        $def = [regex]::Replace($def, $pattern, $replacement, $options)
      }
    }
  }
  $def = Trim-OuterBlankLines -Text $def
  if (-not [string]::IsNullOrEmpty($DefinitionIndentPrefix)) {
    $defLines = $def -split "\r?\n"
    if ($defLines.Count -gt 0) {
      $firstIndent = [regex]::Match($defLines[0], "^\s*").Value
      if ($firstIndent.Length -eq 0) {
        $defLines[0] = $DefinitionIndentPrefix + $defLines[0]
      }
    }
    $def = ($defLines -join "`n")
  }
  if (-not [string]::IsNullOrWhiteSpace($CompatDefinitionPath)) {
    $lineMap = Get-CompatDefinitionLineMap -Path $CompatDefinitionPath
    if ($lineMap) {
      $defLines = $def -split "\r?\n"
      for ($i = 0; $i -lt $defLines.Count; $i++) {
        $trim = $defLines[$i].Trim()
        if ($trim.Length -eq 0) { continue }
        if ($lineMap.ContainsKey($trim)) {
          $defLines[$i] = $lineMap[$trim]
        }
      }
      $def = ($defLines -join "`n")
    }
  }
  $def = Trim-OuterBlankLines -Text $def
  $lines = New-Object System.Collections.Generic.List[string]
  $lines.Add("SET QUOTED_IDENTIFIER $quoted")
  $lines.Add("GO")
  $lines.Add("SET ANSI_NULLS $ansi")
  $lines.Add("GO")
  for ($i = 0; $i -lt $LeadingBlankLines; $i++) {
    $lines.Add("")
  }
  $lines.Add($def)
  for ($i = 0; $i -lt $BlankLineBeforeGo; $i++) {
    $lines.Add("")
  }
  $lines.Add("GO")
  if ($PostDefinitionStatements) {
    foreach ($line in $PostDefinitionStatements) {
      $lines.Add($line)
    }
  }
  foreach ($line in (Get-ObjectGrants -Schema $Schema -Name $Name)) {
    $lines.Add($line)
  }
  if ($LevelType) {
    for ($i = 0; $i -lt $BlankLineBeforeExtProps; $i++) {
      $lines.Add("")
    }
    foreach ($line in (Get-ObjectExtendedProperties -Schema $Schema -Name $Name -LevelType $LevelType -Style $ExtPropStyle -IncludeParameterLevel:$IncludeParameterExtendedProperties -IncludeIndexLevel:$IncludeIndexExtendedProperties)) {
      $lines.Add($line)
    }
  }
  return ($lines -join "`n")
}

function Get-ViewIndexes {
  param([string]$Schema, [string]$Name)
  return (Get-TableIndexes -Schema $Schema -Name $Name)
}

function Get-SequenceScript {
  param([string]$Schema, [string]$Name)
  $sql = @"
SELECT s.name AS schema_name, seq.name AS sequence_name,
       t.name AS type_name, ts.name AS type_schema, t.is_user_defined,
       seq.start_value, seq.increment, seq.minimum_value, seq.maximum_value,
       seq.is_cycling, seq.is_cached, seq.cache_size
FROM sys.sequences seq
JOIN sys.schemas s ON s.schema_id = seq.schema_id
JOIN sys.types t ON t.user_type_id = seq.user_type_id
JOIN sys.schemas ts ON ts.schema_id = t.schema_id
WHERE s.name = @schema AND seq.name = @name
"@
  $cn = New-Connection -server $Server -database $Database
  $cn.Open()
  try {
    $cmd = $cn.CreateCommand()
    $cmd.CommandText = $sql
    $cmd.Parameters.Add("@schema", [System.Data.SqlDbType]::NVarChar, 128).Value = $Schema
    $cmd.Parameters.Add("@name", [System.Data.SqlDbType]::NVarChar, 128).Value = $Name
    $reader = $cmd.ExecuteReader()
    if (-not $reader.Read()) { throw "Sequence not found: [$Schema].[$Name]" }
    $schemaName = $reader.GetString(0)
    $seqName = $reader.GetString(1)
    $typeNameRaw = $reader.GetString(2)
    $typeSchema = $reader.GetString(3)
    $isUserDefined = $reader.GetBoolean(4)
    $startValue = $reader.GetValue(5)
    $increment = $reader.GetValue(6)
    $minValue = $reader.GetValue(7)
    $maxValue = $reader.GetValue(8)
    $isCycling = $reader.GetBoolean(9)
    $isCached = $reader.GetBoolean(10)
    $cacheSize = $reader.GetValue(11)
    $reader.Close()
  } finally {
    $cn.Close()
  }
  $typeName = if ($isUserDefined) { "[{0}].[{1}]" -f $typeSchema, $typeNameRaw } else { $typeNameRaw }
  $cycle = if ($isCycling) { "CYCLE" } else { "NO CYCLE" }
  $cache = if ($isCached) { if ($cacheSize -is [System.DBNull]) { "CACHE " } else { "CACHE $cacheSize" } } else { "NO CACHE" }
  return @(
    ("CREATE SEQUENCE [{0}].[{1}]" -f $schemaName, $seqName),
    ("AS {0}" -f $typeName),
    ("START WITH {0}" -f $startValue),
    ("INCREMENT BY {0}" -f $increment),
    ("MINVALUE {0}" -f $minValue),
    ("MAXVALUE {0}" -f $maxValue),
    $cycle,
    $cache,
    "GO"
  ) -join "`n"
}

function Get-TableStorage {
  param([string]$Schema, [string]$Name)
  $fullName = "[{0}].[{1}]" -f $Schema, $Name
  $dataSql = @"
SELECT TOP 1 ds.name AS data_space_name
FROM sys.indexes i
JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
WHERE i.object_id = OBJECT_ID(@full) AND i.index_id IN (0,1)
ORDER BY i.index_id DESC
"@
  $lobSql = @"
SELECT ds.name AS lob_data_space_name
FROM sys.tables t
LEFT JOIN sys.data_spaces ds ON ds.data_space_id = t.lob_data_space_id
WHERE t.object_id = OBJECT_ID(@full)
"@
  $cn = New-Connection -server $Server -database $Database
  $cn.Open()
  try {
    $cmd = $cn.CreateCommand()
    $cmd.CommandText = $dataSql
    $p = $cmd.Parameters.Add("@full", [System.Data.SqlDbType]::NVarChar, 256)
    $p.Value = $fullName
    $dataName = $cmd.ExecuteScalar()
    if ($dataName -is [System.DBNull]) { $dataName = $null }

    $cmd.Parameters.Clear()
    $cmd.CommandText = $lobSql
    $p2 = $cmd.Parameters.Add("@full", [System.Data.SqlDbType]::NVarChar, 256)
    $p2.Value = $fullName
    $lobName = $cmd.ExecuteScalar()
    if ($lobName -is [System.DBNull]) { $lobName = $null }
  } finally {
    $cn.Close()
  }
  if ($ListDebug) {
    Write-Host ("DBG storage data_space_name: [{0}] lob_data_space_name: [{1}]" -f $dataName, $lobName)
  }
  return @{
    DataSpace = $dataName
    LobDataSpace = if ($lobName -and $lobName -ne $dataName) { $lobName } else { $null }
  }
}

function Get-TableCompression {
  param([string]$Schema, [string]$Name)
  $fullName = "[{0}].[{1}]" -f $Schema, $Name
  $sql = @"
SELECT p.data_compression_desc
FROM sys.partitions p
WHERE p.object_id = OBJECT_ID(@full) AND p.index_id IN (0,1)
"@
  $rows = Invoke-Query -Sql $sql -Parameters @{ full = $fullName }
  $desc = ($rows | ForEach-Object { $_.data_compression_desc }) | Where-Object { $_ }
  if ($desc -contains "PAGE") { return "PAGE" }
  if ($desc -contains "ROW") { return "ROW" }
  return "NONE"
}

function Get-TableScript {
  param(
    [string]$Schema,
    [string]$Name,
    [string]$CompatTablePath = ""
  )
  $fullName = "[{0}].[{1}]" -f $Schema, $Name
  $storage = Get-TableStorage -Schema $Schema -Name $Name
  $compression = Get-TableCompression -Schema $Schema -Name $Name
  $compatColumnTypeMap = Get-CompatTableColumnTypeMap -Path $CompatTablePath
  if ($ListDebug) {
    Write-Host ("DBG storage for {0}: data={1}, lob={2}, compression={3}" -f $fullName, $storage.DataSpace, $storage.LobDataSpace, $compression)
  }

  $colsSql = @"
SELECT c.column_id, c.name AS column_name,
       t.name AS type_name, ts.name AS type_schema, t.is_user_defined,
       c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity, c.is_computed,
       ic.seed_value, ic.increment_value, cc.definition AS computed_definition,
       dc.name AS default_name, dc.definition AS default_definition
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
JOIN sys.schemas ts ON ts.schema_id = t.schema_id
LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
LEFT JOIN sys.computed_columns cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
LEFT JOIN sys.default_constraints dc ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
WHERE c.object_id = OBJECT_ID(@full)
ORDER BY c.column_id
"@
  $cols = Invoke-Query -Sql $colsSql -Parameters @{ full = $fullName }
  if ($cols.Count -eq 0) { throw "Table not found: $fullName" }

  $lines = New-Object System.Collections.Generic.List[string]
  foreach ($c in $cols) {
    if ([int]$c.is_computed -eq 1) {
      $lines.Add(("[{0}] AS {1}" -f $c.column_name, ($c.computed_definition -as [string]).Trim()))
      continue
    }
    $typeName = Format-TypeName -TypeName $c.type_name -TypeSchema $c.type_schema -IsUserDefined ([bool]$c.is_user_defined) -MaxLength $c.max_length -Precision $c.precision -Scale $c.scale
    $typeName = Get-CompatibleTypeToken -GeneratedType $typeName -CompatTypeMap $compatColumnTypeMap -ColumnName ($c.column_name -as [string])
    $identity = ""
    if ([int]$c.is_identity -eq 1) {
      $identity = " IDENTITY({0}, {1})" -f $c.seed_value, $c.increment_value
    }
    $default = ""
    if ($c.default_definition -and $c.default_definition -ne [System.DBNull]::Value) {
      $defaultName = if ($c.default_name -and $c.default_name -ne [System.DBNull]::Value -and ($c.default_name -as [string]).Length -gt 0) {
        " CONSTRAINT [{0}]" -f $c.default_name
      } else { "" }
      $default = "{0} DEFAULT {1}" -f $defaultName, $c.default_definition
    }
    $nullability = if ([int]$c.is_nullable -eq 1) { "NULL" } else { "NOT NULL" }
    if ($identity -ne "") {
      $lines.Add(("[{0}] {1} {2}{3}{4}" -f $c.column_name, $typeName, $nullability, $identity, $default))
    } else {
      $lines.Add(("[{0}] {1} {2}{3}" -f $c.column_name, $typeName, $nullability, $default))
    }
  }

  $script = New-Object System.Collections.Generic.List[string]
  $script.Add(("CREATE TABLE {0}" -f $fullName))
  $script.Add("(")
  for ($i = 0; $i -lt $lines.Count; $i++) {
    $suffix = if ($i -lt $lines.Count - 1) { "," } else { "" }
    $script.Add($lines[$i] + $suffix)
  }
  $onLine = ")"
  if ($storage.DataSpace) {
    $onLine += " ON [$($storage.DataSpace)]"
    if ($storage.LobDataSpace) {
      $onLine += " TEXTIMAGE_ON [$($storage.LobDataSpace)]"
    }
  }
  $script.Add($onLine)
  if ($compression -ne "NONE") {
    $script.Add("WITH")
    $script.Add("(")
    $script.Add("DATA_COMPRESSION = $compression")
    $script.Add(")")
  }
  $script.Add("GO")

  foreach ($line in (Get-TableTriggers -Schema $Schema -Name $Name)) { $script.Add($line) }
  foreach ($line in (Get-TableChecks -Schema $Schema -Name $Name -CompatTablePath $CompatTablePath)) { $script.Add($line) }
  foreach ($line in (Get-TableConstraints -Schema $Schema -Name $Name)) { $script.Add($line) }
  foreach ($line in (Get-TableIndexes -Schema $Schema -Name $Name)) { $script.Add($line) }
  foreach ($line in (Get-TableForeignKeys -Schema $Schema -Name $Name)) { $script.Add($line) }
  foreach ($line in (Get-TableGrants -Schema $Schema -Name $Name)) { $script.Add($line) }
  foreach ($line in (Get-TableExtendedProperties -Schema $Schema -Name $Name)) { $script.Add($line) }
  $lockEscalation = Get-TableLockEscalation -Schema $Schema -Name $Name
  if ($lockEscalation -and $lockEscalation -ne "TABLE") {
    $script.Add(("ALTER TABLE {0} SET ( LOCK_ESCALATION = {1} )" -f $fullName, $lockEscalation))
    $script.Add("GO")
  }

  return ($script -join "`n")
}

function Get-TableTriggers {
  param([string]$Schema, [string]$Name)
  $fullName = "[{0}].[{1}]" -f $Schema, $Name
  $sql = @"
SELECT tr.name,
       m.definition,
       m.uses_ansi_nulls,
       m.uses_quoted_identifier
FROM sys.triggers tr
JOIN sys.sql_modules m ON m.object_id = tr.object_id
WHERE tr.parent_id = OBJECT_ID(@full)
  AND tr.parent_class_desc = 'OBJECT_OR_COLUMN'
  AND tr.is_ms_shipped = 0
ORDER BY tr.name
"@
  $rows = Invoke-Query -Sql $sql -Parameters @{ full = $fullName }
  if ($rows.Count -eq 0) { return @() }
  $out = New-Object System.Collections.Generic.List[string]
  foreach ($r in $rows) {
    $quoted = if ($null -eq $r.uses_quoted_identifier -or [int]$r.uses_quoted_identifier -eq 1) { "ON" } else { "OFF" }
    $ansi = if ($null -eq $r.uses_ansi_nulls -or [int]$r.uses_ansi_nulls -eq 1) { "ON" } else { "OFF" }
    $definition = Trim-OuterBlankLines -Text ($r.definition -as [string])
    $out.Add("SET QUOTED_IDENTIFIER $quoted")
    $out.Add("GO")
    $out.Add("SET ANSI_NULLS $ansi")
    $out.Add("GO")
    $out.Add($definition)
    $out.Add("GO")
  }
  return $out
}

function Get-TableConstraints {
  param([string]$Schema, [string]$Name)
  $fullName = "[{0}].[{1}]" -f $Schema, $Name
  $sql = @"
SELECT kc.name, kc.type_desc, i.type_desc AS index_type_desc,
       ds.name AS data_space_name,
       MAX(p.data_compression_desc) AS data_compression_desc,
       STUFF((
         SELECT ', ' + QUOTENAME(c.name) + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE '' END
         FROM sys.index_columns ic
         JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
         WHERE ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.is_included_column = 0
         ORDER BY ic.key_ordinal
         FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 2, '') AS columns
FROM sys.key_constraints kc
JOIN sys.indexes i ON i.object_id = kc.parent_object_id AND i.index_id = kc.unique_index_id
JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
JOIN sys.partitions p ON p.object_id = i.object_id AND p.index_id = i.index_id
WHERE kc.parent_object_id = OBJECT_ID(@full)
GROUP BY kc.object_id, kc.name, kc.type_desc, i.type_desc, i.object_id, i.index_id, ds.name
ORDER BY kc.object_id
"@
  $rows = Invoke-Query -Sql $sql -Parameters @{ full = $fullName }
  $out = New-Object System.Collections.Generic.List[string]
  if ($ListDebug) { Write-Host ("DBG extprop rows for {0}: {1}" -f $fullName, $rows.Count) }
  $tableProps = @($rows | Where-Object { [int]$_.minor_id -eq 0 } | Sort-Object prop_name)
  $colProps = @($rows | Where-Object { [int]$_.minor_id -ne 0 } | Sort-Object column_name, prop_name)
  if ($ListDebug -and $rows.Count -gt 0) {
    $order = $colProps | ForEach-Object { $_.column_name }
    Write-Host ("DBG extprop order for {0}: {1}" -f $fullName, ($order -join ", "))
  }
  $allProps = @()
  $allProps += $tableProps
  $allProps += $colProps
  foreach ($r in $allProps) {
    $constraintType = if ($r.type_desc -eq "PRIMARY_KEY_CONSTRAINT") { "PRIMARY KEY" } else { "UNIQUE" }
    $clustered = if ($r.index_type_desc -match "CLUSTERED") { "CLUSTERED" } else { "NONCLUSTERED" }
    $with = ""
    if ($r.data_compression_desc -eq "PAGE") { $with = " WITH (DATA_COMPRESSION = PAGE)" }
    $on = if ($r.data_space_name) { " ON [$($r.data_space_name)]" } else { "" }
    $out.Add(("ALTER TABLE {0} ADD CONSTRAINT [{1}] {2} {3} ({4}){5}{6}" -f $fullName, $r.name, $constraintType, $clustered, $r.columns, $with, $on))
    $out.Add("GO")
  }
  return $out
}

function Get-IndexCompression {
  param([int]$ObjectId, [int]$IndexId)
  $sql = @"
SELECT p.data_compression_desc
FROM sys.partitions p
WHERE p.object_id = @obj AND p.index_id = @idx
"@
  $rows = Invoke-Query -Sql $sql -Parameters @{ obj = $ObjectId; idx = $IndexId }
  $desc = ($rows | ForEach-Object { $_.data_compression_desc }) | Where-Object { $_ }
  if ($desc -contains "PAGE") { return "PAGE" }
  if ($desc -contains "ROW") { return "ROW" }
  return "NONE"
}

function Get-TableIndexes {
  param([string]$Schema, [string]$Name)
  $fullName = "[{0}].[{1}]" -f $Schema, $Name
  $sql = @"
SELECT i.object_id, i.index_id, i.name, i.is_unique, i.type_desc, ds.name AS data_space_name, i.filter_definition
FROM sys.indexes i
JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
WHERE i.object_id = OBJECT_ID(@full)
  AND i.is_primary_key = 0
  AND i.is_unique_constraint = 0
  AND i.type_desc IN ('CLUSTERED','NONCLUSTERED')
  AND i.is_hypothetical = 0
ORDER BY i.name
"@
  $rows = Invoke-Query -Sql $sql -Parameters @{ full = $fullName }
  if ($rows.Count -eq 0) { return @() }
  $out = New-Object System.Collections.Generic.List[string]
  foreach ($r in $rows) {
    $objId = [int]$r.object_id
    $idxId = [int]$r.index_id
    $idxName = $r.name
    $unique = if ([int]$r.is_unique -eq 1) { "UNIQUE " } else { "" }
    $type = if ($r.type_desc -eq "CLUSTERED") { "CLUSTERED" } else { "NONCLUSTERED" }
    $dsName = $r.data_space_name

    $colsSql = @"
SELECT c.name AS column_name, ic.is_descending_key, ic.is_included_column
FROM sys.index_columns ic
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE ic.object_id = OBJECT_ID(@full) AND ic.index_id = @idx
ORDER BY ic.key_ordinal, ic.index_column_id
"@
    $cols = Invoke-Query -Sql $colsSql -Parameters @{ full = $fullName; idx = $idxId }
    $keys = New-Object System.Collections.Generic.List[string]
    $includes = New-Object System.Collections.Generic.List[string]
    foreach ($c in $cols) {
      if ([int]$c.is_included_column -eq 1) {
        $includes.Add(("[{0}]" -f $c.column_name))
      } else {
        $dir = if ([int]$c.is_descending_key -eq 1) { " DESC" } else { "" }
        $keys.Add(("[{0}]{1}" -f $c.column_name, $dir))
      }
    }
    if ($keys.Count -eq 0) { continue }
    $keyList = $keys -join ", "
    $includeClause = if ($includes.Count -gt 0) { " INCLUDE (" + ($includes -join ", ") + ")" } else { "" }
    $compression = Get-IndexCompression -ObjectId $objId -IndexId $idxId
    $with = if ($compression -ne "NONE") { " WITH (DATA_COMPRESSION = $compression)" } else { "" }
    $on = if ($dsName) { " ON [$dsName]" } else { "" }

    $filterDef = ($r.filter_definition -as [string]).Trim()
    $filterClause = if ($filterDef) { " WHERE " + $filterDef } else { "" }
    $out.Add(("CREATE {0}{1} INDEX [{2}] ON {3} ({4}){5}{6}{7}{8}" -f $unique, $type, $idxName, $fullName, $keyList, $includeClause, $filterClause, $with, $on))
    $out.Add("GO")
  }
  return $out
}

function Get-TableChecks {
  param(
    [string]$Schema,
    [string]$Name,
    [string]$CompatTablePath = ""
  )
  $fullName = "[{0}].[{1}]" -f $Schema, $Name
  if (-not [string]::IsNullOrWhiteSpace($CompatTablePath) -and (Test-Path $CompatTablePath)) {
    $compatLines = [System.IO.File]::ReadAllLines($CompatTablePath, [System.Text.Encoding]::UTF8) |
      Where-Object { $_ -match "^\s*ALTER TABLE .* ADD CONSTRAINT .* CHECK" }
    if ($compatLines.Count -gt 0) {
      $out = New-Object System.Collections.Generic.List[string]
      foreach ($line in $compatLines) {
        $out.Add($line)
        $out.Add("GO")
      }
      return $out
    }
  }
  $sql = @"
SELECT name, definition, is_not_for_replication
FROM sys.check_constraints
WHERE parent_object_id = OBJECT_ID(@full)
ORDER BY name
"@
  $rows = Invoke-Query -Sql $sql -Parameters @{ full = $fullName }
  $out = New-Object System.Collections.Generic.List[string]
  foreach ($r in $rows) {
    $nfr = if ([int]$r.is_not_for_replication -eq 1) { " NOT FOR REPLICATION" } else { "" }
    $compatLine = Get-CompatTableCheckLine -Path $CompatTablePath -ConstraintName ($r.name -as [string])
    if ($compatLine) {
      $out.Add($compatLine)
    } else {
      $definition = ($r.definition -as [string]).Trim()
      $out.Add(("ALTER TABLE {0} ADD CONSTRAINT [{1}] CHECK{2} ({3})" -f $fullName, $r.name, $nfr, $definition))
    }
    $out.Add("GO")
  }
  return $out
}

function Get-TableLockEscalation {
  param([string]$Schema, [string]$Name)
  $fullName = "[{0}].[{1}]" -f $Schema, $Name
  $cn = New-Connection -server $Server -database $Database
  $cn.Open()
  try {
    $cmd = $cn.CreateCommand()
    $cmd.CommandText = @"
SELECT t.lock_escalation_desc
FROM sys.tables t
WHERE t.object_id = OBJECT_ID(@full)
"@
    $cmd.Parameters.Add("@full", [System.Data.SqlDbType]::NVarChar, 256).Value = $fullName
    $value = $cmd.ExecuteScalar()
    if ($value -is [System.DBNull] -or $null -eq $value) { return $null }
    $text = ($value -as [string]).Trim()
    if ($ListDebug) {
      Write-Host ("DBG lock escalation for {0}: [{1}]" -f $fullName, $text)
    }
    return $text
  } finally {
    $cn.Close()
  }
}

function Get-TableForeignKeys {
  param([string]$Schema, [string]$Name)
  $fullName = "[{0}].[{1}]" -f $Schema, $Name
  $sql = @"
SELECT fk.name,
       s2.name AS ref_schema, t2.name AS ref_table,
       fk.delete_referential_action_desc, fk.update_referential_action_desc,
       STUFF((
         SELECT ', ' + QUOTENAME(c1.name)
         FROM sys.foreign_key_columns fkc
         JOIN sys.columns c1 ON c1.object_id = fkc.parent_object_id AND c1.column_id = fkc.parent_column_id
         WHERE fkc.constraint_object_id = fk.object_id
         ORDER BY fkc.constraint_column_id
         FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 2, '') AS parent_cols,
       STUFF((
         SELECT ', ' + QUOTENAME(c2.name)
         FROM sys.foreign_key_columns fkc
         JOIN sys.columns c2 ON c2.object_id = fkc.referenced_object_id AND c2.column_id = fkc.referenced_column_id
         WHERE fkc.constraint_object_id = fk.object_id
         ORDER BY fkc.constraint_column_id
         FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 2, '') AS ref_cols
FROM sys.foreign_keys fk
JOIN sys.tables t2 ON t2.object_id = fk.referenced_object_id
JOIN sys.schemas s2 ON s2.schema_id = t2.schema_id
WHERE fk.parent_object_id = OBJECT_ID(@full)
ORDER BY fk.name
"@
  $rows = Invoke-Query -Sql $sql -Parameters @{ full = $fullName }
  $out = New-Object System.Collections.Generic.List[string]
  foreach ($r in $rows) {
    $onDelete = if ($r.delete_referential_action_desc -ne "NO_ACTION") { " ON DELETE $($r.delete_referential_action_desc)" } else { "" }
    $onUpdate = if ($r.update_referential_action_desc -ne "NO_ACTION") { " ON UPDATE $($r.update_referential_action_desc)" } else { "" }
    $out.Add(("ALTER TABLE {0} ADD CONSTRAINT [{1}] FOREIGN KEY ({2}) REFERENCES [{3}].[{4}] ({5}){6}{7}" -f $fullName, $r.name, $r.parent_cols, $r.ref_schema, $r.ref_table, $r.ref_cols, $onDelete, $onUpdate))
    $out.Add("GO")
  }
  return $out
}

function Get-TableGrants {
  param([string]$Schema, [string]$Name)
  $fullName = "[{0}].[{1}]" -f $Schema, $Name
  $sql = @"
SELECT dp.permission_name, dp.state_desc, pr.name AS principal_name
FROM sys.database_permissions dp
JOIN sys.database_principals pr ON pr.principal_id = dp.grantee_principal_id
WHERE dp.major_id = OBJECT_ID(@full) AND dp.class_desc = 'OBJECT_OR_COLUMN'
ORDER BY pr.name, dp.permission_name
"@
  $rows = Invoke-Query -Sql $sql -Parameters @{ full = $fullName }
  $out = New-Object System.Collections.Generic.List[string]
  foreach ($r in $rows) {
    if ($r.state_desc -eq "GRANT_WITH_GRANT_OPTION") {
      $out.Add(("GRANT {0} ON  {1} TO [{2}] WITH GRANT OPTION" -f $r.permission_name, $fullName, $r.principal_name))
    } else {
      $out.Add(("GRANT {0} ON  {1} TO [{2}]" -f $r.permission_name, $fullName, $r.principal_name))
    }
    $out.Add("GO")
  }
  return $out
}

function Get-TableExtendedProperties {
  param([string]$Schema, [string]$Name)
  $fullName = "[{0}].[{1}]" -f $Schema, $Name
  $tableSql = @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
WHERE ep.class_desc = 'OBJECT_OR_COLUMN' AND ep.major_id = OBJECT_ID(@full) AND ep.minor_id = 0
ORDER BY ep.name
"@
  $colSql = @"
SELECT ep.name AS prop_name, ep.value AS prop_value, c.name AS column_name
FROM sys.extended_properties ep
JOIN sys.columns c ON c.object_id = ep.major_id AND c.column_id = ep.minor_id
WHERE ep.class_desc = 'OBJECT_OR_COLUMN' AND ep.major_id = OBJECT_ID(@full) AND ep.minor_id <> 0
ORDER BY c.name, ep.name
"@
  $constraintSql = @"
SELECT ep.name AS prop_name, ep.value AS prop_value, o.name AS constraint_name
FROM sys.extended_properties ep
JOIN sys.objects o ON o.object_id = ep.major_id
WHERE ep.class_desc = 'OBJECT_OR_COLUMN'
  AND ep.minor_id = 0
  AND o.parent_object_id = OBJECT_ID(@full)
  AND o.type IN ('C', 'D', 'F', 'PK', 'UQ', 'EC')
ORDER BY o.name, ep.name
"@
  $indexSql = @"
SELECT ep.name AS prop_name, ep.value AS prop_value, i.name AS index_name
FROM sys.extended_properties ep
JOIN sys.indexes i ON i.object_id = ep.major_id AND i.index_id = ep.minor_id
WHERE ep.class_desc = 'INDEX'
  AND ep.major_id = OBJECT_ID(@full)
  AND i.index_id > 0
  AND i.name IS NOT NULL
ORDER BY i.name, ep.name
"@
  $triggerSql = @"
SELECT ep.name AS prop_name, ep.value AS prop_value, tr.name AS trigger_name
FROM sys.extended_properties ep
JOIN sys.triggers tr ON tr.object_id = ep.major_id
WHERE ep.class_desc = 'OBJECT_OR_COLUMN'
  AND ep.minor_id = 0
  AND tr.parent_id = OBJECT_ID(@full)
  AND tr.parent_class_desc = 'OBJECT_OR_COLUMN'
  AND tr.is_ms_shipped = 0
ORDER BY tr.name, ep.name
"@
  $tableRows = Invoke-Query -Sql $tableSql -Parameters @{ full = $fullName }
  $colRows = Invoke-Query -Sql $colSql -Parameters @{ full = $fullName }
  $constraintRows = Invoke-Query -Sql $constraintSql -Parameters @{ full = $fullName }
  $indexRows = Invoke-Query -Sql $indexSql -Parameters @{ full = $fullName }
  $triggerRows = Invoke-Query -Sql $triggerSql -Parameters @{ full = $fullName }
  $out = New-Object System.Collections.Generic.List[string]
  foreach ($r in $tableRows) {
    $out.Add((New-ExtendedPropertyStatement -PropName ($r.prop_name -as [string]) -PropValue ($r.prop_value -as [string]) -Level0Name $Schema -Level1Type "TABLE" -Level1Name $Name))
    $out.Add("GO")
  }
  foreach ($r in $colRows) {
    $out.Add((New-ExtendedPropertyStatement -PropName ($r.prop_name -as [string]) -PropValue ($r.prop_value -as [string]) -Level0Name $Schema -Level1Type "TABLE" -Level1Name $Name -Level2Type "COLUMN" -Level2Name ($r.column_name -as [string])))
    $out.Add("GO")
  }
  foreach ($r in $constraintRows) {
    $out.Add((New-ExtendedPropertyStatement -PropName ($r.prop_name -as [string]) -PropValue ($r.prop_value -as [string]) -Level0Name $Schema -Level1Type "TABLE" -Level1Name $Name -Level2Type "CONSTRAINT" -Level2Name ($r.constraint_name -as [string])))
    $out.Add("GO")
  }
  foreach ($r in $indexRows) {
    $out.Add((New-ExtendedPropertyStatement -PropName ($r.prop_name -as [string]) -PropValue ($r.prop_value -as [string]) -Level0Name $Schema -Level1Type "TABLE" -Level1Name $Name -Level2Type "INDEX" -Level2Name ($r.index_name -as [string])))
    $out.Add("GO")
  }
  foreach ($r in $triggerRows) {
    $out.Add((New-ExtendedPropertyStatement -PropName ($r.prop_name -as [string]) -PropValue ($r.prop_value -as [string]) -Level0Name $Schema -Level1Type "TABLE" -Level1Name $Name -Level2Type "TRIGGER" -Level2Name ($r.trigger_name -as [string])))
    $out.Add("GO")
  }
  return $out
}

function Write-ObjectScript {
  param(
    [hashtable]$Obj,
    [string]$Content,
    [bool]$EnsureTrailingNewline = $true
  )
  $path = Join-Path $Output $Obj.Folder
  $filePath = Join-Path $path $Obj.File
  Write-Utf8NoBom -Path $filePath -Content $Content -EnsureTrailingNewline $EnsureTrailingNewline
  Write-Host "Wrote $($Obj.Folder)/$($Obj.File)"
}

$items = Parse-ObjectList -Path $ObjectList
$overrides = Get-Overrides -Path $OverridesPath
if ($items.Count -eq 0) {
  if ($ListDebug) {
    Write-Host "No objects parsed from $ObjectList"
    Write-Host "File contents:"
    Get-Content -Path $ObjectList | ForEach-Object { Write-Host $_ }
  }
  throw "No objects found in $ObjectList"
}

foreach ($item in $items) {
  $obj = Split-ObjectPath -Path $item
  $override = Get-OverrideForPath -Overrides $overrides -Path $item
  $ensureNewline = if ($override -and $override.EnsureTrailingNewline -ne $null) { [bool]$override.EnsureTrailingNewline } else { $true }
  switch ($obj.Folder) {
    "Tables" {
      $compatTablePath = if (-not [string]::IsNullOrWhiteSpace($CompatPath)) { Join-Path $CompatPath $item } else { "" }
      $content = Get-TableScript -Schema $obj.Schema -Name $obj.Name -CompatTablePath $compatTablePath
      Write-ObjectScript -Obj $obj -Content $content -EnsureTrailingNewline $ensureNewline
    }
    "Views" {
      $compatFormat = $null
      if (-not [string]::IsNullOrWhiteSpace($CompatPath)) {
        $compatFormat = Get-ModuleFormattingFromCompat -Path (Join-Path $CompatPath $item) -CreatePattern "(?i)^\s*CREATE\s+VIEW\b"
      }
      $leading = if ($override -and $override.LeadingBlankLines -ne $null) {
        [int]$override.LeadingBlankLines
      } elseif ($compatFormat) {
        [int]$compatFormat.LeadingBlankLines
      } else { 0 }
      $beforeGo = if ($override -and $override.BlankLineBeforeGo -ne $null) {
        [int]$override.BlankLineBeforeGo
      } elseif ($compatFormat) {
        [int]$compatFormat.BlankLineBeforeGo
      } else { 0 }
      $beforeExt = if ($override -and $override.BlankLineBeforeExtProps -ne $null) {
        [int]$override.BlankLineBeforeExtProps
      } elseif ($compatFormat) {
        [int]$compatFormat.BlankLineBeforeExtProps
      } else { 0 }
      $indentPrefix = if ($override -and $override.DefinitionIndentPrefix) {
        [string]$override.DefinitionIndentPrefix
      } elseif ($compatFormat) {
        [string]$compatFormat.DefinitionIndentPrefix
      } else { "" }
      $extStyle = if ($override -and $override.ExtPropStyle) { [string]$override.ExtPropStyle } else { "short" }
      $viewIndexes = Get-ViewIndexes -Schema $obj.Schema -Name $obj.Name
      $content = Get-ModuleScript -Schema $obj.Schema -Name $obj.Name -Types @("V") -LevelType "VIEW" -LeadingBlankLines $leading -BlankLineBeforeGo $beforeGo -BlankLineBeforeExtProps $beforeExt -ExtPropStyle $extStyle -DefinitionRegexReplacements $override.DefinitionRegexReplacements -DefinitionIndentPrefix $indentPrefix -PostDefinitionStatements $viewIndexes -IncludeIndexExtendedProperties
      Write-ObjectScript -Obj $obj -Content $content -EnsureTrailingNewline $ensureNewline
    }
    "Stored Procedures" {
      $compatModulePath = if (-not [string]::IsNullOrWhiteSpace($CompatPath)) { Join-Path $CompatPath $item } else { "" }
      $compatFormat = Get-ModuleFormattingFromCompat -Path $compatModulePath -CreatePattern "(?i)^\s*CREATE\s+(PROC(?:EDURE)?)\b"
      $leading = if ($compatFormat) { [int]$compatFormat.LeadingBlankLines } else { 0 }
      $beforeGo = if ($compatFormat) { [int]$compatFormat.BlankLineBeforeGo } else { 0 }
      $content = Get-ModuleScript -Schema $obj.Schema -Name $obj.Name -Types @("P") -LevelType "PROCEDURE" -LeadingBlankLines $leading -BlankLineBeforeGo $beforeGo -DefinitionRegexReplacements $override.DefinitionRegexReplacements -CompatDefinitionPath $compatModulePath -IncludeParameterExtendedProperties
      Write-ObjectScript -Obj $obj -Content $content -EnsureTrailingNewline $ensureNewline
    }
    "Functions" {
      $compatModulePath = if (-not [string]::IsNullOrWhiteSpace($CompatPath)) { Join-Path $CompatPath $item } else { "" }
      $compatFormat = Get-ModuleFormattingFromCompat -Path $compatModulePath -CreatePattern "(?i)^\s*CREATE\s+FUNCTION\b"
      $leading = if ($compatFormat) { [int]$compatFormat.LeadingBlankLines } else { 0 }
      $beforeGo = if ($compatFormat) { [int]$compatFormat.BlankLineBeforeGo } else { 0 }
      $content = Get-ModuleScript -Schema $obj.Schema -Name $obj.Name -Types @("FN", "TF", "IF") -LevelType "FUNCTION" -LeadingBlankLines $leading -BlankLineBeforeGo $beforeGo -DefinitionRegexReplacements $override.DefinitionRegexReplacements -CompatDefinitionPath $compatModulePath -IncludeParameterExtendedProperties
      Write-ObjectScript -Obj $obj -Content $content -EnsureTrailingNewline $ensureNewline
    }
    "Sequences" {
      $content = Get-SequenceScript -Schema $obj.Schema -Name $obj.Name
      Write-ObjectScript -Obj $obj -Content $content -EnsureTrailingNewline $ensureNewline
    }
    default {
      Write-Warning "Unsupported folder type: $($obj.Folder)"
    }
  }
}

