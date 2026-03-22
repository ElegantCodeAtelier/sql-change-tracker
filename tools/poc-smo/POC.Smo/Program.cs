using System.Globalization;
using System.Data;
using System.Text;
using Microsoft.Data.SqlClient;

var argsMap = ParseArgs(args);
var serverName = GetArg(argsMap, "--server", "localhost");
var databaseName = GetArg(argsMap, "--database", "SampleDatabase");
var sourceName = GetArg(argsMap, "--source", "");
var hasObjectListArg = argsMap.TryGetValue("--object-list", out var objectListArg) && !string.IsNullOrWhiteSpace(objectListArg);
var hasOutArg = argsMap.TryGetValue("--out", out var outArg) && !string.IsNullOrWhiteSpace(outArg);
var needsSourceDefaults = !hasObjectListArg || !hasOutArg || !string.IsNullOrWhiteSpace(sourceName);
LocalFixtureSourceResolver.FixtureSource? sourceConfig = null;
if (needsSourceDefaults)
{
    try
    {
        sourceConfig = LocalFixtureSourceResolver.Resolve(sourceName);
    }
    catch (Exception ex)
    {
        Console.Error.WriteLine($"Failed to resolve local fixture source: {ex.Message}");
        return 2;
    }
}

var objectListPath = hasObjectListArg ? objectListArg! : sourceConfig!.ObjectListPath;
var outputRoot = hasOutArg ? outArg! : Path.Combine("local", "fixtures", "outputs", $"poc-out-smo-{sourceConfig!.Name}");

if (!File.Exists(objectListPath))
{
    Console.Error.WriteLine($"Object list not found: {objectListPath}");
    return 1;
}

Directory.CreateDirectory(outputRoot);

var objects = ParseObjectList(objectListPath);
if (objects.Count == 0)
{
    Console.Error.WriteLine($"No objects found in: {objectListPath}");
    return 1;
}

using var sql = new SqlConnection(BuildConnectionString(serverName, databaseName));
try
{
    sql.Open();
}
catch (SqlException ex)
{
    Console.Error.WriteLine($"Connection failed for database {databaseName}: {ex.Message}");
    return 1;
}

foreach (var obj in objects)
{
    string? script = obj.Folder switch
    {
        "Tables" => TryGetTableScript(sql, obj.Schema, obj.Name),
        "Views" => TryGetModuleScript(sql, obj.Schema, obj.Name, new[] { "V" }, "VIEW", useCreateOrAlter: false),
        "Stored Procedures" => TryGetModuleScript(sql, obj.Schema, obj.Name, new[] { "P" }, "PROCEDURE", useCreateOrAlter: false),
        "Functions" => TryGetModuleScript(sql, obj.Schema, obj.Name, new[] { "FN", "TF", "IF" }, "FUNCTION", useCreateOrAlter: true),
        "Sequences" => TryGetSequenceScript(sql, obj.Schema, obj.Name),
        _ => null
    };

    if (script == null)
    {
        Console.Error.WriteLine($"SKIP (not found): {obj.RelativePath}");
        continue;
    }

    var outPath = Path.Combine(outputRoot, obj.Folder, obj.FileName);
    Directory.CreateDirectory(Path.GetDirectoryName(outPath)!);
    WriteUtf8NoBom(outPath, script);
    Console.WriteLine($"Wrote {obj.Folder}/{obj.FileName}");
}

return 0;

static string BuildConnectionString(string serverName, string databaseName)
{
    var builder = new SqlConnectionStringBuilder
    {
        DataSource = serverName,
        InitialCatalog = databaseName,
        IntegratedSecurity = true,
        Encrypt = false,
        TrustServerCertificate = true
    };
    return builder.ConnectionString;
}

static Dictionary<string, string> ParseArgs(string[] args)
{
    var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    for (var i = 0; i < args.Length; i++)
    {
        var key = args[i];
        if (!key.StartsWith("--", StringComparison.Ordinal)) continue;
        var value = (i + 1) < args.Length ? args[i + 1] : "";
        if (value.StartsWith("--", StringComparison.Ordinal)) value = "";
        map[key] = value;
    }
    return map;
}

static string GetArg(Dictionary<string, string> map, string key, string fallback)
{
    return map.TryGetValue(key, out var value) && !string.IsNullOrWhiteSpace(value) ? value : fallback;
}

static List<ObjectEntry> ParseObjectList(string path)
{
    var items = new List<ObjectEntry>();
    foreach (var line in File.ReadAllLines(path, Encoding.UTF8))
    {
        var trim = line.Trim();
        if (trim.Length == 0 || trim.StartsWith("#", StringComparison.Ordinal)) continue;
        if (!trim.Contains(".sql", StringComparison.OrdinalIgnoreCase)) continue;
        trim = trim.TrimStart('-').Trim();
        if (!trim.EndsWith(".sql", StringComparison.OrdinalIgnoreCase)) continue;
        var split = trim.Split(new[] { '/' }, 2);
        if (split.Length != 2) continue;
        var folder = split[0];
        var file = split[1];
        var name = Path.GetFileNameWithoutExtension(file);
        var dot = name.IndexOf('.');
        if (dot < 1) continue;
        items.Add(new ObjectEntry(folder, file, name[..dot], name[(dot + 1)..], trim));
    }
    return items;
}

static string? TryGetModuleScript(
    SqlConnection conn,
    string schema,
    string name,
    string[] types,
    string levelType,
    bool useCreateOrAlter)
{
    var typeList = string.Join(",", types.Select(t => $"'{t}'"));
    var sql = $@"
SELECT m.definition, m.uses_ansi_nulls, m.uses_quoted_identifier
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
JOIN sys.sql_modules m ON m.object_id = o.object_id
WHERE s.name = @schema AND o.name = @name AND o.type IN ({typeList})
";
    var rows = QueryRows(conn, sql,
        Param("@schema", SqlDbType.NVarChar, 128, schema),
        Param("@name", SqlDbType.NVarChar, 128, name));

    if (rows.Count == 0) return null;

    var row = rows[0];
    var ansi = ToOnOff(row.TryGetValue("uses_ansi_nulls", out var ansiValue) ? ansiValue : null, defaultOn: true);
    var quoted = ToOnOff(row.TryGetValue("uses_quoted_identifier", out var quotedValue) ? quotedValue : null, defaultOn: true);
    var definition = row.TryGetValue("definition", out var defValue) ? (defValue as string ?? "") : "";

    if (useCreateOrAlter)
    {
        var regex = new System.Text.RegularExpressions.Regex(
            "\\bCREATE\\s+FUNCTION\\b",
            System.Text.RegularExpressions.RegexOptions.IgnoreCase);
        definition = regex.Replace(definition, "CREATE OR ALTER FUNCTION", 1);
    }

    var lines = new List<string>
    {
        $"SET QUOTED_IDENTIFIER {quoted}",
        "GO",
        $"SET ANSI_NULLS {ansi}",
        "GO"
    };
    if (string.Equals(levelType, "VIEW", StringComparison.OrdinalIgnoreCase))
    {
        lines.Add("");
        lines.Add("");
    }
    lines.Add(definition.TrimEnd());
    if (string.Equals(levelType, "VIEW", StringComparison.OrdinalIgnoreCase))
    {
        lines.Add("");
    }
    lines.Add("GO");
    lines.AddRange(GetObjectExtendedProperties(conn, schema, name, levelType));
    return string.Join("\n", lines);
}

static string? TryGetSequenceScript(SqlConnection conn, string schema, string name)
{
    var sql = @"
SELECT s.name AS schema_name, seq.name AS sequence_name,
       t.name AS type_name, ts.name AS type_schema, t.is_user_defined,
       seq.start_value, seq.increment, seq.minimum_value, seq.maximum_value,
       seq.is_cycling, seq.is_cached, seq.cache_size
FROM sys.sequences seq
JOIN sys.schemas s ON s.schema_id = seq.schema_id
JOIN sys.types t ON t.user_type_id = seq.user_type_id
JOIN sys.schemas ts ON ts.schema_id = t.schema_id
WHERE s.name = @schema AND seq.name = @name
";
    var rows = QueryRows(conn, sql,
        Param("@schema", SqlDbType.NVarChar, 128, schema),
        Param("@name", SqlDbType.NVarChar, 128, name));
    if (rows.Count == 0) return null;

    var row = rows[0];
    var schemaName = row["schema_name"] as string ?? schema;
    var seqName = row["sequence_name"] as string ?? name;
    var typeNameRaw = row["type_name"] as string ?? "int";
    var typeSchema = row["type_schema"] as string ?? "dbo";
    var isUserDefined = ToBool(row["is_user_defined"]);
    var startValue = ToInvariant(row["start_value"]);
    var increment = ToInvariant(row["increment"]);
    var minValue = ToInvariant(row["minimum_value"]);
    var maxValue = ToInvariant(row["maximum_value"]);
    var isCycling = ToBool(row["is_cycling"]);
    var isCached = ToBool(row["is_cached"]);
    var cacheSize = row["cache_size"];

    var typeName = isUserDefined ? $"[{typeSchema}].[{typeNameRaw}]" : typeNameRaw;
    var cycle = isCycling ? "CYCLE" : "NO CYCLE";
    var cache = isCached
        ? (cacheSize == null ? "CACHE " : $"CACHE {ToInvariant(cacheSize)}")
        : "NO CACHE";

    return string.Join("\n", new[]
    {
        $"CREATE SEQUENCE [{schemaName}].[{seqName}]",
        $"AS {typeName}",
        $"START WITH {startValue}",
        $"INCREMENT BY {increment}",
        $"MINVALUE {minValue}",
        $"MAXVALUE {maxValue}",
        cycle,
        cache,
        "GO"
    });
}

static string? TryGetTableScript(SqlConnection conn, string schema, string name)
{
    var fullName = $"[{schema}].[{name}]";
    var storage = GetTableStorage(conn, fullName);
    var compression = GetTableCompression(conn, fullName);

    var columnsSql = @"
SELECT c.column_id, c.name AS column_name,
       t.name AS type_name, ts.name AS type_schema, t.is_user_defined,
       c.max_length, c.precision, c.scale, c.is_nullable, c.is_identity, c.is_computed,
       ic.seed_value, ic.increment_value, cc.definition AS computed_definition
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
JOIN sys.schemas ts ON ts.schema_id = t.schema_id
LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
LEFT JOIN sys.computed_columns cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
WHERE c.object_id = OBJECT_ID(@full)
ORDER BY c.column_id
";
    var columns = QueryRows(conn, columnsSql,
        Param("@full", SqlDbType.NVarChar, 256, fullName));
    if (columns.Count == 0) return null;

    var columnLines = new List<string>();
    foreach (var column in columns)
    {
        var columnName = column["column_name"] as string ?? "";
        var isComputed = ToBool(column["is_computed"]);
        if (isComputed)
        {
            var definition = (column["computed_definition"] as string ?? "").Trim();
            columnLines.Add($"[{columnName}] AS {definition}");
            continue;
        }

        var typeName = column["type_name"] as string ?? "";
        var typeSchema = column["type_schema"] as string ?? "dbo";
        var isUserDefined = ToBool(column["is_user_defined"]);
        var maxLength = ToInt(column["max_length"]);
        var precision = ToInt(column["precision"]);
        var scale = ToInt(column["scale"]);
        var typeFormatted = FormatTypeName(typeName, typeSchema, isUserDefined, maxLength, precision, scale);

        var isIdentity = ToBool(column["is_identity"]);
        var nullability = ToBool(column["is_nullable"]) ? "NULL" : "NOT NULL";
        if (isIdentity)
        {
            var seed = ToInvariant(column["seed_value"]);
            var increment = ToInvariant(column["increment_value"]);
            columnLines.Add($"[{columnName}] {typeFormatted} {nullability} IDENTITY({seed}, {increment})");
        }
        else
        {
            columnLines.Add($"[{columnName}] {typeFormatted} {nullability}");
        }
    }

    var script = new List<string>
    {
        $"CREATE TABLE {fullName}",
        "("
    };
    for (var i = 0; i < columnLines.Count; i++)
    {
        var suffix = i < columnLines.Count - 1 ? "," : "";
        script.Add(columnLines[i] + suffix);
    }
    var onLine = ")";
    if (!string.IsNullOrWhiteSpace(storage.DataSpace))
    {
        onLine += $" ON [{storage.DataSpace}]";
        if (!string.IsNullOrWhiteSpace(storage.LobDataSpace))
        {
            onLine += $" TEXTIMAGE_ON [{storage.LobDataSpace}]";
        }
    }
    script.Add(onLine);
    if (!string.Equals(compression, "NONE", StringComparison.OrdinalIgnoreCase))
    {
        script.Add("WITH");
        script.Add("(");
        script.Add($"DATA_COMPRESSION = {compression}");
        script.Add(")");
    }
    script.Add("GO");
    script.AddRange(GetTableConstraints(conn, fullName));
    script.AddRange(GetTableDefaults(conn, fullName));
    script.AddRange(GetTableChecks(conn, fullName));
    script.AddRange(GetTableForeignKeys(conn, fullName));
    script.AddRange(GetTableGrants(conn, fullName));
    script.AddRange(GetTableExtendedProperties(conn, fullName, schema, name));
    script.Add($"ALTER TABLE {fullName} SET ( LOCK_ESCALATION = AUTO )");
    script.Add("GO");

    return string.Join("\n", script);
}

static (string? DataSpace, string? LobDataSpace) GetTableStorage(SqlConnection conn, string fullName)
{
    var dataSql = @"
SELECT TOP 1 ds.name AS data_space_name
FROM sys.indexes i
JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
WHERE i.object_id = OBJECT_ID(@full) AND i.index_id IN (0,1)
ORDER BY i.index_id DESC
";
    var lobSql = @"
SELECT ds.name AS lob_data_space_name
FROM sys.tables t
LEFT JOIN sys.data_spaces ds ON ds.data_space_id = t.lob_data_space_id
WHERE t.object_id = OBJECT_ID(@full)
";
    string? dataName;
    string? lobName;

    using (var cmd = conn.CreateCommand())
    {
        cmd.CommandText = dataSql;
        cmd.Parameters.Add(Param("@full", SqlDbType.NVarChar, 256, fullName));
        var value = cmd.ExecuteScalar();
        dataName = value == null || value is DBNull ? null : value.ToString();
    }

    using (var cmd = conn.CreateCommand())
    {
        cmd.CommandText = lobSql;
        cmd.Parameters.Add(Param("@full", SqlDbType.NVarChar, 256, fullName));
        var value = cmd.ExecuteScalar();
        lobName = value == null || value is DBNull ? null : value.ToString();
    }

    if (!string.IsNullOrWhiteSpace(lobName) && string.Equals(lobName, dataName, StringComparison.OrdinalIgnoreCase))
    {
        lobName = null;
    }

    return (dataName, lobName);
}

static string GetTableCompression(SqlConnection conn, string fullName)
{
    var sql = @"
SELECT p.data_compression_desc
FROM sys.partitions p
WHERE p.object_id = OBJECT_ID(@full) AND p.index_id IN (0,1)
";
    var rows = QueryRows(conn, sql, Param("@full", SqlDbType.NVarChar, 256, fullName));
    var desc = rows
        .Select(r => r.TryGetValue("data_compression_desc", out var value) ? value?.ToString() : null)
        .Where(v => !string.IsNullOrWhiteSpace(v))
        .ToList();
    if (desc.Contains("PAGE")) return "PAGE";
    if (desc.Contains("ROW")) return "ROW";
    return "NONE";
}

static IEnumerable<string> GetTableConstraints(SqlConnection conn, string fullName)
{
    var sql = @"
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
GROUP BY kc.name, kc.type_desc, i.type_desc, i.object_id, i.index_id, ds.name
ORDER BY kc.name
";
    var rows = QueryRows(conn, sql, Param("@full", SqlDbType.NVarChar, 256, fullName));
    var output = new List<string>();
    foreach (var row in rows)
    {
        var name = row["name"] as string ?? "";
        var typeDesc = row["type_desc"] as string ?? "";
        var indexType = row["index_type_desc"] as string ?? "";
        var dataSpace = row["data_space_name"] as string ?? "";
        var compression = row["data_compression_desc"] as string ?? "";
        var columns = row["columns"] as string ?? "";

        var constraintType = string.Equals(typeDesc, "PRIMARY_KEY_CONSTRAINT", StringComparison.OrdinalIgnoreCase)
            ? "PRIMARY KEY"
            : "UNIQUE";
        var clustered = indexType.Contains("CLUSTERED", StringComparison.OrdinalIgnoreCase) ? "CLUSTERED" : "NONCLUSTERED";
        var with = string.Equals(compression, "PAGE", StringComparison.OrdinalIgnoreCase) ? " WITH (DATA_COMPRESSION = PAGE)" : "";
        var on = string.IsNullOrWhiteSpace(dataSpace) ? "" : $" ON [{dataSpace}]";

        output.Add($"ALTER TABLE {fullName} ADD CONSTRAINT [{name}] {constraintType} {clustered} ({columns}){with}{on}");
        output.Add("GO");
    }
    return output;
}

static IEnumerable<string> GetTableDefaults(SqlConnection conn, string fullName)
{
    var sql = @"
SELECT dc.name, dc.definition, c.name AS column_name
FROM sys.default_constraints dc
JOIN sys.columns c ON c.object_id = dc.parent_object_id AND c.column_id = dc.parent_column_id
WHERE dc.parent_object_id = OBJECT_ID(@full)
ORDER BY dc.name
";
    var rows = QueryRows(conn, sql, Param("@full", SqlDbType.NVarChar, 256, fullName));
    var output = new List<string>();
    foreach (var row in rows)
    {
        output.Add($"ALTER TABLE {fullName} ADD CONSTRAINT [{row["name"]}] DEFAULT {row["definition"]} FOR [{row["column_name"]}]");
        output.Add("GO");
    }
    return output;
}

static IEnumerable<string> GetTableChecks(SqlConnection conn, string fullName)
{
    var sql = @"
SELECT name, definition, is_not_for_replication
FROM sys.check_constraints
WHERE parent_object_id = OBJECT_ID(@full)
ORDER BY name
";
    var rows = QueryRows(conn, sql, Param("@full", SqlDbType.NVarChar, 256, fullName));
    var output = new List<string>();
    foreach (var row in rows)
    {
        var nfr = ToBool(row["is_not_for_replication"]) ? " NOT FOR REPLICATION" : "";
        output.Add($"ALTER TABLE {fullName} ADD CONSTRAINT [{row["name"]}] CHECK{nfr} {row["definition"]}");
        output.Add("GO");
    }
    return output;
}

static IEnumerable<string> GetTableForeignKeys(SqlConnection conn, string fullName)
{
    var sql = @"
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
";
    var rows = QueryRows(conn, sql, Param("@full", SqlDbType.NVarChar, 256, fullName));
    var output = new List<string>();
    foreach (var row in rows)
    {
        var onDelete = row["delete_referential_action_desc"] as string ?? "NO_ACTION";
        var onUpdate = row["update_referential_action_desc"] as string ?? "NO_ACTION";
        var deleteClause = string.Equals(onDelete, "NO_ACTION", StringComparison.OrdinalIgnoreCase) ? "" : $" ON DELETE {onDelete}";
        var updateClause = string.Equals(onUpdate, "NO_ACTION", StringComparison.OrdinalIgnoreCase) ? "" : $" ON UPDATE {onUpdate}";
        output.Add($"ALTER TABLE {fullName} ADD CONSTRAINT [{row["name"]}] FOREIGN KEY ({row["parent_cols"]}) REFERENCES [{row["ref_schema"]}].[{row["ref_table"]}] ({row["ref_cols"]}){deleteClause}{updateClause}");
        output.Add("GO");
    }
    return output;
}

static IEnumerable<string> GetTableGrants(SqlConnection conn, string fullName)
{
    var sql = @"
SELECT dp.permission_name, dp.state_desc, pr.name AS principal_name
FROM sys.database_permissions dp
JOIN sys.database_principals pr ON pr.principal_id = dp.grantee_principal_id
WHERE dp.major_id = OBJECT_ID(@full) AND dp.class_desc = 'OBJECT_OR_COLUMN'
ORDER BY pr.name, dp.permission_name
";
    var rows = QueryRows(conn, sql, Param("@full", SqlDbType.NVarChar, 256, fullName));
    var output = new List<string>();
    foreach (var row in rows)
    {
        var permission = row["permission_name"] as string ?? "";
        var state = row["state_desc"] as string ?? "";
        var principal = row["principal_name"] as string ?? "";
        if (string.Equals(state, "GRANT_WITH_GRANT_OPTION", StringComparison.OrdinalIgnoreCase))
        {
            output.Add($"GRANT {permission} ON  {fullName} TO [{principal}] WITH GRANT OPTION");
        }
        else
        {
            output.Add($"GRANT {permission} ON  {fullName} TO [{principal}]");
        }
        output.Add("GO");
    }
    return output;
}

static IEnumerable<string> GetTableExtendedProperties(SqlConnection conn, string fullName, string schema, string name)
{
    var tableSql = @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
WHERE ep.class_desc = 'OBJECT_OR_COLUMN' AND ep.major_id = OBJECT_ID(@full) AND ep.minor_id = 0
ORDER BY ep.name
";
    var colSql = @"
SELECT ep.name AS prop_name, ep.value AS prop_value, c.name AS column_name
FROM sys.extended_properties ep
JOIN sys.columns c ON c.object_id = ep.major_id AND c.column_id = ep.minor_id
WHERE ep.class_desc = 'OBJECT_OR_COLUMN' AND ep.major_id = OBJECT_ID(@full) AND ep.minor_id <> 0
ORDER BY c.name, ep.name
";
    var tableRows = QueryRows(conn, tableSql, Param("@full", SqlDbType.NVarChar, 256, fullName));
    var colRows = QueryRows(conn, colSql, Param("@full", SqlDbType.NVarChar, 256, fullName));
    var output = new List<string>();

    foreach (var row in tableRows)
    {
        var propName = EscapeLiteral(row["prop_name"] as string ?? "");
        var propValue = EscapeLiteral(row["prop_value"] as string ?? "");
        output.Add($"EXEC sp_addextendedproperty N'{propName}', N'{propValue}', 'SCHEMA', N'{schema}', 'TABLE', N'{name}', NULL, NULL");
        output.Add("GO");
    }

    foreach (var row in colRows)
    {
        var propName = EscapeLiteral(row["prop_name"] as string ?? "");
        var propValue = EscapeLiteral(row["prop_value"] as string ?? "");
        var column = row["column_name"] as string ?? "";
        output.Add($"EXEC sp_addextendedproperty N'{propName}', N'{propValue}', 'SCHEMA', N'{schema}', 'TABLE', N'{name}', 'COLUMN', N'{column}'");
        output.Add("GO");
    }

    return output;
}

static IEnumerable<string> GetObjectExtendedProperties(SqlConnection conn, string schema, string name, string levelType)
{
    var fullName = $"[{schema}].[{name}]";
    var sql = @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
WHERE ep.class_desc = 'OBJECT_OR_COLUMN' AND ep.major_id = OBJECT_ID(@full) AND ep.minor_id = 0
ORDER BY ep.name
";
    var rows = QueryRows(conn, sql, Param("@full", SqlDbType.NVarChar, 256, fullName));
    var output = new List<string>();
    foreach (var row in rows)
    {
        var propName = EscapeLiteral(row["prop_name"] as string ?? "");
        var propValue = EscapeLiteral(row["prop_value"] as string ?? "");
        output.Add($"EXEC sp_addextendedproperty N'{propName}', N'{propValue}', 'SCHEMA', N'{schema}', '{levelType}', N'{name}', NULL, NULL");
        output.Add("GO");
    }
    return output;
}

static List<Dictionary<string, object?>> QueryRows(SqlConnection conn, string sql, params SqlParameter[] parameters)
{
    using var cmd = conn.CreateCommand();
    cmd.CommandText = sql;
    if (parameters.Length > 0)
    {
        cmd.Parameters.AddRange(parameters);
    }

    var rows = new List<Dictionary<string, object?>>();
    using var reader = cmd.ExecuteReader();
    while (reader.Read())
    {
        var row = new Dictionary<string, object?>(StringComparer.OrdinalIgnoreCase);
        for (var i = 0; i < reader.FieldCount; i++)
        {
            var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
            row[reader.GetName(i)] = value;
        }
        rows.Add(row);
    }
    return rows;
}

static SqlParameter Param(string name, SqlDbType type, int size, object value)
{
    return new SqlParameter(name, type, size) { Value = value };
}

static string FormatTypeName(string typeName, string typeSchema, bool isUserDefined, int maxLength, int precision, int scale)
{
    var baseName = isUserDefined ? $"[{typeSchema}].[{typeName}]" : $"[{typeName}]";
    switch (typeName.ToLowerInvariant())
    {
        case "varchar":
            return $"{baseName} ({(maxLength < 0 ? "MAX" : maxLength.ToString(CultureInfo.InvariantCulture))})";
        case "char":
            return $"{baseName} ({maxLength.ToString(CultureInfo.InvariantCulture)})";
        case "varbinary":
            return $"{baseName} ({(maxLength < 0 ? "MAX" : maxLength.ToString(CultureInfo.InvariantCulture))})";
        case "binary":
            return $"{baseName} ({maxLength.ToString(CultureInfo.InvariantCulture)})";
        case "nvarchar":
            return $"{baseName} ({(maxLength < 0 ? "MAX" : (maxLength / 2).ToString(CultureInfo.InvariantCulture))})";
        case "nchar":
            return $"{baseName} ({(maxLength / 2).ToString(CultureInfo.InvariantCulture)})";
        case "decimal":
        case "numeric":
            return $"{baseName} ({precision.ToString(CultureInfo.InvariantCulture)}, {scale.ToString(CultureInfo.InvariantCulture)})";
        case "datetime2":
        case "datetimeoffset":
        case "time":
            return $"{baseName} ({scale.ToString(CultureInfo.InvariantCulture)})";
        default:
            return baseName;
    }
}

static string ToOnOff(object? value, bool defaultOn)
{
    if (value == null) return defaultOn ? "ON" : "OFF";
    var intValue = ToInt(value);
    return intValue == 1 ? "ON" : "OFF";
}

static bool ToBool(object? value)
{
    if (value == null) return false;
    if (value is bool b) return b;
    return ToInt(value) == 1;
}

static int ToInt(object? value)
{
    if (value == null) return 0;
    if (value is int i) return i;
    if (value is short s) return s;
    if (value is byte b) return b;
    if (value is long l) return (int)l;
    if (value is decimal d) return (int)d;
    if (value is string str && int.TryParse(str, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed))
    {
        return parsed;
    }
    return Convert.ToInt32(value, CultureInfo.InvariantCulture);
}

static string ToInvariant(object? value)
{
    if (value == null || value is DBNull) return "0";
    return Convert.ToString(value, CultureInfo.InvariantCulture) ?? "0";
}

static string EscapeLiteral(string value)
{
    return value.Replace("'", "''");
}

static void WriteUtf8NoBom(string path, string content)
{
    var utf8 = new UTF8Encoding(false);
    var normalized = System.Text.RegularExpressions.Regex.Replace(content, "\r?\n", "\r\n");
    if (!normalized.EndsWith("\r\n", StringComparison.Ordinal))
    {
        normalized += "\r\n";
    }
    File.WriteAllText(path, normalized, utf8);
}

record ObjectEntry(string Folder, string FileName, string Schema, string Name, string RelativePath);
