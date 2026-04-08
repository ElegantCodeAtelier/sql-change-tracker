using Microsoft.Data.SqlClient;
using SqlChangeTracker.Schema;
using System.Text;
using System.Linq;
using System.Text.RegularExpressions;
using System.Globalization;

namespace SqlChangeTracker.Sql;

internal class SqlServerScripter
{
    private static readonly Regex ReferenceColumnTypeTokenRegex = new(
        @"^\s*\[(?<name>[^\]]+)\]\s+(?<type>(?:\[[^\]]+\](?:\.\[[^\]]+\])?|\w+)(?:\s*\([^)]*\))?)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex SysQualifiedCompatibilityTypeTokenRegex = new(
        @"\[sys\]\.\[(?<name>[^\]]+)\]",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    private static readonly Regex ComputedColumnLineRegex = new(
        @"^\s*\[[^\]]+\]\s+AS\s+",
        RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    private enum TablePostCreateStatementKind
    {
        KeyConstraint,
        NonConstraintIndex,
        XmlIndex
    }

    private sealed record TablePostCreateStatement(
        TablePostCreateStatementKind Kind,
        string Name,
        List<string> Lines);

    private readonly record struct TableStorageInfo(
        string? DataSpace,
        string? PartitionColumn,
        string? LobDataSpace);

    private readonly record struct IndexScriptingOptions(
        string Compression,
        string? PartitionColumn,
        bool StatisticsIncremental);

    private sealed record TableDataColumn(
        int ColumnId,
        string Name,
        string BaseTypeName,
        bool IsIdentity,
        bool IsUnicodeText,
        bool IsSpatial,
        string? SpatialKind,
        string ValueAlias,
        string? SrsIdAlias);

    public virtual string ScriptObject(SqlConnectionOptions options, DbObjectInfo obj)
    {
        return ScriptObject(options, obj, null);
    }

    public virtual string ScriptObject(SqlConnectionOptions options, DbObjectInfo obj, string? referencePath)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();
        var referenceLines = TryReadReferenceLines(referencePath);

        return obj.ObjectType switch
        {
            "Table" => ScriptTable(connection, obj, referenceLines),
            "StoredProcedure" => ScriptModule(connection, obj, true, referenceLines),
            "View" => ScriptView(connection, obj, referenceLines),
            "Function" => ScriptModule(connection, obj, true, referenceLines),
            "Sequence" => ScriptSequence(connection, obj, referenceLines),
            "Schema" => ScriptSchema(connection, obj, referenceLines),
            "Role" => ScriptRole(connection, obj, referenceLines),
            "User" => ScriptUser(connection, obj, referenceLines),
            "PartitionFunction" => ScriptPartitionFunction(connection, obj, referenceLines),
            "PartitionScheme" => ScriptPartitionScheme(connection, obj, referenceLines),
            "Synonym" => ScriptSynonym(connection, obj, referenceLines),
            "UserDefinedType" => ScriptUserDefinedType(connection, obj, referenceLines),
            "TableType" => ScriptTableType(connection, obj),
            "XmlSchemaCollection" => ScriptXmlSchemaCollection(connection, obj, referenceLines),
            "MessageType" => ScriptMessageType(connection, obj, referenceLines),
            "Contract" => ScriptContract(connection, obj, referenceLines),
            "Queue" => ScriptQueue(connection, obj, referenceLines),
            "Service" => ScriptService(connection, obj, referenceLines),
            "Route" => ScriptRoute(connection, obj, referenceLines),
            "EventNotification" => ScriptEventNotification(connection, obj, referenceLines),
            "ServiceBinding" => ScriptServiceBinding(connection, obj, referenceLines),
            "FullTextCatalog" => ScriptFullTextCatalog(connection, obj, referenceLines),
            "FullTextStoplist" => ScriptFullTextStoplist(connection, obj, referenceLines),
            "SearchPropertyList" => ScriptSearchPropertyList(connection, obj, referenceLines),
            "SecurityPolicy" => ScriptSecurityPolicy(connection, obj),
            "ExternalDataSource" => ScriptExternalDataSource(connection, obj),
            "ExternalFileFormat" => ScriptExternalFileFormat(connection, obj),
            "ExternalTable" => ScriptExternalTable(connection, obj),
            "Certificate" => ScriptCertificate(connection, obj),
            "SymmetricKey" => ScriptSymmetricKey(connection, obj),
            "AsymmetricKey" => ScriptAsymmetricKey(connection, obj),
            "Trigger" => ScriptTrigger(connection, obj, referenceLines),
            "Rule" => ScriptRule(connection, obj, referenceLines),
            _ => throw new InvalidOperationException($"Scripting not implemented for type '{obj.ObjectType}'.")
        };
    }

    public virtual string ScriptTableData(SqlConnectionOptions options, ObjectIdentifier table)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();
        return ScriptTableData(connection, table);
    }

    private static string ScriptTableData(SqlConnection connection, ObjectIdentifier table)
    {
        var columns = ReadTableDataColumns(connection, table);
        if (columns.Count == 0)
        {
            if (ReadTableRowCount(connection, table) == 0)
            {
                return string.Empty;
            }

            throw new InvalidOperationException($"Table [{table.Schema}].[{table.Name}] has rows but no insertable columns.");
        }

        var primaryKeyColumns = ReadPrimaryKeyColumns(connection, table);
        var rows = ReadTableDataRows(connection, table, columns, primaryKeyColumns);
        if (rows.Count == 0)
        {
            return string.Empty;
        }

        if (primaryKeyColumns.Count == 0)
        {
            rows = rows
                .OrderBy(row => row.SortKey, StringComparer.Ordinal)
                .ToList();
        }

        var fullName = $"{QuoteIdentifier(table.Schema)}.{QuoteIdentifier(table.Name)}";
        var columnList = string.Join(", ", columns.Select(column => QuoteIdentifier(column.Name)));
        var lines = new List<string>();
        if (columns.Any(column => column.IsIdentity))
        {
            lines.Add($"SET IDENTITY_INSERT {fullName} ON;");
        }

        foreach (var row in rows)
        {
            lines.Add($"INSERT INTO {fullName} ({columnList}) VALUES ({string.Join(", ", row.Literals)});");
        }

        if (columns.Any(column => column.IsIdentity))
        {
            lines.Add($"SET IDENTITY_INSERT {fullName} OFF;");
        }

        return string.Join(Environment.NewLine, lines);
    }

    private static List<TableDataColumn> ReadTableDataColumns(SqlConnection connection, ObjectIdentifier table)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT c.column_id,
       c.name,
       bt.name AS base_type_name,
       c.is_identity,
       CASE WHEN bt.name IN ('nchar','nvarchar','ntext','xml') THEN 1 ELSE 0 END AS is_unicode_text
FROM sys.tables tbl
JOIN sys.schemas sch ON sch.schema_id = tbl.schema_id
JOIN sys.columns c ON c.object_id = tbl.object_id
JOIN sys.types bt ON bt.user_type_id = c.system_type_id AND bt.system_type_id = c.system_type_id
WHERE sch.name = @schema
  AND tbl.name = @name
  AND c.is_computed = 0
  AND c.is_hidden = 0
  AND c.generated_always_type = 0
  AND bt.name NOT IN ('timestamp','rowversion')
ORDER BY c.column_id;";
        command.Parameters.AddWithValue("@schema", table.Schema);
        command.Parameters.AddWithValue("@name", table.Name);

        using var reader = command.ExecuteReader();
        var columns = new List<TableDataColumn>();
        while (reader.Read())
        {
            var columnId = reader.GetInt32(0);
            var name = reader.GetString(1);
            var baseTypeName = reader.GetString(2).ToLowerInvariant();
            var isIdentity = reader.GetBoolean(3);
            var isUnicodeText = reader.GetInt32(4) == 1;
            var isSpatial = baseTypeName is "geometry" or "geography";
            var spatialKind = isSpatial ? baseTypeName : null;
            var valueAlias = $"__sqlct_value_{columnId}";
            var sridAlias = isSpatial ? $"__sqlct_srid_{columnId}" : null;

            columns.Add(new TableDataColumn(
                columnId,
                name,
                baseTypeName,
                isIdentity,
                isUnicodeText,
                isSpatial,
                spatialKind,
                valueAlias,
                sridAlias));
        }

        return columns;
    }

    private static int ReadTableRowCount(SqlConnection connection, ObjectIdentifier table)
    {
        using var command = connection.CreateCommand();
        command.CommandText = $"SELECT COUNT(*) FROM {QuoteIdentifier(table.Schema)}.{QuoteIdentifier(table.Name)};";
        return Convert.ToInt32(command.ExecuteScalar(), CultureInfo.InvariantCulture);
    }

    private static List<string> ReadPrimaryKeyColumns(SqlConnection connection, ObjectIdentifier table)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT c.name
FROM sys.tables tbl
JOIN sys.schemas sch ON sch.schema_id = tbl.schema_id
JOIN sys.key_constraints kc ON kc.parent_object_id = tbl.object_id AND kc.type = 'PK'
JOIN sys.index_columns ic ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE sch.name = @schema
  AND tbl.name = @name
ORDER BY ic.key_ordinal;";
        command.Parameters.AddWithValue("@schema", table.Schema);
        command.Parameters.AddWithValue("@name", table.Name);

        using var reader = command.ExecuteReader();
        var columns = new List<string>();
        while (reader.Read())
        {
            columns.Add(reader.GetString(0));
        }

        return columns;
    }

    private static List<TableDataRow> ReadTableDataRows(
        SqlConnection connection,
        ObjectIdentifier table,
        IReadOnlyList<TableDataColumn> columns,
        IReadOnlyList<string> primaryKeyColumns)
    {
        using var command = connection.CreateCommand();
        command.CommandText = BuildTableDataSelectSql(table, columns, primaryKeyColumns);
        using var reader = command.ExecuteReader();

        var rows = new List<TableDataRow>();
        while (reader.Read())
        {
            var literals = new List<string>(columns.Count);
            foreach (var column in columns)
            {
                var value = reader[column.ValueAlias];
                var srid = column.SrsIdAlias == null ? null : reader[column.SrsIdAlias];
                literals.Add(RenderSqlLiteral(column, value, srid));
            }

            rows.Add(new TableDataRow(literals, string.Join("\u001F", literals)));
        }

        return rows;
    }

    private static string BuildTableDataSelectSql(
        ObjectIdentifier table,
        IReadOnlyList<TableDataColumn> columns,
        IReadOnlyList<string> primaryKeyColumns)
    {
        var selectList = new List<string>(columns.Count * 2);
        foreach (var column in columns)
        {
            var qualifiedName = QuoteIdentifier(column.Name);
            if (string.Equals(column.BaseTypeName, "hierarchyid", StringComparison.OrdinalIgnoreCase))
            {
                selectList.Add($"CASE WHEN {qualifiedName} IS NULL THEN NULL ELSE {qualifiedName}.ToString() END AS {QuoteIdentifier(column.ValueAlias)}");
            }
            else if (column.IsSpatial)
            {
                selectList.Add($"CASE WHEN {qualifiedName} IS NULL THEN NULL ELSE {qualifiedName}.STAsBinary() END AS {QuoteIdentifier(column.ValueAlias)}");
                selectList.Add($"CASE WHEN {qualifiedName} IS NULL THEN NULL ELSE {qualifiedName}.STSrid END AS {QuoteIdentifier(column.SrsIdAlias!)}");
            }
            else
            {
                selectList.Add($"{qualifiedName} AS {QuoteIdentifier(column.ValueAlias)}");
            }
        }

        var orderBy = primaryKeyColumns.Count == 0
            ? string.Empty
            : Environment.NewLine + "ORDER BY " + string.Join(", ", primaryKeyColumns.Select(column => $"{QuoteIdentifier(column)} ASC"));

        return $"""
SELECT {string.Join(", ", selectList)}
FROM {QuoteIdentifier(table.Schema)}.{QuoteIdentifier(table.Name)}
{orderBy};
""";
    }

    private static string RenderSqlLiteral(TableDataColumn column, object value, object? spatialSrid)
    {
        if (value == DBNull.Value || value is null)
        {
            return "NULL";
        }

        return column.BaseTypeName switch
        {
            "bit" => Convert.ToBoolean(value, CultureInfo.InvariantCulture) ? "1" : "0",
            "tinyint" or "smallint" or "int" or "bigint" or "decimal" or "numeric" or "money" or "smallmoney"
                => Convert.ToString(value, CultureInfo.InvariantCulture)!,
            "float" => ((double)value).ToString("R", CultureInfo.InvariantCulture),
            "real" => ((float)value).ToString("R", CultureInfo.InvariantCulture),
            "uniqueidentifier" => $"'{value.ToString()}'",
            "char" or "varchar" or "text" => $"'{EscapeSqlStringLiteral(ReadTextValue(value))}'",
            "nchar" or "nvarchar" or "ntext" or "xml" => $"N'{EscapeSqlStringLiteral(ReadTextValue(value))}'",
            "binary" or "varbinary" or "image" => $"0x{Convert.ToHexString(ReadBinaryValue(value))}",
            "date" => $"'{Convert.ToDateTime(value, CultureInfo.InvariantCulture):yyyy-MM-dd}'",
            "datetime" or "smalldatetime" or "datetime2" => $"'{FormatDateTimeLiteral(Convert.ToDateTime(value, CultureInfo.InvariantCulture))}'",
            "datetimeoffset" => $"'{FormatDateTimeOffsetLiteral((DateTimeOffset)value)}'",
            "time" => $"'{((TimeSpan)value).ToString("hh\\:mm\\:ss\\.fffffff", CultureInfo.InvariantCulture)}'",
            "hierarchyid" => $"hierarchyid::Parse(N'{EscapeSqlStringLiteral(ReadTextValue(value))}')",
            "geometry" or "geography" => $"{column.SpatialKind}::STGeomFromWKB(0x{Convert.ToHexString(ReadBinaryValue(value))}, {Convert.ToInt32(spatialSrid ?? throw new InvalidOperationException($"Missing SRID for spatial column [{column.Name}]."), CultureInfo.InvariantCulture)})",
            _ => throw new InvalidOperationException($"Unsupported insertable data type '{column.BaseTypeName}' on column [{column.Name}].")
        };
    }

    private static string ReadTextValue(object value)
        => value switch
        {
            string s => s,
            System.Data.SqlTypes.SqlString sqlString => sqlString.Value,
            System.Data.SqlTypes.SqlXml sqlXml => sqlXml.Value,
            _ => Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty
        };

    private static byte[] ReadBinaryValue(object value)
        => value switch
        {
            byte[] bytes => bytes,
            System.Data.SqlTypes.SqlBinary sqlBinary => sqlBinary.Value,
            _ => throw new InvalidOperationException($"Unsupported binary runtime type '{value.GetType().FullName}'.")
        };

    private static string FormatDateTimeLiteral(DateTime value)
    {
        var text = value.ToString("yyyy-MM-ddTHH:mm:ss.fffffff", CultureInfo.InvariantCulture);
        return text.TrimEnd('0').TrimEnd('.');
    }

    private static string FormatDateTimeOffsetLiteral(DateTimeOffset value)
    {
        var text = value.ToString("yyyy-MM-ddTHH:mm:ss.fffffffzzz", CultureInfo.InvariantCulture);
        var plusMinus = Math.Max(text.LastIndexOf('+'), text.LastIndexOf('-'));
        if (plusMinus < 0)
        {
            return text;
        }

        var timestamp = text[..plusMinus].TrimEnd('0').TrimEnd('.');
        return timestamp + text[plusMinus..];
    }

    private sealed record TableDataRow(IReadOnlyList<string> Literals, string SortKey);

    private static string ScriptModule(
        SqlConnection connection,
        DbObjectInfo obj,
        bool insertBlankLineAfterSet,
        string[]? referenceLines)
    {
        var fullName = $"[{obj.Schema}].[{obj.Name}]";
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT m.definition, m.uses_ansi_nulls, m.uses_quoted_identifier
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
JOIN sys.sql_modules m ON m.object_id = o.object_id
WHERE s.name = @schema AND o.name = @name";
        command.Parameters.AddWithValue("@schema", obj.Schema);
        command.Parameters.AddWithValue("@name", obj.Name);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException($"Object not found: [{obj.Schema}].[{obj.Name}].");
        }

        var definitionText = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
        var ansiNulls = reader.IsDBNull(1) || reader.GetBoolean(1);
        var quotedIdentifier = reader.IsDBNull(2) || reader.GetBoolean(2);
        reader.Close();

        var (lines, hasGoAfterDefinition) = BuildProgrammableObjectLines(
            definitionText,
            ansiNulls,
            quotedIdentifier,
            insertBlankLineAfterSet,
            referenceLines);
        lines.AddRange(ReadModuleGrants(connection, fullName, referenceLines));
        var moduleProperties = ReadModuleExtendedProperties(connection, obj, referenceLines).ToList();
        if (moduleProperties.Count > 0)
        {
            var blankBeforeProps = CountBlankLinesBeforeExtendedProperties(referenceLines);
            for (var i = 0; i < blankBeforeProps; i++)
            {
                lines.Add(string.Empty);
            }

            lines.AddRange(moduleProperties);
        }
        if (hasGoAfterDefinition)
        {
            AppendTrailingBlankLinesExact(lines, referenceLines);
        }

        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptView(
        SqlConnection connection,
        DbObjectInfo obj,
        string[]? referenceLines)
    {
        var fullName = $"[{obj.Schema}].[{obj.Name}]";
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT m.definition, m.uses_ansi_nulls, m.uses_quoted_identifier
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
JOIN sys.sql_modules m ON m.object_id = o.object_id
WHERE s.name = @schema AND o.name = @name";
        command.Parameters.AddWithValue("@schema", obj.Schema);
        command.Parameters.AddWithValue("@name", obj.Name);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException($"Object not found: [{obj.Schema}].[{obj.Name}].");
        }

        var definitionText = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
        var ansiNulls = reader.IsDBNull(1) || reader.GetBoolean(1);
        var quotedIdentifier = reader.IsDBNull(2) || reader.GetBoolean(2);
        reader.Close();

        var (lines, hasGoAfterDefinition) = BuildProgrammableObjectLines(
            definitionText,
            ansiNulls,
            quotedIdentifier,
            insertBlankLineAfterSet: true,
            referenceLines);

        lines.AddRange(ReadIndexSetOptions(referenceLines));
        lines.AddRange(ReadViewIndexes(connection, fullName, referenceLines));
        lines.AddRange(ReadModuleGrants(connection, fullName, referenceLines));
        var viewProperties = ReadModuleExtendedProperties(connection, obj, referenceLines).ToList();
        if (viewProperties.Count > 0)
        {
            var blankBeforeProps = CountBlankLinesBeforeExtendedProperties(referenceLines);
            for (var i = 0; i < blankBeforeProps; i++)
            {
                lines.Add(string.Empty);
            }

            lines.AddRange(viewProperties);
        }

        if (hasGoAfterDefinition)
        {
            AppendTrailingBlankLinesExact(lines, referenceLines);
        }

        return string.Join(Environment.NewLine, lines);
    }

    private static (List<string> Lines, bool HasGoAfterDefinition) BuildProgrammableObjectLines(
        string definitionText,
        bool ansiNulls,
        bool quotedIdentifier,
        bool insertBlankLineAfterSet,
        string[]? referenceLines)
    {
        var definition = ApplyDefinitionFormatting(definitionText, referenceLines);
        var format = GetModuleFormat(referenceLines);
        var leadingBlankLines = insertBlankLineAfterSet && format != null ? format.LeadingBlankLines : 0;
        var blankBeforeGo = insertBlankLineAfterSet && format != null ? format.BlankLineBeforeGo : 0;
        var hasGoAfterDefinition = ReferenceHasGoAfterDefinition(referenceLines);

        if (insertBlankLineAfterSet && format != null && !string.IsNullOrEmpty(format.DefinitionIndentPrefix))
        {
            definition = ApplyDefinitionIndent(definition, format.DefinitionIndentPrefix);
        }

        definition = TrimOuterBlankLines(definition);

        var quotedLine = $"SET QUOTED_IDENTIFIER {(quotedIdentifier ? "ON" : "OFF")}";
        var ansiLine = $"SET ANSI_NULLS {(ansiNulls ? "ON" : "OFF")}";
        if (referenceLines != null)
        {
            if (referenceLines.Any(line => string.Equals(line.Trim(), quotedLine + ";", StringComparison.OrdinalIgnoreCase)))
            {
                quotedLine += ";";
            }

            if (referenceLines.Any(line => string.Equals(line.Trim(), ansiLine + ";", StringComparison.OrdinalIgnoreCase)))
            {
                ansiLine += ";";
            }
        }

        var lines = BuildSetHeaderLines(referenceLines, quotedLine, ansiLine);
        for (var i = 0; i < leadingBlankLines; i++)
        {
            lines.Add(string.Empty);
        }

        var definitionLines = definition.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None);
        if (hasGoAfterDefinition && blankBeforeGo == 0)
        {
            definitionLines = TrimTrailingBlankLines(definitionLines);
        }

        foreach (var line in definitionLines)
        {
            lines.Add(line);
        }

        if (hasGoAfterDefinition)
        {
            var trailingBlankLines = CountTrailingBlankLinesInDefinition(definitionLines);
            var adjustedBlankBeforeGo = Math.Max(0, blankBeforeGo - trailingBlankLines);
            for (var i = 0; i < adjustedBlankBeforeGo; i++)
            {
                lines.Add(string.Empty);
            }

            lines.Add("GO");
        }

        return (lines, hasGoAfterDefinition);
    }

    private static string ScriptSequence(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT s.name AS schema_name, seq.name AS sequence_name,
       t.name AS type_name, ts.name AS type_schema, t.is_user_defined,
       seq.start_value, seq.increment, seq.minimum_value, seq.maximum_value,
       seq.is_cycling, seq.is_cached, seq.cache_size
FROM sys.sequences seq
JOIN sys.schemas s ON s.schema_id = seq.schema_id
JOIN sys.types t ON t.user_type_id = seq.user_type_id
JOIN sys.schemas ts ON ts.schema_id = t.schema_id
WHERE s.name = @schema AND seq.name = @name";
        command.Parameters.AddWithValue("@schema", obj.Schema);
        command.Parameters.AddWithValue("@name", obj.Name);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException($"Sequence not found: [{obj.Schema}].[{obj.Name}].");
        }

        var schemaName = reader.GetString(0);
        var sequenceName = reader.GetString(1);
        var typeNameRaw = reader.GetString(2);
        var typeSchema = reader.GetString(3);
        var isUserDefined = reader.GetBoolean(4);
        var startValue = reader.GetValue(5);
        var increment = reader.GetValue(6);
        var minValue = reader.GetValue(7);
        var maxValue = reader.GetValue(8);
        var isCycling = reader.GetBoolean(9);
        var isCached = reader.GetBoolean(10);
        var cacheSize = reader.GetValue(11);

        var typeName = isUserDefined ? $"[{typeSchema}].[{typeNameRaw}]" : typeNameRaw;
        var cycle = isCycling ? "CYCLE" : "NO CYCLE";
        var cache = isCached
            ? cacheSize is DBNull ? "CACHE " : $"CACHE {cacheSize}"
            : "NO CACHE";
        reader.Close();

        var lines = new List<string>
        {
            $"CREATE SEQUENCE [{schemaName}].[{sequenceName}]",
            $"AS {typeName}",
            $"START WITH {startValue}",
            $"INCREMENT BY {increment}",
            $"MINVALUE {minValue}",
            $"MAXVALUE {maxValue}",
            cycle,
            cache,
            "GO"
        };

        AppendExtendedPropertyLines(lines, ReadSequenceExtendedProperties(connection, schemaName, sequenceName, referenceLines), referenceLines);
        AppendTrailingBlankLines(lines, referenceLines);
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptSchema(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT s.name, dp.name AS owner_name
FROM sys.schemas s
LEFT JOIN sys.database_principals dp ON dp.principal_id = s.principal_id
WHERE s.name = @name";
        command.Parameters.AddWithValue("@name", obj.Name);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException($"Schema not found: [{obj.Name}].");
        }

        var schemaName = reader.GetString(0);
        var ownerName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
        var lines = new List<string> { $"CREATE SCHEMA [{schemaName}]" };
        if (!string.IsNullOrWhiteSpace(ownerName))
        {
            lines.Add($"AUTHORIZATION [{ownerName}]");
        }
        reader.Close();
        lines.Add("GO");
        AppendExtendedPropertyLines(lines, ReadSchemaExtendedProperties(connection, schemaName, referenceLines), referenceLines);
        AppendTrailingBlankLines(lines, referenceLines);
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptRole(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT dp.name, owner.name AS owner_name, dp.is_fixed_role
FROM sys.database_principals dp
LEFT JOIN sys.database_principals owner ON owner.principal_id = dp.owning_principal_id
WHERE dp.type = 'R' AND dp.name = @name";
        command.Parameters.AddWithValue("@name", obj.Name);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException($"Role not found: [{obj.Name}].");
        }

        var roleName = reader.GetString(0);
        var ownerName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
        var isFixedRole = reader.GetBoolean(2);
        reader.Close();

        var lines = new List<string>();
        if (!isFixedRole)
        {
            lines.Add($"CREATE ROLE [{roleName}]");
            if (!string.IsNullOrWhiteSpace(ownerName))
            {
                lines.Add($"AUTHORIZATION [{ownerName}]");
            }

            lines.Add("GO");
        }

        lines.AddRange(ReadRoleMembershipStatements(connection, roleName));
        if (lines.Count == 0)
        {
            throw new InvalidOperationException($"Role has no scriptable definition: [{roleName}].");
        }

        AppendExtendedPropertyLines(lines, ReadPrincipalExtendedProperties(connection, roleName, referenceLines), referenceLines);
        AppendTrailingBlankLines(lines, referenceLines);
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptUser(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT dp.name,
       dp.default_schema_name,
       dp.authentication_type_desc,
       dp.type_desc,
       login_principal.name AS login_name,
       cert.name AS certificate_name,
       asym_key.name AS asymmetric_key_name
FROM sys.database_principals dp
LEFT JOIN sys.server_principals login_principal ON login_principal.sid = dp.sid
LEFT JOIN sys.certificates cert ON cert.sid = dp.sid
LEFT JOIN sys.asymmetric_keys asym_key ON asym_key.sid = dp.sid
WHERE dp.name = @name";
        command.Parameters.AddWithValue("@name", obj.Name);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException($"User not found: [{obj.Name}].");
        }

        var userName = reader.GetString(0);
        var defaultSchema = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
        var authType = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
        var typeDesc = reader.IsDBNull(3) ? string.Empty : reader.GetString(3);
        var loginName = reader.IsDBNull(4) ? string.Empty : reader.GetString(4);
        var certificateName = reader.IsDBNull(5) ? string.Empty : reader.GetString(5);
        var asymmetricKeyName = reader.IsDBNull(6) ? string.Empty : reader.GetString(6);
        var defaultSchemaClause = BuildUserDefaultSchemaClause(defaultSchema);
        reader.Close();

        var lines = new List<string>();

        if (string.Equals(typeDesc, "CERTIFICATE_MAPPED_USER", StringComparison.OrdinalIgnoreCase))
        {
            if (string.IsNullOrWhiteSpace(certificateName))
            {
                throw new InvalidOperationException($"Certificate-mapped user is missing certificate metadata: [{userName}].");
            }

            lines.Add($"CREATE USER [{userName}] FOR CERTIFICATE [{certificateName}]");
            lines.Add("GO");
            AppendExtendedPropertyLines(lines, ReadPrincipalExtendedProperties(connection, userName, referenceLines), referenceLines);
            AppendTrailingBlankLines(lines, referenceLines);
            return string.Join(Environment.NewLine, lines);
        }

        if (string.Equals(typeDesc, "ASYMMETRIC_KEY_MAPPED_USER", StringComparison.OrdinalIgnoreCase))
        {
            if (string.IsNullOrWhiteSpace(asymmetricKeyName))
            {
                throw new InvalidOperationException($"Asymmetric-key-mapped user is missing asymmetric key metadata: [{userName}].");
            }

            lines.Add($"CREATE USER [{userName}] FOR ASYMMETRIC KEY [{asymmetricKeyName}]");
            lines.Add("GO");
            AppendExtendedPropertyLines(lines, ReadPrincipalExtendedProperties(connection, userName, referenceLines), referenceLines);
            AppendTrailingBlankLines(lines, referenceLines);
            return string.Join(Environment.NewLine, lines);
        }

        if (string.Equals(authType, "NONE", StringComparison.OrdinalIgnoreCase))
        {
            lines.Add($"CREATE USER [{userName}] WITHOUT LOGIN{defaultSchemaClause}");
            lines.Add("GO");
            AppendExtendedPropertyLines(lines, ReadPrincipalExtendedProperties(connection, userName, referenceLines), referenceLines);
            AppendTrailingBlankLines(lines, referenceLines);
            return string.Join(Environment.NewLine, lines);
        }

        if (string.Equals(authType, "EXTERNAL", StringComparison.OrdinalIgnoreCase) &&
            string.IsNullOrWhiteSpace(loginName))
        {
            lines.Add($"CREATE USER [{userName}] FROM EXTERNAL PROVIDER{defaultSchemaClause}");
            lines.Add("GO");
            AppendExtendedPropertyLines(lines, ReadPrincipalExtendedProperties(connection, userName, referenceLines), referenceLines);
            AppendTrailingBlankLines(lines, referenceLines);
            return string.Join(Environment.NewLine, lines);
        }

        if (string.Equals(authType, "DATABASE", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException($"Contained database users with password metadata are not supported: [{userName}].");
        }

        var effectiveLoginName = string.IsNullOrWhiteSpace(loginName) ? userName : loginName;
        lines.Add($"CREATE USER [{userName}] FOR LOGIN [{effectiveLoginName}]{defaultSchemaClause}");
        lines.Add("GO");
        AppendExtendedPropertyLines(lines, ReadPrincipalExtendedProperties(connection, userName, referenceLines), referenceLines);
        AppendTrailingBlankLines(lines, referenceLines);
        return string.Join(Environment.NewLine, lines);
    }

    private static IEnumerable<string> ReadRoleMembershipStatements(SqlConnection connection, string roleName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT member_principal.name
FROM sys.database_role_members drm
JOIN sys.database_principals role_principal ON role_principal.principal_id = drm.role_principal_id
JOIN sys.database_principals member_principal ON member_principal.principal_id = drm.member_principal_id
WHERE role_principal.name = @roleName
  AND member_principal.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys')
ORDER BY member_principal.name;";
        command.Parameters.AddWithValue("@roleName", roleName);

        var lines = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var memberName = reader.GetString(0);
            lines.Add($"EXEC sp_addrolemember N'{EscapeSqlStringLiteral(roleName)}', N'{EscapeSqlStringLiteral(memberName)}'");
            lines.Add("GO");
        }

        return lines;
    }

    private static string BuildUserDefaultSchemaClause(string defaultSchema)
        => string.IsNullOrWhiteSpace(defaultSchema) ||
           string.Equals(defaultSchema, "dbo", StringComparison.OrdinalIgnoreCase)
            ? string.Empty
            : $" WITH DEFAULT_SCHEMA=[{defaultSchema}]";

    private static string ScriptPartitionFunction(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT pf.name, pf.function_id, pf.boundary_value_on_right, t.name AS type_name, ts.name AS type_schema, t.is_user_defined
FROM sys.partition_functions pf
JOIN sys.partition_parameters pp ON pp.function_id = pf.function_id AND pp.parameter_id = 1
JOIN sys.types t ON t.user_type_id = pp.user_type_id
JOIN sys.schemas ts ON ts.schema_id = t.schema_id
WHERE pf.name = @name";
        command.Parameters.AddWithValue("@name", obj.Name);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException($"Partition function not found: [{obj.Name}].");
        }

        var name = reader.GetString(0);
        var functionId = reader.GetInt32(1);
        var boundaryRight = reader.GetBoolean(2);
        var typeNameRaw = reader.GetString(3);
        var typeSchema = reader.GetString(4);
        var isUserDefined = reader.GetBoolean(5);
        reader.Close();

        var format = GetPartitionFunctionFormat(referenceLines);
        var bracketSystemType = format == null || format.BracketSystemType;
        var typeName = isUserDefined
            ? $"[{typeSchema}].[{typeNameRaw}]"
            : (bracketSystemType ? $"[{typeNameRaw}]" : typeNameRaw);
        var rangeSide = boundaryRight ? "RIGHT" : "LEFT";

        command.CommandText = @"
SELECT value
FROM sys.partition_range_values rv
WHERE rv.function_id = @functionId
ORDER BY rv.boundary_id";
        command.Parameters.Clear();
        command.Parameters.AddWithValue("@functionId", functionId);

        var values = new List<string>();
        using (var valueReader = command.ExecuteReader())
        {
            while (valueReader.Read())
            {
                values.Add(valueReader.GetValue(0).ToString() ?? string.Empty);
            }
        }

        var valuesList = values.Count == 0 ? string.Empty : string.Join(", ", values);
        var nameSegment = format != null && format.SpaceBeforeParen ? $"[{name}] " : $"[{name}]";
        var lines = new List<string>();
        if (format != null && format.MultiLine)
        {
            var line1 = $"CREATE PARTITION FUNCTION {nameSegment}({typeName})";
            if (format.Line1TrailingSpace)
            {
                line1 += " ";
            }

            var line2 = $"AS RANGE {rangeSide}";
            if (format.Line2TrailingSpace)
            {
                line2 += " ";
            }

            lines.Add(line1);
            lines.Add(line2);
            lines.Add($"FOR VALUES ({valuesList})");
        }
        else
        {
            var line = $"CREATE PARTITION FUNCTION {nameSegment}({typeName}) AS RANGE {rangeSide} FOR VALUES ({valuesList})";
            if (format != null && format.Line1TrailingSpace)
            {
                line += " ";
            }

            lines.Add(line);
        }

        lines.Add("GO");
        AppendExtendedPropertyLines(lines, ReadPartitionFunctionExtendedProperties(connection, name, referenceLines), referenceLines);
        AppendTrailingBlankLines(lines, referenceLines);
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptPartitionScheme(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT ps.name, pf.name AS function_name
FROM sys.partition_schemes ps
JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
WHERE ps.name = @name";
        command.Parameters.AddWithValue("@name", obj.Name);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException($"Partition scheme not found: [{obj.Name}].");
        }

        var schemeName = reader.GetString(0);
        var functionName = reader.GetString(1);
        reader.Close();

        command.CommandText = @"
SELECT data_space_id
FROM sys.partition_schemes
WHERE name = @schemeName;";
        command.Parameters.Clear();
        command.Parameters.AddWithValue("@schemeName", schemeName);
        var dataSpaceId = command.ExecuteScalar();
        if (dataSpaceId is DBNull || dataSpaceId is null)
        {
            var emptyLines = BuildPartitionSchemeLines(schemeName, functionName, string.Empty, referenceLines);
            AppendExtendedPropertyLines(emptyLines, ReadPartitionSchemeExtendedProperties(connection, schemeName, referenceLines), referenceLines);
            AppendTrailingBlankLines(emptyLines, referenceLines);
            return string.Join(Environment.NewLine, emptyLines);
        }

        command.CommandText = @"
SELECT fg.name
FROM sys.destination_data_spaces dds
JOIN sys.filegroups fg ON fg.data_space_id = dds.data_space_id
WHERE dds.partition_scheme_id = @dataSpaceId
ORDER BY dds.destination_id";
        command.Parameters.Clear();
        command.Parameters.AddWithValue("@dataSpaceId", dataSpaceId);

        var groups = new List<string>();
        using (var groupReader = command.ExecuteReader())
        {
            while (groupReader.Read())
            {
                groups.Add($"[{groupReader.GetString(0)}]");
            }
        }

        var groupList = groups.Count == 0 ? string.Empty : string.Join(", ", groups);
        var lines = BuildPartitionSchemeLines(schemeName, functionName, groupList, referenceLines);
        AppendExtendedPropertyLines(lines, ReadPartitionSchemeExtendedProperties(connection, schemeName, referenceLines), referenceLines);
        AppendTrailingBlankLines(lines, referenceLines);
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptSynonym(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT base_object_name
FROM sys.synonyms syn
JOIN sys.schemas s ON s.schema_id = syn.schema_id
WHERE s.name = @schema AND syn.name = @name;";
        command.Parameters.AddWithValue("@schema", obj.Schema);
        command.Parameters.AddWithValue("@name", obj.Name);

        var baseObject = command.ExecuteScalar() as string;
        if (string.IsNullOrWhiteSpace(baseObject))
        {
            throw new InvalidOperationException($"Synonym not found: [{obj.Schema}].[{obj.Name}].");
        }

        var lines = new List<string>
        {
            $"CREATE SYNONYM [{obj.Schema}].[{obj.Name}] FOR {baseObject}",
            "GO"
        };

        AppendExtendedPropertyLines(lines, ReadSynonymExtendedProperties(connection, obj.Schema, obj.Name, referenceLines), referenceLines);
        AppendTrailingBlankLines(lines, referenceLines);
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptUserDefinedType(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT t.name AS type_name, s.name AS schema_name, t.max_length, t.precision, t.scale, t.is_nullable,
       bt.name AS base_type_name, bts.name AS base_schema_name
FROM sys.types t
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.types bt ON bt.user_type_id = t.system_type_id AND bt.is_user_defined = 0
JOIN sys.schemas bts ON bts.schema_id = bt.schema_id
WHERE t.is_user_defined = 1 AND t.is_table_type = 0 AND s.name = @schema AND t.name = @name;";
        command.Parameters.AddWithValue("@schema", obj.Schema);
        command.Parameters.AddWithValue("@name", obj.Name);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException($"User-defined type not found: [{obj.Schema}].[{obj.Name}].");
        }

        var typeName = reader.GetString(0);
        var schemaName = reader.GetString(1);
        var maxLength = reader.GetInt16(2);
        var precision = reader.GetByte(3);
        var scale = reader.GetByte(4);
        var isNullable = reader.GetBoolean(5);
        var baseTypeName = reader.GetString(6);
        var baseSchemaName = reader.GetString(7);

        var baseType = FormatTypeName(baseTypeName, baseSchemaName, false, maxLength, precision, scale);
        var nullable = isNullable ? "NULL" : "NOT NULL";
        reader.Close();

        var lines = new List<string>
        {
            $"CREATE TYPE [{schemaName}].[{typeName}] FROM {baseType} {nullable}",
            "GO"
        };

        AppendExtendedPropertyLines(lines, ReadUserDefinedTypeExtendedProperties(connection, schemaName, typeName, referenceLines), referenceLines);
        AppendTrailingBlankLines(lines, referenceLines);
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptTableType(SqlConnection connection, DbObjectInfo obj)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT tt.name, s.name AS schema_name, tt.type_table_object_id
FROM sys.table_types tt
JOIN sys.schemas s ON s.schema_id = tt.schema_id
WHERE s.name = @schema AND tt.name = @name;";
        command.Parameters.AddWithValue("@schema", obj.Schema);
        command.Parameters.AddWithValue("@name", obj.Name);

        var fullName = $"{QuoteIdentifier(obj.Schema)}.{QuoteIdentifier(obj.Name)}";
        int tableObjectId;
        using (var reader = command.ExecuteReader())
        {
            if (!reader.Read())
            {
                throw new InvalidOperationException($"Table type not found: {fullName}.");
            }

            tableObjectId = reader.GetInt32(2);
        }

        var entries = new List<string>();
        entries.AddRange(ReadTableColumns(connection, tableObjectId, null));
        entries.AddRange(ReadTableTypeCheckConstraints(connection, tableObjectId));
        entries.AddRange(ReadTableTypeKeyConstraints(connection, tableObjectId));

        var lines = new List<string>
        {
            $"CREATE TYPE {fullName} AS TABLE",
            "("
        };

        for (var i = 0; i < entries.Count; i++)
        {
            var suffix = i < entries.Count - 1 ? "," : string.Empty;
            lines.Add(entries[i] + suffix);
        }

        lines.Add(")");
        lines.Add("GO");
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptXmlSchemaCollection(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT x.name, s.name AS schema_name,
       CONVERT(nvarchar(max), XML_SCHEMA_NAMESPACE(s.name, x.name)) AS definition
FROM sys.xml_schema_collections x
JOIN sys.schemas s ON s.schema_id = x.schema_id
WHERE s.name = @schema AND x.name = @name;";
        command.Parameters.AddWithValue("@schema", obj.Schema);
        command.Parameters.AddWithValue("@name", obj.Name);

        string name;
        string schema;
        string definition;
        using (var reader = command.ExecuteReader())
        {
            if (!reader.Read())
            {
                throw new InvalidOperationException($"XML schema collection not found: [{obj.Schema}].[{obj.Name}].");
            }

            name = reader.GetString(0);
            schema = reader.GetString(1);
            definition = reader.IsDBNull(2) ? string.Empty : reader.GetString(2).Trim();
        }

        var lines = new List<string>
        {
            $"CREATE XML SCHEMA COLLECTION [{schema}].[{name}] AS {definition}",
            "GO"
        };
        lines.AddRange(ReadXmlSchemaCollectionPermissions(connection, schema, name, referenceLines));
        AppendExtendedPropertyLines(lines, ReadXmlSchemaCollectionExtendedProperties(connection, schema, name, referenceLines), referenceLines);
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptMessageType(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT mt.name,
       owner_principal.name AS owner_name,
       mt.validation_desc,
       xss.name AS xml_collection_schema,
       xsc.name AS xml_collection_name
FROM sys.service_message_types mt
LEFT JOIN sys.database_principals owner_principal ON owner_principal.principal_id = mt.principal_id
LEFT JOIN sys.xml_schema_collections xsc ON xsc.xml_collection_id = mt.xml_collection_id
LEFT JOIN sys.schemas xss ON xss.schema_id = xsc.schema_id
WHERE mt.name = @name;";
        command.Parameters.AddWithValue("@name", obj.Name);

        string name;
        string ownerName;
        string validation;
        string? xmlCollectionSchema;
        string? xmlCollectionName;
        using (var reader = command.ExecuteReader())
        {
            if (!reader.Read())
            {
                throw new InvalidOperationException($"Message type not found: {QuoteIdentifier(obj.Name)}.");
            }

            name = reader.GetString(0);
            ownerName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
            validation = reader.GetString(2);
            xmlCollectionSchema = reader.IsDBNull(3) ? null : reader.GetString(3);
            xmlCollectionName = reader.IsDBNull(4) ? null : reader.GetString(4);
        }
        var validationKeyword = string.Equals(validation, "BINARY", StringComparison.OrdinalIgnoreCase)
            ? "NONE"
            : validation;

        var validationClause = !string.IsNullOrWhiteSpace(xmlCollectionSchema) && !string.IsNullOrWhiteSpace(xmlCollectionName)
            ? $"VALIDATION = VALID_XML WITH SCHEMA COLLECTION {QuoteIdentifier(xmlCollectionSchema)}.{QuoteIdentifier(xmlCollectionName)}"
            : $"VALIDATION = {validationKeyword}";

        var lines = new List<string> { $"CREATE MESSAGE TYPE {QuoteIdentifier(name)}" };
        if (!string.IsNullOrWhiteSpace(ownerName))
        {
            lines.Add($"AUTHORIZATION {QuoteIdentifier(ownerName)}");
        }

        lines.Add(validationClause);
        lines.Add("GO");
        lines.AddRange(ReadMessageTypePermissions(connection, name, referenceLines));
        AppendExtendedPropertyLines(lines, ReadMessageTypeExtendedProperties(connection, name, referenceLines), referenceLines);
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptContract(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT c.service_contract_id, c.name, owner_principal.name AS owner_name
FROM sys.service_contracts c
LEFT JOIN sys.database_principals owner_principal ON owner_principal.principal_id = c.principal_id
WHERE c.name = @name;";
        command.Parameters.AddWithValue("@name", obj.Name);

        int contractId;
        string name;
        string ownerName;
        using (var reader = command.ExecuteReader())
        {
            if (!reader.Read())
            {
                throw new InvalidOperationException($"Contract not found: {QuoteIdentifier(obj.Name)}.");
            }

            contractId = reader.GetInt32(0);
            name = reader.GetString(1);
            ownerName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
        }

        command.CommandText = @"
SELECT mt.name, scu.is_sent_by_initiator, scu.is_sent_by_target
FROM sys.service_contract_message_usages scu
JOIN sys.service_message_types mt ON mt.message_type_id = scu.message_type_id
WHERE scu.service_contract_id = @contractId
ORDER BY mt.name;";
        command.Parameters.Clear();
        command.Parameters.AddWithValue("@contractId", contractId);

        var items = new List<string>();
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                var messageName = reader.GetString(0);
                var sentByInitiator = reader.GetBoolean(1);
                var sentByTarget = reader.GetBoolean(2);
                var sentBy = sentByInitiator && sentByTarget
                    ? "ANY"
                    : sentByInitiator ? "INITIATOR" : "TARGET";
                items.Add($"{QuoteIdentifier(messageName)} SENT BY {sentBy}");
            }
        }

        var lines = new List<string> { $"CREATE CONTRACT {QuoteIdentifier(name)}" };
        if (!string.IsNullOrWhiteSpace(ownerName))
        {
            lines.Add($"AUTHORIZATION {QuoteIdentifier(ownerName)}");
        }

        lines.Add("(");
        for (var i = 0; i < items.Count; i++)
        {
            var suffix = i < items.Count - 1 ? "," : string.Empty;
            lines.Add(items[i] + suffix);
        }

        lines.Add(")");
        lines.Add("GO");
        lines.AddRange(ReadContractPermissions(connection, name, referenceLines));
        AppendExtendedPropertyLines(lines, ReadContractExtendedProperties(connection, name, referenceLines), referenceLines);
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptQueue(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT q.name,
       s.name AS schema_name,
       q.is_enqueue_enabled,
       q.is_retention_enabled,
       q.is_poison_message_handling_enabled,
       q.is_activation_enabled,
       q.activation_procedure,
       q.max_readers,
       q.execute_as_principal_id,
       dp.name AS execute_as_name,
       storage.data_space_name
FROM sys.service_queues q
JOIN sys.objects o ON o.object_id = q.object_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
LEFT JOIN sys.database_principals dp ON dp.principal_id = q.execute_as_principal_id
OUTER APPLY (
    SELECT TOP (1) ds.name AS data_space_name
    FROM sys.indexes i
    JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
    WHERE i.object_id = q.object_id
      AND i.index_id IN (0, 1)
    ORDER BY i.index_id DESC
) storage
WHERE s.name = @schema
  AND q.name = @name;";
        command.Parameters.AddWithValue("@schema", obj.Schema);
        command.Parameters.AddWithValue("@name", obj.Name);

        string name;
        string schema;
        string status;
        string retention;
        string poison;
        bool activationEnabled;
        string activationProcedure;
        int maxReaders;
        int? executeAsPrincipalId;
        string executeAsName;
        string dataSpaceName;
        using (var reader = command.ExecuteReader())
        {
            if (!reader.Read())
            {
                throw new InvalidOperationException($"Queue not found: {QuoteIdentifier(obj.Schema)}.{QuoteIdentifier(obj.Name)}.");
            }

            name = reader.GetString(0);
            schema = reader.GetString(1);
            status = reader.GetBoolean(2) ? "ON" : "OFF";
            retention = reader.GetBoolean(3) ? "ON" : "OFF";
            poison = reader.GetBoolean(4) ? "ON" : "OFF";
            activationEnabled = reader.GetBoolean(5);
            activationProcedure = reader.IsDBNull(6) ? string.Empty : reader.GetString(6);
            maxReaders = reader.IsDBNull(7) ? 0 : Convert.ToInt32(reader.GetValue(7), System.Globalization.CultureInfo.InvariantCulture);
            executeAsPrincipalId = reader.IsDBNull(8) ? (int?)null : Convert.ToInt32(reader.GetValue(8), CultureInfo.InvariantCulture);
            executeAsName = reader.IsDBNull(9) ? string.Empty : reader.GetString(9);
            dataSpaceName = reader.IsDBNull(10) ? string.Empty : reader.GetString(10);
        }

        var options = new List<string>
        {
            $"STATUS = {status}",
            $"RETENTION = {retention}",
            $"POISON_MESSAGE_HANDLING (STATUS = {poison})"
        };

        if (activationEnabled || !string.IsNullOrWhiteSpace(activationProcedure) || maxReaders > 0 || executeAsPrincipalId.HasValue)
        {
            var activation = new List<string> { $"STATUS = {(activationEnabled ? "ON" : "OFF")}" };
            if (!string.IsNullOrWhiteSpace(activationProcedure))
            {
                activation.Add($"PROCEDURE_NAME = {activationProcedure}");
            }

            if (maxReaders > 0)
            {
                activation.Add($"MAX_QUEUE_READERS = {maxReaders}");
            }

            var executeAsClause = executeAsPrincipalId switch
            {
                -2 => "OWNER",
                _ when !string.IsNullOrWhiteSpace(executeAsName) => $"'{EscapeSqlStringLiteral(executeAsName)}'",
                _ => string.Empty
            };

            if (!string.IsNullOrWhiteSpace(executeAsClause))
            {
                activation.Add($"EXECUTE AS {executeAsClause}");
            }

            options.Add($"ACTIVATION ({string.Join(", ", activation)})");
        }

        var lines = new List<string> { $"CREATE QUEUE {QuoteIdentifier(schema)}.{QuoteIdentifier(name)}" };
        if (options.Count > 0)
        {
            lines.Add($"WITH {string.Join(", ", options)}");
        }

        if (!string.IsNullOrWhiteSpace(dataSpaceName))
        {
            lines.Add($"ON {QuoteIdentifier(dataSpaceName)}");
        }

        lines.Add("GO");
        var fullName = $"{QuoteIdentifier(schema)}.{QuoteIdentifier(name)}";
        lines.AddRange(ReadSchemaScopedObjectPermissions(connection, fullName, referenceLines));
        AppendExtendedPropertyLines(lines, ReadQueueExtendedProperties(connection, schema, name, referenceLines), referenceLines);
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptService(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT sv.service_id,
       sv.name,
       owner_principal.name AS owner_name,
       q.name AS queue_name,
       qs.name AS queue_schema
FROM sys.services sv
LEFT JOIN sys.database_principals owner_principal ON owner_principal.principal_id = sv.principal_id
JOIN sys.service_queues q ON q.object_id = sv.service_queue_id
JOIN sys.objects qo ON qo.object_id = q.object_id
JOIN sys.schemas qs ON qs.schema_id = qo.schema_id
WHERE sv.name = @name;";
        command.Parameters.AddWithValue("@name", obj.Name);

        int serviceId;
        string name;
        string ownerName;
        string queueName;
        string queueSchema;
        using (var reader = command.ExecuteReader())
        {
            if (!reader.Read())
            {
                throw new InvalidOperationException($"Service not found: {QuoteIdentifier(obj.Name)}.");
            }

            serviceId = reader.GetInt32(0);
            name = reader.GetString(1);
            ownerName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
            queueName = reader.GetString(3);
            queueSchema = reader.GetString(4);
        }

        command.CommandText = @"
SELECT c.name
FROM sys.service_contract_usages scu
JOIN sys.service_contracts c ON c.service_contract_id = scu.service_contract_id
WHERE scu.service_id = @serviceId;";
        command.Parameters.Clear();
        command.Parameters.AddWithValue("@serviceId", serviceId);

        var contracts = new List<string>();
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                contracts.Add(QuoteIdentifier(reader.GetString(0)));
            }
        }

        var lines = new List<string> { $"CREATE SERVICE {QuoteIdentifier(name)}" };
        if (!string.IsNullOrWhiteSpace(ownerName))
        {
            lines.Add($"AUTHORIZATION {QuoteIdentifier(ownerName)}");
        }

        var queueLine = $"ON QUEUE {QuoteIdentifier(queueSchema)}.{QuoteIdentifier(queueName)}";
        if (contracts.Count > 0)
        {
            queueLine += $" ({string.Join(", ", contracts)})";
        }

        lines.Add(queueLine);
        lines.Add("GO");
        lines.AddRange(ReadServicePermissions(connection, name, referenceLines));
        AppendExtendedPropertyLines(lines, ReadServiceExtendedProperties(connection, name, referenceLines), referenceLines);
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptRoute(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT r.name,
       owner_principal.name AS owner_name,
       r.remote_service_name,
       r.broker_instance,
       r.lifetime,
       r.address,
       r.mirror_address
FROM sys.routes r
LEFT JOIN sys.database_principals owner_principal ON owner_principal.principal_id = r.principal_id
WHERE r.name = @name;";
        command.Parameters.AddWithValue("@name", obj.Name);

        string name;
        string ownerName;
        string serviceName;
        string brokerInstance;
        long? lifetime;
        string address;
        string mirrorAddress;
        using (var reader = command.ExecuteReader())
        {
            if (!reader.Read())
            {
                throw new InvalidOperationException($"Route not found: {QuoteIdentifier(obj.Name)}.");
            }

            name = reader.GetString(0);
            ownerName = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
            serviceName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
            brokerInstance = reader.IsDBNull(3) ? string.Empty : reader.GetString(3);
            lifetime = reader.IsDBNull(4) ? (long?)null : Convert.ToInt64(reader.GetValue(4), CultureInfo.InvariantCulture);
            address = reader.IsDBNull(5) ? string.Empty : reader.GetString(5);
            mirrorAddress = reader.IsDBNull(6) ? string.Empty : reader.GetString(6);
        }

        var options = new List<string>();
        if (!string.IsNullOrWhiteSpace(serviceName))
        {
            options.Add($"SERVICE_NAME = '{serviceName.Replace("'", "''", StringComparison.Ordinal)}'");
        }
        if (!string.IsNullOrWhiteSpace(brokerInstance))
        {
            options.Add($"BROKER_INSTANCE = '{brokerInstance}'");
        }

        if (lifetime.HasValue)
        {
            options.Add($"LIFETIME = {lifetime.Value.ToString(CultureInfo.InvariantCulture)}");
        }

        if (!string.IsNullOrWhiteSpace(address))
        {
            options.Add($"ADDRESS = '{address.Replace("'", "''", StringComparison.Ordinal)}'");
        }

        if (!string.IsNullOrWhiteSpace(mirrorAddress))
        {
            options.Add($"MIRROR_ADDRESS = '{mirrorAddress.Replace("'", "''", StringComparison.Ordinal)}'");
        }

        var lines = new List<string> { $"CREATE ROUTE {QuoteIdentifier(name)}" };
        if (!string.IsNullOrWhiteSpace(ownerName))
        {
            lines.Add($"AUTHORIZATION {QuoteIdentifier(ownerName)}");
        }

        lines.Add($"WITH {string.Join(", ", options)}");
        lines.Add("GO");
        lines.AddRange(ReadRoutePermissions(connection, name, referenceLines));
        AppendExtendedPropertyLines(lines, ReadRouteExtendedProperties(connection, name, referenceLines), referenceLines);
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptEventNotification(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT en.object_id,
       en.name,
       en.parent_class_desc,
       en.parent_id,
       en.service_name,
       en.broker_instance
FROM sys.event_notifications en
WHERE en.name = @name;";
        command.Parameters.AddWithValue("@name", obj.Name);

        int objectId;
        string name;
        string parentClass;
        var parentId = 0;
        string serviceName;
        string brokerInstance;
        using (var reader = command.ExecuteReader())
        {
            if (!reader.Read())
            {
                throw new InvalidOperationException($"Event notification not found: {QuoteIdentifier(obj.Name)}.");
            }

            objectId = reader.GetInt32(0);
            name = reader.GetString(1);
            parentClass = reader.GetString(2);
            if (!reader.IsDBNull(3))
            {
                parentId = reader.GetInt32(3);
            }

            serviceName = reader.GetString(4);
            brokerInstance = reader.IsDBNull(5) ? string.Empty : reader.GetString(5);
        }

        command.CommandText = @"
SELECT DISTINCT e.type_desc
FROM sys.events e
WHERE e.object_id = @objectId
  AND e.type_desc IS NOT NULL
ORDER BY e.type_desc;";
        command.Parameters.Clear();
        command.Parameters.AddWithValue("@objectId", objectId);

        var events = new List<string>();
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                events.Add(reader.GetString(0));
            }
        }

        if (events.Count == 0)
        {
            throw new InvalidOperationException($"Event notification {QuoteIdentifier(obj.Name)} does not expose any event metadata.");
        }

        var scope = parentClass switch
        {
            "DATABASE" => "ON DATABASE",
            "SERVER" => "ON SERVER",
            "QUEUE" => $"ON QUEUE {ResolveQueueName(connection, parentId)}",
            _ => "ON DATABASE"
        };

        var brokerClause = string.IsNullOrWhiteSpace(brokerInstance)
            ? string.Empty
            : $", N'{EscapeSqlStringLiteral(brokerInstance)}'";
        var lines = new List<string>
        {
            $"CREATE EVENT NOTIFICATION {QuoteIdentifier(name)} {scope} FOR {string.Join(", ", events)} TO SERVICE N'{EscapeSqlStringLiteral(serviceName)}'{brokerClause}",
            "GO"
        };
        AppendExtendedPropertyLines(lines, ReadEventNotificationExtendedProperties(connection, name, referenceLines), referenceLines);
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptServiceBinding(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT rsb.name, rsb.remote_service_name, dp.name AS user_name, rsb.is_anonymous_on
FROM sys.remote_service_bindings rsb
LEFT JOIN sys.database_principals dp ON dp.principal_id = rsb.remote_principal_id
WHERE rsb.name = @name;";
        command.Parameters.AddWithValue("@name", obj.Name);

        string name;
        string remoteService;
        string userName;
        bool isAnonymous;
        using (var reader = command.ExecuteReader())
        {
            if (!reader.Read())
            {
                throw new InvalidOperationException($"Remote service binding not found: {QuoteIdentifier(obj.Name)}.");
            }

            name = reader.GetString(0);
            remoteService = reader.GetString(1);
            userName = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
            isAnonymous = reader.GetBoolean(3);
        }

        if (string.IsNullOrWhiteSpace(userName))
        {
            throw new InvalidOperationException($"Remote service binding {QuoteIdentifier(obj.Name)} is missing remote principal metadata.");
        }

        var options = new List<string> { $"USER = {QuoteIdentifier(userName)}" };
        if (isAnonymous)
        {
            options.Add("ANONYMOUS = ON");
        }

        var lines = new List<string>
        {
            $"CREATE REMOTE SERVICE BINDING {QuoteIdentifier(name)} TO SERVICE N'{EscapeSqlStringLiteral(remoteService)}' WITH {string.Join(", ", options)}",
            "GO"
        };
        lines.AddRange(ReadServiceBindingPermissions(connection, name, referenceLines));
        AppendExtendedPropertyLines(lines, ReadServiceBindingExtendedProperties(connection, name, referenceLines), referenceLines);
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptFullTextCatalog(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT name, is_default, is_accent_sensitivity_on
FROM sys.fulltext_catalogs
WHERE name = @name;";
        command.Parameters.AddWithValue("@name", obj.Name);

        string name;
        bool isDefault;
        string accent;
        using (var reader = command.ExecuteReader())
        {
            if (!reader.Read())
            {
                throw new InvalidOperationException($"Full-text catalog not found: [{obj.Name}].");
            }

            name = reader.GetString(0);
            isDefault = reader.GetBoolean(1);
            accent = reader.GetBoolean(2) ? "ON" : "OFF";
        }
        var defaultClause = isDefault ? " AS DEFAULT" : string.Empty;

        var lines = new List<string>
        {
            $"CREATE FULLTEXT CATALOG {QuoteIdentifier(name)}{defaultClause} WITH ACCENT_SENSITIVITY = {accent}",
            "GO"
        };
        lines.AddRange(ReadFullTextCatalogPermissions(connection, name, referenceLines));
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptFullTextStoplist(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT stoplist_id, name
FROM sys.fulltext_stoplists
WHERE name = @name
  AND stoplist_id > 0;";
        command.Parameters.AddWithValue("@name", obj.Name);

        int stoplistId;
        string name;
        using (var reader = command.ExecuteReader())
        {
            if (!reader.Read())
            {
                throw new InvalidOperationException($"Full-text stoplist not found: [{obj.Name}].");
            }

            stoplistId = reader.GetInt32(0);
            name = reader.GetString(1);
        }

        command.CommandText = @"
  SELECT sw.stopword, sw.language_id
  FROM sys.fulltext_stopwords sw
  WHERE sw.stoplist_id = @stoplistId
  ORDER BY sw.language_id, sw.stopword;";
        command.Parameters.Clear();
        command.Parameters.AddWithValue("@stoplistId", stoplistId);

        var lines = new List<string>
        {
            $"CREATE FULLTEXT STOPLIST {QuoteIdentifier(name)}",
            "GO"
        };

        using (var stopwordReader = command.ExecuteReader())
        {
            while (stopwordReader.Read())
            {
                var stopword = stopwordReader.GetString(0);
                var languageId = stopwordReader.GetInt32(1);
                lines.Add($"ALTER FULLTEXT STOPLIST {QuoteIdentifier(name)} ADD '{EscapeSqlStringLiteral(stopword)}' LANGUAGE {languageId.ToString(CultureInfo.InvariantCulture)}");
                lines.Add("GO");
            }
        }

        lines.AddRange(ReadFullTextStoplistPermissions(connection, name, referenceLines));
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptSearchPropertyList(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT property_list_id, name
FROM sys.registered_search_property_lists
WHERE name = @name;";
        command.Parameters.AddWithValue("@name", obj.Name);

        int propertyListId;
        string name;
        using (var reader = command.ExecuteReader())
        {
            if (!reader.Read())
            {
                throw new InvalidOperationException($"Search property list not found: [{obj.Name}].");
            }

            propertyListId = reader.GetInt32(0);
            name = reader.GetString(1);
        }

        command.CommandText = @"
SELECT property_name, property_set_guid, property_int_id, property_description
FROM sys.registered_search_properties
WHERE property_list_id = @propertyListId
ORDER BY property_name, property_set_guid, property_int_id;";
        command.Parameters.Clear();
        command.Parameters.AddWithValue("@propertyListId", propertyListId);

        var lines = new List<string>
        {
            $"CREATE SEARCH PROPERTY LIST {QuoteIdentifier(name)}",
            "GO"
        };

        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                var propertyName = reader.GetString(0);
                var propertySetGuid = reader.GetGuid(1).ToString().ToUpperInvariant();
                var propertyIntId = reader.GetInt32(2);
                var propertyDescription = reader.IsDBNull(3) ? string.Empty : reader.GetString(3);

                var options = new List<string>
                {
                    $"PROPERTY_SET_GUID = '{propertySetGuid}'",
                    $"PROPERTY_INT_ID = {propertyIntId.ToString(CultureInfo.InvariantCulture)}"
                };

                if (!string.IsNullOrWhiteSpace(propertyDescription))
                {
                    options.Add($"PROPERTY_DESCRIPTION = N'{EscapeSqlStringLiteral(propertyDescription)}'");
                }

                lines.Add(
                    $"ALTER SEARCH PROPERTY LIST {QuoteIdentifier(name)} ADD N'{EscapeSqlStringLiteral(propertyName)}' WITH ({string.Join(", ", options)})");
                lines.Add("GO");
            }
        }

        lines.AddRange(ReadSearchPropertyListPermissions(connection, name, referenceLines));
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptSecurityPolicy(SqlConnection connection, DbObjectInfo obj)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT p.object_id, p.name, s.name AS schema_name, p.is_enabled
FROM sys.security_policies p
JOIN sys.schemas s ON s.schema_id = p.schema_id
WHERE s.name = @schema AND p.name = @name;";
        command.Parameters.AddWithValue("@schema", obj.Schema);
        command.Parameters.AddWithValue("@name", obj.Name);

        int policyId;
        string name;
        string schema;
        bool enabled;
        using (var reader = command.ExecuteReader())
        {
            if (!reader.Read())
            {
                throw new InvalidOperationException($"Security policy not found: [{obj.Schema}].[{obj.Name}].");
            }
            policyId = reader.GetInt32(0);
            name = reader.GetString(1);
            schema = reader.GetString(2);
            enabled = reader.GetBoolean(3);
        }

        command.CommandText = @"
SELECT predicate_type_desc, predicate_definition, target_object_id
FROM sys.security_predicates
WHERE security_policy_id = @policyId
ORDER BY predicate_type_desc;";
        command.Parameters.Clear();
        command.Parameters.AddWithValue("@policyId", policyId);

        var predicates = new List<string>();
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                var type = reader.GetString(0);
                var definition = reader.GetString(1).Trim();
                var targetId = reader.GetInt32(2);
                var targetName = ResolveObjectName(connection, targetId);
                predicates.Add($"ADD {type} PREDICATE {definition} ON {targetName}");
            }
        }

        var state = enabled ? "ON" : "OFF";
        var body = predicates.Count == 0 ? string.Empty : $" {string.Join(", ", predicates)}";
        return $"CREATE SECURITY POLICY [{schema}].[{name}]{body} WITH (STATE = {state}){Environment.NewLine}GO";
    }

    private static string ScriptExternalDataSource(SqlConnection connection, DbObjectInfo obj)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT ds.name, ds.type_desc, ds.location, ds.resource_manager_location, ds.database_name, ds.shard_map_name,
       c.name AS credential_name
FROM sys.external_data_sources ds
LEFT JOIN sys.database_scoped_credentials c ON c.credential_id = ds.credential_id
JOIN sys.schemas s ON s.schema_id = ds.schema_id
WHERE s.name = @schema AND ds.name = @name;";
        command.Parameters.AddWithValue("@schema", obj.Schema);
        command.Parameters.AddWithValue("@name", obj.Name);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException($"External data source not found: [{obj.Schema}].[{obj.Name}].");
        }

        var name = reader.GetString(0);
        var type = reader.GetString(1);
        var location = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
        var resource = reader.IsDBNull(3) ? string.Empty : reader.GetString(3);
        var database = reader.IsDBNull(4) ? string.Empty : reader.GetString(4);
        var shardMap = reader.IsDBNull(5) ? string.Empty : reader.GetString(5);
        var credential = reader.IsDBNull(6) ? string.Empty : reader.GetString(6);

        var options = new List<string> { $"TYPE = {type}" };
        if (!string.IsNullOrWhiteSpace(location))
        {
            options.Add($"LOCATION = '{location.Replace("'", "''", StringComparison.Ordinal)}'");
        }
        if (!string.IsNullOrWhiteSpace(resource))
        {
            options.Add($"RESOURCE_MANAGER_LOCATION = '{resource.Replace("'", "''", StringComparison.Ordinal)}'");
        }
        if (!string.IsNullOrWhiteSpace(database))
        {
            options.Add($"DATABASE_NAME = '{database.Replace("'", "''", StringComparison.Ordinal)}'");
        }
        if (!string.IsNullOrWhiteSpace(shardMap))
        {
            options.Add($"SHARD_MAP_NAME = '{shardMap.Replace("'", "''", StringComparison.Ordinal)}'");
        }
        if (!string.IsNullOrWhiteSpace(credential))
        {
            options.Add($"CREDENTIAL = [{credential}]");
        }

        return $"CREATE EXTERNAL DATA SOURCE [{obj.Schema}].[{name}] WITH ({string.Join(", ", options)}){Environment.NewLine}GO";
    }

    private static string ScriptExternalFileFormat(SqlConnection connection, DbObjectInfo obj)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT ff.name, ff.format_type_desc, ff.data_compression_desc
FROM sys.external_file_formats ff
JOIN sys.schemas s ON s.schema_id = ff.schema_id
WHERE s.name = @schema AND ff.name = @name;";
        command.Parameters.AddWithValue("@schema", obj.Schema);
        command.Parameters.AddWithValue("@name", obj.Name);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException($"External file format not found: [{obj.Schema}].[{obj.Name}].");
        }

        var name = reader.GetString(0);
        var type = reader.GetString(1);
        var compression = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);

        var options = new List<string> { $"FORMAT_TYPE = {type}" };
        if (!string.IsNullOrWhiteSpace(compression))
        {
            options.Add($"DATA_COMPRESSION = {compression}");
        }

        return $"CREATE EXTERNAL FILE FORMAT [{obj.Schema}].[{name}] WITH ({string.Join(", ", options)}){Environment.NewLine}GO";
    }

    private static string ScriptExternalTable(SqlConnection connection, DbObjectInfo obj)
    {
        var fullName = $"[{obj.Schema}].[{obj.Name}]";
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT et.location, ds.name AS data_source_name, ff.name AS file_format_name
FROM sys.external_tables et
JOIN sys.external_data_sources ds ON ds.data_source_id = et.data_source_id
JOIN sys.external_file_formats ff ON ff.file_format_id = et.file_format_id
WHERE et.object_id = OBJECT_ID(@full);";
        command.Parameters.AddWithValue("@full", fullName);

        string location;
        string dataSource;
        string fileFormat;
        using (var reader = command.ExecuteReader())
        {
            if (!reader.Read())
            {
                throw new InvalidOperationException($"External table not found: {fullName}.");
            }

            location = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
            dataSource = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
            fileFormat = reader.IsDBNull(2) ? string.Empty : reader.GetString(2);
        }

        var columns = ReadTableColumns(connection, fullName, null);
        var lines = new List<string>
        {
            $"CREATE EXTERNAL TABLE {fullName}",
            "("
        };

        for (var i = 0; i < columns.Count; i++)
        {
            var suffix = i < columns.Count - 1 ? "," : string.Empty;
            lines.Add(columns[i] + suffix);
        }

        lines.Add(")");
        var options = new List<string>();
        if (!string.IsNullOrWhiteSpace(location))
        {
            options.Add($"LOCATION = '{location.Replace("'", "''", StringComparison.Ordinal)}'");
        }
        if (!string.IsNullOrWhiteSpace(dataSource))
        {
            options.Add($"DATA_SOURCE = [{dataSource}]");
        }
        if (!string.IsNullOrWhiteSpace(fileFormat))
        {
            options.Add($"FILE_FORMAT = [{fileFormat}]");
        }

        lines.Add($"WITH ({string.Join(", ", options)})");
        lines.Add("GO");
        return string.Join(Environment.NewLine, lines);
    }

    private static string ScriptCertificate(SqlConnection connection, DbObjectInfo obj)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT name, subject
FROM sys.certificates
WHERE name = @name;";
        command.Parameters.AddWithValue("@name", obj.Name);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException($"Certificate not found: [{obj.Name}].");
        }

        var name = reader.GetString(0);
        var subject = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
        var subjectClause = string.IsNullOrWhiteSpace(subject)
            ? string.Empty
            : $" WITH SUBJECT = '{subject.Replace("'", "''", StringComparison.Ordinal)}'";

        return $"CREATE CERTIFICATE [{name}]{subjectClause}{Environment.NewLine}GO";
    }

    private static string ScriptSymmetricKey(SqlConnection connection, DbObjectInfo obj)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT name, algorithm_desc, key_length
FROM sys.symmetric_keys
WHERE name = @name;";
        command.Parameters.AddWithValue("@name", obj.Name);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException($"Symmetric key not found: [{obj.Name}].");
        }

        var name = reader.GetString(0);
        var algorithm = reader.GetString(1);
        var length = reader.IsDBNull(2) ? 0 : reader.GetInt32(2);
        var lengthClause = length > 0 ? $", KEY_LENGTH = {length}" : string.Empty;

        return $"CREATE SYMMETRIC KEY [{name}] WITH ALGORITHM = {algorithm}{lengthClause}{Environment.NewLine}GO";
    }

    private static string ScriptAsymmetricKey(SqlConnection connection, DbObjectInfo obj)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT name, algorithm_desc
FROM sys.asymmetric_keys
WHERE name = @name;";
        command.Parameters.AddWithValue("@name", obj.Name);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            throw new InvalidOperationException($"Asymmetric key not found: [{obj.Name}].");
        }

        var name = reader.GetString(0);
        var algorithm = reader.GetString(1);
        return $"CREATE ASYMMETRIC KEY [{name}] WITH ALGORITHM = {algorithm}{Environment.NewLine}GO";
    }

    private static string ScriptTrigger(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
        => ScriptModule(connection, obj, true, referenceLines);

    private static string ScriptRule(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
        => ScriptModule(connection, obj, true, referenceLines);

    private static string ScriptTable(SqlConnection connection, DbObjectInfo obj, string[]? referenceLines)
    {
        var fullName = $"[{obj.Schema}].[{obj.Name}]";
        var columns = ReadTableColumns(connection, fullName, referenceLines);
        if (columns.Count == 0)
        {
            throw new InvalidOperationException($"Table not found: {fullName}.");
        }

        var storage = ReadTableStorage(connection, fullName);
        var compression = ReadTableCompression(connection, fullName);

        var lines = new List<string>();
        lines.AddRange(ReadLeadingSetOptions(referenceLines));
        var createTableBlock = BuildTableCreateBlock(fullName, columns, storage, referenceLines);
        var referenceCreateBlock = TryGetCompatibleReferenceCreateTableBlock(referenceLines, createTableBlock);
        if (referenceCreateBlock != null)
        {
            lines.AddRange(referenceCreateBlock);
        }
        else
        {
            lines.AddRange(createTableBlock);
        }

        if (!string.Equals(compression, "NONE", StringComparison.OrdinalIgnoreCase))
        {
            lines.Add("WITH");
            lines.Add("(");
            lines.Add($"DATA_COMPRESSION = {compression}");
            lines.Add(")");
        }

        lines.Add("GO");

        lines.AddRange(ReadTableTriggers(connection, fullName, referenceLines));
        lines.AddRange(ReadTableChecks(connection, fullName, referenceLines));
        lines.AddRange(ReadIndexSetOptions(referenceLines));
        var keyConstraintLines = ReadTableKeyConstraints(connection, fullName, referenceLines).ToList();
        var nonConstraintIndexLines = ReadNonConstraintIndexes(connection, fullName, referenceLines).ToList();
        var xmlIndexLines = ReadTableXmlIndexes(connection, fullName).ToList();
        lines.AddRange(ReorderTableKeyAndIndexStatements(referenceLines, keyConstraintLines, nonConstraintIndexLines, xmlIndexLines));
        lines.AddRange(ReadTableForeignKeys(connection, fullName));
        lines.AddRange(ReadTableGrants(connection, fullName));
        lines.AddRange(ReadTableExtendedProperties(connection, obj.Schema, obj.Name, referenceLines));
        lines.AddRange(ReadTableFullTextIndexes(connection, fullName));

        var lockEscalation = ReadTableLockEscalation(connection, fullName);
        var lockLines = ReadLockEscalationStatements(referenceLines, fullName);
        if (lockLines.Count > 0)
        {
            lines.AddRange(lockLines);
        }
        else if (!string.IsNullOrWhiteSpace(lockEscalation) &&
            !string.Equals(lockEscalation, "TABLE", StringComparison.OrdinalIgnoreCase))
        {
            lines.Add($"ALTER TABLE {fullName} SET ( LOCK_ESCALATION = {lockEscalation} )");
            lines.Add("GO");
        }

        lines.AddRange(ReadTrailingSetOptions(referenceLines));

        return string.Join(Environment.NewLine, lines);
    }

    private static List<string> BuildTableCreateBlock(
        string fullName,
        IReadOnlyList<string> columns,
        TableStorageInfo storage,
        string[]? referenceLines)
    {
        var lines = new List<string>
        {
            $"CREATE TABLE {fullName}",
            "("
        };

        for (var i = 0; i < columns.Count; i++)
        {
            var suffix = i < columns.Count - 1 ? "," : string.Empty;
            lines.Add(columns[i] + suffix);
        }

        var onLine = BuildTableStorageLine(storage.DataSpace, storage.PartitionColumn, storage.LobDataSpace);

        var referenceOnLine = TryGetCompatibleReferenceTableOnLine(referenceLines, onLine);
        lines.Add(referenceOnLine ?? onLine);
        return lines;
    }

    internal static string BuildTableStorageLine(string? dataSpace, string? partitionColumn, string? lobDataSpace)
    {
        var line = ")" + BuildIndexOnClause(dataSpace, partitionColumn);
        if (!string.IsNullOrWhiteSpace(lobDataSpace))
        {
            line += $" TEXTIMAGE_ON [{lobDataSpace}]";
        }

        return line;
    }

    internal static string BuildIndexOnClause(string? dataSpace, string? partitionColumn)
    {
        if (string.IsNullOrWhiteSpace(dataSpace))
        {
            return string.Empty;
        }

        var clause = $" ON [{dataSpace}]";
        if (!string.IsNullOrWhiteSpace(partitionColumn))
        {
            clause += $" ([{partitionColumn}])";
        }

        return clause;
    }

    internal static string BuildIndexWithClause(string compression, bool statisticsIncremental)
    {
        var options = new List<string>();
        if (statisticsIncremental)
        {
            options.Add("STATISTICS_INCREMENTAL=ON");
        }

        if (!string.IsNullOrWhiteSpace(compression) &&
            !string.Equals(compression, "NONE", StringComparison.OrdinalIgnoreCase))
        {
            options.Add($"DATA_COMPRESSION = {compression}");
        }

        return options.Count == 0
            ? string.Empty
            : $" WITH ({string.Join(", ", options)})";
    }

    private static string? TryGetCompatibleReferenceTableOnLine(string[]? referenceLines, string generatedOnLine)
    {
        var referenceOnLine = TryGetReferenceTableOnLine(referenceLines);
        if (string.IsNullOrEmpty(referenceOnLine))
        {
            return null;
        }

        return string.Equals(
                NormalizeDefinitionLineKey(referenceOnLine),
                NormalizeDefinitionLineKey(generatedOnLine),
                StringComparison.OrdinalIgnoreCase)
            ? referenceOnLine
            : null;
    }

    private static string? TryGetReferenceTableOnLine(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return null;
        }

        foreach (var line in referenceLines)
        {
            var trimmed = line.TrimStart();
            if (string.Equals(trimmed, ")", StringComparison.OrdinalIgnoreCase) ||
                trimmed.StartsWith(") ON", StringComparison.OrdinalIgnoreCase) ||
                trimmed.StartsWith(") TEXTIMAGE_ON", StringComparison.OrdinalIgnoreCase))
            {
                return line;
            }
        }

        return null;
    }

    internal static List<string>? TryGetCompatibleReferenceCreateTableBlock(
        string[]? referenceLines,
        IReadOnlyList<string> generatedCreateBlock)
    {
        var referenceCreateBlock = TryGetReferenceCreateTableBlock(referenceLines);
        if (referenceCreateBlock == null)
        {
            return null;
        }

        return TableCreateBlocksMatch(generatedCreateBlock, referenceCreateBlock)
            ? referenceCreateBlock
            : null;
    }

    private static List<string>? TryGetReferenceCreateTableBlock(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return null;
        }

        var start = -1;
        for (var i = 0; i < referenceLines.Length; i++)
        {
            if (referenceLines[i].TrimStart().StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase))
            {
                start = i;
                break;
            }
        }

        if (start < 0)
        {
            return null;
        }

        var end = -1;
        for (var i = start + 1; i < referenceLines.Length; i++)
        {
            if (referenceLines[i].TrimStart().StartsWith(")", StringComparison.OrdinalIgnoreCase))
            {
                end = i;
                break;
            }
        }

        if (end < 0)
        {
            return null;
        }

        var lines = new List<string>();
        for (var i = start; i <= end; i++)
        {
            lines.Add(referenceLines[i]);
        }

        return lines;
    }

    private static string[]? TryGetReferenceTriggerBlock(string[]? referenceLines, string triggerName)
    {
        if (referenceLines == null || referenceLines.Length == 0 || string.IsNullOrWhiteSpace(triggerName))
        {
            return null;
        }

        var createIndex = -1;
        for (var i = 0; i < referenceLines.Length; i++)
        {
            var trimmed = referenceLines[i].TrimStart();
            if (!trimmed.StartsWith("CREATE TRIGGER", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (trimmed.IndexOf($"[{triggerName}]", StringComparison.OrdinalIgnoreCase) < 0 &&
                trimmed.IndexOf(triggerName, StringComparison.OrdinalIgnoreCase) < 0)
            {
                continue;
            }

            createIndex = i;
            break;
        }

        if (createIndex < 0)
        {
            return null;
        }

        var start = createIndex;
        while (start > 0)
        {
            var previous = referenceLines[start - 1].TrimStart();
            if (previous.StartsWith("SET ", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(referenceLines[start - 1].Trim(), "GO", StringComparison.OrdinalIgnoreCase) ||
                previous.Length == 0)
            {
                start--;
                continue;
            }

            break;
        }

        var end = referenceLines.Length - 1;
        for (var i = createIndex + 1; i < referenceLines.Length; i++)
        {
            if (string.Equals(referenceLines[i].Trim(), "GO", StringComparison.OrdinalIgnoreCase))
            {
                end = i;
                break;
            }
        }

        var block = new string[end - start + 1];
        Array.Copy(referenceLines, start, block, 0, block.Length);
        return block;
    }

    private static bool TableCreateBlocksMatch(
        IReadOnlyList<string> generatedCreateBlock,
        IReadOnlyList<string> referenceCreateBlock)
    {
        var normalizedGenerated = NormalizeBlockLines(generatedCreateBlock);
        var normalizedReference = NormalizeBlockLines(referenceCreateBlock);
        if (normalizedGenerated.Count != normalizedReference.Count)
        {
            return false;
        }

        for (var i = 0; i < normalizedGenerated.Count; i++)
        {
            if (!string.Equals(normalizedGenerated[i], normalizedReference[i], StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        return true;
    }

    private static List<string> NormalizeBlockLines(IEnumerable<string> lines)
    {
        var normalized = new List<string>();
        foreach (var line in lines)
        {
            var key = NormalizeTableCreateBlockLine(line);
            if (key.Length > 0)
            {
                normalized.Add(key);
            }
        }

        return normalized;
    }

    private static string NormalizeTableCreateBlockLine(string line)
    {
        var normalized = NormalizeDefinitionLineKey(line);
        if (normalized.Length == 0 || !ComputedColumnLineRegex.IsMatch(normalized))
        {
            return normalized;
        }

        return NormalizeComputedColumnCompatibilityTokens(normalized);
    }

    private static string NormalizeComputedColumnCompatibilityTokens(string line)
    {
        var builder = new StringBuilder(line.Length);
        var position = 0;
        while (position < line.Length)
        {
            var convertIndex = line.IndexOf("CONVERT(", position, StringComparison.OrdinalIgnoreCase);
            if (convertIndex < 0)
            {
                builder.Append(line, position, line.Length - position);
                break;
            }

            builder.Append(line, position, convertIndex - position);
            var normalizedCall = NormalizeConvertCall(line, convertIndex);
            if (normalizedCall == null)
            {
                builder.Append(line[convertIndex]);
                position = convertIndex + 1;
                continue;
            }

            builder.Append(normalizedCall.Value.Text);
            position = normalizedCall.Value.NextIndex;
        }

        return builder.ToString();
    }

    private static (string Text, int NextIndex)? NormalizeConvertCall(string line, int startIndex)
    {
        const string convertToken = "CONVERT";
        if (!line.AsSpan(startIndex).StartsWith(convertToken, StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        var openParenIndex = startIndex + convertToken.Length;
        if (openParenIndex >= line.Length || line[openParenIndex] != '(')
        {
            return null;
        }

        var closeParenIndex = FindMatchingParenthesis(line, openParenIndex);
        if (closeParenIndex < 0)
        {
            return null;
        }

        var argumentsText = line.Substring(openParenIndex + 1, closeParenIndex - openParenIndex - 1);
        var arguments = SplitTopLevelArguments(argumentsText);
        if (arguments.Count == 3 && IsDefaultConvertStyleArgument(arguments[2]))
        {
            return ($"CONVERT({arguments[0].Trim()},{arguments[1].Trim()})", closeParenIndex + 1);
        }

        return (line.Substring(startIndex, closeParenIndex - startIndex + 1), closeParenIndex + 1);
    }

    private static int FindMatchingParenthesis(string text, int openParenIndex)
    {
        var depth = 0;
        var inSingleQuotedString = false;
        var inBracketedIdentifier = false;
        for (var i = openParenIndex; i < text.Length; i++)
        {
            var ch = text[i];
            if (inSingleQuotedString)
            {
                if (ch == '\'')
                {
                    if (i + 1 < text.Length && text[i + 1] == '\'')
                    {
                        i++;
                    }
                    else
                    {
                        inSingleQuotedString = false;
                    }
                }

                continue;
            }

            if (inBracketedIdentifier)
            {
                if (ch == ']')
                {
                    inBracketedIdentifier = false;
                }

                continue;
            }

            if (ch == '\'')
            {
                inSingleQuotedString = true;
                continue;
            }

            if (ch == '[')
            {
                inBracketedIdentifier = true;
                continue;
            }

            if (ch == '(')
            {
                depth++;
            }
            else if (ch == ')')
            {
                depth--;
                if (depth == 0)
                {
                    return i;
                }
            }
        }

        return -1;
    }

    private static List<string> SplitTopLevelArguments(string text)
    {
        var arguments = new List<string>();
        var current = new StringBuilder();
        var depth = 0;
        var inSingleQuotedString = false;
        var inBracketedIdentifier = false;

        for (var i = 0; i < text.Length; i++)
        {
            var ch = text[i];
            if (inSingleQuotedString)
            {
                current.Append(ch);
                if (ch == '\'')
                {
                    if (i + 1 < text.Length && text[i + 1] == '\'')
                    {
                        current.Append(text[i + 1]);
                        i++;
                    }
                    else
                    {
                        inSingleQuotedString = false;
                    }
                }

                continue;
            }

            if (inBracketedIdentifier)
            {
                current.Append(ch);
                if (ch == ']')
                {
                    inBracketedIdentifier = false;
                }

                continue;
            }

            if (ch == '\'')
            {
                inSingleQuotedString = true;
                current.Append(ch);
                continue;
            }

            if (ch == '[')
            {
                inBracketedIdentifier = true;
                current.Append(ch);
                continue;
            }

            if (ch == '(')
            {
                depth++;
                current.Append(ch);
                continue;
            }

            if (ch == ')')
            {
                depth--;
                current.Append(ch);
                continue;
            }

            if (ch == ',' && depth == 0)
            {
                arguments.Add(current.ToString());
                current.Clear();
                continue;
            }

            current.Append(ch);
        }

        arguments.Add(current.ToString());
        return arguments;
    }

    private static bool IsDefaultConvertStyleArgument(string argument)
    {
        if (string.IsNullOrWhiteSpace(argument))
        {
            return false;
        }

        var normalized = Regex.Replace(argument, @"\s+", string.Empty);
        return normalized == "0" || normalized == "(0)";
    }

    private static List<string> ReadTableColumns(SqlConnection connection, string fullName, string[]? referenceLines)
    {
        var objectId = ResolveObjectId(connection, fullName);
        if (!objectId.HasValue)
        {
            return [];
        }

        return ReadTableColumns(connection, objectId.Value, referenceLines);
    }

    private static List<string> ReadTableColumns(SqlConnection connection, int objectId, string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT c.name AS column_name, c.is_nullable, c.is_identity, ic.seed_value, ic.increment_value, ic.is_not_for_replication,
       c.is_computed, cc.definition AS computed_definition, cc.is_persisted AS computed_is_persisted,
       t.name AS type_name, ts.name AS type_schema, t.is_user_defined,
       c.max_length, c.precision, c.scale,
       dc.name AS default_name, dc.definition AS default_definition,
       c.is_rowguidcol, c.is_xml_document,
       xss.name AS xml_collection_schema, xsc.name AS xml_collection_name
FROM sys.columns c
JOIN sys.types t ON c.user_type_id = t.user_type_id
JOIN sys.schemas ts ON ts.schema_id = t.schema_id
LEFT JOIN sys.identity_columns ic ON ic.object_id = c.object_id AND ic.column_id = c.column_id
LEFT JOIN sys.computed_columns cc ON cc.object_id = c.object_id AND cc.column_id = c.column_id
LEFT JOIN sys.default_constraints dc ON dc.parent_object_id = c.object_id AND dc.parent_column_id = c.column_id
LEFT JOIN sys.xml_schema_collections xsc ON xsc.xml_collection_id = c.xml_collection_id
LEFT JOIN sys.schemas xss ON xss.schema_id = xsc.schema_id
WHERE c.object_id = @objectId
ORDER BY c.column_id;";
        command.Parameters.AddWithValue("@objectId", objectId);

        var compatibilityTypeMap = BuildReferenceTableColumnTypeMap(referenceLines);
        var rows = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var columnName = reader.GetString(0);
            var isNullable = reader.GetBoolean(1);
            var isIdentity = reader.GetBoolean(2);
            var seed = reader.IsDBNull(3) ? null : reader.GetValue(3);
            var increment = reader.IsDBNull(4) ? null : reader.GetValue(4);
            var isNotForReplication = !reader.IsDBNull(5) && reader.GetBoolean(5);
            var isComputed = reader.GetBoolean(6);
            var computedDefinition = reader.IsDBNull(7) ? string.Empty : reader.GetString(7);
            var isComputedPersisted = !reader.IsDBNull(8) && reader.GetBoolean(8);
            var typeName = reader.GetString(9);
            var typeSchema = reader.GetString(10);
            var isUserDefined = reader.GetBoolean(11);
            var maxLength = reader.GetInt16(12);
            var precision = reader.GetByte(13);
            var scale = reader.GetByte(14);
            var defaultName = reader.IsDBNull(15) ? string.Empty : reader.GetString(15);
            var defaultDefinition = reader.IsDBNull(16) ? string.Empty : reader.GetString(16);
            var isRowGuidCol = !reader.IsDBNull(17) && reader.GetBoolean(17);
            var isXmlDocument = !reader.IsDBNull(18) && reader.GetBoolean(18);
            var xmlCollectionSchema = reader.IsDBNull(19) ? null : reader.GetString(19);
            var xmlCollectionName = reader.IsDBNull(20) ? null : reader.GetString(20);

            if (isComputed)
            {
                var persistedClause = isComputedPersisted ? " PERSISTED" : string.Empty;
                var computedNullability = !isNullable && isComputedPersisted ? " NOT NULL" : string.Empty;
                rows.Add($"[{columnName}] AS {computedDefinition.Trim()}{persistedClause}{computedNullability}");
                continue;
            }

            var type = FormatTypeName(typeName, typeSchema, isUserDefined, maxLength, precision, scale);
            type = ApplyXmlSchemaBinding(type, typeName, isUserDefined, isXmlDocument, xmlCollectionSchema, xmlCollectionName);
            type = GetCompatibleTypeToken(type, compatibilityTypeMap, columnName);
            var nullability = isNullable ? "NULL" : "NOT NULL";
            var rowGuidCol = isRowGuidCol ? " ROWGUIDCOL" : string.Empty;
            var identity = isIdentity && seed != null && increment != null
                ? $" IDENTITY({seed}, {increment}){(isNotForReplication ? " NOT FOR REPLICATION" : string.Empty)}"
                : string.Empty;
            var defaultClause = string.IsNullOrWhiteSpace(defaultDefinition)
                ? string.Empty
                : $"{(string.IsNullOrWhiteSpace(defaultName) ? string.Empty : $" CONSTRAINT [{defaultName}]")} DEFAULT {defaultDefinition}";

            rows.Add($"[{columnName}] {type} {nullability}{rowGuidCol}{identity}{defaultClause}");
        }

        return rows;
    }

    private static IEnumerable<string> ReadTableTypeCheckConstraints(SqlConnection connection, int objectId)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT cc.name, cc.definition, cc.is_not_for_replication
FROM sys.check_constraints cc
WHERE cc.parent_object_id = @objectId
ORDER BY cc.name;";
        command.Parameters.AddWithValue("@objectId", objectId);

        var lines = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var name = reader.GetString(0);
            var definition = reader.GetString(1);
            var notForReplication = !reader.IsDBNull(2) && reader.GetBoolean(2)
                ? " NOT FOR REPLICATION"
                : string.Empty;
            lines.Add($"CONSTRAINT {QuoteIdentifier(name)} CHECK{notForReplication} {definition}");
        }

        return lines;
    }

    private static IEnumerable<string> ReadTableTypeKeyConstraints(SqlConnection connection, int objectId)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT kc.name,
       kc.type_desc,
       i.type_desc AS index_type_desc,
       STUFF((
         SELECT ', ' + QUOTENAME(c.name) + CASE WHEN ic.is_descending_key = 1 THEN ' DESC' ELSE '' END
         FROM sys.index_columns ic
         JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
         WHERE ic.object_id = i.object_id
           AND ic.index_id = i.index_id
           AND ic.is_included_column = 0
         ORDER BY ic.key_ordinal
         FOR XML PATH(''), TYPE).value('.', 'nvarchar(max)'), 1, 2, '') AS key_columns
FROM sys.key_constraints kc
JOIN sys.indexes i ON i.object_id = kc.parent_object_id AND i.index_id = kc.unique_index_id
WHERE kc.parent_object_id = @objectId
ORDER BY kc.name;";
        command.Parameters.AddWithValue("@objectId", objectId);

        var lines = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var name = reader.GetString(0);
            var typeDesc = reader.GetString(1);
            var indexType = reader.GetString(2);
            var keyColumns = reader.GetString(3);

            var constraintType = typeDesc == "PRIMARY_KEY_CONSTRAINT" ? "PRIMARY KEY" : "UNIQUE";
            var clustered = string.Equals(indexType, "CLUSTERED", StringComparison.OrdinalIgnoreCase)
                ? "CLUSTERED"
                : "NONCLUSTERED";

            lines.Add($"CONSTRAINT {QuoteIdentifier(name)} {constraintType} {clustered} ({keyColumns})");
        }

        return lines;
    }

    internal static string ApplyXmlSchemaBinding(
        string type,
        string typeName,
        bool isUserDefined,
        bool isXmlDocument,
        string? xmlCollectionSchema,
        string? xmlCollectionName)
    {
        if (isUserDefined ||
            !string.Equals(typeName, "xml", StringComparison.OrdinalIgnoreCase) ||
            string.IsNullOrWhiteSpace(xmlCollectionSchema) ||
            string.IsNullOrWhiteSpace(xmlCollectionName))
        {
            return type;
        }

        var xmlKind = isXmlDocument ? "DOCUMENT" : "CONTENT";
        return $"{type} ({xmlKind} [{xmlCollectionSchema}].[{xmlCollectionName}])";
    }

    internal static IReadOnlyDictionary<string, string>? BuildReferenceTableColumnTypeMap(string[]? referenceLines)
    {
        var createBlock = TryGetReferenceCreateTableBlock(referenceLines);
        if (createBlock == null || createBlock.Count == 0)
        {
            return null;
        }

        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var inColumns = false;
        foreach (var line in createBlock)
        {
            var trimmed = line.Trim();
            if (!inColumns)
            {
                if (trimmed == "(")
                {
                    inColumns = true;
                }

                continue;
            }

            if (trimmed.StartsWith(")", StringComparison.Ordinal))
            {
                break;
            }

            var match = ReferenceColumnTypeTokenRegex.Match(line);
            if (!match.Success)
            {
                continue;
            }

            var columnName = match.Groups["name"].Value;
            if (!map.ContainsKey(columnName))
            {
                map[columnName] = match.Groups["type"].Value.Trim();
            }
        }

        return map.Count == 0 ? null : map;
    }

    internal static string GetCompatibleTypeToken(
        string generatedType,
        IReadOnlyDictionary<string, string>? compatibilityTypeMap,
        string columnName)
    {
        if (compatibilityTypeMap == null || string.IsNullOrWhiteSpace(columnName))
        {
            return generatedType;
        }

        if (!compatibilityTypeMap.TryGetValue(columnName, out var compatibilityType) ||
            string.IsNullOrWhiteSpace(compatibilityType))
        {
            return generatedType;
        }

        return string.Equals(
                NormalizeCompatibleTypeToken(generatedType),
                NormalizeCompatibleTypeToken(compatibilityType),
                StringComparison.Ordinal)
            ? compatibilityType
            : generatedType;
    }

    internal static string NormalizeCompatibleTypeToken(string token)
    {
        if (string.IsNullOrWhiteSpace(token))
        {
            return string.Empty;
        }

        var normalized = token.Trim();
        normalized = SysQualifiedCompatibilityTypeTokenRegex.Replace(normalized, "[$1]");
        normalized = Regex.Replace(normalized, @"\s+", " ");
        normalized = Regex.Replace(normalized, @"\s*\(\s*", " (");
        normalized = Regex.Replace(normalized, @"\s*,\s*", ", ");
        normalized = Regex.Replace(normalized, @"\s*\)", ")");
        return normalized.ToLowerInvariant();
    }

    private static TableStorageInfo ReadTableStorage(SqlConnection connection, string fullName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT TOP 1 ds.name AS data_space_name,
       partition_col.name AS partition_column_name
FROM sys.indexes i
JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
LEFT JOIN sys.index_columns ic
    ON ic.object_id = i.object_id
   AND ic.index_id = i.index_id
   AND ic.partition_ordinal = 1
LEFT JOIN sys.columns partition_col
    ON partition_col.object_id = ic.object_id
   AND partition_col.column_id = ic.column_id
WHERE i.object_id = OBJECT_ID(@full) AND i.index_id IN (0,1)
ORDER BY i.index_id DESC;";
        command.Parameters.AddWithValue("@full", fullName);
        string? dataSpace = null;
        string? partitionColumn = null;
        using (var reader = command.ExecuteReader())
        {
            if (reader.Read())
            {
                dataSpace = reader.IsDBNull(0) ? null : reader.GetString(0);
                partitionColumn = reader.IsDBNull(1) ? null : reader.GetString(1);
            }
        }

        command.CommandText = @"
SELECT ds.name AS lob_data_space_name
FROM sys.tables t
LEFT JOIN sys.data_spaces ds ON ds.data_space_id = t.lob_data_space_id
WHERE t.object_id = OBJECT_ID(@full);";
        var lobSpace = command.ExecuteScalar() as string;

        return new TableStorageInfo(dataSpace, partitionColumn, lobSpace);
    }

    private static string ReadTableCompression(SqlConnection connection, string fullName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT p.data_compression_desc
FROM sys.partitions p
WHERE p.object_id = OBJECT_ID(@full) AND p.index_id IN (0,1);";
        command.Parameters.AddWithValue("@full", fullName);

        var compressions = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            if (!reader.IsDBNull(0))
            {
                compressions.Add(reader.GetString(0));
            }
        }

        if (compressions.Contains("PAGE"))
        {
            return "PAGE";
        }

        if (compressions.Contains("ROW"))
        {
            return "ROW";
        }

        return "NONE";
    }

    private static IEnumerable<string> ReadTableTriggers(
        SqlConnection connection,
        string fullName,
        string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT t.name, m.definition, m.uses_ansi_nulls, m.uses_quoted_identifier
FROM sys.triggers t
JOIN sys.sql_modules m ON m.object_id = t.object_id
WHERE t.parent_class_desc = 'OBJECT_OR_COLUMN'
  AND t.parent_id = OBJECT_ID(@full)
  AND t.is_ms_shipped = 0
ORDER BY t.name;";
        command.Parameters.AddWithValue("@full", fullName);

        var lines = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var triggerName = reader.GetString(0);
            var definitionText = reader.IsDBNull(1) ? string.Empty : reader.GetString(1);
            var ansiNulls = reader.IsDBNull(2) || reader.GetBoolean(2);
            var quotedIdentifier = reader.IsDBNull(3) || reader.GetBoolean(3);
            var triggerReferenceLines = TryGetReferenceTriggerBlock(referenceLines, triggerName);
            var (triggerLines, _) = BuildProgrammableObjectLines(
                definitionText,
                ansiNulls,
                quotedIdentifier,
                insertBlankLineAfterSet: true,
                triggerReferenceLines);
            lines.AddRange(triggerLines);
        }

        return lines;
    }

    private static IEnumerable<string> ReadTableChecks(
        SqlConnection connection,
        string fullName,
        string[]? referenceLines)
    {
        var checkLineMap = BuildCheckConstraintLineMap(referenceLines);
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT name, definition, is_not_for_replication
FROM sys.check_constraints
WHERE parent_object_id = OBJECT_ID(@full)
ORDER BY name;";
        command.Parameters.AddWithValue("@full", fullName);

        var lines = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var name = reader.GetString(0);
            var definition = reader.GetString(1).Trim();
            var notForReplication = reader.GetBoolean(2) ? " NOT FOR REPLICATION" : string.Empty;
            if (checkLineMap != null && checkLineMap.TryGetValue(name, out var line))
            {
                lines.Add(line);
            }
            else
            {
                lines.Add($"ALTER TABLE {fullName} ADD CONSTRAINT [{name}] CHECK{notForReplication} {definition}");
            }
            lines.Add("GO");
        }

        return lines;
    }

    private static IEnumerable<string> ReadTableKeyConstraints(
        SqlConnection connection,
        string fullName,
        string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT kc.name, kc.type_desc, i.type_desc AS index_type_desc,
       i.object_id,
       i.index_id,
       ds.name AS data_space_name,
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
WHERE kc.parent_object_id = OBJECT_ID(@full)
GROUP BY kc.name, kc.type_desc, i.type_desc, i.object_id, i.index_id, ds.name
ORDER BY i.index_id, kc.name;";
        command.Parameters.AddWithValue("@full", fullName);

        var keyLineMap = BuildKeyConstraintLineMap(referenceLines);
        var constraints = new List<(string Name, string TypeDesc, string IndexType, int ObjectId, int IndexId, string DataSpace, string Columns)>();
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                constraints.Add((
                    reader.GetString(0),
                    reader.GetString(1),
                    reader.GetString(2),
                    reader.GetInt32(3),
                    reader.GetInt32(4),
                    reader.GetString(5),
                    reader.GetString(6)));
            }
        }

        var lines = new List<string>();
        foreach (var constraint in constraints)
        {
            var name = constraint.Name;
            var typeDesc = constraint.TypeDesc;
            var indexType = constraint.IndexType;
            var objectId = constraint.ObjectId;
            var indexId = constraint.IndexId;
            var dataSpace = constraint.DataSpace;
            var columns = constraint.Columns;

            var constraintType = typeDesc == "PRIMARY_KEY_CONSTRAINT" ? "PRIMARY KEY" : "UNIQUE";
            var clustered = string.Equals(indexType, "CLUSTERED", StringComparison.OrdinalIgnoreCase)
                ? "CLUSTERED"
                : "NONCLUSTERED";
            var indexOptions = ReadIndexScriptingOptions(connection, objectId, indexId);
            var withClause = BuildIndexWithClause(indexOptions.Compression, indexOptions.StatisticsIncremental);
            var onClause = BuildIndexOnClause(dataSpace, indexOptions.PartitionColumn);

            if (keyLineMap != null && keyLineMap.TryGetValue(name, out var line))
            {
                lines.Add(line);
            }
            else
            {
                lines.Add($"ALTER TABLE {fullName} ADD CONSTRAINT [{name}] {constraintType} {clustered} ({columns}){withClause}{onClause}");
            }
            lines.Add("GO");
        }

        return lines;
    }

    private static IEnumerable<string> ReadNonConstraintIndexes(
        SqlConnection connection,
        string fullName,
        string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT i.object_id, i.index_id, i.name, i.is_unique, i.type_desc, ds.name AS data_space_name, i.filter_definition
FROM sys.indexes i
JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
WHERE i.object_id = OBJECT_ID(@full)
  AND i.is_primary_key = 0
  AND i.is_unique_constraint = 0
  AND i.type_desc IN ('CLUSTERED','NONCLUSTERED','CLUSTERED COLUMNSTORE','NONCLUSTERED COLUMNSTORE')
  AND i.is_hypothetical = 0
ORDER BY i.name;";
        command.Parameters.AddWithValue("@full", fullName);

        var indexLineMap = BuildIndexLineMap(referenceLines);
        var indexOrder = GetReferenceIndexOrder(referenceLines);
        var indexes = new List<(int ObjectId, int IndexId, string Name, bool IsUnique, string TypeDesc, string DataSpace, string? Filter)>();
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                indexes.Add((
                    reader.GetInt32(0),
                    reader.GetInt32(1),
                    reader.GetString(2),
                    reader.GetBoolean(3),
                    reader.GetString(4),
                    reader.GetString(5),
                    reader.IsDBNull(6) ? null : reader.GetString(6)
                ));
            }
        }

        if (indexOrder != null && indexOrder.Count > 0)
        {
            var orderMap = new Dictionary<string, int>(StringComparer.Ordinal);
            for (var i = 0; i < indexOrder.Count; i++)
            {
                if (!orderMap.ContainsKey(indexOrder[i]))
                {
                    orderMap[indexOrder[i]] = i;
                }
            }

            indexes = indexes
                .OrderBy(entry => orderMap.TryGetValue(entry.Name, out var order) ? order : int.MaxValue)
                .ThenBy(entry => entry.Name, StringComparer.Ordinal)
                .ToList();
        }

        var lines = new List<string>();
        foreach (var index in indexes)
        {
            command.Parameters.Clear();
            command.CommandText = @"
SELECT c.name AS column_name, ic.is_descending_key, ic.is_included_column
FROM sys.index_columns ic
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE ic.object_id = OBJECT_ID(@full) AND ic.index_id = @idx
ORDER BY ic.key_ordinal, ic.index_column_id;";
            command.Parameters.AddWithValue("@full", fullName);
            command.Parameters.AddWithValue("@idx", index.IndexId);

            var keys = new List<string>();
            var includes = new List<string>();
            using (var reader = command.ExecuteReader())
            {
                while (reader.Read())
                {
                    var column = reader.GetString(0);
                    var isDescending = reader.GetBoolean(1);
                    var isIncluded = reader.GetBoolean(2);
                    if (isIncluded)
                    {
                        includes.Add($"[{column}]");
                    }
                    else
                    {
                        keys.Add(isDescending ? $"[{column}] DESC" : $"[{column}]");
                    }
                }
            }

            var isColumnstore = index.TypeDesc.IndexOf("COLUMNSTORE", StringComparison.OrdinalIgnoreCase) >= 0;
            var indexOptions = ReadIndexScriptingOptions(connection, index.ObjectId, index.IndexId);
            if (isColumnstore)
            {
                if (indexLineMap != null && indexLineMap.TryGetValue(index.Name, out var line))
                {
                    lines.Add(line);
                    lines.Add("GO");
                    continue;
                }

                var columnstoreType = index.TypeDesc.StartsWith("CLUSTERED", StringComparison.OrdinalIgnoreCase)
                    ? "CLUSTERED COLUMNSTORE"
                    : "NONCLUSTERED COLUMNSTORE";
                var columnstoreWithClause = BuildIndexWithClause("NONE", indexOptions.StatisticsIncremental);
                var columnstoreOnClause = BuildIndexOnClause(index.DataSpace, indexOptions.PartitionColumn);

                lines.Add($"CREATE {columnstoreType} INDEX [{index.Name}] ON {fullName}{columnstoreWithClause}{columnstoreOnClause}");
                lines.Add("GO");
                continue;
            }

            if (keys.Count == 0)
            {
                continue;
            }

            var unique = index.IsUnique ? "UNIQUE " : string.Empty;
            var type = string.Equals(index.TypeDesc, "CLUSTERED", StringComparison.OrdinalIgnoreCase) ? "CLUSTERED" : "NONCLUSTERED";
            var includeClause = includes.Count > 0 ? $" INCLUDE ({string.Join(", ", includes)})" : string.Empty;
            var filterClause = string.IsNullOrWhiteSpace(index.Filter) ? string.Empty : $" WHERE {index.Filter}";
            var withClause = BuildIndexWithClause(indexOptions.Compression, indexOptions.StatisticsIncremental);
            var onClause = BuildIndexOnClause(index.DataSpace, indexOptions.PartitionColumn);

            if (indexLineMap != null && indexLineMap.TryGetValue(index.Name, out var lineNonColumnstore))
            {
                lines.Add(lineNonColumnstore);
            }
            else
            {
                lines.Add($"CREATE {unique}{type} INDEX [{index.Name}] ON {fullName} ({string.Join(", ", keys)}){includeClause}{filterClause}{withClause}{onClause}");
            }
            lines.Add("GO");
        }

        return lines;
    }

    private static IndexScriptingOptions ReadIndexScriptingOptions(SqlConnection connection, int objectId, int indexId)
    {
        return new IndexScriptingOptions(
            ReadIndexCompression(connection, objectId, indexId),
            ReadIndexPartitionColumn(connection, objectId, indexId),
            ReadIndexStatisticsIncremental(connection, objectId, indexId));
    }

    private static string ReadIndexCompression(SqlConnection connection, int objectId, int indexId)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT p.data_compression_desc
FROM sys.partitions p
WHERE p.object_id = @obj AND p.index_id = @idx;";
        command.Parameters.AddWithValue("@obj", objectId);
        command.Parameters.AddWithValue("@idx", indexId);

        var compressions = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            if (!reader.IsDBNull(0))
            {
                compressions.Add(reader.GetString(0));
            }
        }

        if (compressions.Contains("PAGE"))
        {
            return "PAGE";
        }

        if (compressions.Contains("ROW"))
        {
            return "ROW";
        }

        return "NONE";
    }

    private static string? ReadIndexPartitionColumn(SqlConnection connection, int objectId, int indexId)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT c.name
FROM sys.index_columns ic
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
WHERE ic.object_id = @obj AND ic.index_id = @idx AND ic.partition_ordinal = 1;";
        command.Parameters.AddWithValue("@obj", objectId);
        command.Parameters.AddWithValue("@idx", indexId);
        return command.ExecuteScalar() as string;
    }

    private static bool ReadIndexStatisticsIncremental(SqlConnection connection, int objectId, int indexId)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT s.is_incremental
FROM sys.stats s
WHERE s.object_id = @obj AND s.stats_id = @idx;";
        command.Parameters.AddWithValue("@obj", objectId);
        command.Parameters.AddWithValue("@idx", indexId);

        return command.ExecuteScalar() is bool isIncremental && isIncremental;
    }

    private static IEnumerable<string> ReadTableForeignKeys(SqlConnection connection, string fullName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
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
ORDER BY fk.name;";
        command.Parameters.AddWithValue("@full", fullName);

        var lines = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var name = reader.GetString(0);
            var refSchema = reader.GetString(1);
            var refTable = reader.GetString(2);
            var onDelete = reader.GetString(3);
            var onUpdate = reader.GetString(4);
            var parentCols = reader.GetString(5);
            var refCols = reader.GetString(6);

            var deleteClause = onDelete != "NO_ACTION" ? $" ON DELETE {onDelete}" : string.Empty;
            var updateClause = onUpdate != "NO_ACTION" ? $" ON UPDATE {onUpdate}" : string.Empty;

            lines.Add($"ALTER TABLE {fullName} ADD CONSTRAINT [{name}] FOREIGN KEY ({parentCols}) REFERENCES [{refSchema}].[{refTable}] ({refCols}){deleteClause}{updateClause}");
            lines.Add("GO");
        }

        return lines;
    }

    private static IEnumerable<string> ReadTableGrants(SqlConnection connection, string fullName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT dp.permission_name, dp.state_desc, pr.name AS principal_name
FROM sys.database_permissions dp
JOIN sys.database_principals pr ON pr.principal_id = dp.grantee_principal_id
WHERE dp.major_id = OBJECT_ID(@full) AND dp.class_desc = 'OBJECT_OR_COLUMN'
ORDER BY pr.name, dp.permission_name;";
        command.Parameters.AddWithValue("@full", fullName);

        var lines = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var permission = reader.GetString(0);
            var state = reader.GetString(1);
            var principal = reader.GetString(2);

            if (state == "GRANT_WITH_GRANT_OPTION")
            {
                lines.Add($"GRANT {permission} ON  {fullName} TO [{principal}] WITH GRANT OPTION");
            }
            else if (state == "DENY")
            {
                lines.Add($"DENY {permission} ON  {fullName} TO [{principal}]");
            }
            else
            {
                lines.Add($"GRANT {permission} ON  {fullName} TO [{principal}]");
            }

            lines.Add("GO");
        }

        return lines;
    }

    private static IEnumerable<string> ReadSchemaScopedObjectPermissions(
        SqlConnection connection,
        string fullName,
        string[]? referenceLines)
        => ExecutePermissionQuery(
            connection,
            @"
SELECT dp.permission_name, dp.state_desc, pr.name AS principal_name
FROM sys.database_permissions dp
JOIN sys.database_principals pr ON pr.principal_id = dp.grantee_principal_id
WHERE dp.major_id = OBJECT_ID(@full) AND dp.class_desc = 'OBJECT_OR_COLUMN'
ORDER BY pr.name, dp.permission_name;",
            command => command.Parameters.AddWithValue("@full", fullName),
            fullName,
            referenceLines);

    private static IEnumerable<string> ExecutePermissionQuery(
        SqlConnection connection,
        string commandText,
        Action<SqlCommand> configureCommand,
        string targetClause,
        string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = commandText;
        configureCommand(command);

        var grantLineMap = BuildGrantLineMap(referenceLines);
        var lines = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var permission = reader.GetString(0);
            var normalizedState = NormalizePermissionState(reader.GetString(1));
            var principal = reader.GetString(2);

            if (grantLineMap != null &&
                grantLineMap.TryGetValue($"{normalizedState}|{permission}|{principal}", out var line))
            {
                lines.Add(line);
                lines.Add("GO");
                continue;
            }

            lines.Add(FormatPermissionLine(permission, normalizedState, targetClause, principal));
            lines.Add("GO");
        }

        return lines;
    }

    private static string NormalizePermissionState(string state)
    {
        if (string.Equals(state, "GRANT_WITH_GRANT_OPTION", StringComparison.OrdinalIgnoreCase))
        {
            return "GRANT_WITH_GRANT_OPTION";
        }

        if (string.Equals(state, "DENY", StringComparison.OrdinalIgnoreCase))
        {
            return "DENY";
        }

        return "GRANT";
    }

    private static string FormatPermissionLine(
        string permission,
        string normalizedState,
        string targetClause,
        string principal)
    {
        return normalizedState switch
        {
            "GRANT_WITH_GRANT_OPTION" => $"GRANT {permission} ON {targetClause} TO [{principal}] WITH GRANT OPTION",
            "DENY" => $"DENY {permission} ON {targetClause} TO [{principal}]",
            _ => $"GRANT {permission} ON {targetClause} TO [{principal}]"
        };
    }

    private static bool UsesSysNamedExtendedProperties(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return false;
        }

        foreach (var line in referenceLines)
        {
            if (line.TrimStart().StartsWith("EXEC sys.sp_addextendedproperty", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static string BuildExtendedPropertyStatement(
        string propertyProcedure,
        string propertyName,
        string propertyValue,
        string level0Type,
        string level0Name,
        string? level1Type,
        string? level1Name,
        string? level2Type,
        string? level2Name)
    {
        var propName = EscapeSqlStringLiteral(propertyName);
        var propValueSql = EscapeSqlStringLiteral(propertyValue);
        var level0TypeSql = EscapeSqlStringLiteral(level0Type);
        var level0NameSql = EscapeSqlStringLiteral(level0Name);

        if (string.IsNullOrWhiteSpace(level1Type) || string.IsNullOrWhiteSpace(level1Name))
        {
            return $"{propertyProcedure} N'{propName}', N'{propValueSql}', '{level0TypeSql}', N'{level0NameSql}', NULL, NULL, NULL, NULL";
        }

        var level1TypeSql = EscapeSqlStringLiteral(level1Type);
        var level1NameSql = EscapeSqlStringLiteral(level1Name);

        if (string.IsNullOrWhiteSpace(level2Type) || string.IsNullOrWhiteSpace(level2Name))
        {
            return $"{propertyProcedure} N'{propName}', N'{propValueSql}', '{level0TypeSql}', N'{level0NameSql}', '{level1TypeSql}', N'{level1NameSql}', NULL, NULL";
        }

        var level2TypeSql = EscapeSqlStringLiteral(level2Type);
        var level2NameSql = EscapeSqlStringLiteral(level2Name);
        return $"{propertyProcedure} N'{propName}', N'{propValueSql}', '{level0TypeSql}', N'{level0NameSql}', '{level1TypeSql}', N'{level1NameSql}', '{level2TypeSql}', N'{level2NameSql}'";
    }

    private static string GetExtendedPropertyProcedure(string[]? referenceLines)
        => UsesSysNamedExtendedProperties(referenceLines)
            ? "EXEC sys.sp_addextendedproperty"
            : "EXEC sp_addextendedproperty";

    private static void AppendExtendedPropertyLines(
        List<string> lines,
        IEnumerable<string> extendedProperties,
        string[]? referenceLines)
    {
        var propertyLines = extendedProperties.ToList();
        if (propertyLines.Count == 0)
        {
            return;
        }

        var blankBeforeProps = CountBlankLinesBeforeExtendedProperties(referenceLines);
        for (var i = 0; i < blankBeforeProps; i++)
        {
            lines.Add(string.Empty);
        }

        lines.AddRange(propertyLines);
    }

    private static int CountBlankLinesBeforeExtendedProperties(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return 0;
        }

        var firstProp = -1;
        for (var i = 0; i < referenceLines.Length; i++)
        {
            var trimmed = referenceLines[i].TrimStart();
            if (trimmed.StartsWith("EXEC sp_addextendedproperty", StringComparison.OrdinalIgnoreCase) ||
                trimmed.StartsWith("EXEC sys.sp_addextendedproperty", StringComparison.OrdinalIgnoreCase))
            {
                firstProp = i;
                break;
            }
        }

        if (firstProp < 0)
        {
            return 0;
        }

        var lastGo = -1;
        for (var i = firstProp - 1; i >= 0; i--)
        {
            if (string.Equals(referenceLines[i].Trim(), "GO", StringComparison.OrdinalIgnoreCase))
            {
                lastGo = i;
                break;
            }
        }

        if (lastGo < 0)
        {
            return 0;
        }

        var count = 0;
        for (var i = lastGo + 1; i < firstProp; i++)
        {
            if (referenceLines[i].Trim().Length == 0)
            {
                count++;
            }
        }

        return count;
    }

    private static IEnumerable<string> ReadModuleGrants(
        SqlConnection connection,
        string fullName,
        string[]? referenceLines)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT dp.permission_name, dp.state_desc, pr.name AS principal_name
FROM sys.database_permissions dp
JOIN sys.database_principals pr ON pr.principal_id = dp.grantee_principal_id
WHERE dp.major_id = OBJECT_ID(@full) AND dp.class_desc = 'OBJECT_OR_COLUMN'
ORDER BY pr.name, dp.permission_name;";
        command.Parameters.AddWithValue("@full", fullName);

        var grantLineMap = BuildGrantLineMap(referenceLines);
        var lines = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var permission = reader.GetString(0);
            var state = reader.GetString(1);
            var principal = reader.GetString(2);

            var keyState = state;
            if (string.Equals(state, "GRANT_WITH_GRANT_OPTION", StringComparison.OrdinalIgnoreCase))
            {
                keyState = "GRANT_WITH_GRANT_OPTION";
            }
            else if (string.Equals(state, "DENY", StringComparison.OrdinalIgnoreCase))
            {
                keyState = "DENY";
            }
            else
            {
                keyState = "GRANT";
            }

            if (grantLineMap != null &&
                grantLineMap.TryGetValue($"{keyState}|{permission}|{principal}", out var line))
            {
                lines.Add(line);
                lines.Add("GO");
                continue;
            }

            if (state == "GRANT_WITH_GRANT_OPTION")
            {
                lines.Add($"GRANT {permission} ON  {fullName} TO [{principal}] WITH GRANT OPTION");
            }
            else if (state == "DENY")
            {
                lines.Add($"DENY {permission} ON  {fullName} TO [{principal}]");
            }
            else
            {
                lines.Add($"GRANT {permission} ON  {fullName} TO [{principal}]");
            }

            lines.Add("GO");
        }

        return lines;
    }

    private static IEnumerable<string> ReadModuleExtendedProperties(
        SqlConnection connection,
        DbObjectInfo obj,
        string[]? referenceLines)
    {
        var className = GetExtendedPropertyClassName(obj.ObjectType);
        if (string.IsNullOrWhiteSpace(className))
        {
            return Array.Empty<string>();
        }

        var propertyProcedure = GetExtendedPropertyProcedure(referenceLines);

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
WHERE ep.class_desc = 'OBJECT_OR_COLUMN' AND ep.major_id = OBJECT_ID(@full) AND ep.minor_id = 0
ORDER BY ep.name;";
        command.Parameters.AddWithValue("@full", $"[{obj.Schema}].[{obj.Name}]");

        var lines = new List<string>();
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                var propName = reader.GetString(0);
                var propValue = reader.GetValue(1).ToString() ?? string.Empty;
                lines.Add(BuildExtendedPropertyStatement(
                    propertyProcedure,
                    propName,
                    propValue,
                    "SCHEMA",
                    obj.Schema,
                    className,
                    obj.Name,
                    null,
                    null));
                lines.Add("GO");
            }
        }

        command.CommandText = @"
SELECT ep.name AS prop_name, ep.value AS prop_value, c.name AS column_name
FROM sys.extended_properties ep
JOIN sys.columns c ON c.object_id = ep.major_id AND c.column_id = ep.minor_id
WHERE ep.class_desc = 'OBJECT_OR_COLUMN' AND ep.major_id = OBJECT_ID(@full) AND ep.minor_id <> 0
ORDER BY c.name, ep.name;";
        command.Parameters.Clear();
        command.Parameters.AddWithValue("@full", $"[{obj.Schema}].[{obj.Name}]");

        using (var columnReader = command.ExecuteReader())
        {
            while (columnReader.Read())
            {
                var propName = columnReader.GetString(0);
                var propValue = columnReader.GetValue(1).ToString() ?? string.Empty;
                var columnName = columnReader.GetString(2);
                lines.Add(BuildExtendedPropertyStatement(
                    propertyProcedure,
                    propName,
                    propValue,
                    "SCHEMA",
                    obj.Schema,
                    className,
                    obj.Name,
                    "COLUMN",
                    columnName));
                lines.Add("GO");
            }
        }

        if (string.Equals(obj.ObjectType, "View", StringComparison.OrdinalIgnoreCase))
        {
            command.CommandText = @"
SELECT ep.name AS prop_name, ep.value AS prop_value, i.name AS index_name
FROM sys.extended_properties ep
JOIN sys.indexes i ON i.object_id = ep.major_id AND i.index_id = ep.minor_id
WHERE ep.class_desc = 'INDEX' AND ep.major_id = OBJECT_ID(@full)
ORDER BY i.name, ep.name;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@full", $"[{obj.Schema}].[{obj.Name}]");

            using var indexReader = command.ExecuteReader();
            while (indexReader.Read())
            {
                var propName = indexReader.GetString(0);
                var propValue = indexReader.GetValue(1).ToString() ?? string.Empty;
                var indexName = indexReader.GetString(2);
                lines.Add(BuildExtendedPropertyStatement(
                    propertyProcedure,
                    propName,
                    propValue,
                    "SCHEMA",
                    obj.Schema,
                    className,
                    obj.Name,
                    "INDEX",
                    indexName));
                lines.Add("GO");
            }
        }

        if (string.Equals(obj.ObjectType, "StoredProcedure", StringComparison.OrdinalIgnoreCase) ||
            string.Equals(obj.ObjectType, "Function", StringComparison.OrdinalIgnoreCase))
        {
            command.CommandText = @"
SELECT ep.name AS prop_name, ep.value AS prop_value, p.name AS param_name
FROM sys.extended_properties ep
JOIN sys.parameters p ON p.object_id = ep.major_id AND p.parameter_id = ep.minor_id
WHERE ep.class_desc = 'PARAMETER'
  AND ep.major_id = OBJECT_ID(@full)
  AND p.name IS NOT NULL
ORDER BY p.name, ep.name;";
            command.Parameters.Clear();
            command.Parameters.AddWithValue("@full", $"[{obj.Schema}].[{obj.Name}]");

            using var paramReader = command.ExecuteReader();
            while (paramReader.Read())
            {
                var propName = paramReader.GetString(0);
                var propValue = paramReader.GetValue(1).ToString() ?? string.Empty;
                var paramName = paramReader.GetString(2);
                lines.Add(BuildExtendedPropertyStatement(
                    propertyProcedure,
                    propName,
                    propValue,
                    "SCHEMA",
                    obj.Schema,
                    className,
                    obj.Name,
                    "PARAMETER",
                    paramName));
                lines.Add("GO");
            }
        }

        return lines;
    }

    internal static IReadOnlyList<string> ReorderTableKeyAndIndexStatements(
        string[]? referenceLines,
        IReadOnlyList<string> keyConstraintLines,
        IReadOnlyList<string> nonConstraintIndexLines,
        IReadOnlyList<string> xmlIndexLines)
    {
        var statements = new List<TablePostCreateStatement>();
        statements.AddRange(ParseTablePostCreateStatements(keyConstraintLines, TablePostCreateStatementKind.KeyConstraint));
        statements.AddRange(ParseTablePostCreateStatements(nonConstraintIndexLines, TablePostCreateStatementKind.NonConstraintIndex));
        statements.AddRange(ParseTablePostCreateStatements(xmlIndexLines, TablePostCreateStatementKind.XmlIndex));

        if (statements.Count == 0)
        {
            return Array.Empty<string>();
        }

        var referenceOrder = GetReferenceTableKeyAndIndexOrder(referenceLines);
        if (referenceOrder == null || referenceOrder.Count == 0)
        {
            return FlattenStatements(statements);
        }

        var statementMap = new Dictionary<string, TablePostCreateStatement>(StringComparer.Ordinal);
        foreach (var statement in statements)
        {
            var key = BuildTablePostCreateStatementKey(statement.Kind, statement.Name);
            if (!statementMap.ContainsKey(key))
            {
                statementMap[key] = statement;
            }
        }

        var ordered = new List<TablePostCreateStatement>();
        var emitted = new HashSet<string>(StringComparer.Ordinal);
        foreach (var entry in referenceOrder)
        {
            var key = BuildTablePostCreateStatementKey(entry.Kind, entry.Name);
            if (statementMap.TryGetValue(key, out var statement) && emitted.Add(key))
            {
                ordered.Add(statement);
            }
        }

        foreach (var statement in statements)
        {
            var key = BuildTablePostCreateStatementKey(statement.Kind, statement.Name);
            if (emitted.Add(key))
            {
                ordered.Add(statement);
            }
        }

        return FlattenStatements(ordered);
    }

    private static List<TablePostCreateStatement> ParseTablePostCreateStatements(
        IReadOnlyList<string> lines,
        TablePostCreateStatementKind defaultKind)
    {
        var statements = new List<TablePostCreateStatement>();
        var ordinal = 0;
        foreach (var statementLines in SplitStatements(lines))
        {
            if (TryGetTablePostCreateStatementInfo(statementLines, defaultKind, out var kind, out var name))
            {
                statements.Add(new TablePostCreateStatement(kind, name, statementLines));
            }
            else
            {
                statements.Add(new TablePostCreateStatement(defaultKind, $"__ordinal_{ordinal++}", statementLines));
            }
        }

        return statements;
    }

    private static List<(TablePostCreateStatementKind Kind, string Name)>? GetReferenceTableKeyAndIndexOrder(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return null;
        }

        var order = new List<(TablePostCreateStatementKind Kind, string Name)>();
        foreach (var statementLines in SplitStatements(referenceLines))
        {
            if (TryGetTablePostCreateStatementInfo(statementLines, null, out var kind, out var name))
            {
                order.Add((kind, name));
            }
        }

        return order.Count == 0 ? null : order;
    }

    private static List<List<string>> SplitStatements(IEnumerable<string> lines)
    {
        var statements = new List<List<string>>();
        var current = new List<string>();
        foreach (var line in lines)
        {
            current.Add(line);
            if (string.Equals(line.Trim(), "GO", StringComparison.OrdinalIgnoreCase))
            {
                statements.Add(current);
                current = new List<string>();
            }
        }

        if (current.Count > 0)
        {
            statements.Add(current);
        }

        return statements;
    }

    private static bool TryGetTablePostCreateStatementInfo(
        IReadOnlyList<string> statementLines,
        TablePostCreateStatementKind? defaultKind,
        out TablePostCreateStatementKind kind,
        out string name)
    {
        kind = default;
        name = string.Empty;

        var firstLine = statementLines
            .Select(line => line.TrimStart())
            .FirstOrDefault(line => line.Length > 0 && !string.Equals(line, "GO", StringComparison.OrdinalIgnoreCase));
        if (string.IsNullOrWhiteSpace(firstLine))
        {
            return false;
        }

        if (firstLine.StartsWith("ALTER TABLE", StringComparison.OrdinalIgnoreCase) &&
            firstLine.IndexOf("ADD CONSTRAINT [", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            if (firstLine.IndexOf("FOREIGN KEY", StringComparison.OrdinalIgnoreCase) >= 0 ||
                firstLine.IndexOf(" CHECK", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                return false;
            }

            if (firstLine.IndexOf("PRIMARY KEY", StringComparison.OrdinalIgnoreCase) >= 0 ||
                firstLine.IndexOf("UNIQUE", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                var constraintName = ExtractBracketedIdentifier(firstLine, "ADD CONSTRAINT [");
                if (constraintName == null)
                {
                    return false;
                }

                kind = TablePostCreateStatementKind.KeyConstraint;
                name = constraintName;
                return true;
            }
        }

        if (firstLine.StartsWith("CREATE PRIMARY XML INDEX [", StringComparison.OrdinalIgnoreCase) ||
            firstLine.StartsWith("CREATE XML INDEX [", StringComparison.OrdinalIgnoreCase))
        {
            var indexName = ExtractBracketedIdentifier(firstLine, "INDEX [");
            if (indexName == null)
            {
                return false;
            }

            kind = TablePostCreateStatementKind.XmlIndex;
            name = indexName;
            return true;
        }

        if (firstLine.StartsWith("CREATE", StringComparison.OrdinalIgnoreCase) &&
            firstLine.IndexOf(" INDEX [", StringComparison.OrdinalIgnoreCase) >= 0)
        {
            var indexName = ExtractBracketedIdentifier(firstLine, "INDEX [");
            if (indexName == null)
            {
                return false;
            }

            kind = defaultKind ?? TablePostCreateStatementKind.NonConstraintIndex;
            name = indexName;
            return true;
        }

        return false;
    }

    private static string? ExtractBracketedIdentifier(string line, string marker)
    {
        var start = line.IndexOf(marker, StringComparison.OrdinalIgnoreCase);
        if (start < 0)
        {
            return null;
        }

        start += marker.Length;
        var end = line.IndexOf(']', start);
        return end > start ? line.Substring(start, end - start) : null;
    }

    private static string BuildTablePostCreateStatementKey(TablePostCreateStatementKind kind, string name)
        => $"{kind}|{name}";

    private static List<string> FlattenStatements(IEnumerable<TablePostCreateStatement> statements)
    {
        var lines = new List<string>();
        foreach (var statement in statements)
        {
            lines.AddRange(statement.Lines);
        }

        return lines;
    }

    private static IEnumerable<string> ReadViewIndexes(
        SqlConnection connection,
        string fullName,
        string[]? referenceLines)
        => ReadNonConstraintIndexes(connection, fullName, referenceLines);

    private static IEnumerable<string> ReadTableXmlIndexes(
        SqlConnection connection,
        string fullName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT xi.index_id, xi.name, c.name AS column_name,
       primary_xi.name AS primary_xml_index_name,
       xi.secondary_type_desc,
       xi.xml_index_type_description
FROM sys.xml_indexes xi
JOIN sys.index_columns ic ON ic.object_id = xi.object_id AND ic.index_id = xi.index_id AND ic.index_column_id = 1
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
LEFT JOIN sys.xml_indexes primary_xi ON primary_xi.object_id = xi.object_id AND primary_xi.index_id = xi.using_xml_index_id
WHERE xi.object_id = OBJECT_ID(@full)
ORDER BY xi.index_id;";
        command.Parameters.AddWithValue("@full", fullName);

        var lines = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var indexName = reader.GetString(1);
            var columnName = reader.GetString(2);
            var primaryXmlIndexName = reader.IsDBNull(3) ? null : reader.GetString(3);
            var secondaryType = reader.IsDBNull(4) ? null : reader.GetString(4);
            var xmlIndexType = reader.IsDBNull(5) ? string.Empty : reader.GetString(5);

            if (string.Equals(xmlIndexType, "PRIMARY_XML", StringComparison.OrdinalIgnoreCase))
            {
                lines.Add($"CREATE PRIMARY XML INDEX [{indexName}]");
                lines.Add($"ON {fullName} ([{columnName}])");
                lines.Add("GO");
                continue;
            }

            if (string.IsNullOrWhiteSpace(primaryXmlIndexName) || string.IsNullOrWhiteSpace(secondaryType))
            {
                continue;
            }

            lines.Add($"CREATE XML INDEX [{indexName}]");
            lines.Add($"ON {fullName} ([{columnName}])");
            lines.Add($"USING XML INDEX [{primaryXmlIndexName}]");
            lines.Add($"FOR {secondaryType}");
            lines.Add("GO");
        }

        return lines;
    }

    private static IEnumerable<string> ReadTableFullTextIndexes(
        SqlConnection connection,
        string fullName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT key_index.name AS key_index_name, catalog.name AS catalog_name
FROM sys.fulltext_indexes ft
JOIN sys.indexes key_index ON key_index.object_id = ft.object_id AND key_index.index_id = ft.unique_index_id
JOIN sys.fulltext_catalogs catalog ON catalog.fulltext_catalog_id = ft.fulltext_catalog_id
WHERE ft.object_id = OBJECT_ID(@full);";
        command.Parameters.AddWithValue("@full", fullName);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return Array.Empty<string>();
        }

        var keyIndexName = reader.GetString(0);
        var catalogName = reader.GetString(1);
        reader.Close();

        var lines = new List<string>
        {
            $"CREATE FULLTEXT INDEX ON {fullName} KEY INDEX [{keyIndexName}] ON [{catalogName}]",
            "GO"
        };

        command.CommandText = @"
SELECT c.name AS column_name, type_column.name AS type_column_name, fic.language_id
FROM sys.fulltext_index_columns fic
JOIN sys.columns c ON c.object_id = fic.object_id AND c.column_id = fic.column_id
LEFT JOIN sys.columns type_column ON type_column.object_id = fic.object_id AND type_column.column_id = fic.type_column_id
WHERE fic.object_id = OBJECT_ID(@full)
ORDER BY fic.column_id;";
        command.Parameters.Clear();
        command.Parameters.AddWithValue("@full", fullName);

        using var columnReader = command.ExecuteReader();
        while (columnReader.Read())
        {
            var columnName = columnReader.GetString(0);
            var typeColumnName = columnReader.IsDBNull(1) ? null : columnReader.GetString(1);
            var languageId = columnReader.IsDBNull(2) ? (int?)null : columnReader.GetInt32(2);
            var typeClause = string.IsNullOrWhiteSpace(typeColumnName)
                ? string.Empty
                : $" TYPE COLUMN [{typeColumnName}]";
            var languageClause = languageId.HasValue ? $" LANGUAGE {languageId.Value}" : string.Empty;
            lines.Add($"ALTER FULLTEXT INDEX ON {fullName} ADD ([{columnName}]{typeClause}{languageClause})");
            lines.Add("GO");
        }

        return lines;
    }

    private static string? GetExtendedPropertyClassName(string objectType)
    {
        return objectType switch
        {
            "View" => "VIEW",
            "StoredProcedure" => "PROCEDURE",
            "Function" => "FUNCTION",
            "Trigger" => "TRIGGER",
            _ => null
        };
    }

    private static IEnumerable<string> ReadSchemaExtendedProperties(
        SqlConnection connection,
        string schemaName,
        string[]? referenceLines)
    {
        var propertyProcedure = GetExtendedPropertyProcedure(referenceLines);
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
JOIN sys.schemas s ON s.schema_id = ep.major_id
WHERE ep.class_desc = 'SCHEMA' AND s.name = @name
ORDER BY ep.name;";
        command.Parameters.AddWithValue("@name", schemaName);

        var lines = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var propName = reader.GetString(0);
            var propValue = reader.GetValue(1).ToString() ?? string.Empty;
            lines.Add(BuildExtendedPropertyStatement(
                propertyProcedure,
                propName,
                propValue,
                "SCHEMA",
                schemaName,
                null,
                null,
                null,
                null));
            lines.Add("GO");
        }

        return lines;
    }

    private static IEnumerable<string> ReadPrincipalExtendedProperties(
        SqlConnection connection,
        string principalName,
        string[]? referenceLines)
    {
        var propertyProcedure = GetExtendedPropertyProcedure(referenceLines);
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
JOIN sys.database_principals dp ON dp.principal_id = ep.major_id
WHERE ep.class_desc = 'DATABASE_PRINCIPAL' AND dp.name = @name
ORDER BY ep.name;";
        command.Parameters.AddWithValue("@name", principalName);

        var lines = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var propName = reader.GetString(0);
            var propValue = reader.GetValue(1).ToString() ?? string.Empty;
            lines.Add(BuildExtendedPropertyStatement(
                propertyProcedure,
                propName,
                propValue,
                "USER",
                principalName,
                null,
                null,
                null,
                null));
            lines.Add("GO");
        }

        return lines;
    }

    private static IEnumerable<string> ExecuteExtendedPropertyQuery(
        SqlConnection connection,
        string commandText,
        Action<SqlCommand> configureCommand,
        string[]? referenceLines,
        string level0Type,
        string level0Name,
        string? level1Type,
        string? level1Name)
    {
        var propertyProcedure = GetExtendedPropertyProcedure(referenceLines);
        using var command = connection.CreateCommand();
        command.CommandText = commandText;
        configureCommand(command);

        var lines = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var propName = reader.GetString(0);
            var propValue = reader.GetValue(1).ToString() ?? string.Empty;
            lines.Add(BuildExtendedPropertyStatement(
                propertyProcedure,
                propName,
                propValue,
                level0Type,
                level0Name,
                level1Type,
                level1Name,
                null,
                null));
            lines.Add("GO");
        }

        return lines;
    }

    private static IEnumerable<string> ReadSchemaScopedObjectExtendedProperties(
        SqlConnection connection,
        string schema,
        string name,
        string objectTypeCode,
        string level1Type,
        string[]? referenceLines)
    {
        var propertyProcedure = GetExtendedPropertyProcedure(referenceLines);
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
JOIN sys.objects o ON o.object_id = ep.major_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE ep.class_desc = 'OBJECT_OR_COLUMN'
  AND ep.minor_id = 0
  AND s.name = @schema
  AND o.name = @name
  AND o.type = @type
ORDER BY ep.name;";
        command.Parameters.AddWithValue("@schema", schema);
        command.Parameters.AddWithValue("@name", name);
        command.Parameters.AddWithValue("@type", objectTypeCode);

        var lines = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var propName = reader.GetString(0);
            var propValue = reader.GetValue(1).ToString() ?? string.Empty;
            lines.Add(BuildExtendedPropertyStatement(
                propertyProcedure,
                propName,
                propValue,
                "SCHEMA",
                schema,
                level1Type,
                name,
                null,
                null));
            lines.Add("GO");
        }

        return lines;
    }

    private static IEnumerable<string> ReadXmlSchemaCollectionPermissions(
        SqlConnection connection,
        string schema,
        string name,
        string[]? referenceLines)
        => ExecutePermissionQuery(
            connection,
            @"
SELECT dp.permission_name, dp.state_desc, pr.name AS principal_name
FROM sys.database_permissions dp
JOIN sys.database_principals pr ON pr.principal_id = dp.grantee_principal_id
JOIN sys.xml_schema_collections x ON x.xml_collection_id = dp.major_id
JOIN sys.schemas s ON s.schema_id = x.schema_id
WHERE dp.class_desc = 'XML_SCHEMA_COLLECTION'
  AND s.name = @schema
  AND x.name = @name
ORDER BY pr.name, dp.permission_name;",
            command =>
            {
                command.Parameters.AddWithValue("@schema", schema);
                command.Parameters.AddWithValue("@name", name);
            },
            $"XML SCHEMA COLLECTION::{QuoteIdentifier(schema)}.{QuoteIdentifier(name)}",
            referenceLines);

    private static IEnumerable<string> ReadXmlSchemaCollectionExtendedProperties(
        SqlConnection connection,
        string schema,
        string name,
        string[]? referenceLines)
        => ExecuteExtendedPropertyQuery(
            connection,
            @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
JOIN sys.xml_schema_collections x ON x.xml_collection_id = ep.major_id
JOIN sys.schemas s ON s.schema_id = x.schema_id
WHERE ep.class_desc = 'XML_SCHEMA_COLLECTION'
  AND s.name = @schema
  AND x.name = @name
ORDER BY ep.name;",
            command =>
            {
                command.Parameters.AddWithValue("@schema", schema);
                command.Parameters.AddWithValue("@name", name);
            },
            referenceLines,
            "SCHEMA",
            schema,
            "XML SCHEMA COLLECTION",
            name);

    private static IEnumerable<string> ReadMessageTypePermissions(
        SqlConnection connection,
        string name,
        string[]? referenceLines)
        => ExecutePermissionQuery(
            connection,
            @"
SELECT dp.permission_name, dp.state_desc, pr.name AS principal_name
FROM sys.database_permissions dp
JOIN sys.database_principals pr ON pr.principal_id = dp.grantee_principal_id
JOIN sys.service_message_types mt ON mt.message_type_id = dp.major_id
WHERE dp.class_desc = 'MESSAGE_TYPE'
  AND mt.name = @name
ORDER BY pr.name, dp.permission_name;",
            command => command.Parameters.AddWithValue("@name", name),
            $"MESSAGE TYPE::{QuoteIdentifier(name)}",
            referenceLines);

    private static IEnumerable<string> ReadMessageTypeExtendedProperties(
        SqlConnection connection,
        string name,
        string[]? referenceLines)
        => ExecuteExtendedPropertyQuery(
            connection,
            @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
JOIN sys.service_message_types mt ON mt.message_type_id = ep.major_id
WHERE ep.class_desc = 'MESSAGE_TYPE'
  AND mt.name = @name
ORDER BY ep.name;",
            command => command.Parameters.AddWithValue("@name", name),
            referenceLines,
            "MESSAGE TYPE",
            name,
            null,
            null);

    private static IEnumerable<string> ReadContractPermissions(
        SqlConnection connection,
        string name,
        string[]? referenceLines)
        => ExecutePermissionQuery(
            connection,
            @"
SELECT dp.permission_name, dp.state_desc, pr.name AS principal_name
FROM sys.database_permissions dp
JOIN sys.database_principals pr ON pr.principal_id = dp.grantee_principal_id
JOIN sys.service_contracts c ON c.service_contract_id = dp.major_id
WHERE dp.class_desc = 'SERVICE_CONTRACT'
  AND c.name = @name
ORDER BY pr.name, dp.permission_name;",
            command => command.Parameters.AddWithValue("@name", name),
            $"CONTRACT::{QuoteIdentifier(name)}",
            referenceLines);

    private static IEnumerable<string> ReadContractExtendedProperties(
        SqlConnection connection,
        string name,
        string[]? referenceLines)
        => ExecuteExtendedPropertyQuery(
            connection,
            @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
JOIN sys.service_contracts c ON c.service_contract_id = ep.major_id
WHERE ep.class_desc = 'SERVICE_CONTRACT'
  AND c.name = @name
ORDER BY ep.name;",
            command => command.Parameters.AddWithValue("@name", name),
            referenceLines,
            "CONTRACT",
            name,
            null,
            null);

    private static IEnumerable<string> ReadQueueExtendedProperties(
        SqlConnection connection,
        string schema,
        string name,
        string[]? referenceLines)
        => ReadSchemaScopedObjectExtendedProperties(connection, schema, name, "SQ", "QUEUE", referenceLines);

    private static IEnumerable<string> ReadServicePermissions(
        SqlConnection connection,
        string name,
        string[]? referenceLines)
        => ExecutePermissionQuery(
            connection,
            @"
SELECT dp.permission_name, dp.state_desc, pr.name AS principal_name
FROM sys.database_permissions dp
JOIN sys.database_principals pr ON pr.principal_id = dp.grantee_principal_id
JOIN sys.services sv ON sv.service_id = dp.major_id
WHERE dp.class_desc = 'SERVICE'
  AND sv.name = @name
ORDER BY pr.name, dp.permission_name;",
            command => command.Parameters.AddWithValue("@name", name),
            $"SERVICE::{QuoteIdentifier(name)}",
            referenceLines);

    private static IEnumerable<string> ReadServiceExtendedProperties(
        SqlConnection connection,
        string name,
        string[]? referenceLines)
        => ExecuteExtendedPropertyQuery(
            connection,
            @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
JOIN sys.services sv ON sv.service_id = ep.major_id
WHERE ep.class_desc = 'SERVICE'
  AND sv.name = @name
ORDER BY ep.name;",
            command => command.Parameters.AddWithValue("@name", name),
            referenceLines,
            "SERVICE",
            name,
            null,
            null);

    private static IEnumerable<string> ReadRoutePermissions(
        SqlConnection connection,
        string name,
        string[]? referenceLines)
        => ExecutePermissionQuery(
            connection,
            @"
SELECT dp.permission_name, dp.state_desc, pr.name AS principal_name
FROM sys.database_permissions dp
JOIN sys.database_principals pr ON pr.principal_id = dp.grantee_principal_id
JOIN sys.routes r ON r.route_id = dp.major_id
WHERE dp.class_desc = 'ROUTE'
  AND r.name = @name
ORDER BY pr.name, dp.permission_name;",
            command => command.Parameters.AddWithValue("@name", name),
            $"ROUTE::{QuoteIdentifier(name)}",
            referenceLines);

    private static IEnumerable<string> ReadRouteExtendedProperties(
        SqlConnection connection,
        string name,
        string[]? referenceLines)
        => ExecuteExtendedPropertyQuery(
            connection,
            @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
JOIN sys.routes r ON r.route_id = ep.major_id
WHERE ep.class_desc = 'ROUTE'
  AND r.name = @name
ORDER BY ep.name;",
            command => command.Parameters.AddWithValue("@name", name),
            referenceLines,
            "ROUTE",
            name,
            null,
            null);

    private static IEnumerable<string> ReadEventNotificationExtendedProperties(
        SqlConnection connection,
        string name,
        string[]? referenceLines)
        => ExecuteExtendedPropertyQuery(
            connection,
            @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
JOIN sys.event_notifications en ON en.object_id = ep.major_id
WHERE ep.class_desc = 'OBJECT_OR_COLUMN'
  AND ep.minor_id = 0
  AND en.name = @name
ORDER BY ep.name;",
            command => command.Parameters.AddWithValue("@name", name),
            referenceLines,
            "EVENT NOTIFICATION",
            name,
            null,
            null);

    private static IEnumerable<string> ReadServiceBindingPermissions(
        SqlConnection connection,
        string name,
        string[]? referenceLines)
        => ExecutePermissionQuery(
            connection,
            @"
SELECT dp.permission_name, dp.state_desc, pr.name AS principal_name
FROM sys.database_permissions dp
JOIN sys.database_principals pr ON pr.principal_id = dp.grantee_principal_id
JOIN sys.remote_service_bindings rsb ON rsb.remote_service_binding_id = dp.major_id
WHERE dp.class_desc = 'REMOTE_SERVICE_BINDING'
  AND rsb.name = @name
ORDER BY pr.name, dp.permission_name;",
            command => command.Parameters.AddWithValue("@name", name),
            $"REMOTE SERVICE BINDING::{QuoteIdentifier(name)}",
            referenceLines);

    private static IEnumerable<string> ReadServiceBindingExtendedProperties(
        SqlConnection connection,
        string name,
        string[]? referenceLines)
        => ExecuteExtendedPropertyQuery(
            connection,
            @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
JOIN sys.remote_service_bindings rsb ON rsb.remote_service_binding_id = ep.major_id
WHERE ep.class_desc = 'REMOTE_SERVICE_BINDING'
  AND rsb.name = @name
ORDER BY ep.name;",
            command => command.Parameters.AddWithValue("@name", name),
            referenceLines,
            "REMOTE SERVICE BINDING",
            name,
            null,
            null);

    private static IEnumerable<string> ReadFullTextCatalogPermissions(
        SqlConnection connection,
        string name,
        string[]? referenceLines)
        => ExecutePermissionQuery(
            connection,
            @"
SELECT dp.permission_name, dp.state_desc, pr.name AS principal_name
FROM sys.database_permissions dp
JOIN sys.database_principals pr ON pr.principal_id = dp.grantee_principal_id
JOIN sys.fulltext_catalogs fc ON fc.fulltext_catalog_id = dp.major_id
WHERE dp.class_desc = 'FULLTEXT_CATALOG'
  AND fc.name = @name
ORDER BY pr.name, dp.permission_name;",
            command => command.Parameters.AddWithValue("@name", name),
            $"FULLTEXT CATALOG::{QuoteIdentifier(name)}",
            referenceLines);

    private static IEnumerable<string> ReadFullTextStoplistPermissions(
        SqlConnection connection,
        string name,
        string[]? referenceLines)
        => ExecutePermissionQuery(
            connection,
            @"
SELECT dp.permission_name, dp.state_desc, pr.name AS principal_name
FROM sys.database_permissions dp
JOIN sys.database_principals pr ON pr.principal_id = dp.grantee_principal_id
JOIN sys.fulltext_stoplists fs ON fs.stoplist_id = dp.major_id
WHERE dp.class_desc = 'FULLTEXT_STOPLIST'
  AND fs.name = @name
ORDER BY pr.name, dp.permission_name;",
            command => command.Parameters.AddWithValue("@name", name),
            $"FULLTEXT STOPLIST::{QuoteIdentifier(name)}",
            referenceLines);

    private static IEnumerable<string> ReadSearchPropertyListPermissions(
        SqlConnection connection,
        string name,
        string[]? referenceLines)
        => ExecutePermissionQuery(
            connection,
            @"
SELECT dp.permission_name, dp.state_desc, pr.name AS principal_name
FROM sys.database_permissions dp
JOIN sys.database_principals pr ON pr.principal_id = dp.grantee_principal_id
JOIN sys.registered_search_property_lists spl ON spl.property_list_id = dp.major_id
WHERE dp.class_desc = 'SEARCH_PROPERTY_LIST'
  AND spl.name = @name
ORDER BY pr.name, dp.permission_name;",
            command => command.Parameters.AddWithValue("@name", name),
            $"SEARCH PROPERTY LIST::{QuoteIdentifier(name)}",
            referenceLines);

    private static IEnumerable<string> ReadSequenceExtendedProperties(
        SqlConnection connection,
        string schema,
        string name,
        string[]? referenceLines)
        => ReadSchemaScopedObjectExtendedProperties(connection, schema, name, "SO", "SEQUENCE", referenceLines);

    private static IEnumerable<string> ReadSynonymExtendedProperties(
        SqlConnection connection,
        string schema,
        string name,
        string[]? referenceLines)
        => ReadSchemaScopedObjectExtendedProperties(connection, schema, name, "SN", "SYNONYM", referenceLines);

    private static IEnumerable<string> ReadUserDefinedTypeExtendedProperties(
        SqlConnection connection,
        string schema,
        string name,
        string[]? referenceLines)
    {
        var propertyProcedure = GetExtendedPropertyProcedure(referenceLines);
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
JOIN sys.types t ON t.user_type_id = ep.major_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE ep.class_desc = 'TYPE'
  AND t.is_user_defined = 1
  AND t.is_table_type = 0
  AND s.name = @schema
  AND t.name = @name
ORDER BY ep.name;";
        command.Parameters.AddWithValue("@schema", schema);
        command.Parameters.AddWithValue("@name", name);

        var lines = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var propName = reader.GetString(0);
            var propValue = reader.GetValue(1).ToString() ?? string.Empty;
            lines.Add(BuildExtendedPropertyStatement(
                propertyProcedure,
                propName,
                propValue,
                "SCHEMA",
                schema,
                "TYPE",
                name,
                null,
                null));
            lines.Add("GO");
        }

        return lines;
    }

    private static IEnumerable<string> ReadPartitionFunctionExtendedProperties(
        SqlConnection connection,
        string name,
        string[]? referenceLines)
    {
        var propertyProcedure = GetExtendedPropertyProcedure(referenceLines);
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
JOIN sys.partition_functions pf ON pf.function_id = ep.major_id
WHERE ep.class_desc = 'PARTITION_FUNCTION' AND pf.name = @name
ORDER BY ep.name;";
        command.Parameters.AddWithValue("@name", name);

        var lines = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var propName = reader.GetString(0);
            var propValue = reader.GetValue(1).ToString() ?? string.Empty;
            lines.Add(BuildExtendedPropertyStatement(
                propertyProcedure,
                propName,
                propValue,
                "PARTITION FUNCTION",
                name,
                null,
                null,
                null,
                null));
            lines.Add("GO");
        }

        return lines;
    }

    private static IEnumerable<string> ReadPartitionSchemeExtendedProperties(
        SqlConnection connection,
        string name,
        string[]? referenceLines)
    {
        var propertyProcedure = GetExtendedPropertyProcedure(referenceLines);
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
JOIN sys.partition_schemes ps ON ps.data_space_id = ep.major_id
WHERE ep.class_desc = 'DATASPACE' AND ps.name = @name
ORDER BY ep.name;";
        command.Parameters.AddWithValue("@name", name);

        var lines = new List<string>();
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var propName = reader.GetString(0);
            var propValue = reader.GetValue(1).ToString() ?? string.Empty;
            lines.Add(BuildExtendedPropertyStatement(
                propertyProcedure,
                propName,
                propValue,
                "PARTITION SCHEME",
                name,
                null,
                null,
                null,
                null));
            lines.Add("GO");
        }

        return lines;
    }

    private static IEnumerable<string> ReadTableExtendedProperties(
        SqlConnection connection,
        string schema,
        string name,
        string[]? referenceLines)
    {
        var propertyProcedure = GetExtendedPropertyProcedure(referenceLines);
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT ep.name AS prop_name, ep.value AS prop_value
FROM sys.extended_properties ep
WHERE ep.class_desc = 'OBJECT_OR_COLUMN' AND ep.major_id = OBJECT_ID(@full) AND ep.minor_id = 0
ORDER BY ep.name;";
        command.Parameters.AddWithValue("@full", $"[{schema}].[{name}]");

        var lines = new List<string>();
        using (var reader = command.ExecuteReader())
        {
            while (reader.Read())
            {
                var propName = reader.GetString(0);
                var propValue = reader.GetValue(1).ToString() ?? string.Empty;
                lines.Add(BuildExtendedPropertyStatement(
                    propertyProcedure,
                    propName,
                    propValue,
                    "SCHEMA",
                    schema,
                    "TABLE",
                    name,
                    null,
                    null));
                lines.Add("GO");
            }
        }

        command.CommandText = @"
SELECT ep.name AS prop_name, ep.value AS prop_value, c.name AS column_name
FROM sys.extended_properties ep
JOIN sys.columns c ON c.object_id = ep.major_id AND c.column_id = ep.minor_id
WHERE ep.class_desc = 'OBJECT_OR_COLUMN' AND ep.major_id = OBJECT_ID(@full) AND ep.minor_id <> 0
ORDER BY c.name, ep.name;";
        command.Parameters.Clear();
        command.Parameters.AddWithValue("@full", $"[{schema}].[{name}]");

        using (var columnReader = command.ExecuteReader())
        {
            while (columnReader.Read())
            {
                var propName = columnReader.GetString(0);
                var propValue = columnReader.GetValue(1).ToString() ?? string.Empty;
                var columnName = columnReader.GetString(2);
                lines.Add(BuildExtendedPropertyStatement(
                    propertyProcedure,
                    propName,
                    propValue,
                    "SCHEMA",
                    schema,
                    "TABLE",
                    name,
                    "COLUMN",
                    columnName));
                lines.Add("GO");
            }
        }

        command.CommandText = @"
SELECT ep.name AS prop_name, ep.value AS prop_value, o.name AS constraint_name
FROM sys.extended_properties ep
JOIN sys.objects o ON o.object_id = ep.major_id
WHERE ep.class_desc = 'OBJECT_OR_COLUMN'
  AND ep.minor_id = 0
  AND o.parent_object_id = OBJECT_ID(@full)
  AND o.type IN ('C','D','F','PK','UQ')
ORDER BY o.name, ep.name;";
        command.Parameters.Clear();
        command.Parameters.AddWithValue("@full", $"[{schema}].[{name}]");

        using (var constraintReader = command.ExecuteReader())
        {
            while (constraintReader.Read())
            {
                var propName = constraintReader.GetString(0);
                var propValue = constraintReader.GetValue(1).ToString() ?? string.Empty;
                var constraintName = constraintReader.GetString(2);
                lines.Add(BuildExtendedPropertyStatement(
                    propertyProcedure,
                    propName,
                    propValue,
                    "SCHEMA",
                    schema,
                    "TABLE",
                    name,
                    "CONSTRAINT",
                    constraintName));
                lines.Add("GO");
            }
        }

        command.CommandText = @"
SELECT ep.name AS prop_name, ep.value AS prop_value, i.name AS index_name
FROM sys.extended_properties ep
JOIN sys.indexes i ON i.object_id = ep.major_id AND i.index_id = ep.minor_id
WHERE ep.class_desc = 'INDEX' AND ep.major_id = OBJECT_ID(@full)
ORDER BY i.name, ep.name;";
        command.Parameters.Clear();
        command.Parameters.AddWithValue("@full", $"[{schema}].[{name}]");

        using (var indexReader = command.ExecuteReader())
        {
            while (indexReader.Read())
            {
                var propName = indexReader.GetString(0);
                var propValue = indexReader.GetValue(1).ToString() ?? string.Empty;
                var indexName = indexReader.GetString(2);
                lines.Add(BuildExtendedPropertyStatement(
                    propertyProcedure,
                    propName,
                    propValue,
                    "SCHEMA",
                    schema,
                    "TABLE",
                    name,
                    "INDEX",
                    indexName));
                lines.Add("GO");
            }
        }

        command.CommandText = @"
SELECT ep.name AS prop_name, ep.value AS prop_value, t.name AS trigger_name
FROM sys.extended_properties ep
JOIN sys.triggers t ON t.object_id = ep.major_id
WHERE ep.class_desc = 'OBJECT_OR_COLUMN'
  AND ep.minor_id = 0
  AND t.parent_id = OBJECT_ID(@full)
  AND t.parent_class_desc = 'OBJECT_OR_COLUMN'
ORDER BY t.name, ep.name;";
        command.Parameters.Clear();
        command.Parameters.AddWithValue("@full", $"[{schema}].[{name}]");

        using var triggerReader = command.ExecuteReader();
        while (triggerReader.Read())
        {
            var propName = triggerReader.GetString(0);
            var propValue = triggerReader.GetValue(1).ToString() ?? string.Empty;
            var triggerName = triggerReader.GetString(2);
            lines.Add(BuildExtendedPropertyStatement(
                propertyProcedure,
                propName,
                propValue,
                "SCHEMA",
                schema,
                "TABLE",
                name,
                "TRIGGER",
                triggerName));
            lines.Add("GO");
        }

        return lines;
    }

    private static string? ReadTableLockEscalation(SqlConnection connection, string fullName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT t.lock_escalation_desc
FROM sys.tables t
WHERE t.object_id = OBJECT_ID(@full);";
        command.Parameters.AddWithValue("@full", fullName);
        var value = command.ExecuteScalar();
        return value is DBNull || value == null ? null : value.ToString();
    }

    private static string ResolveQueueName(SqlConnection connection, int objectId)
    {
        using var command = connection.CreateCommand();
        if (ColumnExists(connection, "sys.service_queues", "schema_id"))
        {
            command.CommandText = @"
SELECT s.name, q.name
FROM sys.service_queues q
JOIN sys.schemas s ON s.schema_id = q.schema_id
WHERE q.object_id = @id;";
        }
        else
        {
            command.CommandText = @"
SELECT s.name, q.name
FROM sys.service_queues q
JOIN sys.objects o ON o.object_id = q.object_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE q.object_id = @id;";
        }
        command.Parameters.AddWithValue("@id", objectId);
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return "[dbo].[UnknownQueue]";
        }

        return $"[{reader.GetString(0)}].[{reader.GetString(1)}]";
    }

    private static string ResolveObjectName(SqlConnection connection, int objectId)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT s.name, o.name
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE o.object_id = @id;";
        command.Parameters.AddWithValue("@id", objectId);
        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return "[dbo].[UnknownObject]";
        }

        return $"[{reader.GetString(0)}].[{reader.GetString(1)}]";
    }

    private static bool ColumnExists(SqlConnection connection, string tableName, string columnName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT 1
FROM sys.columns
WHERE object_id = OBJECT_ID(@table) AND name = @column;";
        command.Parameters.AddWithValue("@table", tableName);
        command.Parameters.AddWithValue("@column", columnName);
        var result = command.ExecuteScalar();
        return result is not null && result != DBNull.Value;
    }

    private static string[]? TryReadReferenceLines(string? referencePath)
    {
        if (string.IsNullOrWhiteSpace(referencePath) || !File.Exists(referencePath))
        {
            return null;
        }

        var content = File.ReadAllText(referencePath, Encoding.UTF8);
        return content.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None);
    }

    private static void AppendTrailingBlankLines(ICollection<string> lines, string[]? referenceLines)
    {
        var count = CountTrailingBlankLines(referenceLines);
        if (count == 0)
        {
            return;
        }

        for (var i = 0; i < count; i++)
        {
            lines.Add(string.Empty);
        }
    }

    private static void AppendTrailingBlankLinesExact(ICollection<string> lines, string[]? referenceLines)
    {
        var count = CountTrailingBlankLines(referenceLines);
        if (count == 0)
        {
            return;
        }

        for (var i = 0; i < count; i++)
        {
            lines.Add(string.Empty);
        }
    }

    private static int CountTrailingBlankLines(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return 0;
        }

        var count = 0;
        for (var i = referenceLines.Length - 1; i >= 0; i--)
        {
            if (referenceLines[i].Trim().Length == 0)
            {
                count++;
            }
            else
            {
                break;
            }
        }

        return count;
    }

    private static int CountTrailingBlankLinesInDefinition(string[] lines)
    {
        if (lines == null || lines.Length == 0)
        {
            return 0;
        }

        var count = 0;
        for (var i = lines.Length - 1; i >= 0; i--)
        {
            if (lines[i].Trim().Length == 0)
            {
                count++;
            }
            else
            {
                break;
            }
        }

        return count;
    }

    private static string[] TrimTrailingBlankLines(string[] lines)
    {
        if (lines.Length == 0)
        {
            return lines;
        }

        var end = lines.Length - 1;
        while (end >= 0 && lines[end].Trim().Length == 0)
        {
            end--;
        }

        if (end == lines.Length - 1)
        {
            return lines;
        }

        if (end < 0)
        {
            return Array.Empty<string>();
        }

        var trimmed = new string[end + 1];
        Array.Copy(lines, trimmed, end + 1);
        return trimmed;
    }

    private static bool ReferenceHasGoAfterDefinition(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return true;
        }

        var start = -1;
        for (var i = 0; i < referenceLines.Length; i++)
        {
            if (referenceLines[i].TrimStart().StartsWith("CREATE ", StringComparison.OrdinalIgnoreCase))
            {
                start = i;
                break;
            }
        }

        if (start < 0)
        {
            return true;
        }

        for (var i = start + 1; i < referenceLines.Length; i++)
        {
            if (string.Equals(referenceLines[i].Trim(), "GO", StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    internal static string TrimOuterBlankLines(string? text)
    {
        if (string.IsNullOrEmpty(text))
        {
            return string.Empty;
        }

        var lines = text.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None);
        var start = 0;
        while (start < lines.Length && lines[start].Trim().Length == 0)
        {
            start++;
        }

        var end = lines.Length - 1;
        while (end >= start && lines[end].Trim().Length == 0)
        {
            end--;
        }

        if (start > end)
        {
            return string.Empty;
        }

        var trimmed = new string[end - start + 1];
        Array.Copy(lines, start, trimmed, 0, trimmed.Length);
        return string.Join(Environment.NewLine, trimmed);
    }

    private static string ApplyDefinitionFormatting(string definition, string[]? referenceLines)
    {
        var referenceBlock = GetReferenceDefinitionBlock(referenceLines);
        if (referenceBlock != null && DefinitionMatchesReference(definition, referenceBlock))
        {
            return string.Join(Environment.NewLine, referenceBlock);
        }

        var lineMap = BuildDefinitionLineMap(referenceLines);
        if (lineMap == null || lineMap.Count == 0)
        {
            return definition;
        }

        var lines = definition.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None);
        for (var i = 0; i < lines.Length; i++)
        {
            var key = NormalizeDefinitionLineKey(lines[i]);
            if (key.Length == 0)
            {
                continue;
            }

            if (lineMap.TryGetValue(key, out var refLine))
            {
                lines[i] = refLine;
            }
        }

        return string.Join(Environment.NewLine, lines);
    }

    private static string[]? GetReferenceDefinitionBlock(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return null;
        }

        var start = -1;
        for (var i = 0; i < referenceLines.Length; i++)
        {
            if (referenceLines[i].TrimStart().StartsWith("CREATE ", StringComparison.OrdinalIgnoreCase))
            {
                start = i;
                break;
            }
        }

        if (start < 0)
        {
            return null;
        }

        var end = -1;
        for (var i = start + 1; i < referenceLines.Length; i++)
        {
            if (string.Equals(referenceLines[i].Trim(), "GO", StringComparison.OrdinalIgnoreCase))
            {
                end = i;
                break;
            }
        }

        if (end < 0)
        {
            end = referenceLines.Length;
        }

        if (end <= start)
        {
            return null;
        }

        var block = new string[end - start];
        Array.Copy(referenceLines, start, block, 0, end - start);
        return block;
    }

    private static bool DefinitionMatchesReference(string definition, string[] referenceBlock)
    {
        var definitionLines = definition.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None);
        var normalizedDefinition = NormalizeDefinitionLines(definitionLines);
        var normalizedReference = NormalizeDefinitionLines(referenceBlock);

        if (normalizedDefinition.Count != normalizedReference.Count)
        {
            return false;
        }

        for (var i = 0; i < normalizedDefinition.Count; i++)
        {
            if (!string.Equals(normalizedDefinition[i], normalizedReference[i], StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }
        }

        return true;
    }

    private static List<string> NormalizeDefinitionLines(string[] lines)
    {
        var normalized = new List<string>(lines.Length);
        foreach (var line in lines)
        {
            var key = NormalizeDefinitionLineKey(line);
            if (key.Length > 0)
            {
                normalized.Add(key);
            }
        }

        return normalized;
    }

    private static string ApplyDefinitionIndent(string definition, string indentPrefix)
    {
        if (string.IsNullOrEmpty(indentPrefix))
        {
            return definition;
        }

        var lines = definition.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None);
        if (lines.Length == 0)
        {
            return definition;
        }

        if (lines[0].Trim().Length > 0 && lines[0].Length == lines[0].TrimStart().Length)
        {
            lines[0] = indentPrefix + lines[0];
        }

        return string.Join(Environment.NewLine, lines);
    }

    private static Dictionary<string, string>? BuildDefinitionLineMap(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return null;
        }

        var start = -1;
        for (var i = 0; i < referenceLines.Length; i++)
        {
            if (referenceLines[i].TrimStart().StartsWith("CREATE ", StringComparison.OrdinalIgnoreCase))
            {
                start = i;
                break;
            }
        }

        if (start < 0)
        {
            return null;
        }

        var end = -1;
        for (var i = start + 1; i < referenceLines.Length; i++)
        {
            if (string.Equals(referenceLines[i].Trim(), "GO", StringComparison.OrdinalIgnoreCase))
            {
                end = i;
                break;
            }
        }

        if (end < 0)
        {
            end = referenceLines.Length;
        }

        if (end <= start)
        {
            return null;
        }

        var counts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        var map = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        for (var i = start; i < end; i++)
        {
            var key = NormalizeDefinitionLineKey(referenceLines[i]);
            if (key.Length == 0)
            {
                continue;
            }

            counts[key] = counts.TryGetValue(key, out var count) ? count + 1 : 1;
            if (!map.ContainsKey(key))
            {
                map[key] = referenceLines[i];
            }
        }

        var unique = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var kvp in map)
        {
            if (counts.TryGetValue(kvp.Key, out var count) && count == 1)
            {
                unique[kvp.Key] = kvp.Value;
            }
        }

        return unique;
    }

    private static string NormalizeDefinitionLineKey(string line)
    {
        var trimmed = line.Trim();
        if (trimmed.Length == 0)
        {
            return string.Empty;
        }

        var sb = new StringBuilder(trimmed.Length);
        var pendingSpace = false;
        foreach (var ch in trimmed)
        {
            if (char.IsWhiteSpace(ch))
            {
                pendingSpace = true;
                continue;
            }

            if (pendingSpace && sb.Length > 0)
            {
                sb.Append(' ');
            }

            sb.Append(ch);
            pendingSpace = false;
        }

        var normalized = sb.ToString();
        const string createOrAlter = "CREATE OR ALTER ";
        if (normalized.StartsWith(createOrAlter, StringComparison.OrdinalIgnoreCase))
        {
            normalized = "CREATE " + normalized.Substring(createOrAlter.Length);
        }

        return normalized;
    }

    private static List<string> BuildSetHeaderLines(string[]? referenceLines, string quotedLine, string ansiLine)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return new List<string> { quotedLine, "GO", ansiLine, "GO" };
        }

        var quotedIndex = -1;
        var ansiIndex = -1;
        for (var i = 0; i < referenceLines.Length; i++)
        {
            var trimmed = referenceLines[i].TrimStart();
            if (quotedIndex < 0 &&
                trimmed.StartsWith("SET QUOTED_IDENTIFIER", StringComparison.OrdinalIgnoreCase))
            {
                quotedIndex = i;
            }
            else if (ansiIndex < 0 &&
                trimmed.StartsWith("SET ANSI_NULLS", StringComparison.OrdinalIgnoreCase))
            {
                ansiIndex = i;
            }

            if (quotedIndex >= 0 && ansiIndex >= 0)
            {
                break;
            }
        }

        if (quotedIndex < 0 || ansiIndex < 0)
        {
            return new List<string> { quotedLine, "GO", ansiLine, "GO" };
        }

        var quotedFirst = quotedIndex <= ansiIndex;
        var firstLine = quotedFirst ? quotedLine : ansiLine;
        var secondLine = quotedFirst ? ansiLine : quotedLine;
        var firstSetIndex = quotedFirst ? quotedIndex : ansiIndex;
        var secondSetIndex = quotedFirst ? ansiIndex : quotedIndex;

        var firstGoIndex = -1;
        for (var i = firstSetIndex + 1; i < referenceLines.Length; i++)
        {
            if (string.Equals(referenceLines[i].Trim(), "GO", StringComparison.OrdinalIgnoreCase))
            {
                firstGoIndex = i;
                break;
            }
        }

        var blankLinesBetween = 0;
        if (firstGoIndex >= 0 && secondSetIndex > firstGoIndex)
        {
            for (var i = firstGoIndex + 1; i < secondSetIndex; i++)
            {
                if (referenceLines[i].Trim().Length == 0)
                {
                    blankLinesBetween++;
                }
                else
                {
                    break;
                }
            }
        }

        var lines = new List<string> { firstLine, "GO" };
        for (var i = 0; i < blankLinesBetween; i++)
        {
            lines.Add(string.Empty);
        }

        lines.Add(secondLine);
        lines.Add("GO");
        return lines;
    }

    private static PartitionFunctionFormat? GetPartitionFunctionFormat(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return null;
        }

        var start = -1;
        for (var i = 0; i < referenceLines.Length; i++)
        {
            if (referenceLines[i].TrimStart().StartsWith("CREATE PARTITION FUNCTION", StringComparison.OrdinalIgnoreCase))
            {
                start = i;
                break;
            }
        }

        if (start < 0)
        {
            return null;
        }

        var end = -1;
        for (var i = start + 1; i < referenceLines.Length; i++)
        {
            if (string.Equals(referenceLines[i].Trim(), "GO", StringComparison.OrdinalIgnoreCase))
            {
                end = i;
                break;
            }
        }

        if (end < 0)
        {
            return null;
        }

        var bodyLines = referenceLines.Skip(start).Take(end - start).ToArray();
        if (bodyLines.Length == 0)
        {
            return null;
        }

        var firstLine = bodyLines[0];
        var spaceBeforeParen = firstLine.Contains("] (", StringComparison.Ordinal);
        var bracketSystemType = firstLine.Contains("([", StringComparison.Ordinal);
        if (bodyLines.Length == 1)
        {
            return new PartitionFunctionFormat(
                false,
                spaceBeforeParen,
                bracketSystemType,
                firstLine.EndsWith(" ", StringComparison.Ordinal),
                false);
        }

        var secondLine = bodyLines.Length > 1 ? bodyLines[1] : string.Empty;
        return new PartitionFunctionFormat(
            true,
            spaceBeforeParen,
            bracketSystemType,
            firstLine.EndsWith(" ", StringComparison.Ordinal),
            secondLine.EndsWith(" ", StringComparison.Ordinal));
    }

    private static PartitionSchemeFormat? GetPartitionSchemeFormat(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return null;
        }

        var start = -1;
        for (var i = 0; i < referenceLines.Length; i++)
        {
            if (referenceLines[i].TrimStart().StartsWith("CREATE PARTITION SCHEME", StringComparison.OrdinalIgnoreCase))
            {
                start = i;
                break;
            }
        }

        if (start < 0)
        {
            return null;
        }

        var end = -1;
        for (var i = start + 1; i < referenceLines.Length; i++)
        {
            if (string.Equals(referenceLines[i].Trim(), "GO", StringComparison.OrdinalIgnoreCase))
            {
                end = i;
                break;
            }
        }

        if (end < 0)
        {
            return null;
        }

        var bodyLines = referenceLines.Skip(start).Take(end - start).ToArray();
        if (bodyLines.Length == 0)
        {
            return null;
        }

        if (bodyLines.Length == 1)
        {
            return new PartitionSchemeFormat(
                false,
                bodyLines[0].EndsWith(" ", StringComparison.Ordinal),
                false);
        }

        return new PartitionSchemeFormat(
            true,
            bodyLines[0].EndsWith(" ", StringComparison.Ordinal),
            bodyLines[1].EndsWith(" ", StringComparison.Ordinal));
    }

    private static List<string> BuildPartitionSchemeLines(
        string schemeName,
        string functionName,
        string groupList,
        string[]? referenceLines)
    {
        var format = GetPartitionSchemeFormat(referenceLines);
        var lines = new List<string>();
        if (format != null && format.MultiLine)
        {
            var line1 = $"CREATE PARTITION SCHEME [{schemeName}]";
            if (format.Line1TrailingSpace)
            {
                line1 += " ";
            }

            var line2 = $"AS PARTITION [{functionName}]";
            if (format.Line2TrailingSpace)
            {
                line2 += " ";
            }

            lines.Add(line1);
            lines.Add(line2);
            lines.Add($"TO ({groupList})");
        }
        else
        {
            var line = $"CREATE PARTITION SCHEME [{schemeName}] AS PARTITION [{functionName}] TO ({groupList})";
            if (format != null && format.Line1TrailingSpace)
            {
                line += " ";
            }

            lines.Add(line);
        }

        lines.Add("GO");
        return lines;
    }

    internal static ModuleFormat? GetModuleFormat(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return null;
        }

        var ansiIndex = -1;
        var quotedIndex = -1;
        for (var i = 0; i < referenceLines.Length; i++)
        {
            var normalized = NormalizeStatementTerminator(referenceLines[i]);
            if (ansiIndex < 0 &&
                normalized.StartsWith("SET ANSI_NULLS ", StringComparison.OrdinalIgnoreCase))
            {
                ansiIndex = i;
            }
            else if (quotedIndex < 0 &&
                normalized.StartsWith("SET QUOTED_IDENTIFIER ", StringComparison.OrdinalIgnoreCase))
            {
                quotedIndex = i;
            }

            if (ansiIndex >= 0 && quotedIndex >= 0)
            {
                break;
            }
        }

        if (ansiIndex < 0 && quotedIndex < 0)
        {
            return null;
        }

        var lastSetIndex = Math.Max(ansiIndex, quotedIndex);
        var goIndex = -1;
        for (var i = lastSetIndex + 1; i < referenceLines.Length; i++)
        {
            if (string.Equals(referenceLines[i].Trim(), "GO", StringComparison.OrdinalIgnoreCase))
            {
                goIndex = i;
                break;
            }
        }

        if (goIndex < 0)
        {
            return null;
        }

        var leadingBlank = 0;
        for (var i = goIndex + 1; i < referenceLines.Length; i++)
        {
            if (referenceLines[i].Trim().Length == 0)
            {
                leadingBlank++;
            }
            else
            {
                break;
            }
        }

        var createIndex = -1;
        for (var i = 0; i < referenceLines.Length; i++)
        {
            if (referenceLines[i].TrimStart().StartsWith("CREATE ", StringComparison.OrdinalIgnoreCase))
            {
                createIndex = i;
                break;
            }
        }

        var goAfterDefinition = -1;
        if (createIndex >= 0)
        {
            for (var i = createIndex + 1; i < referenceLines.Length; i++)
            {
                if (string.Equals(referenceLines[i].Trim(), "GO", StringComparison.OrdinalIgnoreCase))
                {
                    goAfterDefinition = i;
                    break;
                }
            }
        }

        var blankBeforeGo = 0;
        var hasGoAfterDefinition = goAfterDefinition >= 0;
        if (goAfterDefinition > 0)
        {
            for (var i = goAfterDefinition - 1; i >= 0; i--)
            {
                if (referenceLines[i].Trim().Length == 0)
                {
                    blankBeforeGo++;
                }
                else
                {
                    break;
                }
            }
        }

        var indentPrefix = string.Empty;
        if (createIndex >= 0)
        {
            var line = referenceLines[createIndex];
            var firstNonSpace = line.Length - line.TrimStart().Length;
            indentPrefix = firstNonSpace > 0 ? line.Substring(0, firstNonSpace) : string.Empty;
        }

        return new ModuleFormat(leadingBlank, blankBeforeGo, indentPrefix, hasGoAfterDefinition);
    }

    private static string NormalizeStatementTerminator(string line)
    {
        var trimmed = line.Trim();
        return trimmed.EndsWith(";", StringComparison.Ordinal)
            ? trimmed.Substring(0, trimmed.Length - 1).TrimEnd()
            : trimmed;
    }

    private static Dictionary<string, string>? BuildCheckConstraintLineMap(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return null;
        }

        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var line in referenceLines)
        {
            var trimmed = line.TrimStart();
            if (!trimmed.StartsWith("ALTER TABLE", StringComparison.OrdinalIgnoreCase) ||
                trimmed.IndexOf("ADD CONSTRAINT [", StringComparison.OrdinalIgnoreCase) < 0 ||
                trimmed.IndexOf("CHECK", StringComparison.OrdinalIgnoreCase) < 0)
            {
                continue;
            }

            var start = trimmed.IndexOf("ADD CONSTRAINT [", StringComparison.OrdinalIgnoreCase);
            if (start < 0)
            {
                continue;
            }

            start += "ADD CONSTRAINT [".Length;
            var end = trimmed.IndexOf(']', start);
            if (end <= start)
            {
                continue;
            }

            var name = trimmed.Substring(start, end - start);
            if (!map.ContainsKey(name))
            {
                map[name] = trimmed;
            }
        }

        return map.Count == 0 ? null : map;
    }

    private static Dictionary<string, string>? BuildKeyConstraintLineMap(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return null;
        }

        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var line in referenceLines)
        {
            var trimmed = line.TrimStart();
            if (!trimmed.StartsWith("ALTER TABLE", StringComparison.OrdinalIgnoreCase) ||
                trimmed.IndexOf("ADD CONSTRAINT [", StringComparison.OrdinalIgnoreCase) < 0)
            {
                continue;
            }

            if (trimmed.IndexOf("PRIMARY KEY", StringComparison.OrdinalIgnoreCase) < 0 &&
                trimmed.IndexOf("UNIQUE", StringComparison.OrdinalIgnoreCase) < 0)
            {
                continue;
            }

            var start = trimmed.IndexOf("ADD CONSTRAINT [", StringComparison.OrdinalIgnoreCase);
            if (start < 0)
            {
                continue;
            }

            start += "ADD CONSTRAINT [".Length;
            var end = trimmed.IndexOf(']', start);
            if (end <= start)
            {
                continue;
            }

            var name = trimmed.Substring(start, end - start);
            if (!map.ContainsKey(name))
            {
                map[name] = trimmed;
            }
        }

        return map.Count == 0 ? null : map;
    }

    private static Dictionary<string, string>? BuildIndexLineMap(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return null;
        }

        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var line in referenceLines)
        {
            var trimmed = line.TrimStart();
            if (!trimmed.StartsWith("CREATE", StringComparison.OrdinalIgnoreCase) ||
                trimmed.IndexOf(" INDEX [", StringComparison.OrdinalIgnoreCase) < 0)
            {
                continue;
            }

            var start = trimmed.IndexOf("INDEX [", StringComparison.OrdinalIgnoreCase);
            if (start < 0)
            {
                continue;
            }

            start += "INDEX [".Length;
            var end = trimmed.IndexOf(']', start);
            if (end <= start)
            {
                continue;
            }

            var name = trimmed.Substring(start, end - start);
            if (!map.ContainsKey(name))
            {
                map[name] = trimmed;
            }
        }

        return map.Count == 0 ? null : map;
    }

    private static List<string>? GetReferenceIndexOrder(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return null;
        }

        var order = new List<string>();
        foreach (var line in referenceLines)
        {
            var trimmed = line.TrimStart();
            if (!trimmed.StartsWith("CREATE", StringComparison.OrdinalIgnoreCase) ||
                trimmed.IndexOf(" INDEX [", StringComparison.OrdinalIgnoreCase) < 0)
            {
                continue;
            }

            var start = trimmed.IndexOf("INDEX [", StringComparison.OrdinalIgnoreCase);
            if (start < 0)
            {
                continue;
            }

            start += "INDEX [".Length;
            var end = trimmed.IndexOf(']', start);
            if (end <= start)
            {
                continue;
            }

            order.Add(trimmed.Substring(start, end - start));
        }

        return order.Count == 0 ? null : order;
    }

    private static Dictionary<string, string>? BuildGrantLineMap(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return null;
        }

        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var line in referenceLines)
        {
            var trimmed = line.TrimStart();
            if (!trimmed.StartsWith("GRANT", StringComparison.OrdinalIgnoreCase) &&
                !trimmed.StartsWith("DENY", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var parts = trimmed.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            if (parts.Length < 2)
            {
                continue;
            }

            var state = trimmed.StartsWith("DENY", StringComparison.OrdinalIgnoreCase)
                ? "DENY"
                : trimmed.IndexOf("WITH GRANT OPTION", StringComparison.OrdinalIgnoreCase) >= 0
                    ? "GRANT_WITH_GRANT_OPTION"
                    : "GRANT";
            var permission = parts[1];

            var toIndex = trimmed.IndexOf(" TO ", StringComparison.OrdinalIgnoreCase);
            if (toIndex < 0)
            {
                continue;
            }

            var principalStart = trimmed.IndexOf('[', toIndex);
            if (principalStart < 0)
            {
                continue;
            }

            var principalEnd = trimmed.IndexOf(']', principalStart + 1);
            if (principalEnd <= principalStart)
            {
                continue;
            }

            var principal = trimmed.Substring(principalStart + 1, principalEnd - principalStart - 1);
            var key = $"{state}|{permission}|{principal}";
            if (!map.ContainsKey(key))
            {
                map[key] = trimmed;
            }
        }

        return map.Count == 0 ? null : map;
    }

    private static IEnumerable<string> ReadIndexSetOptions(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return Array.Empty<string>();
        }

        var firstIndexLine = -1;
        for (var i = 0; i < referenceLines.Length; i++)
        {
            var trimmed = referenceLines[i].TrimStart();
            if (trimmed.StartsWith("CREATE", StringComparison.OrdinalIgnoreCase) &&
                trimmed.IndexOf(" INDEX ", StringComparison.OrdinalIgnoreCase) >= 0)
            {
                firstIndexLine = i;
                break;
            }
        }

        if (firstIndexLine < 0)
        {
            return Array.Empty<string>();
        }

        var start = firstIndexLine;
        while (start > 0)
        {
            var previous = referenceLines[start - 1].TrimStart();
            if (previous.StartsWith("SET ", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(referenceLines[start - 1].Trim(), "GO", StringComparison.OrdinalIgnoreCase) ||
                previous.Length == 0)
            {
                start--;
                continue;
            }

            break;
        }

        if (start >= firstIndexLine ||
            !referenceLines.Skip(start).Take(firstIndexLine - start).Any(line => line.TrimStart().StartsWith("SET ", StringComparison.OrdinalIgnoreCase)))
        {
            return Array.Empty<string>();
        }

        var lines = new List<string>();
        for (var i = start; i < firstIndexLine; i++)
        {
            var trimmed = referenceLines[i].TrimStart();
            if (trimmed.StartsWith("SET ", StringComparison.OrdinalIgnoreCase) ||
                string.Equals(referenceLines[i].Trim(), "GO", StringComparison.OrdinalIgnoreCase) ||
                trimmed.Length == 0)
            {
                lines.Add(referenceLines[i]);
            }
            else
            {
                break;
            }
        }

        return lines;
    }

    private static IEnumerable<string> ReadLeadingSetOptions(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return Array.Empty<string>();
        }

        var createIndex = -1;
        for (var i = 0; i < referenceLines.Length; i++)
        {
            if (referenceLines[i].TrimStart().StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase))
            {
                createIndex = i;
                break;
            }
        }

        if (createIndex <= 0)
        {
            return Array.Empty<string>();
        }

        var lines = new List<string>();
        var hasSet = false;
        for (var i = 0; i < createIndex; i++)
        {
            var trimmed = referenceLines[i].TrimStart();
            if (trimmed.StartsWith("SET ", StringComparison.OrdinalIgnoreCase))
            {
                hasSet = true;
                lines.Add(referenceLines[i]);
                continue;
            }

            if (string.Equals(referenceLines[i].Trim(), "GO", StringComparison.OrdinalIgnoreCase) ||
                trimmed.Length == 0)
            {
                lines.Add(referenceLines[i]);
                continue;
            }

            break;
        }

        return hasSet ? lines : Array.Empty<string>();
    }

    private static IEnumerable<string> ReadTrailingSetOptions(string[]? referenceLines)
    {
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return Array.Empty<string>();
        }

        var end = referenceLines.Length - 1;
        while (end >= 0 && referenceLines[end].Trim().Length == 0)
        {
            end--;
        }

        if (end < 0)
        {
            return Array.Empty<string>();
        }

        var start = end;
        var hasSet = false;
        while (start >= 0)
        {
            var trimmed = referenceLines[start].TrimStart();
            if (trimmed.StartsWith("SET ", StringComparison.OrdinalIgnoreCase))
            {
                hasSet = true;
                start--;
                continue;
            }

            if (string.Equals(referenceLines[start].Trim(), "GO", StringComparison.OrdinalIgnoreCase) ||
                trimmed.Length == 0)
            {
                start--;
                continue;
            }

            break;
        }

        if (!hasSet)
        {
            return Array.Empty<string>();
        }

        start++;
        while (start <= end)
        {
            var trimmed = referenceLines[start].Trim();
            if (trimmed.Length == 0 || string.Equals(trimmed, "GO", StringComparison.OrdinalIgnoreCase))
            {
                start++;
                continue;
            }

            break;
        }

        var lines = new List<string>();
        for (var i = start; i <= end; i++)
        {
            lines.Add(referenceLines[i]);
        }

        return lines;
    }

    private static List<string> ReadLockEscalationStatements(string[]? referenceLines, string fullName)
    {
        var lines = new List<string>();
        if (referenceLines == null || referenceLines.Length == 0)
        {
            return lines;
        }

        var prefix = $"ALTER TABLE {fullName}";
        for (var i = 0; i < referenceLines.Length; i++)
        {
            var trimmed = referenceLines[i].TrimStart();
            if (!trimmed.StartsWith(prefix, StringComparison.OrdinalIgnoreCase) ||
                trimmed.IndexOf("LOCK_ESCALATION", StringComparison.OrdinalIgnoreCase) < 0)
            {
                continue;
            }

            lines.Add(trimmed);
            if (i + 1 < referenceLines.Length &&
                string.Equals(referenceLines[i + 1].Trim(), "GO", StringComparison.OrdinalIgnoreCase))
            {
                lines.Add("GO");
                i++;
            }
        }

        return lines;
    }

    internal sealed record ModuleFormat(int LeadingBlankLines, int BlankLineBeforeGo, string DefinitionIndentPrefix, bool HasGoAfterDefinition);
    private sealed record PartitionFunctionFormat(
        bool MultiLine,
        bool SpaceBeforeParen,
        bool BracketSystemType,
        bool Line1TrailingSpace,
        bool Line2TrailingSpace);
    private sealed record PartitionSchemeFormat(bool MultiLine, bool Line1TrailingSpace, bool Line2TrailingSpace);

    private static string FormatTypeName(
        string typeName,
        string typeSchema,
        bool isUserDefined,
        short maxLength,
        byte precision,
        byte scale)
    {
        var lower = typeName.ToLowerInvariant();
        var baseName = isUserDefined ? $"[{typeSchema}].[{typeName}]" : $"[{lower}]";

        return lower switch
        {
            "varchar" => $"{baseName} ({(maxLength < 0 ? "MAX" : maxLength.ToString())})",
            "char" => $"{baseName} ({maxLength})",
            "varbinary" => $"{baseName} ({(maxLength < 0 ? "MAX" : maxLength.ToString())})",
            "binary" => $"{baseName} ({maxLength})",
            "nvarchar" => $"{baseName} ({(maxLength < 0 ? "MAX" : (maxLength / 2).ToString())})",
            "nchar" => $"{baseName} ({maxLength / 2})",
            "decimal" => $"{baseName} ({precision}, {scale})",
            "numeric" => $"{baseName} ({precision}, {scale})",
            "datetime2" => scale == 7 ? baseName : $"{baseName} ({scale})",
            "datetimeoffset" => scale == 7 ? baseName : $"{baseName} ({scale})",
            "time" => scale == 7 ? baseName : $"{baseName} ({scale})",
            _ => baseName
        };
    }

    private static string EscapeSqlStringLiteral(string value)
        => value.Replace("'", "''", StringComparison.Ordinal);

    private static int? ResolveObjectId(SqlConnection connection, string fullName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT OBJECT_ID(@fullName);";
        command.Parameters.AddWithValue("@fullName", fullName);
        var result = command.ExecuteScalar();
        return result is null || result == DBNull.Value
            ? null
            : Convert.ToInt32(result, CultureInfo.InvariantCulture);
    }

    private static string QuoteIdentifier(string value)
        => $"[{value.Replace("]", "]]", StringComparison.Ordinal)}]";
}
