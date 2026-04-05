using SqlChangeTracker.Sql;
using Xunit;

namespace SqlChangeTracker.Tests.Sql;

public sealed class SqlServerScripterTests
{
    [Fact]
    public void ScriptTable_EmitsCreateTable_WhenConfigured()
    {
        var options = GetOptions();
        var tableSchema = Environment.GetEnvironmentVariable("SQLCT_TEST_TABLE_SCHEMA");
        var tableName = Environment.GetEnvironmentVariable("SQLCT_TEST_TABLE_NAME");
        if (options == null || string.IsNullOrWhiteSpace(tableSchema) || string.IsNullOrWhiteSpace(tableName))
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo(tableSchema, tableName, "Table"));

        Assert.Contains("CREATE TABLE", script);
        Assert.Contains("GO", script);
    }

    [Fact]
    public void ScriptTable_EmitsRowGuidCol_ForAdventureWorksPersonTable()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("Person", "Person", "Table"));

        Assert.Contains("[rowguid] [uniqueidentifier] NOT NULL ROWGUIDCOL", script);
    }

    [Fact]
    public void ScriptTable_EmitsIdentityNotForReplication_ForAdventureWorksAddressTable()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("Person", "Address", "Table"));

        Assert.Contains("[AddressID] [int] NOT NULL IDENTITY(1, 1) NOT FOR REPLICATION", script);
    }

    [Fact]
    public void ScriptTable_EmitsInlineTrigger_ForAdventureWorksPersonTable()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("Person", "Person", "Table"));

        Assert.Contains("CREATE TRIGGER [Person].[iuPerson] ON [Person].[Person]", script);
    }

    [Fact]
    public void ScriptTable_EmitsXmlSchemaBindings_ForAdventureWorksPersonTable()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("Person", "Person", "Table"));

        Assert.Contains("[AdditionalContactInfo] [xml] (CONTENT [Person].[AdditionalContactInfoSchemaCollection]) NULL", script);
        Assert.Contains("[Demographics] [xml] (CONTENT [Person].[IndividualSurveySchemaCollection]) NULL", script);
    }

    [Fact]
    public void ScriptTable_EmitsXmlIndexes_ForAdventureWorksPersonTable()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("Person", "Person", "Table"));

        Assert.Contains("CREATE PRIMARY XML INDEX [PXML_Person_AddContact]", script);
        Assert.Contains("CREATE XML INDEX [XMLPATH_Person_Demographics]", script);
    }

    [Fact]
    public void ScriptTable_OmitsNullability_ForNonPersistedAdventureWorksComputedColumns()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("Sales", "Customer", "Table"));

        Assert.Contains("[AccountNumber] AS (isnull('AW'+[dbo].[ufnLeadingZeros]([CustomerID]),''))", script);
        Assert.DoesNotContain("[AccountNumber] AS (isnull('AW'+[dbo].[ufnLeadingZeros]([CustomerID]),'')) NOT NULL", script);
    }

    [Fact]
    public void ScriptTable_EmitsNullability_ForPersistedAdventureWorksComputedColumns()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("Purchasing", "PurchaseOrderHeader", "Table"));

        Assert.Contains("[TotalDue] AS (isnull(([SubTotal]+[TaxAmt])+[Freight],(0))) PERSISTED NOT NULL", script);
    }

    [Fact]
    public void ScriptTable_EmitsFullTextIndexes_ForAdventureWorksJobCandidateTable()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("HumanResources", "JobCandidate", "Table"));

        Assert.Contains("CREATE FULLTEXT INDEX ON [HumanResources].[JobCandidate] KEY INDEX [PK_JobCandidate_JobCandidateID] ON [AW2016FullTextCatalog]", script);
        Assert.Contains("ALTER FULLTEXT INDEX ON [HumanResources].[JobCandidate] ADD ([Resume] LANGUAGE 1033)", script);
    }

    [Fact]
    public void ScriptTable_EmitsPartitionColumnOnStorageLine_WhenPartitionedTableExists()
    {
        var options = GetOptions();
        if (options == null)
        {
            return;
        }

        var candidate = FindFirstPartitionedTable(options);
        if (candidate == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, candidate.Value.Object);

        Assert.Contains($") ON [{candidate.Value.DataSpace}] ([{candidate.Value.PartitionColumn}])", script);
    }

    [Fact]
    public void ScriptTable_EmitsTextImageOn_WhenTableWithLobStorageExists()
    {
        var options = GetOptions();
        if (options == null)
        {
            return;
        }

        var candidate = FindFirstTableWithLobStorage(options);
        if (candidate == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, candidate.Value.Object);

        Assert.Contains($"TEXTIMAGE_ON [{candidate.Value.LobDataSpace}]", script);
    }

    [Fact]
    public void ScriptTable_EmitsStatisticsIncremental_WhenIncrementalIndexExists()
    {
        var options = GetOptions();
        if (options == null)
        {
            return;
        }

        var candidate = FindFirstIncrementalIndexTable(options);
        if (candidate == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, candidate.Value.Object);
        var line = FindScriptLineContainingName(script, candidate.Value.IndexName);

        Assert.NotNull(line);
        Assert.Contains("STATISTICS_INCREMENTAL=ON", line);
    }

    [Fact]
    public void ScriptView_EmitsIndexedViewIndex_ForAdventureWorksView()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("Production", "vProductAndDescription", "View"));

        Assert.Contains("CREATE UNIQUE CLUSTERED INDEX [IX_vProductAndDescription] ON [Production].[vProductAndDescription] ([CultureID], [ProductID]) ON [PRIMARY]", script);
    }

    [Fact]
    public void ScriptSchemaRoleAndUser_EmitExpectedStatements_WhenSupportedObjectsExist()
    {
        var options = GetOptions();
        if (options == null)
        {
            return;
        }

        var introspector = new SqlServerIntrospector();
        var schema = FindFirstObject(introspector, options, "Schema");
        var role = FindFirstObject(introspector, options, "Role");
        var user = FindFirstObject(introspector, options, "User");
        if (schema == null || role == null || user == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var schemaScript = scripter.ScriptObject(options, schema);
        var roleScript = scripter.ScriptObject(options, role);
        var userScript = scripter.ScriptObject(options, user);

        Assert.Contains($"CREATE SCHEMA [{schema.Name}]", schemaScript);
        Assert.Contains($"CREATE ROLE [{role.Name}]", roleScript);
        Assert.Contains($"CREATE USER [{user.Name}] WITHOUT LOGIN", userScript);
    }

    [Fact]
    public void ScriptPartitionFunctionAndScheme_EmitExpectedStatements_WhenSupportedObjectsExist()
    {
        var options = GetOptions();
        if (options == null)
        {
            return;
        }

        var introspector = new SqlServerIntrospector();
        var partitionFunction = FindFirstObject(introspector, options, "PartitionFunction");
        var partitionScheme = FindFirstObject(introspector, options, "PartitionScheme");
        if (partitionFunction == null || partitionScheme == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var partitionFunctionScript = scripter.ScriptObject(options, partitionFunction);
        var partitionSchemeScript = scripter.ScriptObject(options, partitionScheme);

        Assert.Contains("CREATE PARTITION FUNCTION", partitionFunctionScript);
        Assert.Contains("FOR VALUES", partitionFunctionScript);
        Assert.Contains("CREATE PARTITION SCHEME", partitionSchemeScript);
        Assert.Contains("TO (", partitionSchemeScript);
    }

    [Fact]
    public void ScriptUserDefinedType_EmitsCreateType_ForAdventureWorksWhenConfigured()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var introspector = new SqlServerIntrospector();
        var userDefinedType = FindFirstObject(introspector, options, "UserDefinedType");
        if (userDefinedType == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, userDefinedType);

        Assert.Contains($"CREATE TYPE [{userDefinedType.Schema}].[{userDefinedType.Name}] FROM", script);
        Assert.Contains("GO", script);
    }

    [Fact]
    public void ScriptProcedure_EmitsParameterExtendedProperties_WhenProcedureWithParameterExtendedPropertiesExists()
    {
        var options = GetOptions();
        if (options == null)
        {
            return;
        }

        var objInfo = FindProcedureWithParameterExtendedProperties(options);
        if (objInfo == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, objInfo);

        Assert.Contains("'PARAMETER'", script);
        Assert.Contains("sp_addextendedproperty", script);
    }

    [Fact]
    public void ScriptFunction_EmitsParameterExtendedProperties_WhenFunctionWithParameterExtendedPropertiesExists()
    {
        var options = GetOptions();
        if (options == null)
        {
            return;
        }

        var objInfo = FindFunctionWithParameterExtendedProperties(options);
        if (objInfo == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, objInfo);

        Assert.Contains("'PARAMETER'", script);
        Assert.Contains("sp_addextendedproperty", script);
    }

    private static SqlConnectionOptions? GetOptions()
    {
        var server = Environment.GetEnvironmentVariable("SQLCT_TEST_SERVER");
        var database = Environment.GetEnvironmentVariable("SQLCT_TEST_DATABASE");
        if (string.IsNullOrWhiteSpace(server) || string.IsNullOrWhiteSpace(database))
        {
            return null;
        }

        return new SqlConnectionOptions(
            server,
            database,
            "integrated",
            null,
            null,
            true);
    }

    private static SqlConnectionOptions? GetAdventureWorksOptions()
    {
        return GetDatabaseOptions("AdventureWorks2022");
    }

    private static SqlConnectionOptions? GetDatabaseOptions(string databaseName)
    {
        var options = GetOptions();
        if (options == null)
        {
            return null;
        }

        return string.Equals(
                Environment.GetEnvironmentVariable("SQLCT_TEST_DATABASE"),
                databaseName,
                StringComparison.OrdinalIgnoreCase)
            ? options
            : null;
    }

    private static (DbObjectInfo Object, string DataSpace, string PartitionColumn)? FindFirstPartitionedTable(SqlConnectionOptions options)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT TOP 1 s.name, t.name, ds.name, partition_col.name
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.indexes i ON i.object_id = t.object_id AND i.index_id IN (0, 1)
JOIN sys.data_spaces ds ON ds.data_space_id = i.data_space_id
JOIN sys.index_columns ic
    ON ic.object_id = i.object_id
   AND ic.index_id = i.index_id
   AND ic.partition_ordinal = 1
JOIN sys.columns partition_col
    ON partition_col.object_id = ic.object_id
   AND partition_col.column_id = ic.column_id
WHERE t.is_ms_shipped = 0
ORDER BY s.name, t.name, i.index_id DESC;";

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        return (
            new DbObjectInfo(reader.GetString(0), reader.GetString(1), "Table"),
            reader.GetString(2),
            reader.GetString(3));
    }

    private static (DbObjectInfo Object, string LobDataSpace)? FindFirstTableWithLobStorage(SqlConnectionOptions options)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT TOP 1 s.name, t.name, ds.name
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.data_spaces ds ON ds.data_space_id = t.lob_data_space_id
WHERE t.is_ms_shipped = 0
ORDER BY s.name, t.name;";

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        return (
            new DbObjectInfo(reader.GetString(0), reader.GetString(1), "Table"),
            reader.GetString(2));
    }

    private static (DbObjectInfo Object, string IndexName)? FindFirstIncrementalIndexTable(SqlConnectionOptions options)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT TOP 1 s.name, t.name, i.name
FROM sys.indexes i
JOIN sys.tables t ON t.object_id = i.object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.stats st ON st.object_id = i.object_id AND st.stats_id = i.index_id
WHERE t.is_ms_shipped = 0
  AND i.index_id > 0
  AND i.is_hypothetical = 0
  AND st.is_incremental = 1
ORDER BY s.name, t.name, i.index_id;";

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        return (
            new DbObjectInfo(reader.GetString(0), reader.GetString(1), "Table"),
            reader.GetString(2));
    }

    private static string? FindScriptLineContainingName(string script, string name)
    {
        var token = $"[{name}]";
        return script
            .Split(new[] { "\r\n", "\n" }, StringSplitOptions.None)
            .FirstOrDefault(line => line.Contains(token, StringComparison.Ordinal));
    }

    private static DbObjectInfo? FindFirstObject(SqlServerIntrospector introspector, SqlConnectionOptions options, string objectType)
        => introspector.ListObjects(options)
            .FirstOrDefault(item => string.Equals(item.ObjectType, objectType, StringComparison.OrdinalIgnoreCase));

    private static DbObjectInfo? FindProcedureWithParameterExtendedProperties(SqlConnectionOptions options)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT TOP 1 s.name, o.name
FROM sys.extended_properties ep
JOIN sys.parameters p ON p.object_id = ep.major_id AND p.parameter_id = ep.minor_id
JOIN sys.objects o ON o.object_id = ep.major_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE ep.class_desc = 'OBJECT_OR_COLUMN' AND ep.minor_id <> 0
  AND o.type = 'P'
ORDER BY s.name, o.name, p.name, ep.name;";

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        return new DbObjectInfo(reader.GetString(0), reader.GetString(1), "StoredProcedure");
    }

    private static DbObjectInfo? FindFunctionWithParameterExtendedProperties(SqlConnectionOptions options)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT TOP 1 s.name, o.name
FROM sys.extended_properties ep
JOIN sys.parameters p ON p.object_id = ep.major_id AND p.parameter_id = ep.minor_id
JOIN sys.objects o ON o.object_id = ep.major_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE ep.class_desc = 'OBJECT_OR_COLUMN' AND ep.minor_id <> 0
  AND o.type IN ('FN', 'TF', 'IF')
ORDER BY s.name, o.name, p.name, ep.name;";

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        return new DbObjectInfo(reader.GetString(0), reader.GetString(1), "Function");
    }
}
