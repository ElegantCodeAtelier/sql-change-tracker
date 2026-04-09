using Microsoft.Data.SqlClient;
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

        var fullTextCatalogName = FindFullTextCatalogForTable(options, "HumanResources", "JobCandidate");
        if (string.IsNullOrWhiteSpace(fullTextCatalogName))
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("HumanResources", "JobCandidate", "Table"));

        Assert.Contains($"CREATE FULLTEXT INDEX ON [HumanResources].[JobCandidate] KEY INDEX [PK_JobCandidate_JobCandidateID] ON [{fullTextCatalogName}]", script);
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
    public void ScriptTable_EmitsUserCreatedStatistics_WhenPresent()
    {
        var server = Environment.GetEnvironmentVariable("SQLCT_TEST_SERVER");
        if (string.IsNullOrWhiteSpace(server))
        {
            return;
        }

        var databaseName = $"SqlctUserStats_{Guid.NewGuid():N}";
        try
        {
            var expectedStatisticsLine = CreateUserStatisticsFixtureDatabase(server, databaseName);
            var options = new SqlConnectionOptions(server, databaseName, "integrated", null, null, true);

            var scripter = new SqlServerScripter();
            var script = scripter.ScriptObject(options, new DbObjectInfo("dbo", "SampleTable", "Table"));

            Assert.Contains(expectedStatisticsLine, script);
        }
        finally
        {
            DropDatabase(server, databaseName);
        }
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
    public void ScriptSchema_EmitsExtendedProperties_WhenSchemaWithExtendedPropertiesExists()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var objInfo = FindSchemaWithExtendedProperties(options);
        if (objInfo == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, objInfo);

        Assert.Contains("sp_addextendedproperty", script);
        Assert.Contains("'SCHEMA'", script);
        Assert.Contains($"N'{objInfo.Name}', NULL, NULL, NULL, NULL", script);
    }

    [Fact]
    public void ScriptAdditionalImplementedObjectTypes_EmitExtendedProperties_WhenPropertiesExist()
    {
        var options = GetOptions();
        if (options == null)
        {
            return;
        }

        var cases = new (DbObjectInfo? Object, string RequiredToken)[]
        {
            (FindRoleWithExtendedProperties(options), "'USER'"),
            (FindUserWithExtendedProperties(options), "'USER'"),
            (FindSequenceWithExtendedProperties(options), "'SEQUENCE'"),
            (FindSynonymWithExtendedProperties(options), "'SYNONYM'"),
            (FindUserDefinedTypeWithExtendedProperties(options), "'TYPE'"),
            (FindPartitionFunctionWithExtendedProperties(options), "'PARTITION FUNCTION'"),
            (FindPartitionSchemeWithExtendedProperties(options), "'PARTITION SCHEME'")
        };

        var scripter = new SqlServerScripter();
        foreach (var testCase in cases)
        {
            if (testCase.Object == null)
            {
                continue;
            }

            var script = scripter.ScriptObject(options, testCase.Object);
            Assert.Contains("sp_addextendedproperty", script);
            Assert.Contains(testCase.RequiredToken, script);
        }
    }

    [Fact]
    public void ScriptPartitionFunction_DoesNotLeaveOpenReader_WhenExtendedPropertiesExist()
    {
        var options = GetOptions();
        if (options == null)
        {
            return;
        }

        var objInfo = FindPartitionFunctionWithExtendedProperties(options);
        if (objInfo == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, objInfo);

        Assert.Contains("CREATE PARTITION FUNCTION", script);
        Assert.Contains("'PARTITION FUNCTION'", script);
    }

    [Fact]
    public void ScriptPartitionScheme_DoesNotLeaveOpenReader_WhenExtendedPropertiesExist()
    {
        var options = GetOptions();
        if (options == null)
        {
            return;
        }

        var objInfo = FindPartitionSchemeWithExtendedProperties(options);
        if (objInfo == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, objInfo);

        Assert.Contains("CREATE PARTITION SCHEME", script);
        Assert.Contains("'PARTITION SCHEME'", script);
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
        var userDefinedType = introspector.ListObjects(options)
            .FirstOrDefault(item =>
                string.Equals(item.ObjectType, "UserDefinedType", StringComparison.OrdinalIgnoreCase)
                && item.UserDefinedTypeKind == UserDefinedTypeKind.Scalar);
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

    [Fact]
    public void ScriptProcedure_EmitsParameterExtendedProperties_WhenCompatibilityReferenceHasOnlyObjectLevelProperties()
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

        var referencePath = CreateModuleReferenceWithObjectLevelExtendedProperty("PROCEDURE");
        try
        {
            var scripter = new SqlServerScripter();
            var script = scripter.ScriptObject(options, objInfo, referencePath);

            Assert.Contains("'PARAMETER'", script);
            Assert.Contains("sp_addextendedproperty", script);
        }
        finally
        {
            File.Delete(referencePath);
        }
    }

    [Fact]
    public void ScriptFunction_EmitsParameterExtendedProperties_WhenCompatibilityReferenceHasOnlyObjectLevelProperties()
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

        var referencePath = CreateModuleReferenceWithObjectLevelExtendedProperty("FUNCTION");
        try
        {
            var scripter = new SqlServerScripter();
            var script = scripter.ScriptObject(options, objInfo, referencePath);

            Assert.Contains("'PARAMETER'", script);
            Assert.Contains("sp_addextendedproperty", script);
        }
        finally
        {
            File.Delete(referencePath);
        }
    }

    [Fact]
    public void ScriptFunction_EmitsClrTableValuedFunctionDefinition_WhenPresent()
    {
        var options = GetOptions();
        if (options == null)
        {
            return;
        }

        var objInfo = FindFirstClrTableValuedFunction(options);
        if (objInfo == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, objInfo);

        Assert.Contains("RETURNS TABLE (", script);
        Assert.Contains("EXTERNAL NAME", script);
    }

    [Fact]
    public void ScriptProcedure_EmitsClrStoredProcedureDefinition_WhenPresent()
    {
        var options = GetOptions();
        if (options == null)
        {
            return;
        }

        var objInfo = FindFirstClrStoredProcedure(options);
        if (objInfo == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, objInfo);

        Assert.Contains("CREATE PROCEDURE", script);
        Assert.Contains("AS EXTERNAL NAME", script);
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

    private static string? FindFullTextCatalogForTable(SqlConnectionOptions options, string schema, string tableName)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT TOP (1) fc.name
FROM sys.fulltext_indexes fi
JOIN sys.tables t ON t.object_id = fi.object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.fulltext_catalogs fc ON fc.fulltext_catalog_id = fi.fulltext_catalog_id
WHERE s.name = @schema
  AND t.name = @name;";
        command.Parameters.AddWithValue("@schema", schema);
        command.Parameters.AddWithValue("@name", tableName);

        return command.ExecuteScalar() as string;
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

    private static DbObjectInfo? FindSchemaWithExtendedProperties(SqlConnectionOptions options)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT TOP 1 s.name
FROM sys.extended_properties ep
JOIN sys.schemas s ON s.schema_id = ep.major_id
WHERE ep.class_desc = 'SCHEMA'
ORDER BY s.name, ep.name;";

        var result = command.ExecuteScalar() as string;
        return string.IsNullOrWhiteSpace(result)
            ? null
            : new DbObjectInfo(string.Empty, result, "Schema");
    }

    private static DbObjectInfo? FindRoleWithExtendedProperties(SqlConnectionOptions options)
        => FindDatabasePrincipalWithExtendedProperties(options, "R", "Role");

    private static DbObjectInfo? FindUserWithExtendedProperties(SqlConnectionOptions options)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT TOP 1 dp.name
FROM sys.extended_properties ep
JOIN sys.database_principals dp ON dp.principal_id = ep.major_id
WHERE ep.class_desc = 'DATABASE_PRINCIPAL'
  AND dp.type IN ('S','U','G','E','X','C','K')
ORDER BY dp.name, ep.name;";

        var result = command.ExecuteScalar() as string;
        return string.IsNullOrWhiteSpace(result)
            ? null
            : new DbObjectInfo(string.Empty, result, "User");
    }

    private static DbObjectInfo? FindDatabasePrincipalWithExtendedProperties(SqlConnectionOptions options, string principalType, string objectType)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT TOP 1 dp.name
FROM sys.extended_properties ep
JOIN sys.database_principals dp ON dp.principal_id = ep.major_id
WHERE ep.class_desc = 'DATABASE_PRINCIPAL'
  AND dp.type = @type
ORDER BY dp.name, ep.name;";
        command.Parameters.AddWithValue("@type", principalType);

        var result = command.ExecuteScalar() as string;
        return string.IsNullOrWhiteSpace(result)
            ? null
            : new DbObjectInfo(string.Empty, result, objectType);
    }

    private static DbObjectInfo? FindSequenceWithExtendedProperties(SqlConnectionOptions options)
        => FindSchemaScopedObjectWithExtendedProperties(options, "SO", "Sequence");

    private static DbObjectInfo? FindSynonymWithExtendedProperties(SqlConnectionOptions options)
        => FindSchemaScopedObjectWithExtendedProperties(options, "SN", "Synonym");

    private static DbObjectInfo? FindSchemaScopedObjectWithExtendedProperties(SqlConnectionOptions options, string sqlType, string objectType)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT TOP 1 s.name, o.name
FROM sys.extended_properties ep
JOIN sys.objects o ON o.object_id = ep.major_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE ep.class_desc = 'OBJECT_OR_COLUMN'
  AND ep.minor_id = 0
  AND o.type = @type
ORDER BY s.name, o.name, ep.name;";
        command.Parameters.AddWithValue("@type", sqlType);

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        return new DbObjectInfo(reader.GetString(0), reader.GetString(1), objectType);
    }

    private static DbObjectInfo? FindUserDefinedTypeWithExtendedProperties(SqlConnectionOptions options)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT TOP 1 s.name, t.name
FROM sys.extended_properties ep
JOIN sys.types t ON t.user_type_id = ep.major_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE ep.class_desc = 'TYPE'
  AND t.is_user_defined = 1
  AND t.is_table_type = 0
ORDER BY s.name, t.name, ep.name;";

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        return new DbObjectInfo(reader.GetString(0), reader.GetString(1), "UserDefinedType");
    }

    private static DbObjectInfo? FindPartitionFunctionWithExtendedProperties(SqlConnectionOptions options)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT TOP 1 pf.name
FROM sys.extended_properties ep
JOIN sys.partition_functions pf ON pf.function_id = ep.major_id
WHERE ep.class_desc = 'PARTITION_FUNCTION'
ORDER BY pf.name, ep.name;";

        var result = command.ExecuteScalar() as string;
        return string.IsNullOrWhiteSpace(result)
            ? null
            : new DbObjectInfo(string.Empty, result, "PartitionFunction");
    }

    private static DbObjectInfo? FindPartitionSchemeWithExtendedProperties(SqlConnectionOptions options)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT TOP 1 ps.name
FROM sys.extended_properties ep
JOIN sys.partition_schemes ps ON ps.data_space_id = ep.major_id
WHERE ep.class_desc = 'DATASPACE'
ORDER BY ps.name, ep.name;";

        var result = command.ExecuteScalar() as string;
        return string.IsNullOrWhiteSpace(result)
            ? null
            : new DbObjectInfo(string.Empty, result, "PartitionScheme");
    }

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
WHERE ep.class_desc = 'PARAMETER'
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
WHERE ep.class_desc = 'PARAMETER'
  AND o.type IN ('FN', 'TF', 'IF')
ORDER BY s.name, o.name, p.name, ep.name;";

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        return new DbObjectInfo(reader.GetString(0), reader.GetString(1), "Function");
    }

    private static DbObjectInfo? FindFirstClrTableValuedFunction(SqlConnectionOptions options)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = """
SELECT TOP 1 s.name, o.name
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE o.is_ms_shipped = 0
  AND o.type = 'FT'
ORDER BY s.name, o.name;
""";

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        return new DbObjectInfo(reader.GetString(0), reader.GetString(1), "Function");
    }

    private static DbObjectInfo? FindFirstClrStoredProcedure(SqlConnectionOptions options)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = """
SELECT TOP 1 s.name, o.name
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE o.is_ms_shipped = 0
  AND o.type = 'PC'
ORDER BY s.name, o.name;
""";

        using var reader = command.ExecuteReader();
        if (!reader.Read())
        {
            return null;
        }

        return new DbObjectInfo(reader.GetString(0), reader.GetString(1), "StoredProcedure");
    }

    private static string CreateModuleReferenceWithObjectLevelExtendedProperty(string levelType)
    {
        var path = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}.sql");
        var lines = new[]
        {
            "SET QUOTED_IDENTIFIER ON",
            "GO",
            "SET ANSI_NULLS ON",
            "GO",
            $"CREATE {levelType} [dbo].[CompatStub]",
            "AS",
            "SELECT 1",
            "GO",
            string.Empty,
            $"EXEC sp_addextendedproperty N'MS_Description', N'compat only', 'SCHEMA', N'dbo', '{levelType}', N'CompatStub', NULL, NULL",
            "GO"
        };

        File.WriteAllLines(path, lines);
        return path;
    }

    private static string CreateUserStatisticsFixtureDatabase(string server, string databaseName)
    {
        using var connection = SqlConnectionFactory.Create(new SqlConnectionOptions(server, "master", "integrated", null, null, true));
        connection.Open();

        using (var createDatabase = connection.CreateCommand())
        {
            createDatabase.CommandText = $"CREATE DATABASE [{databaseName}];";
            createDatabase.ExecuteNonQuery();
        }

        using var fixtureConnection = SqlConnectionFactory.Create(new SqlConnectionOptions(server, databaseName, "integrated", null, null, true));
        fixtureConnection.Open();

        var supportsPersistedSamplePercent = HasSystemObjectColumn(fixtureConnection, "dm_db_stats_properties", "persisted_sample_percent");
        var supportsAutoDrop = HasSystemObjectColumn(fixtureConnection, "stats", "auto_drop");
        var statisticsOptions = new List<string>();
        if (supportsPersistedSamplePercent)
        {
            statisticsOptions.Add("SAMPLE 25 PERCENT");
            statisticsOptions.Add("PERSIST_SAMPLE_PERCENT = ON");
        }
        else
        {
            statisticsOptions.Add("FULLSCAN");
        }

        statisticsOptions.Add("NORECOMPUTE");
        if (supportsAutoDrop)
        {
            statisticsOptions.Add("AUTO_DROP = OFF");
        }

        var statisticsStatement = $"CREATE STATISTICS [SampleStats] ON [dbo].[SampleTable] ([KeyAlpha], [KeyBeta], [KeyGamma], [StatusFlag]) WHERE [StatusFlag]=(1) WITH {string.Join(", ", statisticsOptions)};";
        var expectedStatisticsLine = $"CREATE STATISTICS [SampleStats] ON [dbo].[SampleTable] ([KeyAlpha], [KeyBeta], [KeyGamma], [StatusFlag]) WHERE [StatusFlag]=(1) WITH {string.Join(", ", statisticsOptions)}";

        var setupStatements = new[]
        {
            """
CREATE TABLE [dbo].[SampleTable] (
    [KeyAlpha] [int] NOT NULL,
    [KeyBeta] [int] NOT NULL,
    [KeyGamma] [int] NOT NULL,
    [StatusFlag] [bit] NOT NULL,
    [DetailText] [nvarchar](50) NULL
);
""",
            """
INSERT INTO [dbo].[SampleTable] ([KeyAlpha], [KeyBeta], [KeyGamma], [StatusFlag], [DetailText]) VALUES
(1, 10, 100, 1, N'A'),
(2, 20, 200, 0, N'B'),
(3, 30, 300, 1, N'C'),
(4, 40, 400, 0, N'D'),
(5, 50, 500, 1, N'E'),
(6, 60, 600, 0, N'F'),
(7, 70, 700, 1, N'G'),
(8, 80, 800, 0, N'H');
""",
            statisticsStatement
        };

        foreach (var statement in setupStatements)
        {
            using var command = fixtureConnection.CreateCommand();
            command.CommandText = statement;
            command.ExecuteNonQuery();
        }

        return expectedStatisticsLine;
    }

    private static void DropDatabase(string? server, string databaseName)
    {
        if (string.IsNullOrWhiteSpace(server))
        {
            return;
        }

        try
        {
            using var connection = SqlConnectionFactory.Create(new SqlConnectionOptions(server, "master", "integrated", null, null, true));
            connection.Open();

            using var command = connection.CreateCommand();
            command.CommandText = $"""
IF DB_ID(N'{databaseName}') IS NOT NULL
BEGIN
    ALTER DATABASE [{databaseName}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE [{databaseName}];
END;
""";
            command.ExecuteNonQuery();
        }
        catch (SqlException)
        {
        }
    }

    private static bool HasSystemObjectColumn(SqlConnection connection, string objectName, string columnName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT CASE WHEN EXISTS (
    SELECT 1
    FROM sys.all_columns c
    JOIN sys.all_objects o ON o.object_id = c.object_id
    JOIN sys.schemas s ON s.schema_id = o.schema_id
    WHERE s.name = N'sys'
      AND o.name = @objectName
      AND c.name = @columnName)
THEN CAST(1 AS bit)
ELSE CAST(0 AS bit)
END;";
        command.Parameters.AddWithValue("@objectName", objectName);
        command.Parameters.AddWithValue("@columnName", columnName);

        return command.ExecuteScalar() is bool exists && exists;
    }
}
