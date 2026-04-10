using SqlChangeTracker.Sql;
using Xunit;

namespace SqlChangeTracker.Tests.Sql;

public sealed class SqlServerIntrospectorTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void ResolveParallelism_ReturnsProcessorCount_WhenNotPositive(int configured)
    {
        var resolved = SqlServerIntrospector.ResolveParallelism(configured);

        Assert.Equal(Environment.ProcessorCount, resolved);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(16)]
    public void ResolveParallelism_ReturnsConfiguredValue_WhenPositive(int configured)
    {
        var resolved = SqlServerIntrospector.ResolveParallelism(configured);

        Assert.Equal(configured, resolved);
    }

    [Theory]
    [InlineData("sp_alterdiagram", true)]
    [InlineData("sp_creatediagram", true)]
    [InlineData("sp_dropdiagram", true)]
    [InlineData("sp_helpdiagramdefinition", true)]
    [InlineData("sp_helpdiagrams", true)]
    [InlineData("sp_renamediagram", true)]
    [InlineData("sp_upgraddiagrams", true)]
    [InlineData("sp_CustomImport", false)]
    [InlineData("usp_ProcessBatch", false)]
    public void IsExcludedStoredProcedureName_RecognizesOnlyDatabaseDiagramSupportProcedures(string name, bool expected)
    {
        var actual = SqlServerIntrospector.IsExcludedStoredProcedureName(name);

        Assert.Equal(expected, actual);
    }

    [Theory]
    [InlineData("sp_helpdiagrams", false)]
    [InlineData("usp_ProcessBatch", true)]
    public void ShouldIncludeObject_FiltersOnlyExcludedDatabaseDiagramStoredProcedures(string name, bool expected)
    {
        var actual = SqlServerIntrospector.ShouldIncludeObject(new DbObjectInfo("dbo", name, "StoredProcedure"));

        Assert.Equal(expected, actual);
    }

    [Fact]
    public void ListObjects_ReturnsResults_WhenConfigured()
    {
        var options = GetOptions();
        if (options == null)
        {
            return;
        }

        var introspector = new SqlServerIntrospector();
        var results = introspector.ListObjects(options);

        Assert.NotNull(results);
    }

    [Fact]
    public void ListObjects_NormalizesSchemaLessActiveTypes_WhenConfigured()
    {
        var options = GetOptions();
        if (options == null)
        {
            return;
        }

        var introspector = new SqlServerIntrospector();
        var results = introspector.ListObjects(options);

        foreach (var objectInfo in results.Where(item =>
                     item.ObjectType is "Schema" or "Role" or "User" or "PartitionFunction" or "PartitionScheme"))
        {
            Assert.Equal(string.Empty, objectInfo.Schema);
        }
    }

    [Fact]
    public void ListObjects_KeepsSchemaForSchemaScopedAdditionalTypes_WhenConfigured()
    {
        var options = GetOptions();
        if (options == null)
        {
            return;
        }

        var introspector = new SqlServerIntrospector();
        var results = introspector.ListObjects(options);

        foreach (var objectInfo in results.Where(item => item.ObjectType is "Synonym" or "UserDefinedType"))
        {
            Assert.False(string.IsNullOrWhiteSpace(objectInfo.Schema));
        }
    }

    [Fact]
    public void ListObjects_IncludesClrTableValuedFunctions_WhenPresent()
    {
        var options = GetOptions();
        if (options == null)
        {
            return;
        }

        var expected = FindFirstClrTableValuedFunction(options);
        if (expected == null)
        {
            return;
        }

        var introspector = new SqlServerIntrospector();
        var results = introspector.ListObjects(options);

        Assert.Contains(
            results,
            item => string.Equals(item.ObjectType, "Function", StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(item.Schema, expected.Value.Schema, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(item.Name, expected.Value.Name, StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void ListObjects_IncludesClrStoredProcedures_WhenPresent()
    {
        var options = GetOptions();
        if (options == null)
        {
            return;
        }

        var expected = FindFirstClrStoredProcedure(options);
        if (expected == null)
        {
            return;
        }

        var introspector = new SqlServerIntrospector();
        var results = introspector.ListObjects(options);

        Assert.Contains(
            results,
            item => string.Equals(item.ObjectType, "StoredProcedure", StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(item.Schema, expected.Value.Schema, StringComparison.OrdinalIgnoreCase) &&
                    string.Equals(item.Name, expected.Value.Name, StringComparison.OrdinalIgnoreCase));
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

    private static (string Schema, string Name)? FindFirstClrTableValuedFunction(SqlConnectionOptions options)
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

        return (reader.GetString(0), reader.GetString(1));
    }

    private static (string Schema, string Name)? FindFirstClrStoredProcedure(SqlConnectionOptions options)
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

        return (reader.GetString(0), reader.GetString(1));
    }
}
