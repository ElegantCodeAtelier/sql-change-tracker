using SqlChangeTracker.Sql;
using Xunit;

namespace SqlChangeTracker.Tests.Sql;

public sealed class SqlServerIntrospectorTests
{
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
}
