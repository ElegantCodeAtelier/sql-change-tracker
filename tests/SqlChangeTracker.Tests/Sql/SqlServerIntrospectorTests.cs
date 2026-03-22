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
