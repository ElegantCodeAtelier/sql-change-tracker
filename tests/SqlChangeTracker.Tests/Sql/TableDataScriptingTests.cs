using Microsoft.Data.SqlClient;
using SqlChangeTracker.Config;
using SqlChangeTracker.Schema;
using SqlChangeTracker.Sql;
using SqlChangeTracker.Sync;
using Xunit;

namespace SqlChangeTracker.Tests.Sql;

public sealed class TableDataScriptingTests
{
    [Fact]
    public void ScriptTableData_EmitsIdentityInsertAndExcludesNonInsertableColumns_WhenConfigured()
    {
        var server = Environment.GetEnvironmentVariable("SQLCT_TEST_SERVER");
        if (string.IsNullOrWhiteSpace(server))
        {
            return;
        }

        var databaseName = $"SqlctTableData_{Guid.NewGuid():N}";
        try
        {
            CreateFixtureDatabase(server, databaseName);
            var options = new SqlConnectionOptions(server, databaseName, "integrated", null, null, true);

            var scripter = new SqlServerScripter();
            var script = scripter.ScriptTableData(options, new ObjectIdentifier("dbo", "Customer"));

            Assert.Contains("SET IDENTITY_INSERT [dbo].[Customer] ON;", script);
            Assert.Contains("INSERT INTO [dbo].[Customer] ([CustomerID], [Name]) VALUES (1, N'Acme');", script);
            Assert.Contains("SET IDENTITY_INSERT [dbo].[Customer] OFF;", script);
            Assert.DoesNotContain("NameUpper", script);
            Assert.DoesNotContain("Version", script);
        }
        finally
        {
            DropDatabase(server, databaseName);
        }
    }

    [Fact]
    public void PullStatusAndDiff_SynchronizeTrackedTableData_WhenConfigured()
    {
        var server = Environment.GetEnvironmentVariable("SQLCT_TEST_SERVER");
        if (string.IsNullOrWhiteSpace(server))
        {
            return;
        }

        var databaseName = $"SqlctTableDataPull_{Guid.NewGuid():N}";
        var projectDir = Path.Combine(Path.GetTempPath(), "sqlct-tests", Guid.NewGuid().ToString("N"));

        try
        {
            CreateFixtureDatabase(server, databaseName);
            CreateProject(server, databaseName, projectDir, ["dbo.Customer"]);

            var service = new SyncCommandService();
            var pull = service.RunPull(projectDir);

            Assert.True(pull.Success, pull.Error?.Detail ?? pull.Error?.Message);
            var dataPath = Path.Combine(projectDir, "Data", "dbo.Customer_Data.sql");
            Assert.True(File.Exists(dataPath));
            Assert.Contains("SET IDENTITY_INSERT [dbo].[Customer] ON;", File.ReadAllText(dataPath));

            var status = service.RunStatus(projectDir, "db");
            Assert.True(status.Success, status.Error?.Detail ?? status.Error?.Message);
            Assert.Equal(0, status.Payload!.Summary.Schema.Added);
            Assert.Equal(0, status.Payload.Summary.Schema.Changed);
            Assert.Equal(0, status.Payload.Summary.Schema.Deleted);
            Assert.Equal(0, status.Payload.Summary.Data.Added);
            Assert.Equal(0, status.Payload.Summary.Data.Changed);
            Assert.Equal(0, status.Payload.Summary.Data.Deleted);

            var diff = service.RunDiff(projectDir, "db", "data:dbo.Customer");
            Assert.True(diff.Success, diff.Error?.Detail ?? diff.Error?.Message);
            Assert.Equal(string.Empty, diff.Payload!.Diff);
        }
        finally
        {
            TryDeleteProject(projectDir);
            DropDatabase(server, databaseName);
        }
    }

    [Fact]
    public void ScriptTableData_ReturnsEmptyScript_ForEmptyTable_WhenConfigured()
    {
        var server = Environment.GetEnvironmentVariable("SQLCT_TEST_SERVER");
        if (string.IsNullOrWhiteSpace(server))
        {
            return;
        }

        var databaseName = $"SqlctEmptyTableData_{Guid.NewGuid():N}";
        try
        {
            CreateFixtureDatabase(server, databaseName);
            var options = new SqlConnectionOptions(server, databaseName, "integrated", null, null, true);

            var scripter = new SqlServerScripter();
            var script = scripter.ScriptTableData(options, new ObjectIdentifier("dbo", "EmptyCustomer"));

            Assert.Equal(string.Empty, script);
        }
        finally
        {
            DropDatabase(server, databaseName);
        }
    }

    private static void CreateProject(string server, string databaseName, string projectDir, IReadOnlyList<string> trackedTables)
    {
        var seed = new BaselineProjectSeeder().Seed(projectDir);
        Assert.True(seed.Success, seed.Error?.Detail ?? seed.Error?.Message);

        var config = SqlctConfigWriter.CreateDefault();
        config.Database.Server = server;
        config.Database.Name = databaseName;
        config.Database.TrustServerCertificate = true;
        config.Data.TrackedTables.AddRange(trackedTables);

        var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
        Assert.True(write.Success, write.Error?.Detail ?? write.Error?.Message);
    }

    private static void CreateFixtureDatabase(string server, string databaseName)
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

        var setupStatements = new[]
        {
            """
CREATE TABLE [dbo].[Customer] (
    [CustomerID] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [Name] [nvarchar](100) NOT NULL,
    [NameUpper] AS (upper([Name])),
    [Version] [rowversion] NOT NULL
);
""",
            """
CREATE TABLE [dbo].[EmptyCustomer] (
    [CustomerID] [int] IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [Name] [nvarchar](100) NOT NULL
);
""",
            "INSERT INTO [dbo].[Customer] ([Name]) VALUES (N'Acme');"
        };

        foreach (var statement in setupStatements)
        {
            using var command = fixtureConnection.CreateCommand();
            command.CommandText = statement;
            command.ExecuteNonQuery();
        }
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

    private static void TryDeleteProject(string path)
    {
        if (Directory.Exists(path))
        {
            Directory.Delete(path, true);
        }
    }
}
