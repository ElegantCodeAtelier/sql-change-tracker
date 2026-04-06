using Microsoft.Data.SqlClient;
using SqlChangeTracker.Config;
using SqlChangeTracker.Sync;
using SqlChangeTracker.Sql;
using Xunit;

namespace SqlChangeTracker.Tests.Sync;

public sealed class SyncCommandServiceSqlTests
{
    [Fact]
    public void PullAndStatus_SupportAdditionalObjectTypes_ForFixtureDatabase_WhenConfigured()
    {
        var server = Environment.GetEnvironmentVariable("SQLCT_TEST_SERVER");
        if (string.IsNullOrWhiteSpace(server))
        {
            return;
        }

        var databaseName = $"SqlctObjectTypes_{Guid.NewGuid():N}";
        var projectDir = Path.Combine(Path.GetTempPath(), "sqlct-tests", Guid.NewGuid().ToString("N"));

        try
        {
            var fixedRoleName = CreateFixtureDatabase(server, databaseName);
            CreateProject(server, databaseName, projectDir);

            var service = new SyncCommandService();
            var pull = service.RunPull(projectDir);

            Assert.True(pull.Success, pull.Error?.Detail ?? pull.Error?.Message);
            Assert.True(File.Exists(Path.Combine(projectDir, "Security", "Schemas", "Fixtures.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Security", "Roles", "AppRole.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Security", "Roles", $"{fixedRoleName}.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Security", "Users", "AppUser.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Synonyms", "Fixtures.TargetSynonym.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Types", "User-defined Data Types", "Fixtures.CodeType.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Storage", "Partition Functions", "Years_PF.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Storage", "Partition Schemes", "Years_PS.sql")));

            Assert.Contains(
                $"EXEC sp_addrolemember N'{fixedRoleName}', N'AppUser'",
                File.ReadAllText(Path.Combine(projectDir, "Security", "Roles", $"{fixedRoleName}.sql")));
            Assert.Contains(
                "CREATE SYNONYM [Fixtures].[TargetSynonym] FOR [Fixtures].[TargetTable]",
                File.ReadAllText(Path.Combine(projectDir, "Synonyms", "Fixtures.TargetSynonym.sql")));

            var status = service.RunStatus(projectDir, "db");
            Assert.True(status.Success, status.Error?.Detail ?? status.Error?.Message);
            Assert.Equal(0, status.Payload!.Summary.Schema.Added);
            Assert.Equal(0, status.Payload.Summary.Schema.Changed);
            Assert.Equal(0, status.Payload.Summary.Schema.Deleted);
            Assert.Equal(0, status.Payload.Summary.Data.Added);
            Assert.Equal(0, status.Payload.Summary.Data.Changed);
            Assert.Equal(0, status.Payload.Summary.Data.Deleted);

            var bareDiff = service.RunDiff(projectDir, "db", "AppUser");
            Assert.True(bareDiff.Success, bareDiff.Error?.Detail ?? bareDiff.Error?.Message);
            Assert.Equal(string.Empty, bareDiff.Payload!.Diff);

            var typedDiff = service.RunDiff(projectDir, "db", $"Role:{fixedRoleName}");
            Assert.True(typedDiff.Success, typedDiff.Error?.Detail ?? typedDiff.Error?.Message);
            Assert.Equal(string.Empty, typedDiff.Payload!.Diff);

            var schemaScopedDiff = service.RunDiff(projectDir, "db", "Synonym:Fixtures.TargetSynonym");
            Assert.True(schemaScopedDiff.Success, schemaScopedDiff.Error?.Detail ?? schemaScopedDiff.Error?.Message);
            Assert.Equal(string.Empty, schemaScopedDiff.Payload!.Diff);
        }
        finally
        {
            TryDeleteProject(projectDir);
            DropDatabase(server, databaseName);
        }
    }

    private static void CreateProject(string server, string databaseName, string projectDir)
    {
        var seed = new BaselineProjectSeeder().Seed(projectDir);
        Assert.True(seed.Success, seed.Error?.Detail ?? seed.Error?.Message);

        var config = SqlctConfigWriter.CreateDefault();
        config.Database.Server = server;
        config.Database.Name = databaseName;
        config.Database.TrustServerCertificate = true;

        var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
        Assert.True(write.Success, write.Error?.Detail ?? write.Error?.Message);
    }

    private static string CreateFixtureDatabase(string server, string databaseName)
    {
        using var connection = SqlConnectionFactory.Create(new SqlConnectionOptions(server, "master", "integrated", null, null, true));
        connection.Open();

        using var createDatabase = connection.CreateCommand();
        createDatabase.CommandText = $"CREATE DATABASE [{databaseName}];";
        createDatabase.ExecuteNonQuery();

        using var fixtureConnection = SqlConnectionFactory.Create(new SqlConnectionOptions(server, databaseName, "integrated", null, null, true));
        fixtureConnection.Open();

        string fixedRoleName;
        using (var fixedRoleCommand = fixtureConnection.CreateCommand())
        {
            fixedRoleCommand.CommandText = """
SELECT TOP (1) [name]
FROM sys.database_principals
WHERE [type] = 'R'
  AND [is_fixed_role] = 1
  AND [name] <> N'public'
ORDER BY [name];
""";
            fixedRoleName = (string)fixedRoleCommand.ExecuteScalar()!;
        }

        var setupStatements = new[]
        {
            "CREATE SCHEMA [Fixtures] AUTHORIZATION [dbo];",
            "CREATE ROLE [AppRole] AUTHORIZATION [dbo];",
            "CREATE USER [AppUser] WITHOUT LOGIN WITH DEFAULT_SCHEMA=[Fixtures];",
            "CREATE TYPE [Fixtures].[CodeType] FROM [nvarchar](20) NOT NULL;",
            "CREATE TABLE [Fixtures].[TargetTable] ([Id] [int] NOT NULL, [Code] [Fixtures].[CodeType] NOT NULL);",
            "CREATE SYNONYM [Fixtures].[TargetSynonym] FOR [Fixtures].[TargetTable];",
            "CREATE PARTITION FUNCTION [Years_PF] ([int]) AS RANGE LEFT FOR VALUES (2020, 2021);",
            "CREATE PARTITION SCHEME [Years_PS] AS PARTITION [Years_PF] ALL TO ([PRIMARY]);",
            "EXEC sp_addrolemember N'AppRole', N'AppUser';",
            $"EXEC sp_addrolemember N'{fixedRoleName.Replace("'", "''")}', N'AppUser';"
        };

        foreach (var statement in setupStatements)
        {
            using var setup = fixtureConnection.CreateCommand();
            setup.CommandText = statement;
            setup.ExecuteNonQuery();
        }

        return fixedRoleName;
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
