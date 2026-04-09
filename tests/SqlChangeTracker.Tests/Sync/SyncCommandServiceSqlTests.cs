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
            var fixture = CreateFixtureDatabase(server, databaseName);
            CreateProject(server, databaseName, projectDir);

            var service = new SyncCommandService();
            var pull = service.RunPull(projectDir);

            Assert.True(pull.Success, pull.Error?.Detail ?? pull.Error?.Message);
            Assert.True(File.Exists(Path.Combine(projectDir, "Security", "Schemas", "Fixtures.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Security", "Roles", "AppRole.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Security", "Roles", $"{fixture.FixedRoleName}.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Security", "Users", "AppUser.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Synonyms", "Fixtures.TargetSynonym.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Types", "User-defined Data Types", "Fixtures.CodeType.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Types", "User-defined Data Types", "Fixtures.RequestList.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Types", "XML Schema Collections", "Fixtures.PayloadSchema.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Service Broker", "Message Types", "%2F%2FSqlct%2FReply.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Service Broker", "Message Types", "%2F%2FSqlct%2FRequest.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Service Broker", "Contracts", "%2F%2FSqlct%2FContract.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Service Broker", "Event Notifications", "NotifySchemaChanges.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Service Broker", "Queues", "Fixtures.RequestQueue.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Service Broker", "Remote Service Bindings", "SqlctRemoteBinding.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Service Broker", "Services", "SqlctAppService.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Service Broker", "Routes", "SqlctLocalRoute.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Storage", "Partition Functions", "Years_PF.sql")));
            Assert.True(File.Exists(Path.Combine(projectDir, "Storage", "Partition Schemes", "Years_PS.sql")));
            if (fixture.HasFullText)
            {
                Assert.True(File.Exists(Path.Combine(projectDir, "Storage", "Full Text Catalogs", "SqlctCatalog.sql")));
                Assert.True(File.Exists(Path.Combine(projectDir, "Storage", "Full Text Stoplists", "SqlctStoplist.sql")));
            }
            if (fixture.HasSearchPropertyList)
            {
                Assert.True(File.Exists(Path.Combine(projectDir, "Storage", "Search Property Lists", "DocumentProperties.sql")));
            }

            Assert.Contains(
                $"EXEC sp_addrolemember N'{fixture.FixedRoleName}', N'AppUser'",
                File.ReadAllText(Path.Combine(projectDir, "Security", "Roles", $"{fixture.FixedRoleName}.sql")));
            var schemaScript = File.ReadAllText(Path.Combine(projectDir, "Security", "Schemas", "Fixtures.sql"));
            Assert.Contains("GRANT SELECT ON SCHEMA::[Fixtures] TO [AppUser]", schemaScript);
            Assert.Contains(
                "EXEC sp_addextendedproperty N'Caption', N'Fixture schema', 'SCHEMA', N'Fixtures', NULL, NULL, NULL, NULL",
                schemaScript);
            Assert.Contains(
                "CREATE SYNONYM [Fixtures].[TargetSynonym] FOR [Fixtures].[TargetTable]",
                File.ReadAllText(Path.Combine(projectDir, "Synonyms", "Fixtures.TargetSynonym.sql")));
            Assert.Contains(
                "CREATE TYPE [Fixtures].[RequestList] AS TABLE",
                File.ReadAllText(Path.Combine(projectDir, "Types", "User-defined Data Types", "Fixtures.RequestList.sql")));
            Assert.Contains(
                "CREATE XML SCHEMA COLLECTION [Fixtures].[PayloadSchema] AS",
                File.ReadAllText(Path.Combine(projectDir, "Types", "XML Schema Collections", "Fixtures.PayloadSchema.sql")));
            Assert.Contains(
                "GRANT REFERENCES ON XML SCHEMA COLLECTION::[Fixtures].[PayloadSchema] TO [AppUser]",
                File.ReadAllText(Path.Combine(projectDir, "Types", "XML Schema Collections", "Fixtures.PayloadSchema.sql")));
            Assert.Contains(
                "EXEC sp_addextendedproperty N'Caption', N'Fixture XML schema collection', 'SCHEMA', N'Fixtures', 'XML SCHEMA COLLECTION', N'PayloadSchema', NULL, NULL",
                File.ReadAllText(Path.Combine(projectDir, "Types", "XML Schema Collections", "Fixtures.PayloadSchema.sql")));
            Assert.Contains(
                "PRIMARY KEY CLUSTERED ([Id])",
                File.ReadAllText(Path.Combine(projectDir, "Types", "User-defined Data Types", "Fixtures.RequestList.sql")));
            Assert.Contains(
                "CREATE MESSAGE TYPE [//Sqlct/Request]",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Message Types", "%2F%2FSqlct%2FRequest.sql")));
            Assert.Contains(
                "VALIDATION = NONE",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Message Types", "%2F%2FSqlct%2FRequest.sql")));
            Assert.Contains(
                "GRANT REFERENCES ON MESSAGE TYPE::[//Sqlct/Request] TO [AppUser]",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Message Types", "%2F%2FSqlct%2FRequest.sql")));
            Assert.Contains(
                "EXEC sp_addextendedproperty N'Caption', N'Fixture message type', 'MESSAGE TYPE', N'//Sqlct/Request', NULL, NULL, NULL, NULL",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Message Types", "%2F%2FSqlct%2FRequest.sql")));
            Assert.Contains(
                "CREATE CONTRACT [//Sqlct/Contract]",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Contracts", "%2F%2FSqlct%2FContract.sql")));
            Assert.Contains(
                "GRANT REFERENCES ON CONTRACT::[//Sqlct/Contract] TO [AppUser]",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Contracts", "%2F%2FSqlct%2FContract.sql")));
            Assert.Contains(
                "EXEC sp_addextendedproperty N'Caption', N'Fixture contract', 'CONTRACT', N'//Sqlct/Contract', NULL, NULL, NULL, NULL",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Contracts", "%2F%2FSqlct%2FContract.sql")));
            Assert.Contains(
                "CREATE EVENT NOTIFICATION [NotifySchemaChanges] ON DATABASE FOR ALTER_TABLE, CREATE_TABLE TO SERVICE N'SqlctAppService'",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Event Notifications", "NotifySchemaChanges.sql")));
            Assert.Contains(
                "EXEC sp_addextendedproperty N'Caption', N'Fixture event notification', 'EVENT NOTIFICATION', N'NotifySchemaChanges', NULL, NULL, NULL, NULL",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Event Notifications", "NotifySchemaChanges.sql")));
            Assert.Contains(
                "CREATE QUEUE [Fixtures].[RequestQueue]",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Queues", "Fixtures.RequestQueue.sql")));
            Assert.Contains(
                "GRANT RECEIVE ON [Fixtures].[RequestQueue] TO [AppUser]",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Queues", "Fixtures.RequestQueue.sql")));
            Assert.Contains(
                "EXEC sp_addextendedproperty N'Caption', N'Fixture queue', 'SCHEMA', N'Fixtures', 'QUEUE', N'RequestQueue', NULL, NULL",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Queues", "Fixtures.RequestQueue.sql")));
            Assert.Contains(
                "CREATE REMOTE SERVICE BINDING [SqlctRemoteBinding] TO SERVICE N'SqlctRemoteService' WITH USER = [BindingUser], ANONYMOUS = ON",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Remote Service Bindings", "SqlctRemoteBinding.sql")));
            Assert.Contains(
                "GRANT ALTER ON REMOTE SERVICE BINDING::[SqlctRemoteBinding] TO [AppUser]",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Remote Service Bindings", "SqlctRemoteBinding.sql")));
            Assert.Contains(
                "EXEC sp_addextendedproperty N'Caption', N'Fixture remote service binding', 'REMOTE SERVICE BINDING', N'SqlctRemoteBinding', NULL, NULL, NULL, NULL",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Remote Service Bindings", "SqlctRemoteBinding.sql")));
            Assert.Contains(
                "CREATE SERVICE [SqlctAppService]",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Services", "SqlctAppService.sql")));
            Assert.Contains(
                "GRANT SEND ON SERVICE::[SqlctAppService] TO [AppUser]",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Services", "SqlctAppService.sql")));
            Assert.Contains(
                "EXEC sp_addextendedproperty N'Caption', N'Fixture service', 'SERVICE', N'SqlctAppService', NULL, NULL, NULL, NULL",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Services", "SqlctAppService.sql")));
            Assert.Contains(
                "CREATE ROUTE [SqlctLocalRoute]",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Routes", "SqlctLocalRoute.sql")));
            Assert.Contains(
                "GRANT ALTER ON ROUTE::[SqlctLocalRoute] TO [AppUser]",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Routes", "SqlctLocalRoute.sql")));
            Assert.Contains(
                "EXEC sp_addextendedproperty N'Caption', N'Fixture route', 'ROUTE', N'SqlctLocalRoute', NULL, NULL, NULL, NULL",
                File.ReadAllText(Path.Combine(projectDir, "Service Broker", "Routes", "SqlctLocalRoute.sql")));
            if (fixture.HasFullText)
            {
                Assert.Contains(
                    "CREATE FULLTEXT CATALOG [SqlctCatalog]",
                    File.ReadAllText(Path.Combine(projectDir, "Storage", "Full Text Catalogs", "SqlctCatalog.sql")));
                Assert.Contains(
                    "GRANT ALTER ON FULLTEXT CATALOG::[SqlctCatalog] TO [AppUser]",
                    File.ReadAllText(Path.Combine(projectDir, "Storage", "Full Text Catalogs", "SqlctCatalog.sql")));
                Assert.Contains(
                    "CREATE FULLTEXT STOPLIST [SqlctStoplist]",
                    File.ReadAllText(Path.Combine(projectDir, "Storage", "Full Text Stoplists", "SqlctStoplist.sql")));
                Assert.Contains(
                    "ALTER FULLTEXT STOPLIST [SqlctStoplist] ADD 'sqlct' LANGUAGE 1033",
                    File.ReadAllText(Path.Combine(projectDir, "Storage", "Full Text Stoplists", "SqlctStoplist.sql")));
                Assert.Contains(
                    "GRANT ALTER ON FULLTEXT STOPLIST::[SqlctStoplist] TO [AppUser]",
                    File.ReadAllText(Path.Combine(projectDir, "Storage", "Full Text Stoplists", "SqlctStoplist.sql")));
            }
            if (fixture.HasSearchPropertyList)
            {
                var searchPropertyScript = File.ReadAllText(Path.Combine(projectDir, "Storage", "Search Property Lists", "DocumentProperties.sql"));
                Assert.Contains("CREATE SEARCH PROPERTY LIST [DocumentProperties]", searchPropertyScript);
                Assert.Contains("ALTER SEARCH PROPERTY LIST [DocumentProperties] ADD N'Title' WITH (PROPERTY_SET_GUID = 'F29F85E0-4FF9-1068-AB91-08002B27B3D9', PROPERTY_INT_ID = 2", searchPropertyScript);
                Assert.Contains("GRANT REFERENCES ON SEARCH PROPERTY LIST::[DocumentProperties] TO [AppUser]", searchPropertyScript);
            }

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

            var typedDiff = service.RunDiff(projectDir, "db", $"Role:{fixture.FixedRoleName}");
            Assert.True(typedDiff.Success, typedDiff.Error?.Detail ?? typedDiff.Error?.Message);
            Assert.Equal(string.Empty, typedDiff.Payload!.Diff);

            var schemaScopedDiff = service.RunDiff(projectDir, "db", "Synonym:Fixtures.TargetSynonym");
            Assert.True(schemaScopedDiff.Success, schemaScopedDiff.Error?.Detail ?? schemaScopedDiff.Error?.Message);
            Assert.Equal(string.Empty, schemaScopedDiff.Payload!.Diff);

            var schemaDiff = service.RunDiff(projectDir, "db", "Schema:Fixtures");
            Assert.True(schemaDiff.Success, schemaDiff.Error?.Detail ?? schemaDiff.Error?.Message);
            Assert.Equal(string.Empty, schemaDiff.Payload!.Diff);

            var tableTypeDiff = service.RunDiff(projectDir, "db", "UserDefinedType:Fixtures.RequestList");
            Assert.True(tableTypeDiff.Success, tableTypeDiff.Error?.Detail ?? tableTypeDiff.Error?.Message);
            Assert.Equal(string.Empty, tableTypeDiff.Payload!.Diff);

            var xmlSchemaCollectionDiff = service.RunDiff(projectDir, "db", "XmlSchemaCollection:Fixtures.PayloadSchema");
            Assert.True(xmlSchemaCollectionDiff.Success, xmlSchemaCollectionDiff.Error?.Detail ?? xmlSchemaCollectionDiff.Error?.Message);
            Assert.Equal(string.Empty, xmlSchemaCollectionDiff.Payload!.Diff);

            var queueDiff = service.RunDiff(projectDir, "db", "Queue:Fixtures.RequestQueue");
            Assert.True(queueDiff.Success, queueDiff.Error?.Detail ?? queueDiff.Error?.Message);
            Assert.Equal(string.Empty, queueDiff.Payload!.Diff);

            var messageTypeDiff = service.RunDiff(projectDir, "db", "MessageType://Sqlct/Request");
            Assert.True(messageTypeDiff.Success, messageTypeDiff.Error?.Detail ?? messageTypeDiff.Error?.Message);
            Assert.Equal(string.Empty, messageTypeDiff.Payload!.Diff);

            var contractDiff = service.RunDiff(projectDir, "db", "Contract://Sqlct/Contract");
            Assert.True(contractDiff.Success, contractDiff.Error?.Detail ?? contractDiff.Error?.Message);
            Assert.Equal(string.Empty, contractDiff.Payload!.Diff);

            var eventNotificationDiff = service.RunDiff(projectDir, "db", "EventNotification:NotifySchemaChanges");
            Assert.True(eventNotificationDiff.Success, eventNotificationDiff.Error?.Detail ?? eventNotificationDiff.Error?.Message);
            Assert.Equal(string.Empty, eventNotificationDiff.Payload!.Diff);

            var serviceDiff = service.RunDiff(projectDir, "db", "Service:SqlctAppService");
            Assert.True(serviceDiff.Success, serviceDiff.Error?.Detail ?? serviceDiff.Error?.Message);
            Assert.Equal(string.Empty, serviceDiff.Payload!.Diff);

            var serviceBindingDiff = service.RunDiff(projectDir, "db", "ServiceBinding:SqlctRemoteBinding");
            Assert.True(serviceBindingDiff.Success, serviceBindingDiff.Error?.Detail ?? serviceBindingDiff.Error?.Message);
            Assert.Equal(string.Empty, serviceBindingDiff.Payload!.Diff);

            var routeDiff = service.RunDiff(projectDir, "db", "Route:SqlctLocalRoute");
            Assert.True(routeDiff.Success, routeDiff.Error?.Detail ?? routeDiff.Error?.Message);
            Assert.Equal(string.Empty, routeDiff.Payload!.Diff);

            if (fixture.HasFullText)
            {
                var fullTextCatalogDiff = service.RunDiff(projectDir, "db", "FullTextCatalog:SqlctCatalog");
                Assert.True(fullTextCatalogDiff.Success, fullTextCatalogDiff.Error?.Detail ?? fullTextCatalogDiff.Error?.Message);
                Assert.Equal(string.Empty, fullTextCatalogDiff.Payload!.Diff);

                var fullTextStoplistDiff = service.RunDiff(projectDir, "db", "FullTextStoplist:SqlctStoplist");
                Assert.True(fullTextStoplistDiff.Success, fullTextStoplistDiff.Error?.Detail ?? fullTextStoplistDiff.Error?.Message);
                Assert.Equal(string.Empty, fullTextStoplistDiff.Payload!.Diff);
            }

            if (fixture.HasSearchPropertyList)
            {
                var searchPropertyListDiff = service.RunDiff(projectDir, "db", "SearchPropertyList:DocumentProperties");
                Assert.True(searchPropertyListDiff.Success, searchPropertyListDiff.Error?.Detail ?? searchPropertyListDiff.Error?.Message);
                Assert.Equal(string.Empty, searchPropertyListDiff.Payload!.Diff);
            }
        }
        finally
        {
            TryDeleteProject(projectDir);
            DropDatabase(server, databaseName);
        }
    }

    [Fact]
    public void PullAndStatus_ScriptBuiltInDboSchemaSubordinateState_WhenPresent()
    {
        var server = Environment.GetEnvironmentVariable("SQLCT_TEST_SERVER");
        if (string.IsNullOrWhiteSpace(server))
        {
            return;
        }

        var databaseName = $"SqlctDboSchema_{Guid.NewGuid():N}";
        var projectDir = Path.Combine(Path.GetTempPath(), "sqlct-tests", Guid.NewGuid().ToString("N"));

        try
        {
            CreateDboSchemaFixtureDatabase(server, databaseName);
            CreateProject(server, databaseName, projectDir);

            var service = new SyncCommandService();
            var pull = service.RunPull(projectDir);

            Assert.True(pull.Success, pull.Error?.Detail ?? pull.Error?.Message);

            var schemaPath = Path.Combine(projectDir, "Security", "Schemas", "dbo.sql");
            Assert.True(File.Exists(schemaPath));

            var schemaScript = File.ReadAllText(schemaPath);
            Assert.DoesNotContain("CREATE SCHEMA [dbo]", schemaScript);
            Assert.DoesNotContain("AUTHORIZATION [dbo]", schemaScript);
            Assert.Contains("GRANT SELECT ON SCHEMA::[dbo] TO [SchemaGrantRole]", schemaScript);
            Assert.Contains(
                "EXEC sp_addextendedproperty N'Caption', N'Built-in dbo schema', 'SCHEMA', N'dbo', NULL, NULL, NULL, NULL",
                schemaScript);

            var diff = service.RunDiff(projectDir, "db", "Schema:dbo");
            Assert.True(diff.Success, diff.Error?.Detail ?? diff.Error?.Message);
            Assert.Equal(string.Empty, diff.Payload!.Diff);

            var status = service.RunStatus(projectDir, "db");
            Assert.True(status.Success, status.Error?.Detail ?? status.Error?.Message);
            Assert.Equal(0, status.Payload!.Summary.Schema.Added);
            Assert.Equal(0, status.Payload.Summary.Schema.Changed);
            Assert.Equal(0, status.Payload.Summary.Schema.Deleted);
            Assert.Equal(0, status.Payload.Summary.Data.Added);
            Assert.Equal(0, status.Payload.Summary.Data.Changed);
            Assert.Equal(0, status.Payload.Summary.Data.Deleted);
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

    private static void CreateDboSchemaFixtureDatabase(string server, string databaseName)
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

        foreach (var statement in new[]
        {
            "CREATE ROLE [SchemaGrantRole] AUTHORIZATION [dbo];",
            "GRANT SELECT ON SCHEMA::[dbo] TO [SchemaGrantRole];",
            "EXEC sp_addextendedproperty N'Caption', N'Built-in dbo schema', 'SCHEMA', N'dbo';"
        })
        {
            using var command = fixtureConnection.CreateCommand();
            command.CommandText = statement;
            command.ExecuteNonQuery();
        }
    }

    private static FixtureDatabaseInfo CreateFixtureDatabase(string server, string databaseName)
    {
        using var connection = SqlConnectionFactory.Create(new SqlConnectionOptions(server, "master", "integrated", null, null, true));
        connection.Open();

        using var createDatabase = connection.CreateCommand();
        createDatabase.CommandText = $"CREATE DATABASE [{databaseName}];";
        createDatabase.ExecuteNonQuery();

        using (var enableBroker = connection.CreateCommand())
        {
            enableBroker.CommandText = $"ALTER DATABASE [{databaseName}] SET ENABLE_BROKER WITH ROLLBACK IMMEDIATE;";
            enableBroker.ExecuteNonQuery();
        }

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
            "CREATE USER [BindingUser] WITHOUT LOGIN;",
            "CREATE TYPE [Fixtures].[CodeType] FROM [nvarchar](20) NOT NULL;",
            "CREATE TYPE [Fixtures].[RequestList] AS TABLE ([Id] [int] NOT NULL, [Code] [nvarchar](20) NOT NULL, PRIMARY KEY CLUSTERED ([Id]));",
            "CREATE XML SCHEMA COLLECTION [Fixtures].[PayloadSchema] AS N'<xsd:schema xmlns:xsd=\"http://www.w3.org/2001/XMLSchema\" targetNamespace=\"urn:sqlct:payload\" elementFormDefault=\"qualified\"><xsd:element name=\"payload\" type=\"xsd:string\" /></xsd:schema>';",
            "CREATE TABLE [Fixtures].[TargetTable] ([Id] [int] NOT NULL, [Code] [Fixtures].[CodeType] NOT NULL);",
            "CREATE SYNONYM [Fixtures].[TargetSynonym] FOR [Fixtures].[TargetTable];",
            "CREATE MESSAGE TYPE [//Sqlct/Request] VALIDATION = NONE;",
            "CREATE MESSAGE TYPE [//Sqlct/Reply] VALIDATION = NONE;",
            "CREATE CONTRACT [//Sqlct/Contract] ([//Sqlct/Request] SENT BY INITIATOR, [//Sqlct/Reply] SENT BY TARGET);",
            "CREATE QUEUE [Fixtures].[RequestQueue];",
            "CREATE SERVICE [SqlctAppService] ON QUEUE [Fixtures].[RequestQueue] ([//Sqlct/Contract]);",
            "CREATE EVENT NOTIFICATION [NotifySchemaChanges] ON DATABASE FOR CREATE_TABLE, ALTER_TABLE TO SERVICE N'SqlctAppService', N'current database';",
            "CREATE REMOTE SERVICE BINDING [SqlctRemoteBinding] TO SERVICE N'SqlctRemoteService' WITH USER = [BindingUser], ANONYMOUS = ON;",
            "CREATE ROUTE [SqlctLocalRoute] WITH SERVICE_NAME = 'SqlctAppService', ADDRESS = 'LOCAL';",
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

        foreach (var statement in new[]
        {
            "GRANT SELECT ON SCHEMA::[Fixtures] TO [AppUser];",
            "EXEC sp_addextendedproperty N'Caption', N'Fixture schema', 'SCHEMA', N'Fixtures';",
            "GRANT REFERENCES ON XML SCHEMA COLLECTION::[Fixtures].[PayloadSchema] TO [AppUser];",
            "EXEC sp_addextendedproperty N'Caption', N'Fixture XML schema collection', 'SCHEMA', N'Fixtures', 'XML SCHEMA COLLECTION', N'PayloadSchema';",
            "GRANT REFERENCES ON MESSAGE TYPE::[//Sqlct/Request] TO [AppUser];",
            "EXEC sp_addextendedproperty N'Caption', N'Fixture message type', 'MESSAGE TYPE', N'//Sqlct/Request';",
            "GRANT REFERENCES ON CONTRACT::[//Sqlct/Contract] TO [AppUser];",
            "EXEC sp_addextendedproperty N'Caption', N'Fixture contract', 'CONTRACT', N'//Sqlct/Contract';",
            "GRANT RECEIVE ON [Fixtures].[RequestQueue] TO [AppUser];",
            "EXEC sp_addextendedproperty N'Caption', N'Fixture queue', 'SCHEMA', N'Fixtures', 'QUEUE', N'RequestQueue';",
            "GRANT SEND ON SERVICE::[SqlctAppService] TO [AppUser];",
            "EXEC sp_addextendedproperty N'Caption', N'Fixture service', 'SERVICE', N'SqlctAppService';",
            "GRANT ALTER ON ROUTE::[SqlctLocalRoute] TO [AppUser];",
            "EXEC sp_addextendedproperty N'Caption', N'Fixture route', 'ROUTE', N'SqlctLocalRoute';",
            "EXEC sp_addextendedproperty N'Caption', N'Fixture event notification', 'EVENT NOTIFICATION', N'NotifySchemaChanges';",
            "GRANT ALTER ON REMOTE SERVICE BINDING::[SqlctRemoteBinding] TO [AppUser];",
            "EXEC sp_addextendedproperty N'Caption', N'Fixture remote service binding', 'REMOTE SERVICE BINDING', N'SqlctRemoteBinding';"
        })
        {
            using var setup = fixtureConnection.CreateCommand();
            setup.CommandText = statement;
            setup.ExecuteNonQuery();
        }

        var hasFullText = false;
        using (var fullTextCheck = fixtureConnection.CreateCommand())
        {
            fullTextCheck.CommandText = "SELECT CAST(FULLTEXTSERVICEPROPERTY('IsFullTextInstalled') AS int);";
            hasFullText = Convert.ToInt32(fullTextCheck.ExecuteScalar() ?? 0) == 1;
        }

        if (hasFullText)
        {
            var fullTextStatements = new[]
            {
                "CREATE FULLTEXT CATALOG [SqlctCatalog] WITH ACCENT_SENSITIVITY = OFF;",
                "CREATE FULLTEXT STOPLIST [SqlctStoplist];",
                "ALTER FULLTEXT STOPLIST [SqlctStoplist] ADD 'sqlct' LANGUAGE 1033;"
            };

            foreach (var statement in fullTextStatements)
            {
                using var setup = fixtureConnection.CreateCommand();
                setup.CommandText = statement;
                setup.ExecuteNonQuery();
            }

            foreach (var statement in new[]
            {
                "GRANT ALTER ON FULLTEXT CATALOG::[SqlctCatalog] TO [AppUser];",
                "GRANT ALTER ON FULLTEXT STOPLIST::[SqlctStoplist] TO [AppUser];"
            })
            {
                using var setup = fixtureConnection.CreateCommand();
                setup.CommandText = statement;
                setup.ExecuteNonQuery();
            }
        }

        var hasSearchPropertyList = TryCreateSearchPropertyList(fixtureConnection);

        if (hasSearchPropertyList)
        {
            using var setup = fixtureConnection.CreateCommand();
            setup.CommandText = "GRANT REFERENCES ON SEARCH PROPERTY LIST::[DocumentProperties] TO [AppUser];";
            setup.ExecuteNonQuery();
        }

        return new FixtureDatabaseInfo(fixedRoleName, hasFullText, hasSearchPropertyList);
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

    private static bool TryCreateSearchPropertyList(SqlConnection connection)
    {
        if (!CatalogViewExists(connection, "sys.registered_search_property_lists"))
        {
            return false;
        }

        try
        {
            foreach (var statement in new[]
            {
                "CREATE SEARCH PROPERTY LIST [DocumentProperties];",
                "ALTER SEARCH PROPERTY LIST [DocumentProperties] ADD 'Title' WITH (PROPERTY_SET_GUID = 'F29F85E0-4FF9-1068-AB91-08002B27B3D9', PROPERTY_INT_ID = 2, PROPERTY_DESCRIPTION = 'System.Title');"
            })
            {
                using var command = connection.CreateCommand();
                command.CommandText = statement;
                command.ExecuteNonQuery();
            }

            return true;
        }
        catch (SqlException)
        {
            try
            {
                using var cleanup = connection.CreateCommand();
                cleanup.CommandText = """
IF EXISTS (SELECT 1 FROM sys.registered_search_property_lists WHERE name = N'DocumentProperties')
BEGIN
    DROP SEARCH PROPERTY LIST [DocumentProperties];
END;
""";
                cleanup.ExecuteNonQuery();
            }
            catch (SqlException)
            {
            }

            return false;
        }
    }

    private static bool CatalogViewExists(SqlConnection connection, string objectName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT OBJECT_ID(@name);";
        command.Parameters.AddWithValue("@name", objectName);
        var result = command.ExecuteScalar();
        return result is not null && result != DBNull.Value;
    }

    private sealed record FixtureDatabaseInfo(string FixedRoleName, bool HasFullText, bool HasSearchPropertyList);
}
