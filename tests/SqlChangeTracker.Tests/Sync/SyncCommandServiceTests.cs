using SqlChangeTracker.Config;
using SqlChangeTracker.Schema;
using SqlChangeTracker.Sql;
using SqlChangeTracker.Sync;
using System.Text;
using Xunit;

namespace SqlChangeTracker.Tests.Sync;

public sealed class SyncCommandServiceTests
{
    [Theory]
    [InlineData("dbo.Customer", true, "dbo", "Customer")]
    [InlineData("Customer", false, "", "")]
    [InlineData("dbo.", false, "", "")]
    [InlineData(".Customer", false, "", "")]
    public void TryParseSchemaAndName_ValidatesInput(string fileName, bool expected, string expectedSchema, string expectedName)
    {
        var success = SyncCommandService.TryParseSchemaAndName(fileName, out var schema, out var name);

        Assert.Equal(expected, success);
        Assert.Equal(expectedSchema, schema);
        Assert.Equal(expectedName, name);
    }

    [Theory]
    [InlineData("ServiceUser", true, "", "ServiceUser")]
    [InlineData("Security.Reader", true, "", "Security.Reader")]
    [InlineData("%2F%2FApp%2FMessaging%2FRequest", true, "", "//App/Messaging/Request")]
    [InlineData("", false, "", "")]
    [InlineData("   ", false, "", "")]
    public void TryParseObjectFileName_SupportsSchemaLessNames(string fileName, bool expected, string expectedSchema, string expectedName)
    {
        var success = SyncCommandService.TryParseObjectFileName(fileName, isSchemaLess: true, out var schema, out var name);

        Assert.Equal(expected, success);
        Assert.Equal(expectedSchema, schema);
        Assert.Equal(expectedName, name);
    }

    [Theory]
    [InlineData(
        "MessageType",
        "__App_Messaging_Request",
        "CREATE MESSAGE TYPE [//App/Messaging/Request]\r\nVALIDATION = NONE\r\nGO",
        "__App_Messaging_Request",
        true,
        "//App/Messaging/Request")]
    [InlineData(
        "Contract",
        "__App_Messaging_Contract",
        "CREATE CONTRACT [//App/Messaging/Contract]\r\n(\r\n)\r\nGO",
        "__App_Messaging_Contract",
        true,
        "//App/Messaging/Contract")]
    [InlineData(
        "MessageType",
        "%2F%2FApp%2FMessaging%2FRequest",
        "CREATE MESSAGE TYPE [//App/Messaging/Request]\r\nVALIDATION = NONE\r\nGO",
        "//App/Messaging/Request",
        false,
        "")]
    [InlineData(
        "Role",
        "AppReader",
        "CREATE ROLE [OtherRole]\r\nGO",
        "AppReader",
        false,
        "")]
    public void TryResolveSchemaLessFolderIdentityFromScript_UsesDefinitionOnlyForEscapedLegacyNames(
        string objectType,
        string fileName,
        string script,
        string parsedFileName,
        bool expected,
        string expectedName)
    {
        var success = SyncCommandService.TryResolveSchemaLessFolderIdentityFromScript(
            objectType,
            fileName,
            script,
            parsedFileName,
            out var name);

        Assert.Equal(expected, success);
        Assert.Equal(expectedName, name);
    }

    [Theory]
    [InlineData("dbo.Customer", null, "dbo", "Customer", false)]
    [InlineData("ServiceUser", null, "", "ServiceUser", true)]
    [InlineData("App.Core.Assembly", null, "", "App.Core.Assembly", true)]
    [InlineData("Role:AppReader", "Role", "", "AppReader", true)]
    [InlineData("Assembly:AppClr", "Assembly", "", "AppClr", true)]
    [InlineData("SearchPropertyList:DocumentProperties", "SearchPropertyList", "", "DocumentProperties", true)]
    [InlineData("Synonym:Reporting.CurrentSales", "Synonym", "Reporting", "CurrentSales", false)]
    [InlineData("UserDefinedType:dbo.PhoneNumber", "UserDefinedType", "dbo", "PhoneNumber", false)]
    [InlineData("XmlSchemaCollection:dbo.PayloadSchema", "XmlSchemaCollection", "dbo", "PayloadSchema", false)]
    [InlineData("data:dbo.Customer", "TableData", "dbo", "Customer", false)]
    public void ParseObjectSelector_AcceptsSchemaScopedSchemaLessAndTypedSelectors(
        string selector,
        string? expectedType,
        string expectedSchema,
        string expectedName,
        bool expectedSchemaLess)
    {
        var result = SyncCommandService.ParseObjectSelector(selector);

        Assert.True(result.Success);
        Assert.Equal(expectedType, result.Payload!.ObjectType);
        Assert.Equal(expectedSchema, result.Payload.Schema);
        Assert.Equal(expectedName, result.Payload.Name);
        Assert.Equal(expectedSchemaLess, result.Payload.IsSchemaLess);
    }

    [Fact]
    public void RunStatus_MatchesLegacySchemaLessFileNameToScriptObjectName()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var config = SqlctConfigWriter.CreateDefault();
            config.Database.Server = "localhost";
            config.Database.Name = "TestDb";
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
            Assert.True(write.Success);

            var script = "CREATE MESSAGE TYPE [//App/Messaging/Request]\r\nVALIDATION = NONE\r\nGO\r\n";
            CreateFile(projectDir, Path.Combine("Service Broker", "Message Types", "__App_Messaging_Request.sql"), script);

            var introspector = new TrackingIntrospector
            {
                AllObjects = [new DbObjectInfo(string.Empty, "//App/Messaging/Request", "MessageType")]
            };
            var scripter = new TrackingScripter
            {
                ScriptObjectHandler = (_, _, _) => script
            };

            var service = new SyncCommandService(
                new SqlctConfigReader(),
                introspector,
                scripter,
                new SchemaFolderMapper(SupportedSqlObjectTypes.DefaultFolderMap, dataWriteAllFilesInOneDirectory: true));

            var result = service.RunStatus(projectDir, "db");

            Assert.True(result.Success, result.Error?.Detail ?? result.Error?.Message);
            Assert.Equal(ExitCodes.Success, result.ExitCode);
            Assert.Empty(result.Payload!.Objects);
            Assert.Equal(0, result.Payload.Summary.Schema.Added);
            Assert.Equal(0, result.Payload.Summary.Schema.Deleted);
            Assert.Equal(0, result.Payload.Summary.Schema.Changed);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Theory]
    [InlineData("")]
    [InlineData("dbo.")]
    [InlineData(".Customer")]
    [InlineData("UnknownType:dbo.Customer")]
    [InlineData("TableType:dbo.Customer")]
    [InlineData("Synonym:CurrentSales")]
    [InlineData("data:Customer")]
    public void ParseObjectSelector_RejectsInvalidSelectors(string selector)
    {
        var result = SyncCommandService.ParseObjectSelector(selector);

        Assert.False(result.Success);
        Assert.Equal(ExitCodes.InvalidConfig, result.ExitCode);
    }

    [Fact]
    public void MatchesSelector_UsesSchemaLessAndTypeQualifiedRules()
    {
        var bareRole = SyncCommandService.ParseObjectSelector("Sales");
        var typedRole = SyncCommandService.ParseObjectSelector("Role:Sales");
        var schemaScoped = SyncCommandService.ParseObjectSelector("dbo.Sales");
        var dataScoped = SyncCommandService.ParseObjectSelector("data:dbo.Customer");

        Assert.True(bareRole.Success);
        Assert.True(typedRole.Success);
        Assert.True(schemaScoped.Success);
        Assert.True(dataScoped.Success);

        Assert.True(SyncCommandService.MatchesSelector("Role", "", "Sales", bareRole.Payload!));
        Assert.True(SyncCommandService.MatchesSelector("User", "", "Sales", bareRole.Payload!));
        Assert.False(SyncCommandService.MatchesSelector("Table", "dbo", "Sales", bareRole.Payload!));

        Assert.True(SyncCommandService.MatchesSelector("Role", "", "Sales", typedRole.Payload!));
        Assert.False(SyncCommandService.MatchesSelector("User", "", "Sales", typedRole.Payload!));

        Assert.True(SyncCommandService.MatchesSelector("Table", "dbo", "Sales", schemaScoped.Payload!));
        Assert.False(SyncCommandService.MatchesSelector("Role", "", "Sales", schemaScoped.Payload!));

        Assert.True(SyncCommandService.MatchesSelector("TableData", "dbo", "Customer", dataScoped.Payload!));
        Assert.False(SyncCommandService.MatchesSelector("Table", "dbo", "Customer", dataScoped.Payload!));
    }

    [Theory]
    [InlineData("dbo.Customer_Data", true, "dbo", "Customer")]
    [InlineData("Ops%3A1.Customer%3FArchive_Data", true, "Ops:1", "Customer?Archive")]
    [InlineData("dbo.Customer", false, "", "")]
    [InlineData("Customer_Data", false, "", "")]
    public void TryParseDataFileName_SupportsTrackedDataScripts(string fileName, bool expected, string expectedSchema, string expectedName)
    {
        var success = SyncCommandService.TryParseDataFileName(fileName, out var schema, out var name);

        Assert.Equal(expected, success);
        Assert.Equal(expectedSchema, schema);
        Assert.Equal(expectedName, name);
    }

    [Theory]
    [InlineData("Ops%3A1.Customer%3FArchive", true, "Ops:1", "Customer?Archive")]
    [InlineData("%2F%2FApp%2FMessaging%2FRequest", false, "", "")]
    public void TryParseSchemaAndName_DecodesEscapedPartsAndStillValidatesShape(
        string fileName,
        bool expected,
        string expectedSchema,
        string expectedName)
    {
        var success = SyncCommandService.TryParseSchemaAndName(fileName, out var schema, out var name);

        Assert.Equal(expected, success);
        Assert.Equal(expectedSchema, schema);
        Assert.Equal(expectedName, name);
    }

    [Fact]
    public void ComputeChangesForComparison_ClassifiesAndOrdersDeterministically()
    {
        var source = new[]
        {
            new SyncCommandService.ComparableObject("dbo", "BTable", "Table", "CREATE TABLE dbo.BTable;"),
            new SyncCommandService.ComparableObject("dbo", "ATable", "View", "CREATE VIEW dbo.ATable AS SELECT 1;"),
            new SyncCommandService.ComparableObject("dbo", "DTable", "Function", "CREATE FUNCTION dbo.DTable() RETURNS int AS BEGIN RETURN 1 END")
        };
        var target = new[]
        {
            new SyncCommandService.ComparableObject("dbo", "ATable", "View", "CREATE VIEW dbo.ATable AS SELECT 2;"),
            new SyncCommandService.ComparableObject("dbo", "CTable", "Table", "CREATE TABLE dbo.CTable;"),
            new SyncCommandService.ComparableObject("dbo", "DTable", "Function", "CREATE FUNCTION dbo.DTable() RETURNS int AS BEGIN RETURN 1 END")
        };

        var changes = SyncCommandService.ComputeChangesForComparison(source, target);

        Assert.Collection(changes,
            change =>
            {
                Assert.Equal("ATable", change.Object.Name);
                Assert.Equal("changed", change.Change);
            },
            change =>
            {
                Assert.Equal("BTable", change.Object.Name);
                Assert.Equal("added", change.Change);
            },
            change =>
            {
                Assert.Equal("CTable", change.Object.Name);
                Assert.Equal("deleted", change.Change);
            });
    }

    [Fact]
    public void BuildUnifiedDiff_FormatsHeadersAndBody()
    {
        var diff = SyncCommandService.BuildUnifiedDiff(
            "db",
            "folder",
            "CREATE VIEW dbo.V AS SELECT 1;\r\nGO\r\n",
            "CREATE VIEW dbo.V AS SELECT 2;\nGO\n");

        Assert.Contains("--- db", diff);
        Assert.Contains("+++ folder", diff);
        Assert.Contains("@@", diff);
        Assert.Contains("-CREATE VIEW dbo.V AS SELECT 1;", diff);
        Assert.Contains("+CREATE VIEW dbo.V AS SELECT 2;", diff);
    }

    [Fact]
    public void BuildUnifiedDiff_HandlesAddedAndDeletedCases()
    {
        var added = SyncCommandService.BuildUnifiedDiff(
            "db",
            "folder",
            "",
            "CREATE VIEW dbo.V AS SELECT 1;\n");

        var deleted = SyncCommandService.BuildUnifiedDiff(
            "db",
            "folder",
            "CREATE VIEW dbo.V AS SELECT 1;\n",
            "");

        Assert.Contains("--- db", added);
        Assert.Contains("+++ folder", added);
        Assert.Contains("+CREATE VIEW dbo.V AS SELECT 1;", added);

        Assert.Contains("--- db", deleted);
        Assert.Contains("+++ folder", deleted);
        Assert.Contains("-CREATE VIEW dbo.V AS SELECT 1;", deleted);
    }

    [Fact]
    public void BuildUnifiedDiff_ShowsOnlyChangedChunkWithContext()
    {
        // 10-line script with a single changed line in the middle
        var source = string.Join("\n",
            "line1", "line2", "line3", "line4", "line5",
            "CHANGED_SRC",
            "line7", "line8", "line9", "line10");
        var target = string.Join("\n",
            "line1", "line2", "line3", "line4", "line5",
            "CHANGED_TGT",
            "line7", "line8", "line9", "line10");

        var diff = SyncCommandService.BuildUnifiedDiff("db", "folder", source, target, contextLines: 2);

        // Changed lines appear
        Assert.Contains("-CHANGED_SRC", diff);
        Assert.Contains("+CHANGED_TGT", diff);
        // Context lines within 2 lines of the change appear
        Assert.Contains(" line4", diff);
        Assert.Contains(" line5", diff);
        Assert.Contains(" line7", diff);
        Assert.Contains(" line8", diff);
        // Lines beyond context do not appear
        Assert.DoesNotContain(" line1", diff);
        Assert.DoesNotContain(" line2", diff);
        Assert.DoesNotContain(" line9", diff);
        Assert.DoesNotContain(" line10", diff);
    }

    [Fact]
    public void BuildUnifiedDiff_ContextLinesZero_ShowsOnlyChanges()
    {
        var source = string.Join("\n", "before", "REMOVED", "after");
        var target = string.Join("\n", "before", "ADDED", "after");

        var diff = SyncCommandService.BuildUnifiedDiff("db", "folder", source, target, contextLines: 0);

        Assert.Contains("-REMOVED", diff);
        Assert.Contains("+ADDED", diff);
        // No context lines
        Assert.DoesNotContain(" before", diff);
        Assert.DoesNotContain(" after", diff);
    }

    [Fact]
    public void BuildUnifiedDiff_SeparatesDistantChangesIntoMultipleHunks()
    {
        // Two changes far apart (more than 2*contextLines apart)
        var source = string.Join("\n",
            "CHANGE_A",
            "ctx1", "ctx2", "ctx3", "ctx4", "ctx5", "ctx6", "ctx7",
            "CHANGE_B");
        var target = string.Join("\n",
            "CHANGE_A_NEW",
            "ctx1", "ctx2", "ctx3", "ctx4", "ctx5", "ctx6", "ctx7",
            "CHANGE_B_NEW");

        // With contextLines=1, the 7 unchanged lines separate the two changes into distinct hunks
        var diff = SyncCommandService.BuildUnifiedDiff("db", "folder", source, target, contextLines: 1);

        // Both changes present
        Assert.Contains("-CHANGE_A", diff);
        Assert.Contains("+CHANGE_A_NEW", diff);
        Assert.Contains("-CHANGE_B", diff);
        Assert.Contains("+CHANGE_B_NEW", diff);

        // Two @@ hunk headers appear
        var hunkHeaderCount = CountHunkHeaders(diff);
        Assert.True(hunkHeaderCount >= 2, $"Expected at least 2 hunk headers but got {hunkHeaderCount}");
    }

    [Fact]
    public void BuildUnifiedDiff_MergesNearbyChangesIntoOneHunk()
    {
        // Two changes with only 2 unchanged lines between them; contextLines=2 → they merge
        var source = string.Join("\n", "CHANGE_A", "same1", "same2", "CHANGE_B");
        var target = string.Join("\n", "CHANGE_A_NEW", "same1", "same2", "CHANGE_B_NEW");

        var diff = SyncCommandService.BuildUnifiedDiff("db", "folder", source, target, contextLines: 2);

        // Exactly one @@ hunk header because the hunks are merged
        var hunkHeaderCount = CountHunkHeaders(diff);
        Assert.Equal(1, hunkHeaderCount);
        Assert.Contains("-CHANGE_A", diff);
        Assert.Contains("+CHANGE_A_NEW", diff);
        Assert.Contains("-CHANGE_B", diff);
        Assert.Contains("+CHANGE_B_NEW", diff);
    }

    [Fact]
    public void BuildUnifiedDiff_HunkHeaderContainsCorrectLineNumbers()
    {
        // Source: line1, OLD, line3; target: line1, NEW, line3 (change at line 2)
        var source = string.Join("\n", "line1", "OLD", "line3");
        var target = string.Join("\n", "line1", "NEW", "line3");

        // contextLines=0 so only the change itself is shown
        var diff = SyncCommandService.BuildUnifiedDiff("db", "folder", source, target, contextLines: 0);

        // Hunk covers source lines 2..2 (1 line) and target lines 2..2 (1 line)
        Assert.Contains("@@ -2,1 +2,1 @@", diff);
        Assert.Contains("-OLD", diff);
        Assert.Contains("+NEW", diff);
    }

    [Fact]
    public void RunDiff_WithObjectSelector_UsesTargetedDatabaseDiscovery()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var config = SqlctConfigWriter.CreateDefault();
            config.Database.Server = "localhost";
            config.Database.Name = "TestDb";
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
            Assert.True(write.Success);

            CreateFile(projectDir, Path.Combine("Tables", "dbo.Customer.sql"), "CREATE TABLE [dbo].[Customer] ([Id] [int] NOT NULL);\r\n");

            var introspector = new TrackingIntrospector
            {
                MatchingObjects = [new DbObjectInfo("dbo", "Customer", "Table")]
            };
            var scripter = new TrackingScripter
            {
                ScriptObjectHandler = (_, obj, _) => "CREATE TABLE [dbo].[Customer] ([Id] [int] NOT NULL);\r\n"
            };

            var service = new SyncCommandService(
                new SqlctConfigReader(),
                introspector,
                scripter,
                new SchemaFolderMapper(SupportedSqlObjectTypes.DefaultFolderMap, dataWriteAllFilesInOneDirectory: true));

            var result = service.RunDiff(projectDir, "db", "dbo.Customer");

            Assert.True(result.Success, result.Error?.Detail ?? result.Error?.Message);
            Assert.Equal(ExitCodes.Success, result.ExitCode);
            Assert.Equal(string.Empty, result.Payload!.Diff);
            Assert.False(introspector.ListObjectsCalled);
            Assert.True(introspector.ListMatchingObjectsCalled);
            var expectedCandidateTypes = new[] { "Function", "Queue", "Sequence", "StoredProcedure", "Synonym", "Table", "UserDefinedType", "View", "XmlSchemaCollection" };
            Assert.Equal(
                expectedCandidateTypes,
                introspector.LastRequestedObjectTypes.OrderBy(item => item, StringComparer.OrdinalIgnoreCase));
            Assert.Equal(new[] { "Table:dbo.Customer" }, scripter.ScriptedObjects);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunDiff_WithDataSelector_UsesTargetedTableLookup()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var config = SqlctConfigWriter.CreateDefault();
            config.Database.Server = "localhost";
            config.Database.Name = "TestDb";
            config.Data.TrackedTables.Add("dbo.Customer");
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
            Assert.True(write.Success);

            CreateFile(projectDir, Path.Combine("Data", "dbo.Customer_Data.sql"), "INSERT INTO [dbo].[Customer] ([Id]) VALUES (1);\r\n");

            var introspector = new TrackingIntrospector
            {
                MatchingObjects = [new DbObjectInfo("dbo", "Customer", "Table")]
            };
            var scripter = new TrackingScripter
            {
                ScriptTableDataHandler = (_, table) => "INSERT INTO [dbo].[Customer] ([Id]) VALUES (1);\r\n"
            };

            var service = new SyncCommandService(
                new SqlctConfigReader(),
                introspector,
                scripter,
                new SchemaFolderMapper(SupportedSqlObjectTypes.DefaultFolderMap, dataWriteAllFilesInOneDirectory: true));

            var result = service.RunDiff(projectDir, "db", "data:dbo.Customer");

            Assert.True(result.Success, result.Error?.Detail ?? result.Error?.Message);
            Assert.Equal(ExitCodes.Success, result.ExitCode);
            Assert.False(introspector.ListObjectsCalled);
            Assert.True(introspector.ListMatchingObjectsCalled);
            Assert.Equal(new[] { "Table" }, introspector.LastRequestedObjectTypes);
            Assert.Empty(scripter.ScriptedObjects);
            Assert.Equal(new[] { "dbo.Customer" }, scripter.ScriptedTableData);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunDiff_WithSchemaLessObjectSelector_UsesTargetedDatabaseDiscoveryWithEmptySchema()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var config = SqlctConfigWriter.CreateDefault();
            config.Database.Server = "localhost";
            config.Database.Name = "TestDb";
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
            Assert.True(write.Success);

            CreateFile(projectDir, Path.Combine("Security", "Roles", "AppReader.sql"), "EXEC sp_addrolemember N'AppReader', N'ServiceUser';\r\n");

            var introspector = new TrackingIntrospector
            {
                MatchingObjects = [new DbObjectInfo("", "AppReader", "Role")]
            };
            var scripter = new TrackingScripter
            {
                ScriptObjectHandler = (_, obj, _) => "EXEC sp_addrolemember N'AppReader', N'ServiceUser';\r\n"
            };

            var service = new SyncCommandService(
                new SqlctConfigReader(),
                introspector,
                scripter,
                new SchemaFolderMapper(SupportedSqlObjectTypes.DefaultFolderMap, dataWriteAllFilesInOneDirectory: true));

            var result = service.RunDiff(projectDir, "db", "Role:AppReader");

            Assert.True(result.Success, result.Error?.Detail ?? result.Error?.Message);
            Assert.Equal(ExitCodes.Success, result.ExitCode);
            Assert.Equal(string.Empty, result.Payload!.Diff);
            Assert.False(introspector.ListObjectsCalled);
            Assert.True(introspector.ListMatchingObjectsCalled);
            Assert.Equal(new[] { "Role" }, introspector.LastRequestedObjectTypes);
            Assert.Equal(string.Empty, introspector.LastRequestedSchema);
            Assert.Equal("AppReader", introspector.LastRequestedName);
            Assert.Equal(new[] { "Role:.AppReader" }, scripter.ScriptedObjects);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunDiff_WithAssemblySelector_UsesTargetedDatabaseDiscoveryWithEmptySchema()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var config = SqlctConfigWriter.CreateDefault();
            config.Database.Server = "localhost";
            config.Database.Name = "TestDb";
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
            Assert.True(write.Success);

            CreateFile(projectDir, Path.Combine("Assemblies", "AppClr.sql"), "CREATE ASSEMBLY [AppClr]\r\nFROM 0x00\r\nWITH PERMISSION_SET = SAFE\r\nGO\r\n");

            var introspector = new TrackingIntrospector
            {
                MatchingObjects = [new DbObjectInfo(string.Empty, "AppClr", "Assembly")]
            };
            var scripter = new TrackingScripter
            {
                ScriptObjectHandler = (_, _, _) => "CREATE ASSEMBLY [AppClr]\r\nFROM 0x00\r\nWITH PERMISSION_SET = SAFE\r\nGO\r\n"
            };

            var service = new SyncCommandService(
                new SqlctConfigReader(),
                introspector,
                scripter,
                new SchemaFolderMapper(SupportedSqlObjectTypes.DefaultFolderMap, dataWriteAllFilesInOneDirectory: true));

            var result = service.RunDiff(projectDir, "db", "Assembly:AppClr");

            Assert.True(result.Success, result.Error?.Detail ?? result.Error?.Message);
            Assert.Equal(ExitCodes.Success, result.ExitCode);
            Assert.Equal(string.Empty, result.Payload!.Diff);
            Assert.False(introspector.ListObjectsCalled);
            Assert.True(introspector.ListMatchingObjectsCalled);
            Assert.Equal(new[] { "Assembly" }, introspector.LastRequestedObjectTypes);
            Assert.Equal(string.Empty, introspector.LastRequestedSchema);
            Assert.Equal("AppClr", introspector.LastRequestedName);
            Assert.Equal(new[] { "Assembly:.AppClr" }, scripter.ScriptedObjects);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunStatus_WithProjectDirWrappedInSingleQuotes_ResolvesConfigPath()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project with spaces");
            var wrappedProjectDir = $"'{projectDir}{Path.DirectorySeparatorChar}'";

            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var writer = new SqlctConfigWriter();
            var write = writer.Write(SqlctConfigWriter.GetDefaultPath(projectDir), SqlctConfigWriter.CreateDefault());
            Assert.True(write.Success);

            var service = new SyncCommandService();
            var result = service.RunStatus(wrappedProjectDir, "db");

            Assert.False(result.Success);
            Assert.NotNull(result.Error);
            Assert.Equal(ErrorCodes.InvalidConfig, result.Error!.Code);
            Assert.Equal("missing required field: database.server.", result.Error.Detail);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunStatus_WithProjectDirEndingInDoubleQuoteArtifact_ResolvesConfigPath()
    {
        if (!OperatingSystem.IsWindows())
            return;

        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project with spaces");
            var projectDirWithQuoteArtifact = projectDir + Path.DirectorySeparatorChar + '"';

            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var writer = new SqlctConfigWriter();
            var write = writer.Write(SqlctConfigWriter.GetDefaultPath(projectDir), SqlctConfigWriter.CreateDefault());
            Assert.True(write.Success);

            var service = new SyncCommandService();
            var result = service.RunStatus(projectDirWithQuoteArtifact, "db");

            Assert.False(result.Success);
            Assert.NotNull(result.Error);
            Assert.Equal(ErrorCodes.InvalidConfig, result.Error!.Code);
            Assert.Equal("missing required field: database.server.", result.Error.Detail);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void CollectUnsupportedFolderWarnings_FlagsUnsupportedSqlEntriesInStableOrder()
    {
        var tempDir = CreateTempDir();

        try
        {
            var supported = CreateFile(tempDir, Path.Combine("Tables", "dbo.Customer.sql"), "CREATE TABLE dbo.Customer;");
            var supportedRole = CreateFile(tempDir, Path.Combine("Security", "Roles", "AppReader.sql"), "EXEC sp_addrolemember N'AppReader', N'ServiceUser'");
            var supportedPartitionFunction = CreateFile(tempDir, Path.Combine("Storage", "Partition Functions", "FiscalYear_PF.sql"), "CREATE PARTITION FUNCTION [FiscalYear_PF]...");
            var supportedSynonym = CreateFile(tempDir, Path.Combine("Synonyms", "dbo.LegacyCustomer.sql"), "CREATE SYNONYM [dbo].[LegacyCustomer] FOR [dbo].[Customer]");
            var supportedUserDefinedType = CreateFile(tempDir, Path.Combine("Types", "User-defined Data Types", "dbo.PhoneNumber.sql"), "CREATE TYPE [dbo].[PhoneNumber] FROM [nvarchar] (20) NOT NULL");
            var supportedAssembly = CreateFile(tempDir, Path.Combine("Assemblies", "AppClr.sql"), "CREATE ASSEMBLY [AppClr] FROM 0x00 WITH PERMISSION_SET = SAFE");
            var supportedData = CreateFile(tempDir, Path.Combine("Data", "dbo.Customer_Data.sql"), "SELECT 1;");
            CreateFile(tempDir, Path.Combine("Types", "Table Types", "dbo.LegacyType.sql"), "CREATE TYPE [dbo].[LegacyType] AS TABLE ([Id] [int] NOT NULL)");
            CreateFile(tempDir, Path.Combine("Custom", "dbo.Legacy.sql"), "SELECT 1;");
            CreateFile(tempDir, Path.Combine("Data", "Invalid", "dbo.Customer_Data.sql"), "SELECT 1;");

            var warnings = SyncCommandService.CollectUnsupportedFolderWarnings(
                tempDir,
                [supported, supportedRole, supportedPartitionFunction, supportedSynonym, supportedUserDefinedType, supportedAssembly, supportedData]);

            Assert.Collection(warnings,
                warning =>
                {
                    Assert.Equal("unsupported_folder_entry", warning.Code);
                    Assert.Contains(Path.Combine("Custom", "dbo.Legacy.sql"), warning.Message);
                },
                warning =>
                {
                    Assert.Equal("unsupported_folder_entry", warning.Code);
                    Assert.Contains(Path.Combine("Data", "Invalid", "dbo.Customer_Data.sql"), warning.Message);
                },
                warning =>
                {
                    Assert.Equal("legacy_folder_entry", warning.Code);
                    Assert.Contains(Path.Combine("Types", "Table Types", "dbo.LegacyType.sql"), warning.Message);
                });
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void CollectUnsupportedFolderWarnings_IgnoresSupportedFilesAndNonSqlArtifacts()
    {
        var tempDir = CreateTempDir();

        try
        {
            var supportedTable = CreateFile(tempDir, Path.Combine("Tables", "dbo.Customer.sql"), "CREATE TABLE dbo.Customer;");
            var supportedView = CreateFile(tempDir, Path.Combine("Views", "dbo.CustomerView.sql"), "CREATE VIEW dbo.CustomerView AS SELECT 1;");
            File.WriteAllText(Path.Combine(tempDir, "sqlct.config.json"), "{}");
            File.WriteAllText(Path.Combine(tempDir, "notes.txt"), "ignored");

            var warnings = SyncCommandService.CollectUnsupportedFolderWarnings(tempDir, [supportedTable, supportedView]);

            Assert.Empty(warnings);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Theory]
    [InlineData("CREATE TYPE [dbo].[PhoneNumber] FROM [nvarchar] (20) NOT NULL", true)]
    [InlineData("CREATE TYPE [dbo].[RequestList] AS TABLE ([Id] [int] NOT NULL)", true)]
    [InlineData("CREATE TYPE [dbo].[BrokenType] AS SOMETHING ELSE", false)]
    public void TryClassifyUserDefinedTypeScript_DetectsSupportedShapes(string script, bool expected)
    {
        var success = SyncCommandService.TryClassifyUserDefinedTypeScript(script, out _);

        Assert.Equal(expected, success);
    }

    [Theory]
    [InlineData("CREATE TYPE [dbo].[PhoneNumber] FROM [nvarchar] (20) NOT NULL")]
    [InlineData("CREATE TYPE [dbo].[RequestList] AS TABLE ([Id] [int] NOT NULL)")]
    public void RunStatus_RecognizesMergedUserDefinedTypeFolderEntries(string script)
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var config = SqlctConfigWriter.CreateDefault();
            config.Database.Server = "localhost";
            config.Database.Name = "TestDb";
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
            Assert.True(write.Success);

            CreateFile(projectDir, Path.Combine("Types", "User-defined Data Types", "dbo.PhoneNumber.sql"), script);

            var service = new SyncCommandService(
                new SqlctConfigReader(),
                new TrackingIntrospector { AllObjects = [] },
                new TrackingScripter(),
                new SchemaFolderMapper(SupportedSqlObjectTypes.DefaultFolderMap, dataWriteAllFilesInOneDirectory: true));

            var result = service.RunStatus(projectDir, "folder");

            Assert.True(result.Success, result.Error?.Detail ?? result.Error?.Message);
            Assert.Single(result.Payload!.Objects);
            Assert.Equal("UserDefinedType", result.Payload.Objects[0].Type);
            Assert.Empty(result.Payload.Warnings);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunStatus_SkipsUnrecognizedMergedUserDefinedTypeScripts()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var config = SqlctConfigWriter.CreateDefault();
            config.Database.Server = "localhost";
            config.Database.Name = "TestDb";
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
            Assert.True(write.Success);

            CreateFile(projectDir, Path.Combine("Types", "User-defined Data Types", "dbo.BrokenType.sql"), "CREATE TYPE [dbo].[BrokenType] AS SOMETHING ELSE");

            var service = new SyncCommandService(
                new SqlctConfigReader(),
                new TrackingIntrospector { AllObjects = [] },
                new TrackingScripter(),
                new SchemaFolderMapper(SupportedSqlObjectTypes.DefaultFolderMap, dataWriteAllFilesInOneDirectory: true));

            var result = service.RunStatus(projectDir, "folder");

            Assert.True(result.Success, result.Error?.Detail ?? result.Error?.Message);
            Assert.Empty(result.Payload!.Objects);
            Assert.Contains(result.Payload.Warnings, warning => warning.Code == "invalid_user_defined_type_script");
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void NormalizeForComparison_IgnoresLineEndingsAndTrailingNewlines()
    {
        var left = SyncCommandService.NormalizeForComparison("SELECT 1\r\nGO\r\n");
        var right = SyncCommandService.NormalizeForComparison("SELECT 1\nGO\n\n");

        Assert.Equal(left, right);
    }

    [Fact]
    public void NormalizeForComparison_TreatsWhitespaceOnlyLinesAsBlankLines()
    {
        var blank = SyncCommandService.NormalizeForComparison("SET ANSI_NULLS ON\nGO\n\n/* comment */");
        var whitespaceOnly = SyncCommandService.NormalizeForComparison("SET ANSI_NULLS ON\nGO\n   \t\n/* comment */");

        Assert.Equal(blank, whitespaceOnly);
    }

    [Fact]
    public void NormalizeForComparison_NormalizesTrailingSemicolonsOnInsertStatements()
    {
        // Trailing semicolons on INSERT statements are normalized away so that scripts emitted
        // with and without statement terminators compare as compatible.
        var withSemicolon = SyncCommandService.NormalizeForComparison(
            "INSERT INTO [dbo].[T] ([Id]) VALUES (1);\nINSERT INTO [dbo].[T] ([Id]) VALUES (2);");
        var withoutSemicolon = SyncCommandService.NormalizeForComparison(
            "INSERT INTO [dbo].[T] ([Id]) VALUES (1)\nINSERT INTO [dbo].[T] ([Id]) VALUES (2)");

        Assert.Equal(withSemicolon, withoutSemicolon);
    }

    [Fact]
    public void BuildUnifiedDiff_SuppressesTrailingSemicolonOnlyDifferencesInInsertStatements()
    {
        // A diff that consists solely of missing/added trailing semicolons on INSERT statements
        // must produce an empty result — the difference is normalized away as compatible.
        var source = "INSERT INTO [dbo].[T] ([Id]) VALUES (1);\nINSERT INTO [dbo].[T] ([Id]) VALUES (2);";
        var target = "INSERT INTO [dbo].[T] ([Id]) VALUES (1)\nINSERT INTO [dbo].[T] ([Id]) VALUES (2)";

        var diff = SyncCommandService.BuildUnifiedDiff("db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_SuppressesWhitespaceOnlyBlankLineDifferences()
    {
        var source = "SET ANSI_NULLS ON\nGO\n\n/* comment */\nCREATE PROCEDURE [dbo].[Sample]\nAS\nSELECT 1\nGO";
        var target = "SET ANSI_NULLS ON\nGO\n \t \n/* comment */\nCREATE PROCEDURE [dbo].[Sample]\nAS\nSELECT 1\nGO";

        var diff = SyncCommandService.BuildUnifiedDiff("StoredProcedure", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Queue_SuppressesDefaultPrimaryAndDisabledActivationDifferences()
    {
        var source =
            "CREATE QUEUE [dbo].[AppInboxQueue]\n" +
            "WITH STATUS = ON, RETENTION = OFF, POISON_MESSAGE_HANDLING (STATUS = ON), ACTIVATION (STATUS = OFF, EXECUTE AS 'dbo')\n" +
            "GO";
        var target =
            "CREATE QUEUE [dbo].[AppInboxQueue]\n" +
            "WITH STATUS=ON,\n" +
            "RETENTION=OFF,\n" +
            "POISON_MESSAGE_HANDLING (STATUS=ON)\n" +
            "ON [PRIMARY]\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Queue", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Queue_SuppressesEquivalentMultilineActivationFormatting()
    {
        var source =
            "CREATE QUEUE [dbo].[AppWorkQueue]\n" +
            "WITH STATUS = ON, RETENTION = OFF, POISON_MESSAGE_HANDLING (STATUS = ON), ACTIVATION (STATUS = ON, PROCEDURE_NAME = [dbo].[ProcessAppMessages], MAX_QUEUE_READERS = 1, EXECUTE AS OWNER)\n" +
            "GO";
        var target =
            "CREATE QUEUE [dbo].[AppWorkQueue]\n" +
            "WITH STATUS=ON,\n" +
            "RETENTION=OFF,\n" +
            "POISON_MESSAGE_HANDLING (STATUS=ON),\n" +
            "ACTIVATION (\n" +
            "STATUS=ON,\n" +
            "PROCEDURE_NAME=[dbo].[ProcessAppMessages],\n" +
            "MAX_QUEUE_READERS=1,\n" +
            "EXECUTE AS OWNER\n" +
            ")\n" +
            "ON [PRIMARY]\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Queue", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Role_SuppressesLegacyAndAlterRoleMembershipSyntaxDifferences_ForFixedRole()
    {
        var source =
            "EXEC sp_addrolemember N'db_datareader', N'ReadOnlyUser'\n" +
            "GO\n" +
            "EXEC sp_addrolemember N'db_datareader', N'ReportUser'\n" +
            "GO";
        var target =
            "ALTER ROLE [db_datareader] ADD MEMBER [ReadOnlyUser]\n" +
            "GO\n" +
            "ALTER ROLE [db_datareader] ADD MEMBER [ReportUser]\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Role", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Role_SuppressesLegacyAndAlterRoleMembershipSyntaxDifferences_ForUserDefinedRole()
    {
        var source =
            "CREATE ROLE [ReportingRole]\n" +
            "AUTHORIZATION [dbo]\n" +
            "GO\n" +
            "EXEC sp_addrolemember N'ReportingRole', N'ReportUser'\n" +
            "GO";
        var target =
            "CREATE ROLE [ReportingRole]\n" +
            "AUTHORIZATION [dbo]\n" +
            "GO\n" +
            "ALTER ROLE [ReportingRole] ADD MEMBER [ReportUser]\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Role", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Role_PreservesMembershipTargetDifferences()
    {
        var source =
            "EXEC sp_addrolemember N'db_datareader', N'ReadOnlyUser'\n" +
            "GO";
        var target =
            "ALTER ROLE [db_datareader] ADD MEMBER [ReportUser]\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Role", "db", "folder", source, target);

        Assert.Contains("EXEC sp_addrolemember N'db_datareader', N'ReadOnlyUser'", diff);
        Assert.Contains("ALTER ROLE [db_datareader] ADD MEMBER [ReportUser]", diff);
    }

    [Fact]
    public void BuildUnifiedDiff_UserDefinedType_SuppressesEquivalentPermissionOrderDifferences()
    {
        var source =
            "CREATE TYPE [dbo].[SampleType] AS TABLE\n" +
            "(\n" +
            "    [ItemId] [smallint] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "GRANT EXECUTE ON TYPE:: [dbo].[SampleType] TO [AppRole]\n" +
            "GO\n" +
            "GRANT REFERENCES ON TYPE:: [dbo].[SampleType] TO [AppRole]\n" +
            "GO";
        var target =
            "CREATE TYPE [dbo].[SampleType] AS TABLE\n" +
            "(\n" +
            "    [ItemId] [smallint] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "GRANT REFERENCES ON TYPE:: [dbo].[SampleType] TO [AppRole]\n" +
            "GO\n" +
            "GRANT EXECUTE ON TYPE:: [dbo].[SampleType] TO [AppRole]\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("UserDefinedType", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_UserDefinedType_PreservesPermissionDifferencesWhenOrderAlsoDiffers()
    {
        var source =
            "CREATE TYPE [dbo].[SampleType] AS TABLE\n" +
            "(\n" +
            "    [ItemId] [smallint] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "GRANT EXECUTE ON TYPE:: [dbo].[SampleType] TO [AppRole]\n" +
            "GO\n" +
            "GRANT REFERENCES ON TYPE:: [dbo].[SampleType] TO [AppRole]\n" +
            "GO";
        var target =
            "CREATE TYPE [dbo].[SampleType] AS TABLE\n" +
            "(\n" +
            "    [ItemId] [smallint] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "GRANT REFERENCES ON TYPE:: [dbo].[SampleType] TO [AppRole]\n" +
            "GO\n" +
            "GRANT EXECUTE ON TYPE:: [dbo].[SampleType] TO [OtherRole]\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("UserDefinedType", "db", "folder", source, target);

        Assert.Contains("GRANT EXECUTE ON TYPE:: [dbo].[SampleType] TO [AppRole]", diff);
        Assert.Contains("GRANT EXECUTE ON TYPE:: [dbo].[SampleType] TO [OtherRole]", diff);
    }

    [Fact]
    public void BuildUnifiedDiff_MessageType_SuppressesLegacyValidationXmlSynonymAndSpacing()
    {
        var source =
            "CREATE MESSAGE TYPE [//App/Reply]\n" +
            "AUTHORIZATION [dbo]\n" +
            "VALIDATION = XML\n" +
            "GO";
        var target =
            "CREATE MESSAGE TYPE [//App/Reply]\n" +
            "AUTHORIZATION [dbo]\n" +
            "VALIDATION=WELL_FORMED_XML\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("MessageType", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Contract_SuppressesEquivalentFormattingAndMessageUsageOrderDifferences()
    {
        var source =
            "CREATE CONTRACT [//App/Contract]\n" +
            "AUTHORIZATION [dbo]\n" +
            "(\n" +
            "[//App/Reply] SENT BY TARGET,\n" +
            "[//App/Request] SENT BY INITIATOR\n" +
            ")\n" +
            "GO";
        var target =
            "CREATE CONTRACT [//App/Contract]\n" +
            "AUTHORIZATION [dbo] (\n" +
            "[//App/Request] SENT BY INITIATOR,\n" +
            "[//App/Reply] SENT BY TARGET\n" +
            ")\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Contract", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Contract_PreservesSentBySemanticDifferences()
    {
        var source =
            "CREATE CONTRACT [//App/Contract]\n" +
            "AUTHORIZATION [dbo]\n" +
            "(\n" +
            "[//App/Reply] SENT BY TARGET,\n" +
            "[//App/Request] SENT BY INITIATOR\n" +
            ")\n" +
            "GO";
        var target =
            "CREATE CONTRACT [//App/Contract]\n" +
            "AUTHORIZATION [dbo] (\n" +
            "[//App/Request] SENT BY ANY,\n" +
            "[//App/Reply] SENT BY TARGET\n" +
            ")\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Contract", "db", "folder", source, target);

        Assert.Contains("[//App/Request] SENT BY INITIATOR", diff);
        Assert.Contains("[//App/Request] SENT BY ANY", diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Service_SuppressesEquivalentContractListFormatting()
    {
        var source =
            "CREATE SERVICE [AppTargetService]\n" +
            "AUTHORIZATION [dbo]\n" +
            "ON QUEUE [dbo].[AppTargetQueue] ([//App/Contract])\n" +
            "GO";
        var target =
            "CREATE SERVICE [AppTargetService]\n" +
            "AUTHORIZATION [dbo]\n" +
            "ON QUEUE [dbo].[AppTargetQueue]\n" +
            "(\n" +
            "[//App/Contract]\n" +
            ")\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Service", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Service_PreservesContractMembershipDifferences()
    {
        var source =
            "CREATE SERVICE [AppTargetService]\n" +
            "AUTHORIZATION [dbo]\n" +
            "ON QUEUE [dbo].[AppTargetQueue] ([//App/Contract])\n" +
            "GO";
        var target =
            "CREATE SERVICE [AppTargetService]\n" +
            "AUTHORIZATION [dbo]\n" +
            "ON QUEUE [dbo].[AppTargetQueue]\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Service", "db", "folder", source, target);

        Assert.Contains("ON QUEUE [dbo].[AppTargetQueue] ([//App/Contract])", diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Function_SuppressesClrTableValuedFunctionNullOnlyDifferences()
    {
        var source =
            "SET QUOTED_IDENTIFIER OFF\n" +
            "GO\n" +
            "SET ANSI_NULLS OFF\n" +
            "GO\n" +
            "CREATE FUNCTION [dbo].[SplitValues] (@input [nvarchar] (MAX))\n" +
            "RETURNS TABLE (\n" +
            "[Ordinal] [int],\n" +
            "[Value] [nvarchar] (MAX)\n" +
            ")\n" +
            "WITH EXECUTE AS CALLER\n" +
            "EXTERNAL NAME [AppClr].[App.Database.TabularFunctions].[SplitValues]\n" +
            "GO";
        var target =
            "SET QUOTED_IDENTIFIER OFF\n" +
            "GO\n" +
            "SET ANSI_NULLS OFF\n" +
            "GO\n" +
            "CREATE FUNCTION [dbo].[SplitValues] (@input [nvarchar] (MAX))\n" +
            "RETURNS TABLE (\n" +
            "[Ordinal] [int] NULL,\n" +
            "[Value] [nvarchar] (MAX) NULL\n" +
            ")\n" +
            "WITH EXECUTE AS CALLER\n" +
            "EXTERNAL NAME [AppClr].[App.Database.TabularFunctions].[SplitValues]\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Function", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Function_SuppressesClrTableValuedFunctionNullAndCloseParenLineDifferences()
    {
        var source =
            "SET QUOTED_IDENTIFIER OFF\n" +
            "GO\n" +
            "SET ANSI_NULLS OFF\n" +
            "GO\n" +
            "CREATE FUNCTION [dbo].[SplitValues] (@input [nvarchar] (MAX))\n" +
            "RETURNS TABLE (\n" +
            "[Ordinal] [int],\n" +
            "[Value] [nvarchar] (MAX)\n" +
            ")\n" +
            "WITH EXECUTE AS CALLER\n" +
            "EXTERNAL NAME [AppClr].[App.Database.TabularFunctions].[SplitValues]\n" +
            "GO";
        var target =
            "SET QUOTED_IDENTIFIER OFF\n" +
            "GO\n" +
            "SET ANSI_NULLS OFF\n" +
            "GO\n" +
            "CREATE FUNCTION [dbo].[SplitValues] (@input [nvarchar] (MAX))\n" +
            "RETURNS TABLE (\n" +
            "[Ordinal] [int] NULL,\n" +
            "[Value] [nvarchar] (MAX) NULL)\n" +
            "WITH EXECUTE AS CALLER\n" +
            "EXTERNAL NAME [AppClr].[App.Database.TabularFunctions].[SplitValues]\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Function", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Function_ReportsOnlyClrTableValuedFunctionExternalNameDifference_WhenLegacyNullCloseParenAlsoDiffers()
    {
        var source =
            "SET QUOTED_IDENTIFIER OFF\n" +
            "GO\n" +
            "SET ANSI_NULLS OFF\n" +
            "GO\n" +
            "CREATE FUNCTION [dbo].[RandomVector] (@length [int])\n" +
            "RETURNS TABLE (\n" +
            "[RndValue] [int]\n" +
            ")\n" +
            "WITH EXECUTE AS CALLER\n" +
            "EXTERNAL NAME [AppClr].[App.Database.TabularFunctions].[RandomVector]\n" +
            "GO";
        var target =
            "SET QUOTED_IDENTIFIER OFF\n" +
            "GO\n" +
            "SET ANSI_NULLS OFF\n" +
            "GO\n" +
            "CREATE FUNCTION [dbo].[RandomVector] (@length [int])\n" +
            "RETURNS TABLE (\n" +
            "[RndValue] [int] NULL)\n" +
            "WITH EXECUTE AS CALLER\n" +
            "EXTERNAL NAME [AppClrLegacy].[App.Database.TabularFunctions].[RandomVector]\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Function", "db", "folder", source, target);

        Assert.Contains("EXTERNAL NAME [AppClr].[App.Database.TabularFunctions].[RandomVector]", diff);
        Assert.Contains("EXTERNAL NAME [AppClrLegacy].[App.Database.TabularFunctions].[RandomVector]", diff);
        Assert.DoesNotContain("[RndValue] [int] NULL)", diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Function_SuppressesClrTableValuedFunctionCollationAndNullDifferences()
    {
        var source =
            "SET QUOTED_IDENTIFIER OFF\n" +
            "GO\n" +
            "SET ANSI_NULLS OFF\n" +
            "GO\n" +
            "CREATE FUNCTION [dbo].[DecodeToken] (@input [nvarchar] (MAX))\n" +
            "RETURNS TABLE (\n" +
            "[TextCode] [nvarchar] (100),\n" +
            "[SequenceId] [int]\n" +
            ")\n" +
            "WITH EXECUTE AS CALLER\n" +
            "EXTERNAL NAME [AppClr].[App.Database.TabularFunctions].[DecodeToken]\n" +
            "GO";
        var target =
            "SET QUOTED_IDENTIFIER OFF\n" +
            "GO\n" +
            "SET ANSI_NULLS OFF\n" +
            "GO\n" +
            "CREATE FUNCTION [dbo].[DecodeToken] (@input [nvarchar] (MAX))\n" +
            "RETURNS TABLE (\n" +
            "[TextCode] [nvarchar] (100) COLLATE Polish_CI_AS NULL,\n" +
            "[SequenceId] [int]\n" +
            ")\n" +
            "WITH EXECUTE AS CALLER\n" +
            "EXTERNAL NAME [AppClr].[App.Database.TabularFunctions].[DecodeToken]\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Function", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void NormalizeForComparison_TableData_NormalizesLegacyIdentityInsertAndUnicodeLiteralPrefixes()
    {
        var canonical = SyncCommandService.NormalizeForComparison(
            "SET IDENTITY_INSERT [dbo].[Customer] ON;\n" +
            "INSERT INTO [dbo].[Customer] ([CustomerID], [Code], [Description]) VALUES (1, 'A', 'Alpha');\n" +
            "SET IDENTITY_INSERT [dbo].[Customer] OFF;",
            SyncCommandService.TableDataObjectType);
        var legacy = SyncCommandService.NormalizeForComparison(
            "SET IDENTITY_INSERT [dbo].[Customer] ON\n" +
            "INSERT INTO [dbo].[Customer] ([CustomerID], [Code], [Description]) VALUES (1, N'A', N'Alpha')\n" +
            "SET IDENTITY_INSERT [dbo].[Customer] OFF",
            SyncCommandService.TableDataObjectType);

        Assert.Equal(canonical, legacy);
    }

    [Fact]
    public void BuildUnifiedDiff_TableData_SuppressesLegacyIdentityInsertAndUnicodeLiteralPrefixDifferences()
    {
        var source =
            "SET IDENTITY_INSERT [dbo].[Customer] ON;\n" +
            "INSERT INTO [dbo].[Customer] ([CustomerID], [Code], [Description]) VALUES (1, 'A', 'Alpha');\n" +
            "SET IDENTITY_INSERT [dbo].[Customer] OFF;";
        var target =
            "SET IDENTITY_INSERT [dbo].[Customer] ON\n" +
            "INSERT INTO [dbo].[Customer] ([CustomerID], [Code], [Description]) VALUES (1, N'A', N'Alpha')\n" +
            "SET IDENTITY_INSERT [dbo].[Customer] OFF";

        var diff = SyncCommandService.BuildUnifiedDiff(
            SyncCommandService.TableDataObjectType,
            "db",
            "folder",
            source,
            target);

        Assert.Empty(diff);
    }

    [Fact]
    public void NormalizeForComparison_TableData_NormalizesLegacyPrefixesInMultilineInsertValues()
    {
        var canonical = SyncCommandService.NormalizeForComparison(
            "INSERT INTO [dbo].[Variable] ([Description], [GroupCode], [Name], [Flag]) VALUES ('Line 1\n" +
            "Line 2\n" +
            "0 - empty', 'ScoreApl', 'EmploymentBasis', 0);",
            SyncCommandService.TableDataObjectType);
        var legacy = SyncCommandService.NormalizeForComparison(
            "INSERT INTO [dbo].[Variable] ([Description], [GroupCode], [Name], [Flag]) VALUES ('Line 1\n" +
            "Line 2\n" +
            "0 - empty', N'ScoreApl', N'EmploymentBasis', 0)",
            SyncCommandService.TableDataObjectType);

        Assert.Equal(canonical, legacy);
    }

    [Fact]
    public void BuildUnifiedDiff_TableData_SuppressesLegacyPrefixesInMultilineInsertValues()
    {
        var source =
            "INSERT INTO [dbo].[Variable] ([Description], [GroupCode], [Name], [Flag]) VALUES ('Line 1\n" +
            "Line 2\n" +
            "0 - empty', 'ScoreApl', 'EmploymentBasis', 0);";
        var target =
            "INSERT INTO [dbo].[Variable] ([Description], [GroupCode], [Name], [Flag]) VALUES ('Line 1\n" +
            "Line 2\n" +
            "0 - empty', N'ScoreApl', N'EmploymentBasis', 0)";

        var diff = SyncCommandService.BuildUnifiedDiff(
            SyncCommandService.TableDataObjectType,
            "db",
            "folder",
            source,
            target);

        Assert.Empty(diff);
    }

    [Fact]
    public void NormalizeForComparison_TableData_SortsEquivalentInsertStatementsWithinRun()
    {
        var canonical = SyncCommandService.NormalizeForComparison(
            "SET IDENTITY_INSERT [dbo].[LookupValue] ON;\n" +
            "INSERT INTO [dbo].[LookupValue] ([LookupValueID], [LookupCode]) VALUES (1, 'A');\n" +
            "INSERT INTO [dbo].[LookupValue] ([LookupValueID], [LookupCode]) VALUES (2, 'B');\n" +
            "INSERT INTO [dbo].[LookupValue] ([LookupValueID], [LookupCode]) VALUES (3, 'C');\n" +
            "SET IDENTITY_INSERT [dbo].[LookupValue] OFF;",
            SyncCommandService.TableDataObjectType);
        var reordered = SyncCommandService.NormalizeForComparison(
            "SET IDENTITY_INSERT [dbo].[LookupValue] ON\n" +
            "INSERT INTO [dbo].[LookupValue] ([LookupValueID], [LookupCode]) VALUES (3, N'C')\n" +
            "INSERT INTO [dbo].[LookupValue] ([LookupValueID], [LookupCode]) VALUES (1, N'A')\n" +
            "INSERT INTO [dbo].[LookupValue] ([LookupValueID], [LookupCode]) VALUES (2, N'B')\n" +
            "SET IDENTITY_INSERT [dbo].[LookupValue] OFF",
            SyncCommandService.TableDataObjectType);

        Assert.Equal(canonical, reordered);
    }

    [Fact]
    public void BuildUnifiedDiff_TableData_SuppressesEquivalentInsertOrderDifferences()
    {
        var source =
            "SET IDENTITY_INSERT [dbo].[LookupValue] ON;\n" +
            "INSERT INTO [dbo].[LookupValue] ([LookupValueID], [LookupCode]) VALUES (1, 'A');\n" +
            "INSERT INTO [dbo].[LookupValue] ([LookupValueID], [LookupCode]) VALUES (2, 'B');\n" +
            "INSERT INTO [dbo].[LookupValue] ([LookupValueID], [LookupCode]) VALUES (3, 'C');\n" +
            "SET IDENTITY_INSERT [dbo].[LookupValue] OFF;";
        var target =
            "SET IDENTITY_INSERT [dbo].[LookupValue] ON\n" +
            "INSERT INTO [dbo].[LookupValue] ([LookupValueID], [LookupCode]) VALUES (3, 'C')\n" +
            "INSERT INTO [dbo].[LookupValue] ([LookupValueID], [LookupCode]) VALUES (1, 'A')\n" +
            "INSERT INTO [dbo].[LookupValue] ([LookupValueID], [LookupCode]) VALUES (2, 'B')\n" +
            "SET IDENTITY_INSERT [dbo].[LookupValue] OFF";

        var diff = SyncCommandService.BuildUnifiedDiff(
            SyncCommandService.TableDataObjectType,
            "db",
            "folder",
            source,
            target);

        Assert.Empty(diff);
    }

    [Fact]
    public void NormalizeForComparison_TableData_NormalizesEquivalentInsertColumnOrderDifferences()
    {
        var canonical = SyncCommandService.NormalizeForComparison(
            "INSERT INTO [dbo].[SampleConfig] ([ConfigCode], [TaskCode], [DisplayName], [IsEnabled]) VALUES ('A01', 'TaskAlpha', 'Alpha', 1);",
            SyncCommandService.TableDataObjectType);
        var reordered = SyncCommandService.NormalizeForComparison(
            "INSERT INTO [dbo].[SampleConfig] ([ConfigCode], [DisplayName], [IsEnabled], [TaskCode]) VALUES ('A01', 'Alpha', 1, 'TaskAlpha')",
            SyncCommandService.TableDataObjectType);

        Assert.Equal(canonical, reordered);
    }

    [Fact]
    public void BuildUnifiedDiff_TableData_SuppressesEquivalentInsertColumnOrderDifferences()
    {
        var source =
            "INSERT INTO [dbo].[SampleConfig] ([ConfigCode], [TaskCode], [DisplayName], [IsEnabled]) VALUES ('A01', 'TaskAlpha', 'Alpha', 1);";
        var target =
            "INSERT INTO [dbo].[SampleConfig] ([ConfigCode], [DisplayName], [IsEnabled], [TaskCode]) VALUES ('A01', 'Alpha', 1, 'TaskAlpha')";

        var diff = SyncCommandService.BuildUnifiedDiff(
            SyncCommandService.TableDataObjectType,
            "db",
            "folder",
            source,
            target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_TableData_PreservesValueDifferencesWhenInsertOrderAlsoDiffers()
    {
        var source =
            "INSERT INTO [dbo].[LookupValue] ([LookupValueID], [LookupCode]) VALUES (1, 'A');\n" +
            "INSERT INTO [dbo].[LookupValue] ([LookupValueID], [LookupCode]) VALUES (2, 'B');";
        var target =
            "INSERT INTO [dbo].[LookupValue] ([LookupValueID], [LookupCode]) VALUES (2, 'B')\n" +
            "INSERT INTO [dbo].[LookupValue] ([LookupValueID], [LookupCode]) VALUES (1, 'Z')";

        var diff = SyncCommandService.BuildUnifiedDiff(
            SyncCommandService.TableDataObjectType,
            "db",
            "folder",
            source,
            target);

        Assert.Contains("VALUES (1, 'A')", diff);
        Assert.Contains("VALUES (1, 'Z')", diff);
    }

    [Fact]
    public void BuildUnifiedDiff_TableData_PreservesValueDifferencesWhenInsertColumnOrderAlsoDiffers()
    {
        var source =
            "INSERT INTO [dbo].[SampleConfig] ([ConfigCode], [TaskCode], [DisplayName], [IsEnabled]) VALUES ('A01', 'TaskAlpha', 'Alpha', 1);";
        var target =
            "INSERT INTO [dbo].[SampleConfig] ([ConfigCode], [DisplayName], [IsEnabled], [TaskCode]) VALUES ('A01', 'Alpha', 1, 'TaskBeta')";

        var diff = SyncCommandService.BuildUnifiedDiff(
            SyncCommandService.TableDataObjectType,
            "db",
            "folder",
            source,
            target);

        Assert.Contains("'TaskAlpha'", diff);
        Assert.Contains("'TaskBeta'", diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Table_SuppressesEquivalentExtendedPropertyOrderAndSpacingDifferences()
    {
        var source =
            "CREATE TABLE [dbo].[SessionLog]\n" +
            "(\n" +
            "[SessionLogID] [int] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "EXEC sp_addextendedproperty N'MS_Description', N'System details', 'SCHEMA', N'dbo', 'TABLE', N'SessionLog', 'COLUMN', N'SystemInfo'\n" +
            "GO\n" +
            "EXEC sp_addextendedproperty N'MS_Description', N'Client software details', 'SCHEMA', N'dbo', 'TABLE', N'SessionLog', 'COLUMN', N'UserAgentInfo'\n" +
            "GO\n" +
            "ALTER TABLE [dbo].[SessionLog] SET (LOCK_ESCALATION = AUTO)\n" +
            "GO";
        var target =
            "CREATE TABLE [dbo].[SessionLog]\n" +
            "(\n" +
            "[SessionLogID] [int] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "EXEC sp_addextendedproperty N'MS_Description', N'Client software details' , 'SCHEMA', N'dbo', 'TABLE', N'SessionLog','COLUMN', N'UserAgentInfo'\n" +
            "GO\n" +
            "EXEC sp_addextendedproperty N'MS_Description', N'System details', 'SCHEMA', N'dbo', 'TABLE', N'SessionLog', 'COLUMN', N'SystemInfo'\n" +
            "GO\n" +
            "ALTER TABLE [dbo].[SessionLog] SET (LOCK_ESCALATION = AUTO)\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Table", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Table_SuppressesEquivalentExtendedPropertyOrderAndUnicodePrefixDifferences()
    {
        var source =
            "CREATE TABLE [dbo].[SessionLog]\n" +
            "(\n" +
            "[SessionLogID] [int] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "EXEC sp_addextendedproperty 'MS_Description', N'Primary detail', 'SCHEMA', 'dbo', 'TABLE', 'SessionLog', 'COLUMN', 'DetailAlpha'\n" +
            "GO\n" +
            "EXEC sp_addextendedproperty 'MS_Description', N'Secondary detail', 'SCHEMA', 'dbo', 'TABLE', 'SessionLog', 'COLUMN', 'DetailBeta'\n" +
            "GO";
        var target =
            "CREATE TABLE [dbo].[SessionLog]\n" +
            "(\n" +
            "[SessionLogID] [int] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "EXEC sp_addextendedproperty 'MS_Description', 'Secondary detail', 'SCHEMA', 'dbo', 'TABLE', 'SessionLog', 'COLUMN', 'DetailBeta'\n" +
            "GO\n" +
            "EXEC sp_addextendedproperty 'MS_Description', 'Primary detail', 'SCHEMA', 'dbo', 'TABLE', 'SessionLog', 'COLUMN', 'DetailAlpha'\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Table", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Table_PreservesExtendedPropertyValueDifferencesWhenOrderAlsoDiffers()
    {
        var source =
            "CREATE TABLE [dbo].[SessionLog]\n" +
            "(\n" +
            "[SessionLogID] [int] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "EXEC sp_addextendedproperty N'MS_Description', N'System details', 'SCHEMA', N'dbo', 'TABLE', N'SessionLog', 'COLUMN', N'SystemInfo'\n" +
            "GO\n" +
            "EXEC sp_addextendedproperty N'MS_Description', N'Client software details', 'SCHEMA', N'dbo', 'TABLE', N'SessionLog', 'COLUMN', N'UserAgentInfo'\n" +
            "GO";
        var target =
            "CREATE TABLE [dbo].[SessionLog]\n" +
            "(\n" +
            "[SessionLogID] [int] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "EXEC sp_addextendedproperty N'MS_Description', N'Client software details', 'SCHEMA', N'dbo', 'TABLE', N'SessionLog', 'COLUMN', N'UserAgentInfo'\n" +
            "GO\n" +
            "EXEC sp_addextendedproperty N'MS_Description', N'Updated system details', 'SCHEMA', N'dbo', 'TABLE', N'SessionLog', 'COLUMN', N'SystemInfo'\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Table", "db", "folder", source, target);

        Assert.Contains("'System details'", diff);
        Assert.Contains("'Updated system details'", diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Table_SuppressesRedundantEmptyGoBatchDifferences()
    {
        var source =
            "CREATE TABLE [Accounting].[ExchangeRate]\n" +
            "(\n" +
            "[RateId] [int] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "EXEC sp_addextendedproperty 'MS_Description', N'Row versioning column', 'SCHEMA', 'Accounting', 'TABLE', 'ExchangeRate', 'COLUMN', 'RateId'\n" +
            "GO\n" +
            "SET ANSI_NULLS ON\n" +
            "GO\n" +
            "SET ANSI_PADDING ON\n" +
            "GO";
        var target =
            "CREATE TABLE [Accounting].[ExchangeRate]\n" +
            "(\n" +
            "[RateId] [int] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "EXEC sp_addextendedproperty 'MS_Description', N'Row versioning column', 'SCHEMA', 'Accounting', 'TABLE', 'ExchangeRate', 'COLUMN', 'RateId'\n" +
            "GO\n" +
            "GO\n" +
            "SET ANSI_NULLS ON\n" +
            "GO\n" +
            "SET ANSI_PADDING ON\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Table", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Table_SuppressesRedundantSemicolonOnlyGoBatchDifferences()
    {
        var source =
            "CREATE TABLE [Accounting].[ExchangeRate]\n" +
            "(\n" +
            "[RateId] [int] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "ALTER TABLE [Accounting].[ExchangeRate] SET (LOCK_ESCALATION = AUTO)\n" +
            "GO";
        var target =
            "CREATE TABLE [Accounting].[ExchangeRate]\n" +
            "(\n" +
            "[RateId] [int] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            ";\n" +
            "GO\n" +
            "ALTER TABLE [Accounting].[ExchangeRate] SET (LOCK_ESCALATION = AUTO)\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Table", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Table_SuppressesPureBlankLineDifferences()
    {
        var source =
            "CREATE TABLE [Accounting].[RateCache]\n" +
            "(\n" +
            "[RateId] [int] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "EXEC sp_addextendedproperty 'MS_Description', N'Rate identifier', 'SCHEMA', 'Accounting', 'TABLE', 'RateCache', 'COLUMN', 'RateId'\n" +
            "GO";
        var target =
            "CREATE TABLE [Accounting].[RateCache]\n" +
            "(\n" +
            "[RateId] [int] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "\n" +
            "EXEC sp_addextendedproperty 'MS_Description', N'Rate identifier', 'SCHEMA', 'Accounting', 'TABLE', 'RateCache', 'COLUMN', 'RateId'\n" +
            "GO\n" +
            "\n";

        var diff = SyncCommandService.BuildUnifiedDiff("Table", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_View_SuppressesEquivalentExtendedPropertyNamedArgumentDifferences()
    {
        var source =
            "CREATE VIEW [Reporting].[ExchangeRateView]\n" +
            "AS\n" +
            "SELECT 1 AS [Rate]\n" +
            "GO\n" +
            "EXEC sp_addextendedproperty N'MS_Description', N'Lightweight exchange-rate view', 'SCHEMA', N'Reporting', 'VIEW', N'ExchangeRateView', NULL, NULL\n" +
            "GO";
        var target =
            "CREATE VIEW [Reporting].[ExchangeRateView]\n" +
            "AS\n" +
            "SELECT 1 AS [Rate]\n" +
            "GO\n" +
            "EXEC sp_addextendedproperty @name=N'MS_Description', @value=N'Lightweight exchange-rate view', @level0type=N'SCHEMA', @level0name=N'Reporting', @level1type=N'VIEW', @level1name=N'ExchangeRateView'\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("View", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Table_SuppressesEquivalentPostCreatePackageOrderDifferences()
    {
        var source =
            "CREATE TABLE [dbo].[ExternalDef]\n" +
            "(\n" +
            "[ExternalId] [int] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "ALTER TABLE [dbo].[ExternalDef] ADD CONSTRAINT [PK_ExternalDef] PRIMARY KEY CLUSTERED ([ExternalId]) ON [PRIMARY]\n" +
            "GO\n" +
            "SET ANSI_NULLS ON\n" +
            "GO\n" +
            "SET QUOTED_IDENTIFIER ON\n" +
            "GO\n" +
            "CREATE TRIGGER [dbo].[TR_ExternalDef_Audit] ON [dbo].[ExternalDef] AFTER INSERT AS\n" +
            "BEGIN\n" +
            "SELECT 1\n" +
            "END\n" +
            "GO\n" +
            "CREATE UNIQUE NONCLUSTERED INDEX [IX_ExternalDef_Key] ON [dbo].[ExternalDef] ([ExternalId]) ON [PRIMARY]\n" +
            "GO";
        var target =
            "CREATE TABLE [dbo].[ExternalDef]\n" +
            "(\n" +
            "[ExternalId] [int] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "SET ANSI_NULLS ON\n" +
            "GO\n" +
            "SET QUOTED_IDENTIFIER ON\n" +
            "GO\n" +
            "CREATE TRIGGER [dbo].[TR_ExternalDef_Audit] ON [dbo].[ExternalDef] AFTER INSERT AS\n" +
            "BEGIN\n" +
            "SELECT 1\n" +
            "END\n" +
            "GO\n" +
            "ALTER TABLE [dbo].[ExternalDef] ADD CONSTRAINT [PK_ExternalDef] PRIMARY KEY CLUSTERED ([ExternalId]) ON [PRIMARY]\n" +
            "GO\n" +
            "CREATE UNIQUE NONCLUSTERED INDEX [IX_ExternalDef_Key] ON [dbo].[ExternalDef] ([ExternalId]) ON [PRIMARY]\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Table", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Table_SuppressesEquivalentLegacyTableFormattingDifferences()
    {
        var source =
            "CREATE TABLE [stg].[SampleImport]\n" +
            "(\n" +
            "[RowId] [int] NOT NULL,\n" +
            "[LoadFlag] [bit] NOT NULL CONSTRAINT [DF_SampleImport_LoadFlag] DEFAULT ((1)),\n" +
            "[BatchCode] [varchar] (100) NULL\n" +
            ") ON [PRIMARY]\n" +
            "GO\n" +
            "ALTER TABLE [stg].[SampleImport] ADD CONSTRAINT [PK_SampleImport] PRIMARY KEY CLUSTERED ([RowId]) WITH (FILLFACTOR = 90) ON [PRIMARY]\n" +
            "GO\n" +
            "ALTER TABLE [stg].[SampleImport] SET ( LOCK_ESCALATION = AUTO )\n" +
            "GO";
        var target =
            "CREATE TABLE stg.SampleImport(\n" +
            "       RowId INT NOT NULL,\n" +
            "       LoadFlag BIT NOT NULL CONSTRAINT DF_SampleImport_LoadFlag DEFAULT(1),\n" +
            "       BatchCode VARCHAR(100) NULL\n" +
            ") ON PRIMARY;\n" +
            "GO\n" +
            "ALTER TABLE stg.SampleImport ADD CONSTRAINT PK_SampleImport PRIMARY KEY CLUSTERED (RowId) WITH (FILLFACTOR=90) ON PRIMARY;\n" +
            "GO\n" +
            "\n" +
            "ALTER TABLE stg.SampleImport SET ( LOCK_ESCALATION = AUTO )\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Table", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_UserDefinedType_SuppressesEquivalentLegacyTableValuedTypeFormattingDifferences()
    {
        var source =
            "CREATE TYPE [Accounting].[RateWindow] AS TABLE\n" +
            "(\n" +
            "[EffectiveDate] [date] NOT NULL,\n" +
            "[RateValue] [decimal] (15, 8) NOT NULL,\n" +
            "[YearFraction] [decimal] (15, 12) NOT NULL\n" +
            ")\n" +
            "GO";
        var target =
            "CREATE TYPE Accounting.RateWindow AS TABLE\n" +
            "(\n" +
            "       [EffectiveDate] DATE NOT NULL,\n" +
            "       RateValue DECIMAL(15,8) NOT NULL,\n" +
            "       YearFraction Decimal(15,12) NOT NULL\n" +
            ");\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("UserDefinedType", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Table_SuppressesEquivalentInlineKeyConstraintAndPostCreateKeyConstraintDifferences()
    {
        var source =
            "CREATE TABLE [dbo].[MigrationLog]\n" +
            "(\n" +
            "    [MigrationName] [varchar] (255) NOT NULL,\n" +
            "    [ExecutedAt] [datetime] NOT NULL\n" +
            ") ON [PRIMARY]\n" +
            "GO\n" +
            "ALTER TABLE [dbo].[MigrationLog] ADD CONSTRAINT [PK_MigrationLog] PRIMARY KEY CLUSTERED ([MigrationName]) ON [PRIMARY]\n" +
            "GO";
        var target =
            "CREATE TABLE dbo.MigrationLog(\n" +
            "    MigrationName VARCHAR(255) NOT NULL,\n" +
            "    ExecutedAt DATETIME NOT NULL,\n" +
            "    CONSTRAINT [PK_MigrationLog] PRIMARY KEY CLUSTERED ( MigrationName ) ON PRIMARY\n" +
            ") ON PRIMARY\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Table", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Table_PreservesInlineKeyConstraintSemanticDifferences()
    {
        var source =
            "CREATE TABLE [dbo].[MigrationLog]\n" +
            "(\n" +
            "    [MigrationName] [varchar] (255) NOT NULL,\n" +
            "    [ExecutedAt] [datetime] NOT NULL\n" +
            ") ON [PRIMARY]\n" +
            "GO\n" +
            "ALTER TABLE [dbo].[MigrationLog] ADD CONSTRAINT [PK_MigrationLog] PRIMARY KEY CLUSTERED ([MigrationName]) ON [PRIMARY]\n" +
            "GO";
        var target =
            "CREATE TABLE dbo.MigrationLog(\n" +
            "    MigrationName VARCHAR(255) NOT NULL,\n" +
            "    ExecutedAt DATETIME NOT NULL,\n" +
            "    CONSTRAINT [PK_MigrationLog] PRIMARY KEY CLUSTERED ( ExecutedAt ) ON PRIMARY\n" +
            ") ON PRIMARY\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Table", "db", "folder", source, target);

        Assert.Contains("PRIMARY KEY CLUSTERED ([MigrationName])", diff);
        Assert.Contains("PRIMARY KEY CLUSTERED ( ExecutedAt )", diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Table_PreservesPostCreatePackageContentDifferencesWhenOrderAlsoDiffers()
    {
        var source =
            "CREATE TABLE [dbo].[ExternalDef]\n" +
            "(\n" +
            "[ExternalId] [int] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "ALTER TABLE [dbo].[ExternalDef] ADD CONSTRAINT [PK_ExternalDef] PRIMARY KEY CLUSTERED ([ExternalId]) ON [PRIMARY]\n" +
            "GO\n" +
            "SET ANSI_NULLS ON\n" +
            "GO\n" +
            "SET QUOTED_IDENTIFIER ON\n" +
            "GO\n" +
            "CREATE TRIGGER [dbo].[TR_ExternalDef_Audit] ON [dbo].[ExternalDef] AFTER INSERT AS\n" +
            "BEGIN\n" +
            "SELECT 1\n" +
            "END\n" +
            "GO";
        var target =
            "CREATE TABLE [dbo].[ExternalDef]\n" +
            "(\n" +
            "[ExternalId] [int] NOT NULL\n" +
            ")\n" +
            "GO\n" +
            "SET ANSI_NULLS ON\n" +
            "GO\n" +
            "SET QUOTED_IDENTIFIER ON\n" +
            "GO\n" +
            "CREATE TRIGGER [dbo].[TR_ExternalDef_Audit] ON [dbo].[ExternalDef] AFTER INSERT AS\n" +
            "BEGIN\n" +
            "SELECT 1\n" +
            "END\n" +
            "GO\n" +
            "ALTER TABLE [dbo].[ExternalDef] ADD CONSTRAINT [PK_ExternalDef] PRIMARY KEY CLUSTERED ([ExternalId]) WITH (DATA_COMPRESSION = PAGE) ON [PRIMARY]\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Table", "db", "folder", source, target);

        Assert.NotEmpty(diff);
        Assert.Contains("data_compression", diff, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void BuildUnifiedDiff_Table_PreservesReadableStatementText_WhenLegacyFormattingAlsoNormalizes()
    {
        var source =
            "CREATE TABLE [lab].[SampleMeasure]\n" +
            "(\n" +
            "[BatchId] [int] NOT NULL,\n" +
            "[MeasureValue] [decimal] (15, 8) NULL\n" +
            ") ON [PRIMARY]\n" +
            "GO";
        var target =
            "create table lab.samplemeasure(batchid int not null,measurevalue decimal(15,9) null,) on primary;\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Table", "db", "folder", source, target);

        Assert.Contains(" CREATE TABLE [lab].[SampleMeasure]", diff);
        Assert.Contains("     [BatchId] [int] NOT NULL,", diff);
        Assert.Contains("-    [MeasureValue] [decimal] (15, 8) NULL", diff);
        Assert.Contains("+    measurevalue decimal(15,9) null", diff, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("create table lab.samplemeasure(batchid int not null,measurevalue decimal(15,9) null) on primary", diff, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildUnifiedDiff_Table_PinpointsChangedBodyEntryAndCloseLine_WhenLegacyFormattingAlsoDiffers()
    {
        var source =
            "CREATE TABLE [lab].[SampleAmount]\n" +
            "(\n" +
            "[BatchId] [int] NOT NULL,\n" +
            "[ItemId] [int] NOT NULL,\n" +
            "[SourceAmount] [decimal] (15, 2) NOT NULL,\n" +
            "[TargetAmount] [decimal] (15, 3) NOT NULL\n" +
            ") ON [DATAFG]\n" +
            "GO\n" +
            "ALTER TABLE [lab].[SampleAmount] ADD CONSTRAINT [PK_SampleAmount] PRIMARY KEY CLUSTERED ([BatchId], [ItemId]) WITH (FILLFACTOR = 90) ON [DATAFG]\n" +
            "GO";
        var target =
            "create table lab.sampleamount(batchid int not null,itemid int not null,sourceamount decimal(15,2) not null,targetamount decimal(15,2) not null) on datafg with(data_compression=page)\n" +
            "GO\n" +
            "alter table lab.sampleamount add constraint pk_sampleamount primary key clustered(batchid,itemid) with(fillfactor=90,data_compression=page) on datafg\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Table", "db", "folder", source, target);

        Assert.Contains("     [BatchId] [int] NOT NULL,", diff);
        Assert.DoesNotContain("-    [BatchId] [int] NOT NULL,", diff);
        Assert.DoesNotContain("+    batchid int not null,", diff, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("-    [TargetAmount] [decimal] (15, 3) NOT NULL", diff);
        Assert.Contains("+    targetamount decimal(15,2) not null", diff, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("-) ON [DATAFG]", diff);
        Assert.Contains("+) ON datafg with(data_compression=page)", diff, StringComparison.OrdinalIgnoreCase);
        Assert.Contains("-ALTER TABLE [lab].[SampleAmount] ADD CONSTRAINT [PK_SampleAmount] PRIMARY KEY CLUSTERED ([BatchId], [ItemId]) WITH (FILLFACTOR = 90) ON [DATAFG]", diff);
        Assert.Contains("+alter table lab.sampleamount add constraint pk_sampleamount primary key clustered(batchid,itemid) with(fillfactor=90,data_compression=page) on datafg", diff, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void BuildUnifiedDiff_Table_CanRenderNormalizedDiffForDebuggingWhenRequested()
    {
        var source =
            "CREATE TABLE [lab].[SampleMeasure]\n" +
            "(\n" +
            "[BatchId] [int] NOT NULL,\n" +
            "[MeasureValue] [decimal] (15, 8) NULL\n" +
            ")\n" +
            "GO";
        var target =
            "create table lab.samplemeasure(batchid int not null,measurevalue decimal(15,9) null,) on primary;\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Table", "db", "folder", source, target, normalizedDiff: true);

        Assert.Contains("create table lab.samplemeasure", diff, StringComparison.Ordinal);
        Assert.Contains("measurevalue decimal(15,8) null", diff, StringComparison.Ordinal);
        Assert.Contains("measurevalue decimal(15,9) null", diff, StringComparison.Ordinal);
        Assert.DoesNotContain("[lab].[SampleMeasure]", diff, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildUnifiedDiff_User_PreservesReadablePermissionText_ByDefault()
    {
        var source =
            "CREATE USER [AppReader] FOR LOGIN [AppReader]\n" +
            "GO\n" +
            "GRANT CONNECT TO [AppReader]\n" +
            "GO";
        var target =
            "CREATE USER [AppReader] FOR LOGIN [AppReader]\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("User", "db", "folder", source, target);

        Assert.Contains("GRANT CONNECT TO [AppReader]", diff, StringComparison.Ordinal);
        Assert.DoesNotContain("grant connect to appreader", diff, StringComparison.Ordinal);
    }

    [Fact]
    public void BuildUnifiedDiff_Role_SuppressesMissingTerminalGoAfterFinalStatement()
    {
        var source =
            "CREATE ROLE [AppViewer]\n" +
            "GO\n" +
            "GRANT VIEW DATABASE STATE TO [AppViewer]\n" +
            "GO";
        var target =
            "CREATE ROLE [AppViewer]\n" +
            "GO\n" +
            "GRANT VIEW DATABASE STATE TO [AppViewer]";

        var diff = SyncCommandService.BuildUnifiedDiff("Role", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_StoredProcedure_SuppressesLeadingSsmsHeaderComment()
    {
        var source =
            "SET ANSI_NULLS ON\n" +
            "GO\n" +
            "SET QUOTED_IDENTIFIER ON\n" +
            "GO\n" +
            "CREATE PROCEDURE [stg].[LoadLookup]\n" +
            "AS\n" +
            "SELECT 1\n" +
            "GO";
        var target =
            "/****** Object:  StoredProcedure [stg].[LoadLookup]    Script Date: 2017-08-10 00:32:47 ******/\n" +
            "SET ANSI_NULLS ON\n" +
            "GO\n" +
            "SET QUOTED_IDENTIFIER ON\n" +
            "GO\n" +
            "CREATE PROCEDURE [stg].[LoadLookup]\n" +
            "AS\n" +
            "SELECT 1\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("StoredProcedure", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Assembly_SuppressesEquivalentBannerWrappedHexAndQuotedAddFileDifferences()
    {
        var source =
            "CREATE ASSEMBLY [AppLibrary]\n" +
            "AUTHORIZATION [dbo]\n" +
            "FROM 0xAABBCCDD\n" +
            "WITH PERMISSION_SET = SAFE\n" +
            "GO\n" +
            "ALTER ASSEMBLY [AppLibrary] ADD FILE FROM 0x11223344 AS [AppLibrary.pdb]\n" +
            "GO";
        var target =
            "--Assembly applibrary, version=0.0.0.0, culture=neutral, publickeytoken=null, processorarchitecture=msil\n" +
            "--Assembly applibrary, version=0.0.0.0, culture=neutral, publickeytoken=null, processorarchitecture=msil\n" +
            "CREATE ASSEMBLY applibrary\n" +
            "AUTHORIZATION dbo\n" +
            "FROM 0xaabb\\\n" +
            "ccdd\n" +
            "WITH PERMISSION_SET=safe\n" +
            "GO\n" +
            "ALTER ASSEMBLY applibrary\n" +
            "ADD FILE FROM\n" +
            "0x1122\\\n" +
            "3344\n" +
            "AS 'AppLibrary.pdb'\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Assembly", "db", "folder", source, target);

        Assert.Empty(diff);
    }

    [Fact]
    public void BuildUnifiedDiff_Assembly_PreservesPermissionSetDifferencesWhenLegacyFormattingAlsoDiffers()
    {
        var source =
            "CREATE ASSEMBLY [AppLibrary]\n" +
            "AUTHORIZATION [dbo]\n" +
            "FROM 0xAABBCCDD\n" +
            "WITH PERMISSION_SET = SAFE\n" +
            "GO";
        var target =
            "--Assembly applibrary, version=0.0.0.0, culture=neutral, publickeytoken=null, processorarchitecture=msil\n" +
            "CREATE ASSEMBLY applibrary\n" +
            "AUTHORIZATION dbo\n" +
            "FROM 0xaabb\\\n" +
            "ccdd\n" +
            "WITH PERMISSION_SET=unsafe\n" +
            "GO";

        var diff = SyncCommandService.BuildUnifiedDiff("Assembly", "db", "folder", source, target);

        Assert.NotEmpty(diff);
        Assert.Contains("PERMISSION_SET", diff, StringComparison.OrdinalIgnoreCase);
        Assert.DoesNotContain("--Assembly", diff, StringComparison.Ordinal);
    }

    [Fact]
    public void NormalizeForComparison_DoesNotNormalizeUnicodeLiteralPrefixesOutsideTableData()
    {
        var plain = SyncCommandService.NormalizeForComparison(
            "INSERT INTO [dbo].[T] ([Name]) VALUES ('Alpha')");
        var prefixed = SyncCommandService.NormalizeForComparison(
            "INSERT INTO [dbo].[T] ([Name]) VALUES (N'Alpha')");

        Assert.NotEqual(plain, prefixed);
    }

    [Fact]
    public void DetectExistingStyle_AndApplyStyle_PreservesEncodingAndLineBehavior()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "sqlct-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            var path = Path.Combine(tempDir, "sample.sql");
            File.WriteAllText(path, "LINE1\nLINE2", new UTF8Encoding(true));

            var style = SyncCommandService.DetectExistingStyle(path);
            var styled = SyncCommandService.ApplyStyle("LINE1\r\nLINE2\r\n", style);

            Assert.Equal("\n", style.NewLine);
            Assert.False(style.HasTrailingNewline);
            Assert.Equal("LINE1\nLINE2", styled);
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }

    [Fact]
    public void ApplyStyle_Default_UsesCrLfWithTrailingNewline()
    {
        var styled = SyncCommandService.ApplyStyle(
            "LINE1\nLINE2",
            SyncCommandService.FileContentStyle.Default);

        Assert.Equal("LINE1\r\nLINE2\r\n", styled);
    }

    [Theory]
    [InlineData("dbo.Customer", "dbo\\.Customer", true)]
    [InlineData("dbo.Customer", "dbo\\..*", true)]
    [InlineData("dbo.Customer", ".*Customer.*", true)]
    [InlineData("dbo.Customer", "Customer", false)]
    [InlineData("dbo.Order", "dbo\\.Customer", false)]
    [InlineData("AppReader", "AppReader", true)]
    [InlineData("AppReader", "app.*", true)]
    [InlineData("AppReader", "dbo\\..*", false)]
    public void MatchesObjectPatterns_AppliesRegexCaseInsensitively(string displayName, string pattern, bool expected)
    {
        var regex = new System.Text.RegularExpressions.Regex(pattern, System.Text.RegularExpressions.RegexOptions.IgnoreCase, TimeSpan.FromSeconds(1));
        var result = SyncCommandService.MatchesObjectPatterns(displayName, [regex]);

        Assert.Equal(expected, result);
    }

    [Fact]
    public void MatchesObjectPatterns_ReturnsTrueWhenAnyPatternMatches()
    {
        var patterns = new[]
        {
            new System.Text.RegularExpressions.Regex("dbo\\.Customer", System.Text.RegularExpressions.RegexOptions.IgnoreCase, TimeSpan.FromSeconds(1)),
            new System.Text.RegularExpressions.Regex("dbo\\.Order", System.Text.RegularExpressions.RegexOptions.IgnoreCase, TimeSpan.FromSeconds(1))
        };

        Assert.True(SyncCommandService.MatchesObjectPatterns("dbo.Customer", patterns));
        Assert.True(SyncCommandService.MatchesObjectPatterns("dbo.Order", patterns));
        Assert.False(SyncCommandService.MatchesObjectPatterns("dbo.Product", patterns));
    }

    [Fact]
    public void RunDiff_WithInvalidFilterPattern_ReturnsInvalidConfigError()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var config = SqlctConfigWriter.CreateDefault();
            config.Database.Server = "localhost";
            config.Database.Name = "TestDb";
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
            Assert.True(write.Success);

            var service = new SyncCommandService();
            var result = service.RunDiff(projectDir, "db", null, filterPatterns: ["[invalid"]);

            Assert.False(result.Success);
            Assert.Equal(ExitCodes.InvalidConfig, result.ExitCode);
            Assert.NotNull(result.Error);
            Assert.Equal(ErrorCodes.InvalidConfig, result.Error!.Code);
            Assert.Equal("invalid filter pattern.", result.Error.Message);
            Assert.Contains("[invalid", result.Error.Detail);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunPull_WithInvalidFilterPattern_ReturnsInvalidConfigError()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var config = SqlctConfigWriter.CreateDefault();
            config.Database.Server = "localhost";
            config.Database.Name = "TestDb";
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
            Assert.True(write.Success);

            var service = new SyncCommandService();
            var result = service.RunPull(projectDir, filterPatterns: ["[invalid"]);

            Assert.False(result.Success);
            Assert.Equal(ExitCodes.InvalidConfig, result.ExitCode);
            Assert.NotNull(result.Error);
            Assert.Equal(ErrorCodes.InvalidConfig, result.Error!.Code);
            Assert.Equal("invalid filter pattern.", result.Error.Message);
            Assert.Contains("[invalid", result.Error.Detail);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunPull_WithInvalidObjectSelector_ReturnsInvalidConfigError()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var config = SqlctConfigWriter.CreateDefault();
            config.Database.Server = "localhost";
            config.Database.Name = "TestDb";
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
            Assert.True(write.Success);

            var service = new SyncCommandService();
            var result = service.RunPull(projectDir, objectSelector: "dbo.");

            Assert.False(result.Success);
            Assert.Equal(ExitCodes.InvalidConfig, result.ExitCode);
            Assert.NotNull(result.Error);
            Assert.Equal(ErrorCodes.InvalidConfig, result.Error!.Code);
            Assert.Equal("invalid object selector.", result.Error.Message);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunPull_WithObjectSelector_UsesTargetedDatabaseDiscovery()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var config = SqlctConfigWriter.CreateDefault();
            config.Database.Server = "localhost";
            config.Database.Name = "TestDb";
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
            Assert.True(write.Success);

            CreateFile(projectDir, Path.Combine("Tables", "dbo.Customer.sql"), "CREATE TABLE [dbo].[Customer] ([Id] [int] NOT NULL);\r\n");

            var introspector = new TrackingIntrospector
            {
                MatchingObjects = [new DbObjectInfo("dbo", "Customer", "Table")]
            };
            var scripter = new TrackingScripter
            {
                ScriptObjectHandler = (_, obj, _) => "CREATE TABLE [dbo].[Customer] ([Id] [int] NOT NULL);\r\n"
            };

            var service = new SyncCommandService(
                new SqlctConfigReader(),
                introspector,
                scripter,
                new SchemaFolderMapper(SupportedSqlObjectTypes.DefaultFolderMap, dataWriteAllFilesInOneDirectory: true));

            var result = service.RunPull(projectDir, objectSelector: "dbo.Customer");

            Assert.True(result.Success, result.Error?.Detail ?? result.Error?.Message);
            Assert.Equal(ExitCodes.Success, result.ExitCode);
            Assert.False(introspector.ListObjectsCalled);
            Assert.True(introspector.ListMatchingObjectsCalled);
            var expectedCandidateTypes = new[] { "Function", "Queue", "Sequence", "StoredProcedure", "Synonym", "Table", "UserDefinedType", "View", "XmlSchemaCollection" };
            Assert.Equal(
                expectedCandidateTypes,
                introspector.LastRequestedObjectTypes.OrderBy(item => item, StringComparer.OrdinalIgnoreCase));
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunPull_WithDottedSchemaLessObjectSelector_DeletesMatchingAssemblyFile()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var config = SqlctConfigWriter.CreateDefault();
            config.Database.Server = "localhost";
            config.Database.Name = "TestDb";
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
            Assert.True(write.Success);

            var assemblyPath = CreateFile(
                projectDir,
                Path.Combine("Assemblies", "App.Core.Legacy.sql"),
                "CREATE ASSEMBLY [App.Core.Legacy]\r\nFROM 0x00\r\nWITH PERMISSION_SET = SAFE\r\nGO\r\n");

            var introspector = new TrackingIntrospector();
            var service = new SyncCommandService(
                new SqlctConfigReader(),
                introspector,
                new TrackingScripter(),
                new SchemaFolderMapper(SupportedSqlObjectTypes.DefaultFolderMap, dataWriteAllFilesInOneDirectory: true));

            var result = service.RunPull(projectDir, objectSelector: "App.Core.Legacy");

            Assert.True(result.Success, result.Error?.Detail ?? result.Error?.Message);
            Assert.Equal(ExitCodes.Success, result.ExitCode);
            Assert.False(introspector.ListObjectsCalled);
            Assert.True(introspector.ListMatchingObjectsCalled);
            Assert.Equal(string.Empty, introspector.LastRequestedSchema);
            Assert.Equal("App.Core.Legacy", introspector.LastRequestedName);
            Assert.Equal(1, result.Payload!.Summary.Schema.Deleted);
            Assert.Single(result.Payload.Objects);
            Assert.Equal("deleted", result.Payload.Objects[0].Change);
            Assert.Equal("App.Core.Legacy", result.Payload.Objects[0].Name);
            Assert.False(File.Exists(assemblyPath));
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunDiff_WithFilterPattern_LimitsDbScriptingToMatchingObjects()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var config = SqlctConfigWriter.CreateDefault();
            config.Database.Server = "localhost";
            config.Database.Name = "TestDb";
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
            Assert.True(write.Success);

            var introspector = new TrackingIntrospector
            {
                AllObjects =
                [
                    new DbObjectInfo("dbo", "Customer", "Table"),
                    new DbObjectInfo("dbo", "Order", "Table"),
                    new DbObjectInfo("dbo", "Product", "Table")
                ]
            };
            var scripter = new TrackingScripter
            {
                ScriptObjectHandler = (_, obj, _) => $"CREATE TABLE [dbo].[{obj.Name}];\r\n"
            };

            var service = new SyncCommandService(
                new SqlctConfigReader(),
                introspector,
                scripter,
                new SchemaFolderMapper(SupportedSqlObjectTypes.DefaultFolderMap, dataWriteAllFilesInOneDirectory: true));

            var result = service.RunDiff(projectDir, "db", null, filterPatterns: ["dbo\\.Customer"]);

            Assert.True(result.Success, result.Error?.Detail ?? result.Error?.Message);
            Assert.True(introspector.ListObjectsCalled);
            Assert.False(introspector.ListMatchingObjectsCalled);
            Assert.Equal(new[] { "Table:dbo.Customer" }, scripter.ScriptedObjects);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunDiff_Table_SuppressesCompatibleOmittedTextImageOnDifference_WhenDbMetadataAllowsOmission()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var config = SqlctConfigWriter.CreateDefault();
            config.Database.Server = "localhost";
            config.Database.Name = "TestDb";
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
            Assert.True(write.Success);

            CreateFile(
                projectDir,
                Path.Combine("Tables", "dbo.DocumentStore.sql"),
                "CREATE TABLE [dbo].[DocumentStore]\r\n(\r\n[DocumentId] [int] NOT NULL,\r\n[Payload] [varchar] (max) NULL\r\n) ON [PRIMARY]\r\nGO\r\n");

            var introspector = new TrackingIntrospector
            {
                MatchingObjects = [new DbObjectInfo("dbo", "DocumentStore", "Table")]
            };
            introspector.CompatibleOmittedTextImageOnDataSpaceNames["dbo.DocumentStore"] = "PRIMARY";

            var scripter = new TrackingScripter
            {
                ScriptObjectHandler = (_, _, _) =>
                    "CREATE TABLE [dbo].[DocumentStore]\r\n(\r\n[DocumentId] [int] NOT NULL,\r\n[Payload] [varchar] (max) NULL\r\n) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]\r\nGO\r\n"
            };

            var service = new SyncCommandService(
                new SqlctConfigReader(),
                introspector,
                scripter,
                new SchemaFolderMapper(SupportedSqlObjectTypes.DefaultFolderMap, dataWriteAllFilesInOneDirectory: true));

            var result = service.RunDiff(projectDir, "db", "dbo.DocumentStore");

            Assert.True(result.Success, result.Error?.Detail ?? result.Error?.Message);
            Assert.Equal(ExitCodes.Success, result.ExitCode);
            Assert.Equal(string.Empty, result.Payload!.Diff);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunStatus_Table_PreservesTextImageOnDifference_WhenDbMetadataDoesNotAllowOmission()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var config = SqlctConfigWriter.CreateDefault();
            config.Database.Server = "localhost";
            config.Database.Name = "TestDb";
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
            Assert.True(write.Success);

            CreateFile(
                projectDir,
                Path.Combine("Tables", "dbo.DocumentStore.sql"),
                "CREATE TABLE [dbo].[DocumentStore]\r\n(\r\n[DocumentId] [int] NOT NULL,\r\n[Payload] [varchar] (max) NULL\r\n) ON [PRIMARY]\r\nGO\r\n");

            var introspector = new TrackingIntrospector
            {
                AllObjects = [new DbObjectInfo("dbo", "DocumentStore", "Table")]
            };

            var scripter = new TrackingScripter
            {
                ScriptObjectHandler = (_, _, _) =>
                    "CREATE TABLE [dbo].[DocumentStore]\r\n(\r\n[DocumentId] [int] NOT NULL,\r\n[Payload] [varchar] (max) NULL\r\n) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]\r\nGO\r\n"
            };

            var service = new SyncCommandService(
                new SqlctConfigReader(),
                introspector,
                scripter,
                new SchemaFolderMapper(SupportedSqlObjectTypes.DefaultFolderMap, dataWriteAllFilesInOneDirectory: true));

            var result = service.RunStatus(projectDir, "db");

            Assert.True(result.Success, result.Error?.Detail ?? result.Error?.Message);
            Assert.Equal(ExitCodes.DiffExists, result.ExitCode);
            Assert.Equal(1, result.Payload!.Summary.Schema.Changed);
            Assert.Single(result.Payload.Objects);
            Assert.Equal("changed", result.Payload.Objects[0].Change);
            Assert.Equal("dbo.DocumentStore", result.Payload.Objects[0].Name);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunPull_WithFilterPattern_LimitsDbScriptingToMatchingObjects()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var seed = new BaselineProjectSeeder().Seed(projectDir);
            Assert.True(seed.Success);

            var config = SqlctConfigWriter.CreateDefault();
            config.Database.Server = "localhost";
            config.Database.Name = "TestDb";
            var write = new SqlctConfigWriter().Write(SqlctConfigWriter.GetDefaultPath(projectDir), config, overwriteExisting: true);
            Assert.True(write.Success);

            var introspector = new TrackingIntrospector
            {
                AllObjects =
                [
                    new DbObjectInfo("dbo", "Customer", "Table"),
                    new DbObjectInfo("dbo", "Order", "Table"),
                    new DbObjectInfo("dbo", "Product", "Table")
                ]
            };
            var scripter = new TrackingScripter
            {
                ScriptObjectHandler = (_, obj, _) => $"CREATE TABLE [dbo].[{obj.Name}];\r\n"
            };

            var service = new SyncCommandService(
                new SqlctConfigReader(),
                introspector,
                scripter,
                new SchemaFolderMapper(SupportedSqlObjectTypes.DefaultFolderMap, dataWriteAllFilesInOneDirectory: true));

            var result = service.RunPull(projectDir, filterPatterns: ["dbo\\.Customer"]);

            Assert.True(result.Success, result.Error?.Detail ?? result.Error?.Message);
            Assert.True(introspector.ListObjectsCalled);
            Assert.False(introspector.ListMatchingObjectsCalled);
            Assert.Equal(new[] { "Table:dbo.Customer" }, scripter.ScriptedObjects);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    private static string CreateTempDir()
    {
        var path = Path.Combine(Path.GetTempPath(), "sqlct-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static int CountHunkHeaders(string diff)
        => diff.Split('\n').Count(line => line.StartsWith("@@"));

    private static void CleanupTempDir(string path)
    {
        if (Directory.Exists(path))
        {
            Directory.Delete(path, true);
        }
    }

    [Fact]
    public void RunStatus_WithInvalidAuthMode_ReturnsInvalidConfig()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            WriteConfigWithAuth(projectDir, "kerberos", user: null, password: null);

            var service = new SyncCommandService();
            var result = service.RunStatus(projectDir, "db");

            Assert.False(result.Success);
            Assert.Equal(ExitCodes.InvalidConfig, result.ExitCode);
            Assert.Equal("database.auth must be 'integrated' or 'sql'.", result.Error!.Detail);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunStatus_WithSqlAuthAndNoUser_ReturnsInvalidConfig()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            WriteConfigWithAuth(projectDir, "sql", user: null, password: null);

            var service = new SyncCommandService();
            var result = service.RunStatus(projectDir, "db");

            Assert.False(result.Success);
            Assert.Equal(ExitCodes.InvalidConfig, result.ExitCode);
            Assert.Equal("missing required field: database.user for sql authentication.", result.Error!.Detail);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunStatus_WithSqlAuthAndEmptyUser_ReturnsInvalidConfig()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            WriteConfigWithAuth(projectDir, "sql", user: "  ", password: null);

            var service = new SyncCommandService();
            var result = service.RunStatus(projectDir, "db");

            Assert.False(result.Success);
            Assert.Equal(ExitCodes.InvalidConfig, result.ExitCode);
            Assert.Equal("missing required field: database.user for sql authentication.", result.Error!.Detail);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunStatus_WithSqlAuthAndValidUser_PassesAuthValidation()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            WriteConfigWithAuth(projectDir, "sql", user: "sa", password: "secret");

            var service = new SyncCommandService();
            var result = service.RunStatus(projectDir, "db");

            // Auth validation passes; failure here is a runtime error, not a config validation error.
            Assert.False(result.Success);
            Assert.NotEqual(ExitCodes.InvalidConfig, result.ExitCode);
            Assert.NotEqual(ErrorCodes.InvalidConfig, result.Error!.Code);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void RunStatus_WithIntegratedAuth_PassesAuthValidation()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            WriteConfigWithAuth(projectDir, "integrated", user: null, password: null);

            var service = new SyncCommandService();
            var result = service.RunStatus(projectDir, "db");

            // Auth validation passes; failure here is a runtime error, not a config validation error.
            Assert.False(result.Success);
            Assert.NotEqual(ExitCodes.InvalidConfig, result.ExitCode);
            Assert.NotEqual(ErrorCodes.InvalidConfig, result.Error!.Code);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    private static void WriteConfigWithAuth(string projectDir, string auth, string? user, string? password)
    {
        Directory.CreateDirectory(projectDir);
        var configPath = Path.Combine(projectDir, "sqlct.config.json");
        var userLine = user != null ? $"""
                "user": "{user}",
        """ : string.Empty;
        var passwordLine = password != null ? $"""
                "password": "{password}",
        """ : string.Empty;
        File.WriteAllText(configPath, $$"""
            {
              "database": {
                "server": "non-existent-server-for-auth-test",
                "name": "TestDb",
                "auth": "{{auth}}",
                {{userLine}}
                {{passwordLine}}
                "trustServerCertificate": true
              }
            }
            """);
    }

    private static string CreateFile(string root, string relativePath, string content)
    {
        var fullPath = Path.Combine(root, relativePath);
        Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
        File.WriteAllText(fullPath, content);
        return fullPath;
    }

    private sealed class TrackingIntrospector : SqlServerIntrospector
    {
        public bool ListObjectsCalled { get; private set; }

        public bool ListMatchingObjectsCalled { get; private set; }

        public IReadOnlyList<string> LastRequestedObjectTypes { get; private set; } = [];

        public string LastRequestedSchema { get; private set; } = "";

        public string LastRequestedName { get; private set; } = "";

        public IReadOnlyList<DbObjectInfo>? AllObjects { get; init; }

        public IReadOnlyList<DbObjectInfo> MatchingObjects { get; init; } = [];

        public Dictionary<string, string?> CompatibleOmittedTextImageOnDataSpaceNames { get; } =
            new(StringComparer.OrdinalIgnoreCase);

        public override IReadOnlyList<DbObjectInfo> ListObjects(SqlConnectionOptions options, int maxParallelism = 0)
        {
            ListObjectsCalled = true;
            if (AllObjects is not null)
                return AllObjects;
            throw new InvalidOperationException("full database discovery should not be used for diff --object");
        }

        public override IReadOnlyList<DbObjectInfo> ListMatchingObjects(
            SqlConnectionOptions options,
            IReadOnlyList<string> objectTypes,
            string schema,
            string name,
            int maxParallelism = 0)
        {
            ListMatchingObjectsCalled = true;
            LastRequestedObjectTypes = objectTypes.ToArray();
            LastRequestedSchema = schema;
            LastRequestedName = name;
            return MatchingObjects;
        }

        public override string? GetTableCompatibleOmittedTextImageOnDataSpaceName(
            SqlConnectionOptions options,
            string schema,
            string name)
            => CompatibleOmittedTextImageOnDataSpaceNames.TryGetValue($"{schema}.{name}", out var value)
                ? value
                : null;
    }

    private sealed class TrackingScripter : SqlServerScripter
    {
        public Func<SqlConnectionOptions, DbObjectInfo, string?, string>? ScriptObjectHandler { get; init; }

        public Func<SqlConnectionOptions, ObjectIdentifier, string>? ScriptTableDataHandler { get; init; }

        public List<string> ScriptedObjects { get; } = [];

        public List<string> ScriptedTableData { get; } = [];

        public override string ScriptObject(SqlConnectionOptions options, DbObjectInfo obj, string? referencePath)
        {
            ScriptedObjects.Add($"{obj.ObjectType}:{obj.Schema}.{obj.Name}");
            return ScriptObjectHandler?.Invoke(options, obj, referencePath)
                ?? throw new InvalidOperationException("unexpected ScriptObject call");
        }

        public override string ScriptTableData(SqlConnectionOptions options, ObjectIdentifier table)
        {
            ScriptedTableData.Add($"{table.Schema}.{table.Name}");
            return ScriptTableDataHandler?.Invoke(options, table)
                ?? throw new InvalidOperationException("unexpected ScriptTableData call");
        }
    }
}
