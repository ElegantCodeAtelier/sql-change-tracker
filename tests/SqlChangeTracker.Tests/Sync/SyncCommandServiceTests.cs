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
    [InlineData("dbo.Customer", null, "dbo", "Customer", false)]
    [InlineData("ServiceUser", null, "", "ServiceUser", true)]
    [InlineData("Role:AppReader", "Role", "", "AppReader", true)]
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

    [Theory]
    [InlineData("")]
    [InlineData("dbo.")]
    [InlineData(".Customer")]
    [InlineData("UnknownType:dbo.Customer")]
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
            var expectedCandidateTypes = new[] { "Function", "Queue", "Sequence", "StoredProcedure", "Synonym", "Table", "TableType", "UserDefinedType", "View", "XmlSchemaCollection" };
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
            var supportedData = CreateFile(tempDir, Path.Combine("Data", "dbo.Customer_Data.sql"), "SELECT 1;");
            CreateFile(tempDir, Path.Combine("Custom", "dbo.Legacy.sql"), "SELECT 1;");
            CreateFile(tempDir, Path.Combine("Data", "Invalid", "dbo.Customer_Data.sql"), "SELECT 1;");

            var warnings = SyncCommandService.CollectUnsupportedFolderWarnings(
                tempDir,
                [supported, supportedRole, supportedPartitionFunction, supportedSynonym, supportedUserDefinedType, supportedData]);

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

    [Fact]
    public void NormalizeForComparison_IgnoresLineEndingsAndTrailingNewlines()
    {
        var left = SyncCommandService.NormalizeForComparison("SELECT 1\r\nGO\r\n");
        var right = SyncCommandService.NormalizeForComparison("SELECT 1\nGO\n\n");

        Assert.Equal(left, right);
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
            var expectedCandidateTypes = new[] { "Function", "Queue", "Sequence", "StoredProcedure", "Synonym", "Table", "TableType", "UserDefinedType", "View", "XmlSchemaCollection" };
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
