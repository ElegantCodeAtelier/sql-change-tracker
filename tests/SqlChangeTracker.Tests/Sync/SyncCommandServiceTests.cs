using SqlChangeTracker.Config;
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
    [InlineData("Synonym:Reporting.CurrentSales", "Synonym", "Reporting", "CurrentSales", false)]
    [InlineData("UserDefinedType:dbo.PhoneNumber", "UserDefinedType", "dbo", "PhoneNumber", false)]
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
    [InlineData("dbo.Customer", false, "", "")]
    [InlineData("Customer_Data", false, "", "")]
    public void TryParseDataFileName_SupportsTrackedDataScripts(string fileName, bool expected, string expectedSchema, string expectedName)
    {
        var success = SyncCommandService.TryParseDataFileName(fileName, out var schema, out var name);

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

    private static string CreateTempDir()
    {
        var path = Path.Combine(Path.GetTempPath(), "sqlct-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(path);
        return path;
    }

    private static void CleanupTempDir(string path)
    {
        if (Directory.Exists(path))
        {
            Directory.Delete(path, true);
        }
    }

    private static string CreateFile(string root, string relativePath, string content)
    {
        var fullPath = Path.Combine(root, relativePath);
        Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);
        File.WriteAllText(fullPath, content);
        return fullPath;
    }
}
