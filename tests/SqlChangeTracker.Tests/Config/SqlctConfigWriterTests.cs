using SqlChangeTracker.Config;
using Xunit;

namespace SqlChangeTracker.Tests.Config;

public sealed class SqlctConfigWriterTests
{
    [Fact]
    public void Read_WhenConfigMissing_ReturnsMissingLinkWithConfigPath()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "sqlct-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            var configPath = Path.Combine(tempDir, "project with spaces", ConfigFileNames.SqlctConfigFileName);

            var reader = new SqlctConfigReader();
            var result = reader.Read(configPath);

            Assert.False(result.Success);
            Assert.NotNull(result.Error);
            Assert.Equal(ErrorCodes.MissingLink, result.Error!.Code);
            Assert.Equal(configPath, result.Error.File);
            Assert.Contains(configPath, result.Error.Detail);
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
    public void Read_WhenLegacyJsonConfigExists_ReturnsMigrationHint()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "sqlct-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            // Write the old JSON config file (no YAML file present).
            var legacyPath = Path.Combine(tempDir, ConfigFileNames.SqlctConfigLegacyFileName);
            File.WriteAllText(legacyPath, "{}");

            var configPath = Path.Combine(tempDir, ConfigFileNames.SqlctConfigFileName);
            var reader = new SqlctConfigReader();
            var result = reader.Read(configPath);

            Assert.False(result.Success);
            Assert.Equal(ErrorCodes.MissingLink, result.Error!.Code);
            Assert.NotNull(result.Error.Hint);
            Assert.Contains(ConfigFileNames.SqlctConfigLegacyFileName, result.Error.Hint);
            Assert.Contains(ConfigFileNames.SqlctConfigFileName, result.Error.Hint);
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
    public void Write_CreatesConfigWithDefaults()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "sqlct-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            var configPath = SqlctConfigWriter.GetDefaultPath(tempDir);
            var config = SqlctConfigWriter.CreateDefault();

            var writer = new SqlctConfigWriter();
            var result = writer.Write(configPath, config);

            Assert.True(result.Success);
            Assert.Contains(ConfigFileNames.SqlctConfigFileName, result.Created);
            Assert.True(File.Exists(configPath));

            var yamlText = File.ReadAllText(configPath);
            Assert.StartsWith("#", yamlText);

            // Deserialize to verify values via the reader.
            var readResult = new SqlctConfigReader().Read(configPath);
            Assert.True(readResult.Success);
            Assert.Equal(string.Empty, readResult.Config!.Database.Server);
            Assert.Equal(string.Empty, readResult.Config.Database.Name);
            Assert.Equal("integrated", readResult.Config.Database.Auth);
            Assert.Empty(readResult.Config.Data.TrackedTables);
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
    public void ReadThenWrite_LegacyConfig_RemovesDeprecatedFields()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "sqlct-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            var configPath = SqlctConfigWriter.GetDefaultPath(tempDir);
            // Write a YAML config that contains deprecated fields (IgnoreUnmatchedProperties drops them).
            File.WriteAllText(configPath, """
                database:
                  server: localhost
                  name: MyDb
                  auth: integrated
                  user: ''
                  password: ''
                  trustServerCertificate: false
                options:
                  includeSchemas:
                    - dbo
                  excludeObjects:
                    - sys.*
                  orderByDependencies: false
                  comparison:
                    ignoreWhitespace: true
                """);

            var reader = new SqlctConfigReader();
            var read = reader.Read(configPath);
            Assert.True(read.Success);

            var writer = new SqlctConfigWriter();
            var write = writer.Write(configPath, read.Config!, overwriteExisting: true);
            Assert.True(write.Success);

            var yamlText = File.ReadAllText(configPath);
            // Deprecated keys must not appear in the rewritten output.
            Assert.DoesNotContain("includeSchemas", yamlText);
            Assert.DoesNotContain("excludeObjects", yamlText);
            Assert.DoesNotContain("orderByDependencies", yamlText);
            Assert.DoesNotContain("comparison", yamlText);

            // Re-read and verify the data section is empty.
            var readAgain = new SqlctConfigReader().Read(configPath);
            Assert.True(readAgain.Success);
            Assert.Empty(readAgain.Config!.Data.TrackedTables);
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
    public void Write_NormalizesTrackedTables()
    {
        var tempDir = Path.Combine(Path.GetTempPath(), "sqlct-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(tempDir);

        try
        {
            var configPath = SqlctConfigWriter.GetDefaultPath(tempDir);
            var config = SqlctConfigWriter.CreateDefault();
            config.Data.TrackedTables.AddRange([" dbo.Customer ", "sales.Order", "dbo.customer", "", "Sales.Order"]);

            var writer = new SqlctConfigWriter();
            var result = writer.Write(configPath, config);

            Assert.True(result.Success);

            // Read back via the reader and verify normalization.
            var readResult = new SqlctConfigReader().Read(configPath);
            Assert.True(readResult.Success);
            Assert.Equal(new[] { "dbo.Customer", "sales.Order" }, readResult.Config!.Data.TrackedTables.ToArray());
        }
        finally
        {
            if (Directory.Exists(tempDir))
            {
                Directory.Delete(tempDir, true);
            }
        }
    }
}

