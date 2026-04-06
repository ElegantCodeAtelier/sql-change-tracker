using SqlChangeTracker.Config;
using System.Text.Json;
using Xunit;

namespace SqlChangeTracker.Tests.Config;

public sealed class SqlctConfigWriterTests
{
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

            using var document = JsonDocument.Parse(File.ReadAllText(configPath));
            var root = document.RootElement;
            Assert.Equal(string.Empty, root.GetProperty("database").GetProperty("server").GetString());
            Assert.Equal(string.Empty, root.GetProperty("database").GetProperty("name").GetString());
            Assert.Equal("integrated", root.GetProperty("database").GetProperty("auth").GetString());
            Assert.True(root.GetProperty("options").GetProperty("orderByDependencies").GetBoolean());
            Assert.Equal(0, root.GetProperty("data").GetProperty("trackedTables").GetArrayLength());
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
            File.WriteAllText(configPath, """
                {
                  "database": {
                    "server": "localhost",
                    "name": "MyDb",
                    "auth": "integrated",
                    "user": "",
                    "password": "",
                    "trustServerCertificate": false
                  },
                  "options": {
                    "includeSchemas": ["dbo"],
                    "excludeObjects": ["sys.*"],
                    "orderByDependencies": false,
                    "comparison": {
                      "ignoreWhitespace": true
                    }
                  }
                }
                """);

            var reader = new SqlctConfigReader();
            var read = reader.Read(configPath);
            Assert.True(read.Success);

            var writer = new SqlctConfigWriter();
            var write = writer.Write(configPath, read.Config!, overwriteExisting: true);
            Assert.True(write.Success);

            using var document = JsonDocument.Parse(File.ReadAllText(configPath));
            var options = document.RootElement.GetProperty("options");
            Assert.True(options.TryGetProperty("orderByDependencies", out _));
            Assert.False(options.TryGetProperty("includeSchemas", out _));
            Assert.False(options.TryGetProperty("excludeObjects", out _));
            Assert.False(options.TryGetProperty("comparison", out _));
            Assert.Equal(0, document.RootElement.GetProperty("data").GetProperty("trackedTables").GetArrayLength());
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

            using var document = JsonDocument.Parse(File.ReadAllText(configPath));
            var tracked = document.RootElement
                .GetProperty("data")
                .GetProperty("trackedTables")
                .EnumerateArray()
                .Select(item => item.GetString())
                .ToArray();

            Assert.Equal(new[] { "dbo.Customer", "sales.Order" }, tracked);
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
