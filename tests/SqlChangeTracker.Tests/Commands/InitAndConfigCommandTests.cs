using System.Text.Json;
using SqlChangeTracker.Config;
using Xunit;

namespace SqlChangeTracker.Tests.Commands;

public sealed class InitAndConfigCommandTests
{
    [Theory]
    [InlineData("--version")]
    [InlineData("-v")]
    public void Version_ReturnsSuccess(string flag)
    {
        var exitCode = Program.Main([flag]);

        Assert.Equal(ExitCodes.Success, exitCode);
    }

    [Fact]
    public void Init_WithProjectDir_CreatesProjectStructureAndConfig()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");

            var exitCode = Program.Main(["init", "--project-dir", projectDir]);

            Assert.Equal(ExitCodes.Success, exitCode);
            Assert.True(File.Exists(Path.Combine(projectDir, ConfigFileNames.SqlctConfigFileName)));
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Tables")));
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Stored Procedures")));
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void Init_WithConfigSwitch_ReturnsParsingFailure()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var customConfigPath = Path.Combine(projectDir, "custom", "sqlct.custom.json");

            var exitCode = Program.Main(["init", "--project-dir", projectDir, "--config", customConfigPath]);

            Assert.Equal(ExitCodes.InvalidConfig, exitCode);
            Assert.False(File.Exists(Path.Combine(projectDir, ConfigFileNames.SqlctConfigFileName)));
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void Init_WithoutProjectDir_WhenDeclined_ReturnsInvalidConfig()
    {
        var tempDir = CreateTempDir();
        var originalCurrentDirectory = Environment.CurrentDirectory;
        var originalInput = Console.In;

        try
        {
            Environment.CurrentDirectory = tempDir;
            Console.SetIn(new StringReader("n" + Environment.NewLine));

            var exitCode = Program.Main(["init"]);

            Assert.Equal(ExitCodes.InvalidConfig, exitCode);
            Assert.False(File.Exists(Path.Combine(tempDir, ConfigFileNames.SqlctConfigFileName)));
        }
        finally
        {
            Console.SetIn(originalInput);
            Environment.CurrentDirectory = originalCurrentDirectory;
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void Config_WhenConfigMissing_CreatesDefaultConfig()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            Directory.CreateDirectory(projectDir);
            var configPath = Path.Combine(projectDir, ConfigFileNames.SqlctConfigFileName);

            var exitCode = Program.Main(["config", "--project-dir", projectDir]);

            Assert.Equal(ExitCodes.Success, exitCode);
            Assert.True(File.Exists(configPath));

            using var document = JsonDocument.Parse(File.ReadAllText(configPath));
            Assert.Equal("integrated", document.RootElement
                .GetProperty("database")
                .GetProperty("auth")
                .GetString());
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void Config_WithConfigSwitch_ReturnsParsingFailure()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            var customConfigPath = Path.Combine(projectDir, "custom", "sqlct.custom.json");

            var exitCode = Program.Main(["config", "--project-dir", projectDir, "--config", customConfigPath]);

            Assert.Equal(ExitCodes.InvalidConfig, exitCode);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    private static string CreateTempDir()
    {
        var dir = Path.Combine(Path.GetTempPath(), "sqlct-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dir);
        return dir;
    }

    private static void CleanupTempDir(string tempDir)
    {
        if (Directory.Exists(tempDir))
        {
            Directory.Delete(tempDir, true);
        }
    }
}
