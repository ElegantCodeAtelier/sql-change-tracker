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
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Synonyms")));
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Types", "User-defined Data Types")));
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Types", "Table Types")));
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Types", "XML Schema Collections")));
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Security", "Roles")));
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Service Broker", "Event Notifications")));
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Service Broker", "Message Types")));
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Service Broker", "Queues")));
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Service Broker", "Remote Service Bindings")));
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Storage", "Full Text Catalogs")));
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Storage", "Partition Functions")));
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Storage", "Search Property Lists")));
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void Init_WithProjectDirWrappedInSingleQuotes_AndTrailingSlash_CreatesProjectStructureAndConfig()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project with spaces");
            var wrappedProjectDir = $"'{projectDir}{Path.DirectorySeparatorChar}'";

            var exitCode = Program.Main(["init", "--project-dir", wrappedProjectDir]);

            Assert.Equal(ExitCodes.Success, exitCode);
            Assert.True(File.Exists(Path.Combine(projectDir, ConfigFileNames.SqlctConfigFileName)));
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Tables")));
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void Init_WithProjectDirEndingInDoubleQuoteArtifact_CreatesProjectStructureAndConfig()
    {
        if (!OperatingSystem.IsWindows())
            return;

        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project with spaces");
            var projectDirWithQuoteArtifact = projectDir + Path.DirectorySeparatorChar + '"';

            var exitCode = Program.Main(["init", "--project-dir", projectDirWithQuoteArtifact]);

            Assert.Equal(ExitCodes.Success, exitCode);
            Assert.True(File.Exists(Path.Combine(projectDir, ConfigFileNames.SqlctConfigFileName)));
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Tables")));
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
    public void Config_WhenConfigMissing_ReturnsInvalidConfig()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");
            Directory.CreateDirectory(projectDir);
            var configPath = Path.Combine(projectDir, ConfigFileNames.SqlctConfigFileName);

            var exitCode = Program.Main(["config", "--project-dir", projectDir]);

            Assert.Equal(ExitCodes.InvalidConfig, exitCode);
            Assert.False(File.Exists(configPath));
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
