using SqlChangeTracker.Config;
using SqlChangeTracker.Commands;
using SqlChangeTracker.Sql;
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
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Security", "Roles")));
            Assert.True(Directory.Exists(Path.Combine(projectDir, "Storage", "Partition Functions")));
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
    public void Init_WithoutProjectDir_WhenAccepted_CreatesProjectStructureAndConfig()
    {
        var tempDir = CreateTempDir();
        var originalCurrentDirectory = Environment.CurrentDirectory;
        var originalInput = Console.In;

        try
        {
            Environment.CurrentDirectory = tempDir;
            // Use a stub connection tester to avoid slow real-connection attempts.
            // Interactive prompts receive null/default responses (stdin exhausted after "y").
            InitCommand.ConnectionTesterOverride = new StubConnectionTester(false, "no server");
            try
            {
                // "y" answers the directory confirmation; subsequent prompts receive
                // null (stdin exhausted) and fall back to defaults (server = localhost).
                Console.SetIn(new StringReader("y" + Environment.NewLine));

                var exitCode = Program.Main(["init"]);

                Assert.Equal(ExitCodes.Success, exitCode);
                Assert.True(File.Exists(Path.Combine(tempDir, ConfigFileNames.SqlctConfigFileName)));
            }
            finally
            {
                InitCommand.ConnectionTesterOverride = null;
            }
        }
        finally
        {
            Console.SetIn(originalInput);
            Environment.CurrentDirectory = originalCurrentDirectory;
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void Init_WithServerAndDatabaseFlags_WritesConnectionDetailsToConfig()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");

            var exitCode = Program.Main([
                "init",
                "--project-dir", projectDir,
                "--server", "myserver",
                "--database", "mydb",
                "--auth", "integrated",
                "--skip-connection-test"
            ]);

            Assert.Equal(ExitCodes.Success, exitCode);

            var configPath = Path.Combine(projectDir, ConfigFileNames.SqlctConfigFileName);
            Assert.True(File.Exists(configPath));

            var configJson = File.ReadAllText(configPath);
            Assert.Contains("myserver", configJson);
            Assert.Contains("mydb", configJson);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void Init_WithServerFlag_AndSkipConnectionTest_ReturnsSuccess()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");

            var exitCode = Program.Main([
                "init",
                "--project-dir", projectDir,
                "--server", "myserver",
                "--database", "mydb",
                "--skip-connection-test"
            ]);

            Assert.Equal(ExitCodes.Success, exitCode);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void Init_WithSqlAuth_WritesUserToConfig()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");

            var exitCode = Program.Main([
                "init",
                "--project-dir", projectDir,
                "--server", "myserver",
                "--database", "mydb",
                "--auth", "sql",
                "--user", "sa",
                "--password", "secret",
                "--skip-connection-test"
            ]);

            Assert.Equal(ExitCodes.Success, exitCode);

            var configJson = File.ReadAllText(Path.Combine(projectDir, ConfigFileNames.SqlctConfigFileName));
            Assert.Contains("\"sql\"", configJson);
            Assert.Contains("sa", configJson);
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void Init_WithServerFlag_WhenConnectionFails_StillReturnsSuccess()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");

            InitCommand.ConnectionTesterOverride = new StubConnectionTester(false, "Connection refused by server.");
            try
            {
                var exitCode = Program.Main([
                    "init",
                    "--project-dir", projectDir,
                    "--server", "myserver",
                    "--database", "mydb"
                ]);

                Assert.Equal(ExitCodes.Success, exitCode);
                Assert.True(File.Exists(Path.Combine(projectDir, ConfigFileNames.SqlctConfigFileName)));
            }
            finally
            {
                InitCommand.ConnectionTesterOverride = null;
            }
        }
        finally
        {
            CleanupTempDir(tempDir);
        }
    }

    [Fact]
    public void Init_WithServerFlag_WhenConnectionSucceeds_ReturnsSuccess()
    {
        var tempDir = CreateTempDir();

        try
        {
            var projectDir = Path.Combine(tempDir, "project");

            InitCommand.ConnectionTesterOverride = new StubConnectionTester(true, null);
            try
            {
                var exitCode = Program.Main([
                    "init",
                    "--project-dir", projectDir,
                    "--server", "myserver",
                    "--database", "mydb"
                ]);

                Assert.Equal(ExitCodes.Success, exitCode);
            }
            finally
            {
                InitCommand.ConnectionTesterOverride = null;
            }
        }
        finally
        {
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

file sealed class StubConnectionTester : IConnectionTester
{
    private readonly bool _success;
    private readonly string? _errorMessage;

    public StubConnectionTester(bool success, string? errorMessage)
    {
        _success = success;
        _errorMessage = errorMessage;
    }

    public ConnectionTestResult Test(SqlConnectionOptions options)
        => new(_success, _errorMessage);
}
