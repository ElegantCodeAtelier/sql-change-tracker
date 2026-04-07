using Spectre.Console.Cli;
using System.Threading;
using SqlChangeTracker.Config;
using SqlChangeTracker.Output;
using SqlChangeTracker.Sql;

namespace SqlChangeTracker.Commands;

internal sealed class InitCommand : Command<InitCommandSettings>
{
    // Test seam: set to a custom IConnectionTester before calling Program.Main in tests.
    // Reset to null after use to restore the default.
    internal static IConnectionTester? ConnectionTesterOverride { get; set; }

    private IConnectionTester GetConnectionTester() => ConnectionTesterOverride ?? new SqlConnectionTester();

    public override int Execute(CommandContext context, InitCommandSettings settings, CancellationToken cancellationToken)
    {
        var output = new OutputFormatter(settings.Json);
        var projectDirFromCurrentDirectory = string.IsNullOrWhiteSpace(settings.ProjectDir);
        var resolvedProjectDir = ProjectPathResolver.Resolve(settings.ProjectDir);
        var projectDir = resolvedProjectDir.FullPath;
        var displayProjectDir = resolvedProjectDir.DisplayPath;

        if (projectDirFromCurrentDirectory && !ConfirmCurrentDirectory(displayProjectDir))
        {
            output.WriteError(new ErrorResult("init", new ErrorInfo(
                ErrorCodes.InvalidConfig,
                "init cancelled.",
                Detail: "current directory initialization was not confirmed.")));
            return ExitCodes.InvalidConfig;
        }

        var projectSeeder = new BaselineProjectSeeder();
        var projectSeedResult = projectSeeder.Seed(projectDir);
        if (!projectSeedResult.Success)
        {
            output.WriteError(new ErrorResult("init", projectSeedResult.Error!));
            return projectSeedResult.ExitCode;
        }

        var connectionSetup = ResolveConnectionSetup(settings,
            promptInteractively: projectDirFromCurrentDirectory && string.IsNullOrWhiteSpace(settings.Server) && !settings.Json);
        var config = BuildConfig(connectionSetup);
        var configWriter = new SqlctConfigWriter();
        var configPath = SqlctConfigWriter.GetDefaultPath(projectDir);
        var configResult = configWriter.Write(configPath, config);
        if (!configResult.Success)
        {
            output.WriteError(new ErrorResult("init", configResult.Error!));
            return configResult.ExitCode;
        }

        InitConnectionTestResult? connectionTestResult = null;
        if (!settings.SkipConnectionTest && !string.IsNullOrWhiteSpace(connectionSetup.Server))
        {
            connectionTestResult = RunConnectionTest(connectionSetup, GetConnectionTester());
            if (!connectionTestResult.Success && !settings.Json)
            {
                PrintConnectionFailureHints(connectionSetup);
            }
        }

        var nextSteps = GetNextSteps(connectionTestResult);

        var created = projectSeedResult.Created.Concat(configResult.Created).ToList();
        var skipped = projectSeedResult.Skipped.Concat(configResult.Skipped).ToList();
        var result = new InitResult("init", displayProjectDir, created, skipped, connectionTestResult, nextSteps);
        output.WriteInit(result);
        return ExitCodes.Success;
    }

    private static ConnectionSetup ResolveConnectionSetup(InitCommandSettings settings, bool promptInteractively)
    {
        if (!string.IsNullOrWhiteSpace(settings.Server))
        {
            return new ConnectionSetup(
                settings.Server,
                settings.Database ?? string.Empty,
                settings.Auth ?? "integrated",
                settings.User,
                settings.Password,
                settings.TrustServerCertificate);
        }

        if (promptInteractively)
        {
            return PromptForConnectionSetup();
        }

        return new ConnectionSetup(string.Empty, string.Empty, "integrated", null, null, false);
    }

    private static ConnectionSetup PromptForConnectionSetup()
    {
        Console.WriteLine();
        Console.WriteLine("Connection setup:");
        Console.Write("  Server [localhost]: ");
        var server = Console.ReadLine()?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(server))
        {
            server = "localhost";
        }

        Console.Write("  Database: ");
        var database = Console.ReadLine()?.Trim() ?? string.Empty;

        Console.Write("  Auth (integrated/sql) [integrated]: ");
        var auth = Console.ReadLine()?.Trim() ?? string.Empty;
        if (string.IsNullOrWhiteSpace(auth))
        {
            auth = "integrated";
        }

        string? user = null;
        string? password = null;
        if (string.Equals(auth, "sql", StringComparison.OrdinalIgnoreCase))
        {
            Console.Write("  Username: ");
            user = Console.ReadLine()?.Trim();
            Console.Write("  Password: ");
            password = Console.ReadLine()?.Trim();
        }

        Console.Write("  Trust server certificate? [y/N]: ");
        var trustResponse = Console.ReadLine()?.Trim() ?? string.Empty;
        var trustServerCertificate =
            string.Equals(trustResponse, "y", StringComparison.OrdinalIgnoreCase)
            || string.Equals(trustResponse, "yes", StringComparison.OrdinalIgnoreCase);

        return new ConnectionSetup(server, database, auth, user, password, trustServerCertificate);
    }

    private static InitConnectionTestResult RunConnectionTest(ConnectionSetup setup, IConnectionTester tester)
    {
        var options = new SqlConnectionOptions(
            setup.Server,
            setup.Database,
            setup.Auth,
            setup.User,
            setup.Password,
            setup.TrustServerCertificate);
        var result = tester.Test(options);
        return new InitConnectionTestResult(result.Success, result.ErrorMessage);
    }

    private static void PrintConnectionFailureHints(ConnectionSetup setup)
    {
        Console.WriteLine("Troubleshooting tips:");
        Console.WriteLine("  - Verify the server name and check that SQL Server is running.");
        Console.WriteLine("  - Ensure the database exists and your account has access.");
        if (string.Equals(setup.Auth, "sql", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine("  - Check your SQL login username and password.");
        }
        else
        {
            Console.WriteLine("  - For integrated auth, ensure your Windows/AD account has database access.");
        }
        if (!setup.TrustServerCertificate)
        {
            Console.WriteLine("  - If using a self-signed certificate, retry with --trust-server-certificate.");
        }
        Console.WriteLine("  - Run 'sqlct config' to review or update connection settings.");
    }

    private static IReadOnlyList<string> GetNextSteps(InitConnectionTestResult? connectionTest)
    {
        if (connectionTest != null && connectionTest.Success)
        {
            return
            [
                "Run 'sqlct pull' to pull the current database schema into your folder.",
                "Run 'sqlct status' to compare the database against your schema folder.",
                "Run 'sqlct diff' to view schema differences.",
            ];
        }

        return
        [
            "Edit 'sqlct.config.json' to configure your database connection.",
            "Run 'sqlct config' to validate your configuration.",
            "Run 'sqlct pull' to pull the current database schema into your folder.",
        ];
    }

    private static SqlctConfig BuildConfig(ConnectionSetup setup)
    {
        var config = SqlctConfigWriter.CreateDefault();
        config.Database.Server = setup.Server;
        config.Database.Name = setup.Database;
        config.Database.Auth = setup.Auth;
        config.Database.User = setup.User ?? string.Empty;
        config.Database.Password = setup.Password ?? string.Empty;
        config.Database.TrustServerCertificate = setup.TrustServerCertificate;
        return config;
    }

    private static bool ConfirmCurrentDirectory(string displayProjectDir)
    {
        Console.Write($"Initialize project in current directory '{displayProjectDir}'? [y/N]: ");
        var response = Console.ReadLine();
        return string.Equals(response?.Trim(), "y", StringComparison.OrdinalIgnoreCase)
            || string.Equals(response?.Trim(), "yes", StringComparison.OrdinalIgnoreCase);
    }

    private sealed record ConnectionSetup(
        string Server,
        string Database,
        string Auth,
        string? User,
        string? Password,
        bool TrustServerCertificate);
}
