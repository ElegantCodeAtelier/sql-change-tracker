using Spectre.Console.Cli;
using System.Threading;
using SqlChangeTracker.Config;
using SqlChangeTracker.Output;

namespace SqlChangeTracker.Commands;

internal sealed class InitCommand : Command<InitCommandSettings>
{
    public override int Execute(CommandContext context, InitCommandSettings settings, CancellationToken cancellationToken)
    {
        var output = new OutputFormatter(settings.Json);
        var projectDirFromCurrentDirectory = string.IsNullOrWhiteSpace(settings.ProjectDir);
        var projectDirInput = projectDirFromCurrentDirectory ? Environment.CurrentDirectory : settings.ProjectDir!;
        var projectDir = ResolveSchemaDir(projectDirInput);
        var displayProjectDir = NormalizeDisplayPath(projectDir, projectDirInput);

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

        var config = SqlctConfigWriter.CreateDefault();
        var configWriter = new SqlctConfigWriter();
        var configPath = SqlctConfigWriter.GetDefaultPath(projectDir);
        var configResult = configWriter.Write(configPath, config);
        if (!configResult.Success)
        {
            output.WriteError(new ErrorResult("init", configResult.Error!));
            return configResult.ExitCode;
        }

        var created = projectSeedResult.Created.Concat(configResult.Created).ToList();
        var skipped = projectSeedResult.Skipped.Concat(configResult.Skipped).ToList();
        var result = new InitResult("init", displayProjectDir, created, skipped);
        output.WriteInit(result);
        return ExitCodes.Success;
    }

    private static bool ConfirmCurrentDirectory(string displayProjectDir)
    {
        Console.Write($"Initialize project in current directory '{displayProjectDir}'? [y/N]: ");
        var response = Console.ReadLine();
        return string.Equals(response?.Trim(), "y", StringComparison.OrdinalIgnoreCase)
            || string.Equals(response?.Trim(), "yes", StringComparison.OrdinalIgnoreCase);
    }

    private static string ResolveSchemaDir(string schemaDir)
        => Path.GetFullPath(schemaDir, Environment.CurrentDirectory);

    private static string NormalizeDisplayPath(string fullPath, string originalInput)
    {
        if (Path.IsPathRooted(originalInput))
        {
            return fullPath;
        }

        var relative = Path.GetRelativePath(Environment.CurrentDirectory, fullPath);
        if (relative.StartsWith("."))
        {
            return relative;
        }

        var prefix = Path.DirectorySeparatorChar == '\\' ? ".\\" : "./";
        return prefix + relative;
    }
}
