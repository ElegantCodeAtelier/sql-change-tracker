using Spectre.Console.Cli;
using System.Threading;
using SqlChangeTracker.Config;
using SqlChangeTracker.Output;

namespace SqlChangeTracker.Commands;

internal sealed class ConfigCommand : Command<ConfigCommandSettings>
{
    public override int Execute(CommandContext context, ConfigCommandSettings settings, CancellationToken cancellationToken)
    {
        var output = new OutputFormatter(settings.Json);
        var projectDir = ResolveProjectDir(settings.ProjectDir);
        var configPath = SqlctConfigWriter.GetDefaultPath(projectDir.FullPath);

        var reader = new SqlctConfigReader();
        var readResult = reader.Read(configPath);
        if (!readResult.Success)
        {
            if (readResult.Error?.Code == ErrorCodes.MissingLink)
            {
                output.WriteError(new ErrorResult("config", new ErrorInfo(
                    ErrorCodes.InvalidConfig,
                    "project directory is not initialized.",
                    Hint: "run `sqlct init` first.")));
                return ExitCodes.InvalidConfig;
            }
            else
            {
                output.WriteError(new ErrorResult("config", readResult.Error!));
                return readResult.ExitCode;
            }
        }

        var config = readResult.Config!;

        var compatibilitySync = new CompatibilitySync();
        var scanResult = compatibilitySync.Scan(projectDir.FullPath);
        if (!scanResult.Success)
        {
            output.WriteError(new ErrorResult("config", scanResult.Error!));
            return scanResult.ExitCode;
        }

        var writer = new SqlctConfigWriter();
        var writeResult = writer.Write(configPath, config, overwriteExisting: true);
        if (!writeResult.Success)
        {
            output.WriteError(new ErrorResult("config", writeResult.Error!));
            return writeResult.ExitCode;
        }

        output.WriteConfig(new ConfigResult(
            "config",
            projectDir.DisplayPath,
            true,
            Array.Empty<ConfigError>(),
            NormalizeDisplayPath(configPath, configPath),
            config,
            scanResult.Scan!));

        return ExitCodes.Success;
    }

    private static ResolvedPath ResolveProjectDir(string? projectDir)
    {
        var input = string.IsNullOrWhiteSpace(projectDir) ? Environment.CurrentDirectory : projectDir;
        var fullPath = Path.GetFullPath(input!, Environment.CurrentDirectory);
        var displayPath = NormalizeDisplayPath(fullPath, input!);
        return new ResolvedPath(fullPath, displayPath);
    }

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

    private sealed record ResolvedPath(string FullPath, string DisplayPath);
}
