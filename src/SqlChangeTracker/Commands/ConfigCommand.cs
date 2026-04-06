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
        var projectDir = ProjectPathResolver.Resolve(settings.ProjectDir);
        var configPath = SqlctConfigWriter.GetDefaultPath(projectDir.FullPath);

        var reader = new SqlctConfigReader();
        SqlctConfig config;
        var readResult = reader.Read(configPath);
        if (!readResult.Success)
        {
            if (readResult.Error?.Code == ErrorCodes.MissingLink)
            {
                config = SqlctConfigWriter.CreateDefault();
            }
            else
            {
                output.WriteError(new ErrorResult("config", readResult.Error!));
                return readResult.ExitCode;
            }
        }
        else
        {
            config = readResult.Config!;
        }

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
            ProjectPathResolver.NormalizeDisplayPath(configPath, configPath),
            config,
            scanResult.Scan!));

        return ExitCodes.Success;
    }
}
