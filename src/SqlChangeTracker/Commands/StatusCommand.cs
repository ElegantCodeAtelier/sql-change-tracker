using Spectre.Console.Cli;
using System.Threading;
using SqlChangeTracker.Config;
using SqlChangeTracker.Output;
using SqlChangeTracker.Progress;
using SqlChangeTracker.Sync;

namespace SqlChangeTracker.Commands;

internal sealed class StatusCommand : Command<StatusCommandSettings>
{
    internal ISyncCommandService SyncService { get; set; } = new SyncCommandService();

    public override int Execute(CommandContext context, StatusCommandSettings settings, CancellationToken cancellationToken)
    {
        var output = new OutputFormatter(settings.Json);
        var showProgress = !settings.Json && !settings.NoProgress;
        var result = ProgressRunner.Run("Running status...", showProgress,
            progress => SyncService.RunStatus(settings.ProjectDir, settings.Target, progress));
        if (!result.Success)
        {
            output.WriteError(new ErrorResult("status", result.Error!));
            return result.ExitCode;
        }

        output.WriteStatus(result.Payload!);
        return result.ExitCode;
    }
}
