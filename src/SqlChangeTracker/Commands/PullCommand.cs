using Spectre.Console.Cli;
using System.Threading;
using SqlChangeTracker.Config;
using SqlChangeTracker.Output;
using SqlChangeTracker.Progress;
using SqlChangeTracker.Sync;

namespace SqlChangeTracker.Commands;

internal sealed class PullCommand : Command<PullCommandSettings>
{
    internal ISyncCommandService SyncService { get; set; } = new SyncCommandService();

    public override int Execute(CommandContext context, PullCommandSettings settings, CancellationToken cancellationToken)
    {
        var output = new OutputFormatter(settings.Json);
        var showProgress = !settings.Json && !settings.NoProgress;
        var result = ProgressRunner.Run("Running pull...", showProgress,
            progress => SyncService.RunPull(settings.ProjectDir, settings.ObjectSelector, settings.FilterPatterns, progress));
        if (!result.Success)
        {
            output.WriteError(new ErrorResult("pull", result.Error!));
            return result.ExitCode;
        }

        output.WritePull(result.Payload!);
        return result.ExitCode;
    }
}
