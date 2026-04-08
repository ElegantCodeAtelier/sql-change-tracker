using Spectre.Console.Cli;
using System.Threading;
using SqlChangeTracker.Config;
using SqlChangeTracker.Output;
using SqlChangeTracker.Progress;
using SqlChangeTracker.Sync;

namespace SqlChangeTracker.Commands;

internal sealed class DiffCommand : Command<DiffCommandSettings>
{
    internal ISyncCommandService SyncService { get; set; } = new SyncCommandService();

    public override int Execute(CommandContext context, DiffCommandSettings settings, CancellationToken cancellationToken)
    {
        var output = new OutputFormatter(settings.Json);
        var showProgress = !settings.Json && !settings.NoProgress;
        var result = ProgressRunner.Run("Running diff...", showProgress,
            progress => SyncService.RunDiff(settings.ProjectDir, settings.Target, settings.ObjectSelector, settings.FilterPatterns, settings.ContextLines ?? 3, progress));
        if (!result.Success)
        {
            output.WriteError(new ErrorResult("diff", result.Error!));
            return result.ExitCode;
        }

        output.WriteDiff(result.Payload!);
        return result.ExitCode;
    }
}
