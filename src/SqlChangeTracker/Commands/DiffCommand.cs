using Spectre.Console.Cli;
using System.Threading;
using SqlChangeTracker.Config;
using SqlChangeTracker.Output;
using SqlChangeTracker.Sync;

namespace SqlChangeTracker.Commands;

internal sealed class DiffCommand : Command<DiffCommandSettings>
{
    internal ISyncCommandService SyncService { get; set; } = new SyncCommandService();

    public override int Execute(CommandContext context, DiffCommandSettings settings, CancellationToken cancellationToken)
    {
        var output = new OutputFormatter(settings.Json);
        var result = SyncService.RunDiff(settings.ProjectDir, settings.Target, settings.ObjectName);
        if (!result.Success)
        {
            output.WriteError(new ErrorResult("diff", result.Error!));
            return result.ExitCode;
        }

        output.WriteDiff(result.Payload!);
        return result.ExitCode;
    }
}
