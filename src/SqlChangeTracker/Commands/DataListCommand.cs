using Spectre.Console.Cli;
using System.Threading;
using SqlChangeTracker.Config;
using SqlChangeTracker.Data;
using SqlChangeTracker.Output;

namespace SqlChangeTracker.Commands;

internal sealed class DataListCommand : Command<DataListCommandSettings>
{
    internal IDataTrackingService DataTrackingService { get; set; } = new DataTrackingService();

    public override int Execute(CommandContext context, DataListCommandSettings settings, CancellationToken cancellationToken)
    {
        var output = new OutputFormatter(settings.Json);
        var result = DataTrackingService.RunList(settings.ProjectDir);
        if (!result.Success)
        {
            output.WriteError(new ErrorResult("data list", result.Error!));
            return result.ExitCode;
        }

        output.WriteDataList(result.Payload!);
        return ExitCodes.Success;
    }
}
