using Spectre.Console.Cli;
using System.Threading;
using SqlChangeTracker.Config;
using SqlChangeTracker.Data;
using SqlChangeTracker.Output;

namespace SqlChangeTracker.Commands;

internal sealed class DataTrackCommand : Command<DataTrackCommandSettings>
{
    internal IDataTrackingService DataTrackingService { get; set; } = new DataTrackingService();

    public override int Execute(CommandContext context, DataTrackCommandSettings settings, CancellationToken cancellationToken)
    {
        var output = new OutputFormatter(settings.Json);
        var prepare = DataTrackingService.PrepareTrack(settings.ProjectDir, settings.Pattern);
        if (!prepare.Success)
        {
            output.WriteError(new ErrorResult("data track", prepare.Error!));
            return prepare.ExitCode;
        }

        var plan = prepare.Payload!;
        if (plan.MatchedTables.Count == 0)
        {
            output.WriteDataTrack(new DataTrackResult(
                "data track",
                plan.ProjectDisplayPath,
                plan.Pattern,
                false,
                false,
                plan.MatchedTables,
                plan.CurrentTrackedTables));
            return ExitCodes.Success;
        }

        var confirmed = ConfirmationPrompt.WritePreviewAndConfirm(
            settings.Json,
            "Matching tables:",
            plan.MatchedTables,
            "Track these tables? [y/N]: ");
        if (confirmed == null)
        {
            output.WriteError(new ErrorResult(
                "data track",
                new ErrorInfo(
                    ErrorCodes.ConfirmationRequired,
                    "confirmation required.",
                    Detail: "run interactively or provide confirmation on stdin.")));
            return ExitCodes.ExecutionFailure;
        }

        if (!confirmed.Value)
        {
            output.WriteDataTrack(new DataTrackResult(
                "data track",
                plan.ProjectDisplayPath,
                plan.Pattern,
                false,
                true,
                plan.MatchedTables,
                plan.CurrentTrackedTables));
            return ExitCodes.Success;
        }

        var apply = DataTrackingService.ApplyTrack(plan);
        if (!apply.Success)
        {
            output.WriteError(new ErrorResult("data track", apply.Error!));
            return apply.ExitCode;
        }

        output.WriteDataTrack(apply.Payload!);
        return ExitCodes.Success;
    }
}
