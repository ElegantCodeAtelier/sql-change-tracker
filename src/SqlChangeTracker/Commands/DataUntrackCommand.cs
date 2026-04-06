using Spectre.Console.Cli;
using System.Threading;
using SqlChangeTracker.Config;
using SqlChangeTracker.Data;
using SqlChangeTracker.Output;

namespace SqlChangeTracker.Commands;

internal sealed class DataUntrackCommand : Command<DataUntrackCommandSettings>
{
    internal IDataTrackingService DataTrackingService { get; set; } = new DataTrackingService();

    public override int Execute(CommandContext context, DataUntrackCommandSettings settings, CancellationToken cancellationToken)
    {
        var output = new OutputFormatter(settings.Json);
        var prepare = DataTrackingService.PrepareUntrack(settings.ProjectDir, settings.Pattern);
        if (!prepare.Success)
        {
            output.WriteError(new ErrorResult("data untrack", prepare.Error!));
            return prepare.ExitCode;
        }

        var plan = prepare.Payload!;
        if (plan.MatchedTables.Count == 0)
        {
            output.WriteDataUntrack(new DataUntrackResult(
                "data untrack",
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
            "Matching tracked tables:",
            plan.MatchedTables,
            "Untrack these tables? [y/N]: ");
        if (confirmed == null)
        {
            output.WriteError(new ErrorResult(
                "data untrack",
                new ErrorInfo(
                    ErrorCodes.ConfirmationRequired,
                    "confirmation required.",
                    Detail: "run interactively or provide confirmation on stdin.")));
            return ExitCodes.ExecutionFailure;
        }

        if (!confirmed.Value)
        {
            output.WriteDataUntrack(new DataUntrackResult(
                "data untrack",
                plan.ProjectDisplayPath,
                plan.Pattern,
                false,
                true,
                plan.MatchedTables,
                plan.CurrentTrackedTables));
            return ExitCodes.Success;
        }

        var apply = DataTrackingService.ApplyUntrack(plan);
        if (!apply.Success)
        {
            output.WriteError(new ErrorResult("data untrack", apply.Error!));
            return apply.ExitCode;
        }

        output.WriteDataUntrack(apply.Payload!);
        return ExitCodes.Success;
    }
}
