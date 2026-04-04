using Spectre.Console;

namespace SqlChangeTracker.Progress;

internal static class ProgressRunner
{
    internal static T Run<T>(string statusMessage, bool showProgress, Func<Action<string>?, T> action)
    {
        var isInteractive = AnsiConsole.Profile.Capabilities.Ansi && !Console.IsOutputRedirected;
        if (!showProgress || !isInteractive)
            return action(null);

        T? result = default;
        AnsiConsole.Status()
            .Start(statusMessage, ctx =>
            {
                result = action(status => { ctx.Status = status; });
            });
        return result!;
    }
}
