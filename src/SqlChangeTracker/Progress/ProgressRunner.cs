using Spectre.Console;

namespace SqlChangeTracker.Progress;

internal static class ProgressRunner
{
    internal static T Run<T>(string statusMessage, bool showProgress, Func<T> action)
    {
        var isInteractive = AnsiConsole.Profile.Capabilities.Ansi && !Console.IsOutputRedirected;
        if (!showProgress || !isInteractive)
            return action();

        T? result = default;
        AnsiConsole.Status()
            .Start(statusMessage, _ => { result = action(); });
        return result!;
    }
}
