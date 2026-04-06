using System.Reflection;
using Spectre.Console.Cli;
using SqlChangeTracker.Commands;

namespace SqlChangeTracker;

internal static class Program
{
    public static int Main(string[] args)
    {
        if (args.Any(arg =>
            string.Equals(arg, "--config", StringComparison.OrdinalIgnoreCase)
            || arg.StartsWith("--config=", StringComparison.OrdinalIgnoreCase)))
        {
            Console.WriteLine("Error: --config is not supported. Use --project-dir.");
            return Config.ExitCodes.InvalidConfig;
        }

        var app = new CommandApp();
        app.Configure(config =>
        {
            config.SetApplicationName("sqlct");
            config.SetApplicationVersion(GetCleanVersion());
            config.AddCommand<InitCommand>("init")
                .WithDescription("Initialize project configuration and schema folder structure.");
            config.AddCommand<ConfigCommand>("config")
                .WithDescription("Parse, validate, and write project configuration.");
            config.AddCommand<StatusCommand>("status")
                .WithDescription("Show object-level differences.");
            config.AddCommand<DiffCommand>("diff")
                .WithDescription("Show textual diffs.");
            config.AddCommand<PullCommand>("pull")
                .WithDescription("Write database changes into the schema folder.");
            config.AddBranch<DataBranchSettings>("data", data =>
            {
                data.SetDescription("Manage tracked tables for selective data scripting.");
                data.AddCommand<DataTrackCommand>("track")
                    .WithDescription("Track tables for data scripting.");
                data.AddCommand<DataUntrackCommand>("untrack")
                    .WithDescription("Stop tracking tables for data scripting.");
                data.AddCommand<DataListCommand>("list")
                    .WithDescription("List tracked tables.");
            });
        });

        return app.Run(args);
    }

    private static string GetCleanVersion()
    {
        var raw = typeof(Program).Assembly
            .GetCustomAttribute<AssemblyInformationalVersionAttribute>()
            ?.InformationalVersion ?? "unknown";
        var plus = raw.IndexOf('+');
        return plus >= 0 ? raw[..plus] : raw;
    }
}
