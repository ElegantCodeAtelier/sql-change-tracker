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
        });

        return app.Run(args);
    }
}
