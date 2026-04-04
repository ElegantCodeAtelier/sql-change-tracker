using Spectre.Console.Cli;

namespace SqlChangeTracker.Commands;

internal abstract class GlobalSettings : CommandSettings
{
    [CommandOption("--json")]
    public bool Json { get; set; }

    [CommandOption("--verbose")]
    public bool Verbose { get; set; }

    [CommandOption("--no-progress")]
    public bool NoProgress { get; set; }
}

internal class ProjectCommandSettings : GlobalSettings
{
    [CommandOption("--project-dir <PATH>")]
    public string? ProjectDir { get; set; }
}

internal sealed class ConfigCommandSettings : ProjectCommandSettings
{
}

internal sealed class InitCommandSettings : ProjectCommandSettings
{
}

internal class StatusCommandSettings : ProjectCommandSettings
{
    [CommandOption("--target <db|folder>")]
    public string? Target { get; set; }
}

internal sealed class DiffCommandSettings : StatusCommandSettings
{
    [CommandOption("--object <SCHEMA_NAME>")]
    public string? ObjectName { get; set; }
}

internal sealed class PullCommandSettings : ProjectCommandSettings
{
}



