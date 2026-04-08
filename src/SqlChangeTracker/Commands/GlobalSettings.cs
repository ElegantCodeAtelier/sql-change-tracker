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
    [CommandOption("--object <SELECTOR>")]
    public string? ObjectSelector { get; set; }

    [CommandOption("--filter <PATTERN>")]
    public string[]? FilterPatterns { get; set; }

    [CommandOption("--context <N>")]
    public int? ContextLines { get; set; }
}

internal sealed class PullCommandSettings : ProjectCommandSettings
{
    [CommandOption("--object <SELECTOR>")]
    public string? ObjectSelector { get; set; }

    [CommandOption("--filter <PATTERN>")]
    public string[]? FilterPatterns { get; set; }
}

internal class DataBranchSettings : CommandSettings
{
}

internal class DataCommandSettings : DataBranchSettings
{
    [CommandOption("--json")]
    public bool Json { get; set; }

    [CommandOption("--verbose")]
    public bool Verbose { get; set; }

    [CommandOption("--no-progress")]
    public bool NoProgress { get; set; }

    [CommandOption("--project-dir <PATH>")]
    public string? ProjectDir { get; set; }
}

internal sealed class DataTrackCommandSettings : DataCommandSettings
{
    [CommandArgument(0, "[pattern]")]
    public string? Pattern { get; set; }

    [CommandOption("--object <PATTERN>")]
    public string? ObjectPattern { get; set; }

    [CommandOption("--filter <PATTERN>")]
    public string? FilterPattern { get; set; }
}

internal sealed class DataUntrackCommandSettings : DataCommandSettings
{
    [CommandArgument(0, "[pattern]")]
    public string? Pattern { get; set; }

    [CommandOption("--object <PATTERN>")]
    public string? ObjectPattern { get; set; }

    [CommandOption("--filter <PATTERN>")]
    public string? FilterPattern { get; set; }
}

internal sealed class DataListCommandSettings : DataCommandSettings
{
}



