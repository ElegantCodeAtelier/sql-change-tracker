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
    [CommandOption("--server <SERVER>")]
    public string? Server { get; set; }

    [CommandOption("--database <DATABASE>")]
    public string? Database { get; set; }

    [CommandOption("--auth <AUTH>")]
    public string? Auth { get; set; }

    [CommandOption("--user <USER>")]
    public string? User { get; set; }

    [CommandOption("--password <PASSWORD>")]
    public string? Password { get; set; }

    [CommandOption("--trust-server-certificate")]
    public bool TrustServerCertificate { get; set; }

    [CommandOption("--skip-connection-test")]
    public bool SkipConnectionTest { get; set; }
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
    [CommandArgument(0, "<pattern>")]
    public string Pattern { get; set; } = string.Empty;
}

internal sealed class DataUntrackCommandSettings : DataCommandSettings
{
    [CommandArgument(0, "<pattern>")]
    public string Pattern { get; set; } = string.Empty;
}

internal sealed class DataListCommandSettings : DataCommandSettings
{
}



