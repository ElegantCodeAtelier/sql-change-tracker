namespace SqlChangeTracker.Config;

internal sealed class SqlctConfig
{
    public DatabaseConfig Database { get; set; } = new();

    public OptionsConfig Options { get; set; } = new();
}

internal sealed class DatabaseConfig
{
    public string Server { get; set; } = string.Empty;

    public string Name { get; set; } = string.Empty;

    public string Auth { get; set; } = "integrated";

    public string User { get; set; } = string.Empty;

    public string Password { get; set; } = string.Empty;

    public bool TrustServerCertificate { get; set; }
}

internal sealed class OptionsConfig
{
    public bool OrderByDependencies { get; set; } = true;
}
