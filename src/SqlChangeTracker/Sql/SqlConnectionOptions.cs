namespace SqlChangeTracker.Sql;

internal sealed record SqlConnectionOptions(
    string Server,
    string Database,
    string Auth,
    string? User,
    string? Password,
    bool TrustServerCertificate);
