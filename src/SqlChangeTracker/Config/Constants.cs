namespace SqlChangeTracker.Config;

internal static class ExitCodes
{
    public const int Success = 0;
    public const int DiffExists = 1;
    public const int InvalidConfig = 2;
    public const int ConnectionFailure = 3;
    public const int ExecutionFailure = 4;
}

internal static class ConfigFileNames
{
    public const string SqlctConfigFileName = "sqlct.config.yaml";
    public const string SqlctConfigLegacyFileName = "sqlct.config.json";
}

internal static class ErrorCodes
{
    public const string MissingLink = "missing_link";
    public const string InvalidConfig = "invalid_config";
    public const string IoFailed = "io_failed";
    public const string ConnectionFailed = "connection_failed";
    public const string DatabaseNotFound = "database_not_found";
    public const string ExecutionFailed = "execution_failed";
    public const string ConfirmationRequired = "confirmation_required";
    public const string NotImplemented = "not_implemented";
}
