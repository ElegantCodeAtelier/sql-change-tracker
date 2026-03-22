using System.Text.Json;

namespace SqlChangeTracker.Config;

internal sealed class SqlctConfigWriter
{
    public static string GetDefaultPath(string baseDirectory)
        => Path.Combine(baseDirectory, ConfigFileNames.SqlctConfigFileName);

    public static SqlctConfig CreateDefault()
        => new();

    public ConfigWriteResult Write(string configPath, SqlctConfig config, bool overwriteExisting = false)
    {
        try
        {
            var configDirectory = Path.GetDirectoryName(configPath)
                ?? throw new IOException("invalid config path.");
            Directory.CreateDirectory(configDirectory);

            if (File.Exists(configPath) && !overwriteExisting)
            {
                return ConfigWriteResult.Ok(Array.Empty<string>(), new[] { ConfigFileNames.SqlctConfigFileName });
            }

            var options = new JsonSerializerOptions
            {
                WriteIndented = true,
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            };

            var payload = JsonSerializer.Serialize(config, options);
            File.WriteAllText(configPath, payload);

            return ConfigWriteResult.Ok(new[] { ConfigFileNames.SqlctConfigFileName }, Array.Empty<string>());
        }
        catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
        {
            return ConfigWriteResult.Failure(
                new ErrorInfo(ErrorCodes.IoFailed, "failed to write config file.", Detail: ex.Message),
                ExitCodes.ExecutionFailure);
        }
    }
}

internal sealed record ConfigWriteResult(
    bool Success,
    IReadOnlyList<string> Created,
    IReadOnlyList<string> Skipped,
    ErrorInfo? Error,
    int ExitCode)
{
    public static ConfigWriteResult Ok(IReadOnlyList<string> created, IReadOnlyList<string> skipped)
        => new(true, created, skipped, null, ExitCodes.Success);

    public static ConfigWriteResult Failure(ErrorInfo error, int exitCode)
        => new(false, Array.Empty<string>(), Array.Empty<string>(), error, exitCode);
}
