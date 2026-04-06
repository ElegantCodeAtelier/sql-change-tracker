using System.Text.Json;

namespace SqlChangeTracker.Config;

internal sealed class SqlctConfigReader
{
    public SqlctConfigReadResult Read(string configPath)
    {
        if (!File.Exists(configPath))
        {
            return SqlctConfigReadResult.Failure(
                new ErrorInfo(
                    ErrorCodes.MissingLink,
                    "no linked schema folder found.",
                    File: configPath,
                    Detail: $"expected config file at '{configPath}'.",
                    Hint: "run `sqlct init` or `sqlct config`."),
                ExitCodes.InvalidConfig);
        }

        try
        {
            var json = File.ReadAllText(configPath);
            var config = JsonSerializer.Deserialize<SqlctConfig>(json, new JsonSerializerOptions
            {
                PropertyNameCaseInsensitive = true,
                ReadCommentHandling = JsonCommentHandling.Skip,
                AllowTrailingCommas = true
            });

            if (config == null)
            {
                return SqlctConfigReadResult.Failure(
                    new ErrorInfo(ErrorCodes.InvalidConfig, "invalid config file.", Detail: "config was empty."),
                    ExitCodes.InvalidConfig);
            }

            SqlctConfigNormalizer.Normalize(config);
            return SqlctConfigReadResult.Ok(config);
        }
        catch (JsonException ex)
        {
            return SqlctConfigReadResult.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid config file.", Detail: $"invalid JSON: {ex.Message}"),
                ExitCodes.InvalidConfig);
        }
        catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
        {
            return SqlctConfigReadResult.Failure(
                new ErrorInfo(ErrorCodes.IoFailed, "failed to read config file.", Detail: ex.Message),
                ExitCodes.ExecutionFailure);
        }
    }
}

internal sealed record SqlctConfigReadResult(
    bool Success,
    SqlctConfig? Config,
    ErrorInfo? Error,
    int ExitCode)
{
    public static SqlctConfigReadResult Ok(SqlctConfig config)
        => new(true, config, null, ExitCodes.Success);

    public static SqlctConfigReadResult Failure(ErrorInfo error, int exitCode)
        => new(false, null, error, exitCode);
}

