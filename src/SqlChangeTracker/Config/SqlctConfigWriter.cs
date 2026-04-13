using YamlDotNet.Serialization;
using YamlDotNet.Serialization.NamingConventions;

namespace SqlChangeTracker.Config;

internal sealed class SqlctConfigWriter
{
    private const string HeaderComment =
        "# SQL Change Tracker (sqlct)\n" +
        "# https://github.com/ElegantCodeAtelier/sql-change-tracker\n" +
        "#\n" +
        "# Installation:\n" +
        "#   dotnet tool install --global sqlct\n" +
        "#\n" +
        "# Update:\n" +
        "#   dotnet tool update --global sqlct\n" +
        "#\n" +
        "# Usage:\n" +
        "#   sqlct --help   - print help\n" +
        "#   sqlct init     - initialize this project\n" +
        "#   sqlct config   - validate and rewrite configuration\n" +
        "#   sqlct status   - compare database against schema folder\n" +
        "#   sqlct diff     - show textual schema differences\n" +
        "#   sqlct pull     - pull database schema into folder\n" +
        "#\n";

    public static string GetDefaultPath(string baseDirectory)
        => Path.Combine(baseDirectory, ConfigFileNames.SqlctConfigFileName);

    public static SqlctConfig CreateDefault()
        => new();

    public ConfigWriteResult Write(string configPath, SqlctConfig config, bool overwriteExisting = false)
    {
        try
        {
            SqlctConfigNormalizer.Normalize(config);
            var configDirectory = Path.GetDirectoryName(configPath)
                ?? throw new IOException("invalid config path.");
            Directory.CreateDirectory(configDirectory);

            if (File.Exists(configPath) && !overwriteExisting)
            {
                return ConfigWriteResult.Ok(Array.Empty<string>(), new[] { ConfigFileNames.SqlctConfigFileName });
            }

            var serializer = new SerializerBuilder()
                .WithNamingConvention(CamelCaseNamingConvention.Instance)
                .Build();

            var yaml = serializer.Serialize(config);
            var payload = HeaderComment + yaml;
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
