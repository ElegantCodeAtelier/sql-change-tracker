namespace SqlChangeTracker.Config;

internal sealed class BaselineProjectSeeder
{
    private static readonly string[] RequiredFolders =
    {
        "Data",
        "Functions",
        "Security",
        Path.Combine("Security", "Roles"),
        Path.Combine("Security", "Schemas"),
        Path.Combine("Security", "Users"),
        "Sequences",
        "Storage",
        Path.Combine("Storage", "Partition Functions"),
        Path.Combine("Storage", "Partition Schemes"),
        "Stored Procedures",
        "Tables",
        "Views"
    };

    public SeedResult Seed(string projectDir)
    {
        try
        {
            Directory.CreateDirectory(projectDir);
            var created = new List<string>();
            var skipped = new List<string>();

            foreach (var relativePath in RequiredFolders)
            {
                var fullPath = Path.Combine(projectDir, relativePath);
                if (Directory.Exists(fullPath))
                {
                    skipped.Add(relativePath);
                    continue;
                }

                Directory.CreateDirectory(fullPath);
                created.Add(relativePath);
            }

            return SeedResult.Ok(created, skipped);
        }
        catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
        {
            return SeedResult.Failure(
                new ErrorInfo(
                    ErrorCodes.IoFailed,
                    "failed to initialize project folders.",
                    Detail: ex.Message),
                ExitCodes.ExecutionFailure);
        }
    }
}

internal sealed record SeedResult(
    bool Success,
    IReadOnlyList<string> Created,
    IReadOnlyList<string> Skipped,
    ErrorInfo? Error,
    int ExitCode)
{
    public static SeedResult Ok(IReadOnlyList<string> created, IReadOnlyList<string> skipped)
        => new(true, created, skipped, null, ExitCodes.Success);

    public static SeedResult Failure(ErrorInfo error, int exitCode)
        => new(false, Array.Empty<string>(), Array.Empty<string>(), error, exitCode);
}
