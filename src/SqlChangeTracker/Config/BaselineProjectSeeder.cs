using SqlChangeTracker.Schema;

namespace SqlChangeTracker.Config;

internal sealed class BaselineProjectSeeder
{
    public SeedResult Seed(string projectDir)
    {
        try
        {
            Directory.CreateDirectory(projectDir);
            var created = new List<string>();
            var skipped = new List<string>();

            foreach (var relativePath in SupportedSqlObjectTypes.RequiredProjectFolders)
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
