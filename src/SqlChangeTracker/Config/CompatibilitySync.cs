namespace SqlChangeTracker.Config;

internal sealed class CompatibilitySync
{
    public CompatibilityScanResult Scan(string projectDir)
    {
        try
        {
            return CompatibilityScanResult.Ok(new CompatibilityScan());
        }
        catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
        {
            return CompatibilityScanResult.Failure(
                new ErrorInfo(
                    ErrorCodes.IoFailed,
                    "failed to scan compatibility files.",
                    Detail: ex.Message),
                ExitCodes.ExecutionFailure);
        }
    }
}

internal sealed record CompatibilityScan();

internal sealed record CompatibilityScanResult(
    bool Success,
    CompatibilityScan? Scan,
    ErrorInfo? Error,
    int ExitCode)
{
    public static CompatibilityScanResult Ok(CompatibilityScan scan)
        => new(true, scan, null, ExitCodes.Success);

    public static CompatibilityScanResult Failure(ErrorInfo error, int exitCode)
        => new(false, null, error, exitCode);
}
