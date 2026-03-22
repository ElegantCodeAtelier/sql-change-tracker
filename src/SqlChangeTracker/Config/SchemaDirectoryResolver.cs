namespace SqlChangeTracker.Config;

internal sealed record ResolvedSchemaDir(string FullPath, string DisplayPath);

internal sealed class SchemaDirectoryResolver
{
    public ResolveSchemaDirResult Resolve(string? configPath, string? schemaDirOverride)
    {
        if (!string.IsNullOrWhiteSpace(schemaDirOverride))
        {
            var fullPath = Path.GetFullPath(schemaDirOverride, Environment.CurrentDirectory);
            var displayPath = NormalizeDisplayPath(fullPath, schemaDirOverride);
            return ResolveSchemaDirResult.Ok(new ResolvedSchemaDir(fullPath, displayPath));
        }

        var resolvedConfigPath = ResolveConfigPath(configPath);
        if (!File.Exists(resolvedConfigPath))
        {
            return ResolveSchemaDirResult.Failure(
                new ErrorInfo(
                    ErrorCodes.MissingLink,
                    "no linked schema folder found.",
                    Hint: "run `sqlct init` or `sqlct config`, or pass `--project-dir`."),
                ExitCodes.InvalidConfig);
        }

        try
        {
            _ = File.ReadAllText(resolvedConfigPath);
            var fullPath = Path.GetDirectoryName(resolvedConfigPath) ?? Environment.CurrentDirectory;
            var displayPath = NormalizeDisplayPath(fullPath, fullPath);
            return ResolveSchemaDirResult.Ok(new ResolvedSchemaDir(fullPath, displayPath));
        }
        catch (IOException ex)
        {
            return ResolveSchemaDirResult.Failure(
                new ErrorInfo(ErrorCodes.IoFailed, "failed to read config file.", Detail: ex.Message),
                ExitCodes.ExecutionFailure);
        }
    }

    private static string ResolveConfigPath(string? configPath)
    {
        if (!string.IsNullOrWhiteSpace(configPath))
        {
            return Path.GetFullPath(configPath, Environment.CurrentDirectory);
        }

        return Path.GetFullPath(Path.Combine(Environment.CurrentDirectory, ConfigFileNames.SqlctConfigFileName));
    }

    private static string NormalizeDisplayPath(string fullPath, string originalInput)
    {
        if (Path.IsPathRooted(originalInput))
        {
            return fullPath;
        }

        var relative = Path.GetRelativePath(Environment.CurrentDirectory, fullPath);
        if (relative.StartsWith("."))
        {
            return relative;
        }

        var prefix = Path.DirectorySeparatorChar == '\\' ? ".\\" : "./";
        return prefix + relative;
    }
}

internal sealed record ResolveSchemaDirResult(
    bool Success,
    ResolvedSchemaDir? SchemaDir,
    ErrorInfo? Error,
    int ExitCode)
{
    public static ResolveSchemaDirResult Ok(ResolvedSchemaDir schemaDir)
        => new(true, schemaDir, null, ExitCodes.Success);

    public static ResolveSchemaDirResult Failure(ErrorInfo error, int exitCode)
        => new(false, null, error, exitCode);
}

