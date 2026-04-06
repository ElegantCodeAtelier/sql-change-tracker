namespace SqlChangeTracker;

internal static class ProjectPathResolver
{
    public static ResolvedPath Resolve(string? projectDir)
    {
        var input = string.IsNullOrWhiteSpace(projectDir)
            ? Environment.CurrentDirectory
            : NormalizeQuotedPath(projectDir!);
        var fullPath = Path.GetFullPath(input, Environment.CurrentDirectory);
        var displayPath = NormalizeDisplayPath(fullPath, input);
        return new ResolvedPath(fullPath, displayPath);
    }

    public static string NormalizeDisplayPath(string fullPath, string originalInput)
    {
        var normalizedInput = NormalizeQuotedPath(originalInput);
        if (Path.IsPathRooted(normalizedInput))
        {
            return fullPath;
        }

        var relative = Path.GetRelativePath(Environment.CurrentDirectory, fullPath);
        if (relative.StartsWith(".", StringComparison.Ordinal))
        {
            return relative;
        }

        var prefix = Path.DirectorySeparatorChar == '\\' ? ".\\" : "./";
        return prefix + relative;
    }

    private static string NormalizeQuotedPath(string path)
    {
        var normalized = path;

        // Windows native argument parsing can leave a stray double-quote
        // when a quoted path ends with a trailing backslash.
        if (OperatingSystem.IsWindows())
        {
            normalized = normalized.Replace("\"", string.Empty, StringComparison.Ordinal);
        }

        return UnwrapOuterQuotes(normalized);
    }

    private static string UnwrapOuterQuotes(string path)
    {
        if (path.Length >= 2)
        {
            var first = path[0];
            var last = path[^1];
            if ((first == '"' && last == '"') || (first == '\'' && last == '\''))
            {
                return path[1..^1];
            }
        }

        return path;
    }
}

internal sealed record ResolvedPath(string FullPath, string DisplayPath);
