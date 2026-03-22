using System.Text.Json;

internal static class LocalFixtureSourceResolver
{
    internal sealed record FixtureSource(string Name, string ReferencePath, string ObjectListPath);

    internal static FixtureSource Resolve(string? sourceName)
    {
        var configPath = FindConfigPath();
        using var document = JsonDocument.Parse(File.ReadAllText(configPath));
        var root = document.RootElement;

        if (root.TryGetProperty("compatExportPath", out _))
        {
            throw new InvalidOperationException(
                "Legacy key 'compatExportPath' is not supported. Migrate local/fixtures.local.json to the source-map schema.");
        }

        if (!root.TryGetProperty("defaultSource", out var defaultSourceNode) ||
            defaultSourceNode.ValueKind != JsonValueKind.String ||
            string.IsNullOrWhiteSpace(defaultSourceNode.GetString()))
        {
            throw new InvalidOperationException($"Missing required key 'defaultSource' in {configPath}");
        }

        if (!root.TryGetProperty("sources", out var sourcesNode) || sourcesNode.ValueKind != JsonValueKind.Object)
        {
            throw new InvalidOperationException($"Missing required object 'sources' in {configPath}");
        }

        var selectedSource = string.IsNullOrWhiteSpace(sourceName)
            ? defaultSourceNode.GetString()!
            : sourceName!;

        if (!sourcesNode.TryGetProperty(selectedSource, out var sourceNode) || sourceNode.ValueKind != JsonValueKind.Object)
        {
            var available = string.Join(", ", sourcesNode.EnumerateObject().Select(prop => prop.Name));
            var suffix = string.IsNullOrWhiteSpace(available) ? "" : $" Available sources: {available}";
            throw new InvalidOperationException($"Unknown source '{selectedSource}'.{suffix}");
        }

        var referencePath = ReadRequiredPath(sourceNode, selectedSource, "referencePath", configPath);
        var objectListPath = ReadRequiredPath(sourceNode, selectedSource, "objectListPath", configPath);
        var repoRoot = Directory.GetParent(Path.GetDirectoryName(configPath)!)!.FullName;

        return new FixtureSource(
            selectedSource,
            ToAbsolutePath(repoRoot, referencePath),
            ToAbsolutePath(repoRoot, objectListPath));
    }

    private static string ReadRequiredPath(JsonElement sourceNode, string sourceName, string key, string configPath)
    {
        if (!sourceNode.TryGetProperty(key, out var valueNode) ||
            valueNode.ValueKind != JsonValueKind.String ||
            string.IsNullOrWhiteSpace(valueNode.GetString()))
        {
            throw new InvalidOperationException($"Missing required key 'sources.{sourceName}.{key}' in {configPath}");
        }

        return valueNode.GetString()!;
    }

    private static string ToAbsolutePath(string repoRoot, string pathValue)
    {
        var absolute = Path.IsPathRooted(pathValue)
            ? pathValue
            : Path.Combine(repoRoot, pathValue);
        return Path.GetFullPath(absolute);
    }

    private static string FindConfigPath()
    {
        var current = Directory.GetCurrentDirectory();
        while (!string.IsNullOrWhiteSpace(current))
        {
            var candidate = Path.Combine(current, "local", "fixtures.local.json");
            if (File.Exists(candidate))
            {
                return Path.GetFullPath(candidate);
            }

            current = Directory.GetParent(current)?.FullName;
        }

        throw new InvalidOperationException("Local fixture config not found. Expected local/fixtures.local.json.");
    }
}
