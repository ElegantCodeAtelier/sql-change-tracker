using System.Linq;

namespace SqlChangeTracker.Schema;

internal sealed record ObjectIdentifier(string Schema, string Name);

internal sealed class SchemaFolderMapper
{
    private readonly Dictionary<string, string> _folderMap;
    private readonly bool _dataWriteAllFilesInOneDirectory;

    public SchemaFolderMapper(IReadOnlyList<Config.FolderMapEntry> folderMap, bool? dataWriteAllFilesInOneDirectory)
    {
        _folderMap = folderMap
            .Where(entry => !string.IsNullOrWhiteSpace(entry.Key) && !string.IsNullOrWhiteSpace(entry.Value))
            .ToDictionary(entry => entry.Key!, entry => entry.Value!, StringComparer.OrdinalIgnoreCase);
        _dataWriteAllFilesInOneDirectory = dataWriteAllFilesInOneDirectory ?? true;
    }

    public bool TryGetFolder(string objectType, out string folder)
    {
        if (_folderMap.TryGetValue(objectType, out var candidate) && !string.IsNullOrEmpty(candidate))
        {
            folder = candidate;
            return true;
        }

        if (_folderMap.TryGetValue("Default", out candidate) && !string.IsNullOrEmpty(candidate))
        {
            folder = candidate;
            return true;
        }

        folder = string.Empty;
        return false;
    }

    public string GetObjectPath(string objectType, ObjectIdentifier identifier, bool isData)
    {
        if (isData)
        {
            var dataFolder = ResolveDataFolder(objectType);
            return Path.Combine(dataFolder, FormatFileName(identifier, true, includeSchema: true));
        }

        if (!TryGetFolder(objectType, out var folder))
        {
            throw new InvalidOperationException($"No folder mapping defined for object type '{objectType}'.");
        }

        var includeSchema = !SupportedSqlObjectTypes.IsSchemaLess(objectType);
        return Path.Combine(folder, FormatFileName(identifier, false, includeSchema));
    }

    private string ResolveDataFolder(string objectType)
    {
        if (_folderMap.TryGetValue("Data", out var dataFolder))
        {
            return dataFolder;
        }

        if (_dataWriteAllFilesInOneDirectory)
        {
            return "Data";
        }

        return TryGetFolder(objectType, out var folder) ? folder : "Data";
    }

    private static string FormatFileName(ObjectIdentifier identifier, bool isData, bool includeSchema)
    {
        var suffix = isData ? "_Data" : string.Empty;
        var name = EscapeFileNamePart(identifier.Name);
        if (!includeSchema || string.IsNullOrWhiteSpace(identifier.Schema))
        {
            return $"{name}{suffix}.sql";
        }

        var schema = EscapeFileNamePart(identifier.Schema);
        return $"{schema}.{name}{suffix}.sql";
    }

    private static string EscapeFileNamePart(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return value;
        }

        var invalid = new HashSet<char>(Path.GetInvalidFileNameChars());
        var builder = new System.Text.StringBuilder(value.Length);

        foreach (var ch in value)
        {
            if (invalid.Contains(ch))
            {
                builder.Append('%');
                builder.Append(((int)ch).ToString("X2", System.Globalization.CultureInfo.InvariantCulture));
            }
            else
            {
                builder.Append(ch);
            }
        }

        return builder.ToString();
    }
}
