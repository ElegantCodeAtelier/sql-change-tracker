using SqlChangeTracker.Config;

namespace SqlChangeTracker.Schema;

internal sealed record SupportedSqlObjectType(
    string ObjectType,
    string RelativeFolder,
    bool IsSchemaLess,
    bool ActiveInSync);

internal static class SupportedSqlObjectTypes
{
    private const string DefaultFolder = "Other";

    public static IReadOnlyList<SupportedSqlObjectType> All { get; } =
    [
        new("Assembly", "Assemblies", true, true),
        new("Table", "Tables", false, true),
        new("View", "Views", false, true),
        new("StoredProcedure", "Stored Procedures", false, true),
        new("Function", "Functions", false, true),
        new("Aggregate", "Functions", false, true),
        new("Sequence", "Sequences", false, true),
        new("Schema", Path.Combine("Security", "Schemas"), true, true),
        new("Role", Path.Combine("Security", "Roles"), true, true),
        new("User", Path.Combine("Security", "Users"), true, true),
        new("XmlSchemaCollection", Path.Combine("Types", "XML Schema Collections"), false, true),
        new("MessageType", Path.Combine("Service Broker", "Message Types"), true, true),
        new("Contract", Path.Combine("Service Broker", "Contracts"), true, true),
        new("EventNotification", Path.Combine("Service Broker", "Event Notifications"), true, true),
        new("Queue", Path.Combine("Service Broker", "Queues"), false, true),
        new("ServiceBinding", Path.Combine("Service Broker", "Remote Service Bindings"), true, true),
        new("Service", Path.Combine("Service Broker", "Services"), true, true),
        new("Route", Path.Combine("Service Broker", "Routes"), true, true),
        new("PartitionFunction", Path.Combine("Storage", "Partition Functions"), true, true),
        new("PartitionScheme", Path.Combine("Storage", "Partition Schemes"), true, true),
        new("FullTextCatalog", Path.Combine("Storage", "Full Text Catalogs"), true, true),
        new("FullTextStoplist", Path.Combine("Storage", "Full Text Stoplists"), true, true),
        new("SearchPropertyList", Path.Combine("Storage", "Search Property Lists"), true, true),
        new("Synonym", "Synonyms", false, true),
        new("UserDefinedType", Path.Combine("Types", "User-defined Data Types"), false, true)
    ];

    public static IReadOnlyList<SupportedSqlObjectType> ActiveSync { get; } =
        All.Where(entry => entry.ActiveInSync).ToArray();

    public static IReadOnlyList<string> ActiveSyncObjectTypes { get; } =
        ActiveSync.Select(entry => entry.ObjectType).ToArray();

    public static IReadOnlyList<string> ActiveSyncFolders { get; } =
        ActiveSync.Select(entry => entry.RelativeFolder).ToArray();

    public static IReadOnlyList<FolderMapEntry> DefaultFolderMap { get; } =
        ActiveSync
            .Select(entry => new FolderMapEntry(entry.ObjectType, entry.RelativeFolder))
            .Concat([new FolderMapEntry("Default", DefaultFolder)])
            .ToArray();

    public static IReadOnlyList<string> RequiredProjectFolders { get; } = BuildRequiredProjectFolders();

    public static bool TryGet(string objectType, out SupportedSqlObjectType entry)
    {
        var match = ActiveSync.FirstOrDefault(candidate =>
            string.Equals(candidate.ObjectType, objectType, StringComparison.OrdinalIgnoreCase));
        if (match is null)
        {
            entry = null!;
            return false;
        }

        entry = match;
        return true;
    }

    public static bool IsSchemaLess(string objectType)
        => TryGet(objectType, out var entry) && entry.IsSchemaLess;

    public static bool IsActiveInSync(string objectType)
        => TryGet(objectType, out _);

    private static IReadOnlyList<string> BuildRequiredProjectFolders()
    {
        var folders = new List<string> { "Data" };
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        seen.Add("Data");

        foreach (var entry in ActiveSync)
        {
            AddFolderWithParents(entry.RelativeFolder, folders, seen);
        }

        return folders;
    }

    private static void AddFolderWithParents(string relativeFolder, List<string> folders, HashSet<string> seen)
    {
        var current = relativeFolder;
        var segments = new Stack<string>();
        while (!string.IsNullOrEmpty(current))
        {
            segments.Push(current);
            current = Path.GetDirectoryName(current);
        }

        while (segments.Count > 0)
        {
            var candidate = segments.Pop();
            if (seen.Add(candidate))
            {
                folders.Add(candidate);
            }
        }
    }
}
