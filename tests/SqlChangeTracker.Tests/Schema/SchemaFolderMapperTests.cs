using SqlChangeTracker.Config;
using SqlChangeTracker.Schema;
using Xunit;

namespace SqlChangeTracker.Tests.Schema;

public sealed class SchemaFolderMapperTests
{
    [Fact]
    public void Maps_AllConfiguredEntries()
    {
        var folderMap = BuildFolderMap();
        var mapper = new SchemaFolderMapper(folderMap, dataWriteAllFilesInOneDirectory: true);

        foreach (var entry in folderMap)
        {
            Assert.True(mapper.TryGetFolder(entry.Key, out var folder));
            Assert.Equal(entry.Value, folder);
        }
    }

    [Fact]
    public void Formats_ObjectAndDataPaths()
    {
        var mapper = new SchemaFolderMapper(BuildFolderMap(), dataWriteAllFilesInOneDirectory: true);
        var identifier = new ObjectIdentifier("dbo", "Customer");

        var tablePath = mapper.GetObjectPath("Table", identifier, false);
        var dataPath = mapper.GetObjectPath("Table", identifier, true);

        Assert.Equal(Path.Combine("Tables", "dbo.Customer.sql"), tablePath);
        Assert.Equal(Path.Combine("Data", "dbo.Customer_Data.sql"), dataPath);
    }

    [Theory]
    [InlineData("Assembly", "Assemblies", "AppClr")]
    [InlineData("Schema", "Security\\Schemas", "AppSecurity")]
    [InlineData("Role", "Security\\Roles", "AppReader")]
    [InlineData("User", "Security\\Users", "ServiceUser")]
    [InlineData("MessageType", "Service Broker\\Message Types", "//App/Messaging/Request")]
    [InlineData("Contract", "Service Broker\\Contracts", "//App/Messaging/Contract")]
    [InlineData("EventNotification", "Service Broker\\Event Notifications", "NotifySchemaChanges")]
    [InlineData("ServiceBinding", "Service Broker\\Remote Service Bindings", "AppRemoteBinding")]
    [InlineData("Service", "Service Broker\\Services", "AppInitiatorService")]
    [InlineData("Route", "Service Broker\\Routes", "CustomRoute")]
    [InlineData("PartitionFunction", "Storage\\Partition Functions", "FiscalYear_PF")]
    [InlineData("PartitionScheme", "Storage\\Partition Schemes", "FiscalYear_PS")]
    [InlineData("FullTextCatalog", "Storage\\Full Text Catalogs", "DocumentCatalog")]
    [InlineData("FullTextStoplist", "Storage\\Full Text Stoplists", "CustomStoplist")]
    [InlineData("SearchPropertyList", "Storage\\Search Property Lists", "DocumentProperties")]
    public void Formats_SchemaLessObjects(string objectType, string folder, string name)
    {
        var mapper = new SchemaFolderMapper(BuildFolderMap(), dataWriteAllFilesInOneDirectory: true);
        var identifier = new ObjectIdentifier("dbo", name);

        var path = mapper.GetObjectPath(objectType, identifier, false);

        Assert.Equal(FolderPath(folder, $"{EscapeFileNamePart(name)}.sql"), path);
    }

    [Theory]
    [InlineData("Synonym", "Synonyms", "Reporting", "CurrentSales")]
    [InlineData("UserDefinedType", "Types\\User-defined Data Types", "dbo", "PhoneNumber")]
    [InlineData("TableType", "Types\\Table Types", "dbo", "CustomerCodes")]
    [InlineData("XmlSchemaCollection", "Types\\XML Schema Collections", "dbo", "PayloadSchema")]
    [InlineData("Queue", "Service Broker\\Queues", "dbo", "Log_InitiatorQueue")]
    public void Formats_SchemaScopedAdditionalObjects(string objectType, string folder, string schema, string name)
    {
        var mapper = new SchemaFolderMapper(BuildFolderMap(), dataWriteAllFilesInOneDirectory: true);
        var identifier = new ObjectIdentifier(schema, name);

        var path = mapper.GetObjectPath(objectType, identifier, false);

        Assert.Equal(FolderPath(folder, $"{schema}.{name}.sql"), path);
    }

    // Builds an expected path from a folder string that may use either separator
    // character, mirroring the cross-platform normalisation performed by the mapper.
    private static string FolderPath(string folder, string fileName)
        => Path.Combine([..folder.Split(['\\', '/'], StringSplitOptions.RemoveEmptyEntries), fileName]);

    private static string EscapeFileNamePart(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return value;
        }

        var invalid = new HashSet<char>(
            Path.GetInvalidFileNameChars().Concat(['"', '<', '>', '|', ':', '*', '?', '\\']));
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

    [Fact]
    public void Escapes_InvalidFileNameCharacters()
    {
        var mapper = new SchemaFolderMapper(BuildFolderMap(), dataWriteAllFilesInOneDirectory: true);
        var identifier = new ObjectIdentifier("dbo", "http://schemas.microsoft.com/SQL/Notifications");

        var path = mapper.GetObjectPath("MessageType", identifier, false);

        Assert.Equal(
            Path.Combine("Service Broker", "Message Types", "http%3A%2F%2Fschemas.microsoft.com%2FSQL%2FNotifications.sql"),
            path);
    }

    private static IReadOnlyList<FolderMapEntry> BuildFolderMap()
        => new[]
        {
            new FolderMapEntry("Default", "Other"),
            new FolderMapEntry("Assembly", "Assemblies"),
            new FolderMapEntry("Table", "Tables"),
            new FolderMapEntry("View", "Views"),
            new FolderMapEntry("StoredProcedure", "Stored Procedures"),
            new FolderMapEntry("Function", "Functions"),
            new FolderMapEntry("Sequence", "Sequences"),
            new FolderMapEntry("Schema", @"Security\Schemas"),
            new FolderMapEntry("Role", @"Security\Roles"),
            new FolderMapEntry("User", @"Security\Users"),
            new FolderMapEntry("EventNotification", @"Service Broker\Event Notifications"),
            new FolderMapEntry("MessageType", @"Service Broker\Message Types"),
            new FolderMapEntry("Contract", @"Service Broker\Contracts"),
            new FolderMapEntry("Queue", @"Service Broker\Queues"),
            new FolderMapEntry("ServiceBinding", @"Service Broker\Remote Service Bindings"),
            new FolderMapEntry("Service", @"Service Broker\Services"),
            new FolderMapEntry("Route", @"Service Broker\Routes"),
            new FolderMapEntry("PartitionFunction", @"Storage\Partition Functions"),
            new FolderMapEntry("PartitionScheme", @"Storage\Partition Schemes"),
            new FolderMapEntry("FullTextCatalog", @"Storage\Full Text Catalogs"),
            new FolderMapEntry("FullTextStoplist", @"Storage\Full Text Stoplists"),
            new FolderMapEntry("SearchPropertyList", @"Storage\Search Property Lists"),
            new FolderMapEntry("Synonym", "Synonyms"),
            new FolderMapEntry("TableType", @"Types\Table Types"),
            new FolderMapEntry("XmlSchemaCollection", @"Types\XML Schema Collections"),
            new FolderMapEntry("UserDefinedType", @"Types\User-defined Data Types"),
            new FolderMapEntry("Data", "Data")
        };
}
