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
    [InlineData("Schema", "Security\\Schemas", "CAD")]
    [InlineData("Role", "Security\\Roles", "corep_datareader")]
    [InlineData("User", "Security\\Users", "ReadonlyUser")]
    [InlineData("PartitionFunction", "Storage\\Partition Functions", "DATA_Loadset_PF")]
    [InlineData("PartitionScheme", "Storage\\Partition Schemes", "DATA_Loadset_PS")]
    public void Formats_SchemaLessObjects(string objectType, string folder, string name)
    {
        var mapper = new SchemaFolderMapper(BuildFolderMap(), dataWriteAllFilesInOneDirectory: true);
        var identifier = new ObjectIdentifier("dbo", name);

        var path = mapper.GetObjectPath(objectType, identifier, false);

        Assert.Equal(Path.Combine(folder, $"{name}.sql"), path);
    }

    [Fact]
    public void Escapes_InvalidFileNameCharacters()
    {
        var mapper = new SchemaFolderMapper(BuildFolderMap(), dataWriteAllFilesInOneDirectory: true);
        var identifier = new ObjectIdentifier("dbo", "http://schemas.microsoft.com/SQL/Notifications");

        var path = mapper.GetObjectPath("MessageType", identifier, false);

        Assert.Equal(
            Path.Combine("Service Broker", "Message Types", "dbo.http%3A%2F%2Fschemas.microsoft.com%2FSQL%2FNotifications.sql"),
            path);
    }

    private static IReadOnlyList<FolderMapEntry> BuildFolderMap()
        => new[]
        {
            new FolderMapEntry("Default", "Other"),
            new FolderMapEntry("Table", "Tables"),
            new FolderMapEntry("View", "Views"),
            new FolderMapEntry("StoredProcedure", "Stored Procedures"),
            new FolderMapEntry("Function", "Functions"),
            new FolderMapEntry("Sequence", "Sequences"),
            new FolderMapEntry("Schema", @"Security\Schemas"),
            new FolderMapEntry("Role", @"Security\Roles"),
            new FolderMapEntry("User", @"Security\Users"),
            new FolderMapEntry("PartitionFunction", @"Storage\Partition Functions"),
            new FolderMapEntry("PartitionScheme", @"Storage\Partition Schemes"),
            new FolderMapEntry("MessageType", @"Service Broker\Message Types"),
            new FolderMapEntry("Data", "Data")
        };
}
