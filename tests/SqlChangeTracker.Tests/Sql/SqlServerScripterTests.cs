using SqlChangeTracker.Sql;
using Xunit;

namespace SqlChangeTracker.Tests.Sql;

public sealed class SqlServerScripterTests
{
    [Fact]
    public void ScriptTable_EmitsCreateTable_WhenConfigured()
    {
        var options = GetOptions();
        var tableSchema = Environment.GetEnvironmentVariable("SQLCT_TEST_TABLE_SCHEMA");
        var tableName = Environment.GetEnvironmentVariable("SQLCT_TEST_TABLE_NAME");
        if (options == null || string.IsNullOrWhiteSpace(tableSchema) || string.IsNullOrWhiteSpace(tableName))
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo(tableSchema, tableName, "Table"));

        Assert.Contains("CREATE TABLE", script);
        Assert.Contains("GO", script);
    }

    [Fact]
    public void ScriptTable_EmitsRowGuidCol_ForAdventureWorksPersonTable()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("Person", "Person", "Table"));

        Assert.Contains("[rowguid] [uniqueidentifier] NOT NULL ROWGUIDCOL", script);
    }

    [Fact]
    public void ScriptTable_EmitsIdentityNotForReplication_ForAdventureWorksAddressTable()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("Person", "Address", "Table"));

        Assert.Contains("[AddressID] [int] NOT NULL IDENTITY(1, 1) NOT FOR REPLICATION", script);
    }

    [Fact]
    public void ScriptTable_EmitsInlineTrigger_ForAdventureWorksPersonTable()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("Person", "Person", "Table"));

        Assert.Contains("CREATE TRIGGER [Person].[iuPerson] ON [Person].[Person]", script);
    }

    [Fact]
    public void ScriptTable_EmitsXmlSchemaBindings_ForAdventureWorksPersonTable()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("Person", "Person", "Table"));

        Assert.Contains("[AdditionalContactInfo] [xml] (CONTENT [Person].[AdditionalContactInfoSchemaCollection]) NULL", script);
        Assert.Contains("[Demographics] [xml] (CONTENT [Person].[IndividualSurveySchemaCollection]) NULL", script);
    }

    [Fact]
    public void ScriptTable_EmitsXmlIndexes_ForAdventureWorksPersonTable()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("Person", "Person", "Table"));

        Assert.Contains("CREATE PRIMARY XML INDEX [PXML_Person_AddContact]", script);
        Assert.Contains("CREATE XML INDEX [XMLPATH_Person_Demographics]", script);
    }

    [Fact]
    public void ScriptTable_OmitsNullability_ForNonPersistedAdventureWorksComputedColumns()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("Sales", "Customer", "Table"));

        Assert.Contains("[AccountNumber] AS (isnull('AW'+[dbo].[ufnLeadingZeros]([CustomerID]),''))", script);
        Assert.DoesNotContain("[AccountNumber] AS (isnull('AW'+[dbo].[ufnLeadingZeros]([CustomerID]),'')) NOT NULL", script);
    }

    [Fact]
    public void ScriptTable_EmitsNullability_ForPersistedAdventureWorksComputedColumns()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("Purchasing", "PurchaseOrderHeader", "Table"));

        Assert.Contains("[TotalDue] AS (isnull(([SubTotal]+[TaxAmt])+[Freight],(0))) PERSISTED NOT NULL", script);
    }

    [Fact]
    public void ScriptTable_EmitsFullTextIndexes_ForAdventureWorksJobCandidateTable()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("HumanResources", "JobCandidate", "Table"));

        Assert.Contains("CREATE FULLTEXT INDEX ON [HumanResources].[JobCandidate] KEY INDEX [PK_JobCandidate_JobCandidateID] ON [AW2016FullTextCatalog]", script);
        Assert.Contains("ALTER FULLTEXT INDEX ON [HumanResources].[JobCandidate] ADD ([Resume] LANGUAGE 1033)", script);
    }

    [Fact]
    public void ScriptView_EmitsIndexedViewIndex_ForAdventureWorksView()
    {
        var options = GetAdventureWorksOptions();
        if (options == null)
        {
            return;
        }

        var scripter = new SqlServerScripter();
        var script = scripter.ScriptObject(options, new DbObjectInfo("Production", "vProductAndDescription", "View"));

        Assert.Contains("CREATE UNIQUE CLUSTERED INDEX [IX_vProductAndDescription] ON [Production].[vProductAndDescription] ([CultureID], [ProductID]) ON [PRIMARY]", script);
    }

    private static SqlConnectionOptions? GetOptions()
    {
        var server = Environment.GetEnvironmentVariable("SQLCT_TEST_SERVER");
        var database = Environment.GetEnvironmentVariable("SQLCT_TEST_DATABASE");
        if (string.IsNullOrWhiteSpace(server) || string.IsNullOrWhiteSpace(database))
        {
            return null;
        }

        return new SqlConnectionOptions(
            server,
            database,
            "integrated",
            null,
            null,
            true);
    }

    private static SqlConnectionOptions? GetAdventureWorksOptions()
    {
        var options = GetOptions();
        if (options == null)
        {
            return null;
        }

        return string.Equals(
                Environment.GetEnvironmentVariable("SQLCT_TEST_DATABASE"),
                "AdventureWorks2022",
                StringComparison.OrdinalIgnoreCase)
            ? options
            : null;
    }
}
