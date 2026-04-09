using SqlChangeTracker.Sql;
using Xunit;

namespace SqlChangeTracker.Tests.Sql;

public sealed class SqlServerScripterCompatibilityTests
{
    [Fact]
    public void TrimOuterBlankLines_RemovesLeadingAndTrailingBlankLines()
    {
        var text = "\r\n\r\nCREATE VIEW [dbo].[Sample]\r\nAS\r\nSELECT 1;\r\n\r\n";

        var trimmed = SqlServerScripter.TrimOuterBlankLines(text);

        Assert.Equal($"CREATE VIEW [dbo].[Sample]{Environment.NewLine}AS{Environment.NewLine}SELECT 1;", trimmed);
    }

    [Fact]
    public void GetModuleFormat_ReadsReferenceSpacing_WhenSetOptionsAreOff()
    {
        var referenceLines = new[]
        {
            "SET QUOTED_IDENTIFIER OFF",
            "GO",
            "SET ANSI_NULLS OFF",
            "GO",
            string.Empty,
            "CREATE PROCEDURE [dbo].[Sample]",
            "AS",
            "SELECT 1",
            string.Empty,
            "GO"
        };

        var format = SqlServerScripter.GetModuleFormat(referenceLines);

        Assert.NotNull(format);
        Assert.Equal(1, format!.LeadingBlankLines);
        Assert.Equal(1, format.BlankLineBeforeGo);
        Assert.True(format.HasGoAfterDefinition);
    }

    [Fact]
    public void ApplyDefinitionFormatting_PreservesReferenceCommentBeforeCreate()
    {
        var definition = string.Join(Environment.NewLine, new[]
        {
            "\t/* =============================================",
            string.Empty,
            "\tAuthor: example",
            "CREATE PROCEDURE [dbo].[Sample]",
            "\t@ExecutionID int",
            "AS",
            "SELECT 1"
        });

        var referenceLines = new[]
        {
            "SET QUOTED_IDENTIFIER ON",
            "GO",
            "SET ANSI_NULLS ON",
            "GO",
            "/* =============================================",
            string.Empty,
            "\tAuthor: example",
            "CREATE PROCEDURE [dbo].[Sample]",
            "\t@ExecutionID int",
            "AS",
            "SELECT 1",
            "GO"
        };

        var formatted = SqlServerScripter.ApplyDefinitionFormatting(definition, referenceLines);
        var firstLine = formatted.Split(new[] { "\r\n", "\n" }, StringSplitOptions.None)[0];

        Assert.Equal("/* =============================================", firstLine);
    }

    [Fact]
    public void ApplyDefinitionFormatting_PreservesReferenceCreateLineIdentifierQuoting()
    {
        var definition = string.Join(Environment.NewLine, new[]
        {
            "CREATE PROCEDURE Reporting.Sample_Proc",
            "\t@ModelConfigID int",
            "AS",
            "BEGIN",
            "\tSELECT @ModelConfigID",
            "END"
        });

        var referenceLines = new[]
        {
            "SET QUOTED_IDENTIFIER ON",
            "GO",
            "SET ANSI_NULLS ON",
            "GO",
            string.Empty,
            "CREATE PROCEDURE [Reporting].[Sample_Proc]",
            "\t@ModelConfigID int",
            "AS",
            "BEGIN",
            "\tSELECT @ModelConfigID",
            "END",
            "GO"
        };

        var formatted = SqlServerScripter.ApplyDefinitionFormatting(definition, referenceLines);
        var createLine = formatted
            .Split(new[] { "\r\n", "\n" }, StringSplitOptions.None)
            .First(line => line.Length > 0);

        Assert.Equal("CREATE PROCEDURE [Reporting].[Sample_Proc]", createLine);
    }

    [Fact]
    public void BuildReferenceTableColumnTypeMap_ReadsCompatibleTypeTokens()
    {
        var referenceLines = new[]
        {
            "CREATE TABLE [dbo].[DatabaseLog]",
            "(",
            "[DatabaseLogID] [int] NOT NULL,",
            "[DatabaseUser] [sys].[sysname] NOT NULL,",
            "[TSQL] [nvarchar] (max) NOT NULL,",
            "[XmlEvent] [xml] (CONTENT [dbo].[DatabaseLogSchema]) NOT NULL",
            ") ON [PRIMARY]"
        };

        var compatibilityMap = SqlServerScripter.BuildReferenceTableColumnTypeMap(referenceLines);

        Assert.NotNull(compatibilityMap);
        Assert.Equal("[sys].[sysname]", compatibilityMap!["DatabaseUser"]);
        Assert.Equal("[nvarchar] (max)", compatibilityMap["TSQL"]);
        Assert.Equal("[xml] (CONTENT [dbo].[DatabaseLogSchema])", compatibilityMap["XmlEvent"]);
    }

    [Fact]
    public void GetCompatibleTypeToken_PreservesEquivalentBuiltInAndSystemTokens()
    {
        IReadOnlyDictionary<string, string> compatibilityMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["DatabaseUser"] = "[sys].[sysname]",
            ["TSQL"] = "[nvarchar] (max)"
        };

        var sysnameType = SqlServerScripter.GetCompatibleTypeToken("[sysname]", compatibilityMap, "DatabaseUser");
        var nvarcharType = SqlServerScripter.GetCompatibleTypeToken("[nvarchar] (MAX)", compatibilityMap, "TSQL");

        Assert.Equal("[sys].[sysname]", sysnameType);
        Assert.Equal("[nvarchar] (max)", nvarcharType);
    }

    [Theory]
    [InlineData(false, "[xml] (CONTENT [Person].[AdditionalContactInfoSchemaCollection])")]
    [InlineData(true, "[xml] (DOCUMENT [dbo].[DocumentSchema])")]
    public void ApplyXmlSchemaBinding_AppendsBindingClause(bool isDocument, string expected)
    {
        var type = SqlServerScripter.ApplyXmlSchemaBinding(
            "[xml]",
            "xml",
            isUserDefined: false,
            isDocument,
            isDocument ? "dbo" : "Person",
            isDocument ? "DocumentSchema" : "AdditionalContactInfoSchemaCollection");

        Assert.Equal(expected, type);
    }

    [Fact]
    public void TryGetCompatibleReferenceCreateTableBlock_RejectsXmlSchemaBindingMismatches()
    {
        var referenceLines = new[]
        {
            "CREATE TABLE [Person].[Person]",
            "(",
            "[AdditionalContactInfo] [xml] (CONTENT [Person].[AdditionalContactInfoSchemaCollection]) NULL",
            ") ON [PRIMARY]"
        };

        var generatedCreateBlock = new List<string>
        {
            "CREATE TABLE [Person].[Person]",
            "(",
            "[AdditionalContactInfo] [xml] NULL",
            ") ON [PRIMARY]"
        };

        var compatibleBlock = SqlServerScripter.TryGetCompatibleReferenceCreateTableBlock(referenceLines, generatedCreateBlock);

        Assert.Null(compatibleBlock);
    }

    [Fact]
    public void TryGetCompatibleReferenceCreateTableBlock_RejectsIdentityNotForReplicationMismatches()
    {
        var referenceLines = new[]
        {
            "CREATE TABLE [Person].[Address]",
            "(",
            "[AddressID] [int] NOT NULL IDENTITY(1, 1) NOT FOR REPLICATION",
            ") ON [PRIMARY]"
        };

        var generatedCreateBlock = new List<string>
        {
            "CREATE TABLE [Person].[Address]",
            "(",
            "[AddressID] [int] NOT NULL IDENTITY(1, 1)",
            ") ON [PRIMARY]"
        };

        var compatibleBlock = SqlServerScripter.TryGetCompatibleReferenceCreateTableBlock(referenceLines, generatedCreateBlock);

        Assert.Null(compatibleBlock);
    }

    [Fact]
    public void TryGetCompatibleReferenceCreateTableBlock_RejectsSemanticMismatches()
    {
        var referenceLines = new[]
        {
            "CREATE TABLE [dbo].[Employee]",
            "(",
            "[BusinessEntityID] [int] NOT NULL,",
            "[rowguid] [uniqueidentifier] NOT NULL ROWGUIDCOL",
            ") ON [PRIMARY]"
        };

        var generatedCreateBlock = new List<string>
        {
            "CREATE TABLE [dbo].[Employee]",
            "(",
            "[BusinessEntityID] [int] NOT NULL,",
            "[rowguid] [uniqueidentifier] NOT NULL",
            ") ON [PRIMARY]"
        };

        var compatibleBlock = SqlServerScripter.TryGetCompatibleReferenceCreateTableBlock(referenceLines, generatedCreateBlock);

        Assert.Null(compatibleBlock);
    }

    [Fact]
    public void TryGetCompatibleReferenceCreateTableBlock_PreservesEquivalentComputedColumnConvertTokens()
    {
        var referenceLines = new[]
        {
            "CREATE TABLE [Sales].[SalesOrderHeader]",
            "(",
            "[SalesOrderNumber] AS (isnull(N'SO'+CONVERT([nvarchar](23),[SalesOrderID],(0)),N'*** ERROR ***'))",
            ") ON [PRIMARY]"
        };

        var generatedCreateBlock = new List<string>
        {
            "CREATE TABLE [Sales].[SalesOrderHeader]",
            "(",
            "[SalesOrderNumber] AS (isnull(N'SO'+CONVERT([nvarchar](23),[SalesOrderID]),N'*** ERROR ***'))",
            ") ON [PRIMARY]"
        };

        var compatibleBlock = SqlServerScripter.TryGetCompatibleReferenceCreateTableBlock(referenceLines, generatedCreateBlock);

        Assert.NotNull(compatibleBlock);
        Assert.Equal(referenceLines, compatibleBlock);
    }

    [Fact]
    public void TryGetCompatibleReferenceCreateTableBlock_PreservesEquivalentComputedColumnArithmeticGrouping()
    {
        var referenceLines = new[]
        {
            "CREATE TABLE [Example].[SampleTable]",
            "(",
            "[AmountDelta] AS (([BaseAmount]-[OffsetAmount])-(([AdjustedBase]-[AdjustedOffset])/[ScaleFactor]))",
            ") ON [PRIMARY]"
        };

        var generatedCreateBlock = new List<string>
        {
            "CREATE TABLE [Example].[SampleTable]",
            "(",
            "[AmountDelta] AS (([BaseAmount]-[OffsetAmount])-([AdjustedBase]-[AdjustedOffset])/[ScaleFactor])",
            ") ON [PRIMARY]"
        };

        var compatibleBlock = SqlServerScripter.TryGetCompatibleReferenceCreateTableBlock(referenceLines, generatedCreateBlock);

        Assert.NotNull(compatibleBlock);
        Assert.Equal(referenceLines, compatibleBlock);
    }

    [Fact]
    public void ReorderTableKeyAndIndexStatements_UsesCompatibleReferenceOrder()
    {
        var referenceLines = new[]
        {
            "ALTER TABLE [Production].[Document] ADD CONSTRAINT [CK_Document_Status] CHECK (([Status]>=(1) AND [Status]<=(3)))",
            "GO",
            "ALTER TABLE [Production].[Document] ADD CONSTRAINT [PK_Document_DocumentNode] PRIMARY KEY CLUSTERED ([DocumentNode]) ON [PRIMARY]",
            "GO",
            "CREATE UNIQUE NONCLUSTERED INDEX [AK_Document_DocumentLevel_DocumentNode] ON [Production].[Document] ([DocumentLevel], [DocumentNode]) ON [PRIMARY]",
            "GO",
            "CREATE NONCLUSTERED INDEX [IX_Document_FileName_Revision] ON [Production].[Document] ([FileName], [Revision]) ON [PRIMARY]",
            "GO",
            "CREATE UNIQUE NONCLUSTERED INDEX [AK_Document_rowguid] ON [Production].[Document] ([rowguid]) ON [PRIMARY]",
            "GO",
            "ALTER TABLE [Production].[Document] ADD CONSTRAINT [UQ__Document__F73921F7C81C642F] UNIQUE NONCLUSTERED ([rowguid]) ON [PRIMARY]",
            "GO",
            "ALTER TABLE [Production].[Document] ADD CONSTRAINT [FK_Document_Employee_Owner] FOREIGN KEY ([Owner]) REFERENCES [HumanResources].[Employee] ([BusinessEntityID])",
            "GO"
        };

        var keyConstraintLines = new List<string>
        {
            "ALTER TABLE [Production].[Document] ADD CONSTRAINT [PK_Document_DocumentNode] PRIMARY KEY CLUSTERED ([DocumentNode]) ON [PRIMARY]",
            "GO",
            "ALTER TABLE [Production].[Document] ADD CONSTRAINT [UQ__Document__F73921F7C81C642F] UNIQUE NONCLUSTERED ([rowguid]) ON [PRIMARY]",
            "GO"
        };

        var nonConstraintIndexLines = new List<string>
        {
            "CREATE UNIQUE NONCLUSTERED INDEX [AK_Document_DocumentLevel_DocumentNode] ON [Production].[Document] ([DocumentLevel], [DocumentNode]) ON [PRIMARY]",
            "GO",
            "CREATE NONCLUSTERED INDEX [IX_Document_FileName_Revision] ON [Production].[Document] ([FileName], [Revision]) ON [PRIMARY]",
            "GO",
            "CREATE UNIQUE NONCLUSTERED INDEX [AK_Document_rowguid] ON [Production].[Document] ([rowguid]) ON [PRIMARY]",
            "GO"
        };

        var reordered = SqlServerScripter.ReorderTableKeyAndIndexStatements(
            referenceLines,
            keyConstraintLines,
            nonConstraintIndexLines,
            Array.Empty<string>(),
            Array.Empty<string>());

        Assert.Equal(new[]
        {
            "ALTER TABLE [Production].[Document] ADD CONSTRAINT [PK_Document_DocumentNode] PRIMARY KEY CLUSTERED ([DocumentNode]) ON [PRIMARY]",
            "GO",
            "CREATE UNIQUE NONCLUSTERED INDEX [AK_Document_DocumentLevel_DocumentNode] ON [Production].[Document] ([DocumentLevel], [DocumentNode]) ON [PRIMARY]",
            "GO",
            "CREATE NONCLUSTERED INDEX [IX_Document_FileName_Revision] ON [Production].[Document] ([FileName], [Revision]) ON [PRIMARY]",
            "GO",
            "CREATE UNIQUE NONCLUSTERED INDEX [AK_Document_rowguid] ON [Production].[Document] ([rowguid]) ON [PRIMARY]",
            "GO",
            "ALTER TABLE [Production].[Document] ADD CONSTRAINT [UQ__Document__F73921F7C81C642F] UNIQUE NONCLUSTERED ([rowguid]) ON [PRIMARY]",
            "GO"
        }, reordered);
    }

    [Fact]
    public void ReorderTableKeyAndIndexStatements_PreservesCompatibleStatisticOrder()
    {
        var referenceLines = new[]
        {
            "CREATE NONCLUSTERED INDEX [IX_SampleTable_KeyBeta] ON [Example].[SampleTable] ([KeyBeta]) ON [PRIMARY]",
            "GO",
            "CREATE STATISTICS [STAT_SampleTable_KeyAlpha_KeyBeta] ON [Example].[SampleTable] ([KeyAlpha], [KeyBeta]) WITH NORECOMPUTE",
            "GO",
            "CREATE XML INDEX [XML_SampleTable_DetailXml] ON [Example].[SampleTable] ([DetailXml]) USING XML INDEX [PXML_SampleTable_DetailXml] FOR PATH",
            "GO"
        };

        var nonConstraintIndexLines = new List<string>
        {
            "CREATE NONCLUSTERED INDEX [IX_SampleTable_KeyBeta] ON [Example].[SampleTable] ([KeyBeta]) ON [PRIMARY]",
            "GO"
        };

        var userCreatedStatisticLines = new List<string>
        {
            "CREATE STATISTICS [STAT_SampleTable_KeyAlpha_KeyBeta] ON [Example].[SampleTable] ([KeyAlpha], [KeyBeta]) WITH NORECOMPUTE",
            "GO"
        };

        var xmlIndexLines = new List<string>
        {
            "CREATE XML INDEX [XML_SampleTable_DetailXml] ON [Example].[SampleTable] ([DetailXml]) USING XML INDEX [PXML_SampleTable_DetailXml] FOR PATH",
            "GO"
        };

        var reordered = SqlServerScripter.ReorderTableKeyAndIndexStatements(
            referenceLines,
            Array.Empty<string>(),
            nonConstraintIndexLines,
            userCreatedStatisticLines,
            xmlIndexLines);

        Assert.Equal(new[]
        {
            "CREATE NONCLUSTERED INDEX [IX_SampleTable_KeyBeta] ON [Example].[SampleTable] ([KeyBeta]) ON [PRIMARY]",
            "GO",
            "CREATE STATISTICS [STAT_SampleTable_KeyAlpha_KeyBeta] ON [Example].[SampleTable] ([KeyAlpha], [KeyBeta]) WITH NORECOMPUTE",
            "GO",
            "CREATE XML INDEX [XML_SampleTable_DetailXml] ON [Example].[SampleTable] ([DetailXml]) USING XML INDEX [PXML_SampleTable_DetailXml] FOR PATH",
            "GO"
        }, reordered);
    }

    [Fact]
    public void BuildTableStorageLine_EmitsPartitionColumnAndTextImageOn_WhenPresent()
    {
        var line = SqlServerScripter.BuildTableStorageLine("ExamplePartitionScheme", "PartitionKeyId", "PRIMARY");

        Assert.Equal(") ON [ExamplePartitionScheme] ([PartitionKeyId]) TEXTIMAGE_ON [PRIMARY]", line);
    }

    [Fact]
    public void BuildIndexWithClause_EmitsStatisticsIncrementalBeforeCompression_WhenBothPresent()
    {
        var clause = SqlServerScripter.BuildIndexWithClause("PAGE", statisticsIncremental: true);

        Assert.Equal(" WITH (STATISTICS_INCREMENTAL=ON, DATA_COMPRESSION = PAGE)", clause);
    }

    [Fact]
    public void BuildIndexOnClause_EmitsPartitionColumn_WhenPresent()
    {
        var clause = SqlServerScripter.BuildIndexOnClause("ExamplePartitionScheme", "PartitionKeyId");

        Assert.Equal(" ON [ExamplePartitionScheme] ([PartitionKeyId])", clause);
    }

    [Fact]
    public void BuildStatisticsSamplingClause_UsesPersistedSamplePercent_WhenAvailable()
    {
        var clause = SqlServerScripter.BuildStatisticsSamplingClause(
            rowCount: 200,
            rowsSampled: 80,
            persistedSamplePercent: 25d);

        Assert.Equal("SAMPLE 25 PERCENT", clause);
    }

    [Fact]
    public void BuildStatisticsSamplingClause_EmitsFullscan_WhenAllRowsWereSampled()
    {
        var clause = SqlServerScripter.BuildStatisticsSamplingClause(
            rowCount: 200,
            rowsSampled: 200,
            persistedSamplePercent: null);

        Assert.Equal("FULLSCAN", clause);
    }

    [Fact]
    public void BuildStatisticsWithClause_EmitsStatisticsOptionsInDeterministicOrder()
    {
        var clause = SqlServerScripter.BuildStatisticsWithClause(
            samplingClause: "SAMPLE 25 PERCENT",
            persistSamplePercent: true,
            noRecompute: true,
            incremental: true,
            autoDrop: false);

        Assert.Equal(" WITH SAMPLE 25 PERCENT, PERSIST_SAMPLE_PERCENT = ON, NORECOMPUTE, INCREMENTAL=ON, AUTO_DROP = OFF", clause);
    }
}
