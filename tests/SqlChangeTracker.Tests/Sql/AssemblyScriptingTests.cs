using SqlChangeTracker.Sql;
using Xunit;

namespace SqlChangeTracker.Tests.Sql;

public sealed class AssemblyScriptingTests
{
    [Fact]
    public void BuildAssemblyDefinitionLines_EmitsManifestAdditionalFilesAndVisibilityDeterministically()
    {
        var assembly = new SqlServerScripter.AssemblyScriptingInfo(
            "AppClr",
            "dbo",
            "SAFE_ACCESS",
            false,
            [
                new SqlServerScripter.AssemblyBinaryFile(2, "Helpers.dll", [0xCC, 0xDD]),
                new SqlServerScripter.AssemblyBinaryFile(1, "AppClr.dll", [0xAA, 0xBB])
            ]);

        var lines = SqlServerScripter.BuildAssemblyDefinitionLines(assembly);

        Assert.Equal(
            [
                "CREATE ASSEMBLY [AppClr]",
                "AUTHORIZATION [dbo]",
                "FROM 0xAABB",
                "WITH PERMISSION_SET = SAFE",
                "GO",
                "ALTER ASSEMBLY [AppClr] ADD FILE FROM 0xCCDD AS [Helpers.dll]",
                "GO",
                "ALTER ASSEMBLY [AppClr] WITH VISIBILITY = OFF",
                "GO"
            ],
            lines);
    }

    [Fact]
    public void BuildAssemblyDefinitionLines_ThrowsWhenManifestFileIsMissing()
    {
        var assembly = new SqlServerScripter.AssemblyScriptingInfo(
            "AppClr",
            null,
            "SAFE",
            true,
            [
                new SqlServerScripter.AssemblyBinaryFile(2, "Helpers.dll", [0xAA])
            ]);

        var error = Assert.Throws<InvalidOperationException>(() => SqlServerScripter.BuildAssemblyDefinitionLines(assembly));

        Assert.Contains("manifest file_id 1", error.Message);
    }

    [Theory]
    [InlineData("SAFE", "SAFE")]
    [InlineData("SAFE_ACCESS", "SAFE")]
    [InlineData("EXTERNAL_ACCESS", "EXTERNAL_ACCESS")]
    [InlineData("UNSAFE", "UNSAFE")]
    [InlineData("UNSAFE_ACCESS", "UNSAFE")]
    public void NormalizeAssemblyPermissionSet_MapsSqlServerPermissionSetValues(string input, string expected)
    {
        var actual = SqlServerScripter.NormalizeAssemblyPermissionSet(input);

        Assert.Equal(expected, actual);
    }
}
