using Microsoft.Data.SqlClient;
using SqlChangeTracker.Sql;
using Xunit;

namespace SqlChangeTracker.Tests.Sql;

public sealed class SqlConnectionFactoryTests
{
    [Fact]
    public void Create_WithIntegratedAuth_SetsIntegratedSecurity()
    {
        var options = new SqlConnectionOptions(
            Server: "myserver",
            Database: "mydb",
            Auth: "integrated",
            User: null,
            Password: null,
            TrustServerCertificate: false);

        var connection = SqlConnectionFactory.Create(options);

        var builder = new SqlConnectionStringBuilder(connection.ConnectionString);
        Assert.True(builder.IntegratedSecurity);
    }

    [Fact]
    public void Create_WithIntegratedAuth_IsCaseInsensitive()
    {
        var options = new SqlConnectionOptions(
            Server: "myserver",
            Database: "mydb",
            Auth: "INTEGRATED",
            User: null,
            Password: null,
            TrustServerCertificate: false);

        var connection = SqlConnectionFactory.Create(options);

        var builder = new SqlConnectionStringBuilder(connection.ConnectionString);
        Assert.True(builder.IntegratedSecurity);
    }

    [Fact]
    public void Create_WithSqlAuth_SetsUserIdAndPassword()
    {
        var options = new SqlConnectionOptions(
            Server: "myserver",
            Database: "mydb",
            Auth: "sql",
            User: "sa",
            Password: "secret",
            TrustServerCertificate: false);

        var connection = SqlConnectionFactory.Create(options);

        var builder = new SqlConnectionStringBuilder(connection.ConnectionString);
        Assert.False(builder.IntegratedSecurity);
        Assert.Equal("sa", builder.UserID);
        Assert.Equal("secret", builder.Password);
    }

    [Fact]
    public void Create_WithSqlAuth_IsCaseInsensitive()
    {
        var options = new SqlConnectionOptions(
            Server: "myserver",
            Database: "mydb",
            Auth: "SQL",
            User: "sa",
            Password: "secret",
            TrustServerCertificate: false);

        var connection = SqlConnectionFactory.Create(options);

        var builder = new SqlConnectionStringBuilder(connection.ConnectionString);
        Assert.False(builder.IntegratedSecurity);
        Assert.Equal("sa", builder.UserID);
    }

    [Fact]
    public void Create_SetsServerAndDatabase()
    {
        var options = new SqlConnectionOptions(
            Server: "myserver\\instance",
            Database: "MyDatabase",
            Auth: "integrated",
            User: null,
            Password: null,
            TrustServerCertificate: false);

        var connection = SqlConnectionFactory.Create(options);

        var builder = new SqlConnectionStringBuilder(connection.ConnectionString);
        Assert.Equal("myserver\\instance", builder.DataSource);
        Assert.Equal("MyDatabase", builder.InitialCatalog);
    }

    [Fact]
    public void Create_WithTrustServerCertificate_SetsTrustServerCertificate()
    {
        var options = new SqlConnectionOptions(
            Server: "myserver",
            Database: "mydb",
            Auth: "integrated",
            User: null,
            Password: null,
            TrustServerCertificate: true);

        var connection = SqlConnectionFactory.Create(options);

        var builder = new SqlConnectionStringBuilder(connection.ConnectionString);
        Assert.True(builder.TrustServerCertificate);
    }

    [Fact]
    public void Create_WithTrustServerCertificateFalse_DoesNotTrustCertificate()
    {
        var options = new SqlConnectionOptions(
            Server: "myserver",
            Database: "mydb",
            Auth: "integrated",
            User: null,
            Password: null,
            TrustServerCertificate: false);

        var connection = SqlConnectionFactory.Create(options);

        var builder = new SqlConnectionStringBuilder(connection.ConnectionString);
        Assert.False(builder.TrustServerCertificate);
    }
}
