using Microsoft.Data.SqlClient;

namespace SqlChangeTracker.Sql;

internal interface IConnectionTester
{
    ConnectionTestResult Test(SqlConnectionOptions options);
}

internal sealed record ConnectionTestResult(bool Success, string? ErrorMessage);

internal sealed class SqlConnectionTester : IConnectionTester
{
    public ConnectionTestResult Test(SqlConnectionOptions options)
    {
        try
        {
            using var connection = SqlConnectionFactory.Create(options);
            connection.Open();
            return new ConnectionTestResult(true, null);
        }
        catch (SqlException ex)
        {
            return new ConnectionTestResult(false, ex.Message);
        }
        catch (Exception ex)
        {
            return new ConnectionTestResult(false, ex.Message);
        }
    }
}
