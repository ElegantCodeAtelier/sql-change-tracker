using Microsoft.Data.SqlClient;

namespace SqlChangeTracker.Sql;

internal static class SqlConnectionFactory
{
    public static SqlConnection Create(SqlConnectionOptions options)
    {
        var builder = new SqlConnectionStringBuilder
        {
            DataSource = options.Server,
            InitialCatalog = options.Database,
            Encrypt = true,
            TrustServerCertificate = options.TrustServerCertificate
        };

        if (string.Equals(options.Auth, "sql", StringComparison.OrdinalIgnoreCase))
        {
            builder.IntegratedSecurity = false;
            builder.UserID = options.User;
            builder.Password = options.Password;
        }
        else
        {
            builder.IntegratedSecurity = true;
        }

        return new SqlConnection(builder.ConnectionString);
    }
}
