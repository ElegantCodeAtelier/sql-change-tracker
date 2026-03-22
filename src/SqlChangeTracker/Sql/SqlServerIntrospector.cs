using Microsoft.Data.SqlClient;

namespace SqlChangeTracker.Sql;

internal sealed class SqlServerIntrospector
{
    public IReadOnlyList<DbObjectInfo> ListObjects(SqlConnectionOptions options)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        var results = new List<DbObjectInfo>();
        results.AddRange(QueryObjects(connection, @"
SELECT s.name AS schema_name, o.name AS object_name, o.type
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE o.is_ms_shipped = 0 AND o.type IN ('U','V','P','FN','TF','IF')
ORDER BY s.name, o.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT s.name AS schema_name, seq.name AS object_name, 'SQ' AS type
FROM sys.sequences seq
JOIN sys.schemas s ON s.schema_id = seq.schema_id
ORDER BY s.name, seq.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT s.name AS schema_name, s.name AS object_name, 'SC' AS type
FROM sys.schemas s
WHERE s.name NOT IN ('sys','INFORMATION_SCHEMA')
ORDER BY s.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT s.name AS schema_name, syn.name AS object_name, 'SY' AS type
FROM sys.synonyms syn
JOIN sys.schemas s ON s.schema_id = syn.schema_id
ORDER BY s.name, syn.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT s.name AS schema_name, t.name AS object_name, 'UDT' AS type
FROM sys.types t
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE t.is_user_defined = 1 AND t.is_table_type = 0
ORDER BY s.name, t.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT s.name AS schema_name, tt.name AS object_name, 'TT' AS type
FROM sys.table_types tt
JOIN sys.schemas s ON s.schema_id = tt.schema_id
ORDER BY s.name, tt.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT s.name AS schema_name, x.name AS object_name, 'XSC' AS type
FROM sys.xml_schema_collections x
JOIN sys.schemas s ON s.schema_id = x.schema_id
WHERE s.name NOT IN ('sys','INFORMATION_SCHEMA')
ORDER BY s.name, x.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT 'dbo' AS schema_name, mt.name AS object_name, 'MT' AS type
FROM sys.service_message_types mt
ORDER BY mt.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT 'dbo' AS schema_name, c.name AS object_name, 'CT' AS type
FROM sys.service_contracts c
ORDER BY c.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT s.name AS schema_name, q.name AS object_name, 'Q' AS type
FROM sys.service_queues q
JOIN sys.objects o ON o.object_id = q.object_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
ORDER BY s.name, q.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT 'dbo' AS schema_name, sv.name AS object_name, 'SRV' AS type
FROM sys.services sv
ORDER BY sv.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT 'dbo' AS schema_name, r.name AS object_name, 'ROUTE' AS type
FROM sys.routes r
ORDER BY r.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT 'dbo' AS schema_name, en.name AS object_name, 'EN' AS type
FROM sys.event_notifications en
ORDER BY en.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT 'dbo' AS schema_name, rsb.name AS object_name, 'RSB' AS type
FROM sys.remote_service_bindings rsb
ORDER BY rsb.name;", MapObjectType));

        AddIfExists(results, connection, "sys.fulltext_catalogs", @"
SELECT 'dbo' AS schema_name, fc.name AS object_name, 'FTC' AS type
FROM sys.fulltext_catalogs fc
ORDER BY fc.name;", MapObjectType);

        AddIfExists(results, connection, "sys.fulltext_stoplists", @"
SELECT 'dbo' AS schema_name, fs.name AS object_name, 'FTS' AS type
FROM sys.fulltext_stoplists fs
ORDER BY fs.name;", MapObjectType);

        AddIfExists(results, connection, "sys.fulltext_search_property_lists", @"
SELECT 'dbo' AS schema_name, sp.name AS object_name, 'SPL' AS type
FROM sys.fulltext_search_property_lists sp
ORDER BY sp.name;", MapObjectType);

        AddIfExists(results, connection, "sys.security_policies", @"
SELECT s.name AS schema_name, p.name AS object_name, 'SEC' AS type
FROM sys.security_policies p
JOIN sys.schemas s ON s.schema_id = p.schema_id
ORDER BY s.name, p.name;", MapObjectType);

        AddIfExists(results, connection, "sys.external_data_sources", @"
SELECT 'dbo' AS schema_name, ds.name AS object_name, 'EDS' AS type
FROM sys.external_data_sources ds
ORDER BY ds.name;", MapObjectType);

        AddIfExists(results, connection, "sys.external_file_formats", @"
SELECT 'dbo' AS schema_name, ff.name AS object_name, 'EFF' AS type
FROM sys.external_file_formats ff
ORDER BY ff.name;", MapObjectType);

        AddIfExists(results, connection, "sys.external_tables", @"
SELECT s.name AS schema_name, et.name AS object_name, 'EXT' AS type
FROM sys.external_tables et
JOIN sys.schemas s ON s.schema_id = et.schema_id
ORDER BY s.name, et.name;", MapObjectType);

        results.AddRange(QueryObjects(connection, @"
SELECT 'dbo' AS schema_name, c.name AS object_name, 'CERT' AS type
FROM sys.certificates c
WHERE c.name NOT IN ('##MS_AgentSigningCertificate##')
ORDER BY c.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT 'dbo' AS schema_name, sk.name AS object_name, 'SYM' AS type
FROM sys.symmetric_keys sk
WHERE sk.name NOT IN ('##MS_DatabaseMasterKey##', '##MS_ServiceMasterKey##')
ORDER BY sk.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT 'dbo' AS schema_name, ak.name AS object_name, 'ASYM' AS type
FROM sys.asymmetric_keys ak
ORDER BY ak.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT dp.name AS schema_name, dp.name AS object_name, 'ROLE' AS type
FROM sys.database_principals dp
WHERE dp.type = 'R' AND dp.is_fixed_role = 0
ORDER BY dp.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT dp.default_schema_name AS schema_name, dp.name AS object_name, 'USR' AS type
FROM sys.database_principals dp
WHERE dp.type IN ('S','U','G','E','X','C','K')
  AND dp.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys')
ORDER BY dp.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT s.name AS schema_name, o.name AS object_name, 'RULE' AS type
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE o.type = 'R' AND o.is_ms_shipped = 0
ORDER BY s.name, o.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT s.name AS schema_name, t.name AS object_name, 'TR' AS type
FROM sys.triggers t
JOIN sys.objects o ON o.object_id = t.parent_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE t.parent_class_desc = 'OBJECT_OR_COLUMN' AND t.is_ms_shipped = 0
ORDER BY s.name, t.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT 'dbo' AS schema_name, pf.name AS object_name, 'PF' AS type
FROM sys.partition_functions pf
ORDER BY pf.name;", MapObjectType));

        results.AddRange(QueryObjects(connection, @"
SELECT 'dbo' AS schema_name, ps.name AS object_name, 'PS' AS type
FROM sys.partition_schemes ps
ORDER BY ps.name;", MapObjectType));

        return results;
    }

    private static IEnumerable<DbObjectInfo> QueryObjects(
        SqlConnection connection,
        string sql,
        Func<string, string> typeMapper)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        using var reader = command.ExecuteReader();

        while (reader.Read())
        {
            var schema = reader.IsDBNull(0) ? "dbo" : reader.GetString(0);
            var name = reader.GetString(1);
            var type = reader.GetString(2);
            yield return new DbObjectInfo(schema, name, typeMapper(type));
        }
    }

    private static void AddIfExists(
        ICollection<DbObjectInfo> results,
        SqlConnection connection,
        string objectName,
        string sql,
        Func<string, string> typeMapper)
    {
        if (!ObjectExists(connection, objectName))
        {
            return;
        }

        foreach (var item in QueryObjects(connection, sql, typeMapper))
        {
            results.Add(item);
        }
    }

    private static bool ObjectExists(SqlConnection connection, string objectName)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT OBJECT_ID(@name);";
        command.Parameters.AddWithValue("@name", objectName);
        var result = command.ExecuteScalar();
        return result is not null && result != DBNull.Value;
    }

    private static string MapObjectType(string type)
    {
        var normalized = type.Trim();
        return normalized switch
        {
            "U" => "Table",
            "V" => "View",
            "P" => "StoredProcedure",
            "FN" or "TF" or "IF" => "Function",
            "SQ" => "Sequence",
            "SC" => "Schema",
            "SY" => "Synonym",
            "UDT" => "UserDefinedType",
            "TT" => "TableType",
            "XSC" => "XmlSchemaCollection",
            "MT" => "MessageType",
            "CT" => "Contract",
            "Q" => "Queue",
            "SRV" => "Service",
            "ROUTE" => "Route",
            "EN" => "EventNotification",
            "RSB" => "ServiceBinding",
            "FTC" => "FullTextCatalog",
            "FTS" => "FullTextStoplist",
            "SPL" => "SearchPropertyList",
            "SEC" => "SecurityPolicy",
            "EDS" => "ExternalDataSource",
            "EFF" => "ExternalFileFormat",
            "EXT" => "ExternalTable",
            "CERT" => "Certificate",
            "SYM" => "SymmetricKey",
            "ASYM" => "AsymmetricKey",
            "RULE" => "Rule",
            "TR" => "Trigger",
            "ROLE" => "Role",
            "USR" => "User",
            "PF" => "PartitionFunction",
            "PS" => "PartitionScheme",
            _ => "Unknown"
        };
    }
}
