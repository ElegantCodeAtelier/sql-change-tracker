using System.Collections.Concurrent;
using Microsoft.Data.SqlClient;
using SqlChangeTracker.Schema;

namespace SqlChangeTracker.Sql;

internal class SqlServerIntrospector
{
    public virtual IReadOnlyList<DbObjectInfo> ListObjects(SqlConnectionOptions options, int maxParallelism = 0)
    {
        var dop = ResolveParallelism(maxParallelism);

        // Each entry is a self-contained unit that opens its own pooled connection.
        var queries = new List<Func<IEnumerable<DbObjectInfo>>>
        {
            () => RunQuery(options, @"
SELECT '' AS schema_name, a.name AS object_name, 'ASSEMBLY' AS type
FROM sys.assemblies a
WHERE a.is_user_defined = 1
ORDER BY a.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT s.name AS schema_name, o.name AS object_name, o.type
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE o.is_ms_shipped = 0 AND o.type IN ('U','V','P','PC','FN','TF','IF','FS','FT')
ORDER BY s.name, o.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT s.name AS schema_name, seq.name AS object_name, 'SQ' AS type
FROM sys.sequences seq
JOIN sys.schemas s ON s.schema_id = seq.schema_id
ORDER BY s.name, seq.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT s.name AS schema_name, s.name AS object_name, 'SC' AS type
FROM sys.schemas s
WHERE (
        s.name NOT IN (
            'dbo',
            'guest',
            'sys',
            'INFORMATION_SCHEMA',
            'db_accessadmin',
            'db_backupoperator',
            'db_datareader',
            'db_datawriter',
            'db_ddladmin',
            'db_denydatareader',
            'db_denydatawriter',
            'db_owner',
            'db_securityadmin')
        OR (
            s.name = 'dbo'
            AND (
                EXISTS (
                    SELECT 1
                    FROM sys.database_permissions dp
                    WHERE dp.class_desc = 'SCHEMA'
                      AND dp.major_id = s.schema_id)
                OR EXISTS (
                    SELECT 1
                    FROM sys.extended_properties ep
                    WHERE ep.class_desc = 'SCHEMA'
                      AND ep.major_id = s.schema_id))
        )
      )
ORDER BY s.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT s.name AS schema_name, syn.name AS object_name, 'SY' AS type
FROM sys.synonyms syn
JOIN sys.schemas s ON s.schema_id = syn.schema_id
ORDER BY s.name, syn.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT s.name AS schema_name, t.name AS object_name, 'UDT_SCALAR' AS type
FROM sys.types t
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE t.is_user_defined = 1 AND t.is_table_type = 0
ORDER BY s.name, t.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT s.name AS schema_name, tt.name AS object_name, 'UDT_TABLE' AS type
FROM sys.table_types tt
JOIN sys.schemas s ON s.schema_id = tt.schema_id
ORDER BY s.name, tt.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT s.name AS schema_name, x.name AS object_name, 'XSC' AS type
FROM sys.xml_schema_collections x
JOIN sys.schemas s ON s.schema_id = x.schema_id
WHERE s.name NOT IN ('sys','INFORMATION_SCHEMA')
ORDER BY s.name, x.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT '' AS schema_name, mt.name AS object_name, 'MT' AS type
FROM sys.service_message_types mt
WHERE mt.name <> 'DEFAULT'
  AND mt.name NOT LIKE 'http://schemas.microsoft.com/SQL/%'
ORDER BY mt.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT '' AS schema_name, c.name AS object_name, 'CT' AS type
FROM sys.service_contracts c
WHERE c.name <> 'DEFAULT'
  AND c.name NOT LIKE 'http://schemas.microsoft.com/SQL/%'
ORDER BY c.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT s.name AS schema_name, q.name AS object_name, 'Q' AS type
FROM sys.service_queues q
JOIN sys.objects o ON o.object_id = q.object_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE q.name NOT IN ('ServiceBrokerQueue', 'QueryNotificationErrorsQueue', 'EventNotificationErrorsQueue')
ORDER BY s.name, q.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT '' AS schema_name, sv.name AS object_name, 'SRV' AS type
FROM sys.services sv
WHERE sv.name NOT LIKE 'http://schemas.microsoft.com/SQL/%'
ORDER BY sv.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT '' AS schema_name, r.name AS object_name, 'ROUTE' AS type
FROM sys.routes r
WHERE r.name <> 'AutoCreatedLocal'
ORDER BY r.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT '' AS schema_name, en.name AS object_name, 'EN' AS type
FROM sys.event_notifications en
ORDER BY en.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT '' AS schema_name, rsb.name AS object_name, 'RSB' AS type
FROM sys.remote_service_bindings rsb
ORDER BY rsb.name;", MapObjectType),

            () => RunQueryIfExists(options, "sys.fulltext_catalogs", @"
SELECT '' AS schema_name, fc.name AS object_name, 'FTC' AS type
FROM sys.fulltext_catalogs fc
ORDER BY fc.name;", MapObjectType),

            () => RunQueryIfExists(options, "sys.fulltext_stoplists", @"
SELECT '' AS schema_name, fs.name AS object_name, 'FTS' AS type
FROM sys.fulltext_stoplists fs
WHERE fs.stoplist_id > 0
ORDER BY fs.name;", MapObjectType),

            () => RunQueryIfExists(options, "sys.registered_search_property_lists", @"
SELECT '' AS schema_name, sp.name AS object_name, 'SPL' AS type
FROM sys.registered_search_property_lists sp
ORDER BY sp.name;", MapObjectType),

            () => RunQueryIfExists(options, "sys.security_policies", @"
SELECT s.name AS schema_name, p.name AS object_name, 'SEC' AS type
FROM sys.security_policies p
JOIN sys.schemas s ON s.schema_id = p.schema_id
ORDER BY s.name, p.name;", MapObjectType),

            () => RunQueryIfExists(options, "sys.external_data_sources", @"
SELECT 'dbo' AS schema_name, ds.name AS object_name, 'EDS' AS type
FROM sys.external_data_sources ds
ORDER BY ds.name;", MapObjectType),

            () => RunQueryIfExists(options, "sys.external_file_formats", @"
SELECT 'dbo' AS schema_name, ff.name AS object_name, 'EFF' AS type
FROM sys.external_file_formats ff
ORDER BY ff.name;", MapObjectType),

            () => RunQueryIfExists(options, "sys.external_tables", @"
SELECT s.name AS schema_name, et.name AS object_name, 'EXT' AS type
FROM sys.external_tables et
JOIN sys.schemas s ON s.schema_id = et.schema_id
ORDER BY s.name, et.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT 'dbo' AS schema_name, c.name AS object_name, 'CERT' AS type
FROM sys.certificates c
WHERE c.name NOT IN ('##MS_AgentSigningCertificate##')
ORDER BY c.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT 'dbo' AS schema_name, sk.name AS object_name, 'SYM' AS type
FROM sys.symmetric_keys sk
WHERE sk.name NOT IN ('##MS_DatabaseMasterKey##', '##MS_ServiceMasterKey##')
ORDER BY sk.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT 'dbo' AS schema_name, ak.name AS object_name, 'ASYM' AS type
FROM sys.asymmetric_keys ak
ORDER BY ak.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT '' AS schema_name, dp.name AS object_name, 'ROLE' AS type
FROM sys.database_principals dp
WHERE dp.type = 'R'
  AND dp.name <> 'public'
  AND (
    dp.is_fixed_role = 0
    OR EXISTS (
      SELECT 1
      FROM sys.database_role_members drm
      JOIN sys.database_principals member_principal ON member_principal.principal_id = drm.member_principal_id
      WHERE drm.role_principal_id = dp.principal_id
        AND member_principal.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys')
    )
  )
ORDER BY dp.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT '' AS schema_name, dp.name AS object_name, 'USR' AS type
FROM sys.database_principals dp
WHERE dp.type IN ('S','U','G','E','X','C','K')
  AND dp.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys')
ORDER BY dp.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT s.name AS schema_name, o.name AS object_name, 'RULE' AS type
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE o.type = 'R' AND o.is_ms_shipped = 0
ORDER BY s.name, o.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT s.name AS schema_name, t.name AS object_name, 'TR' AS type
FROM sys.triggers t
JOIN sys.objects o ON o.object_id = t.parent_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE t.parent_class_desc = 'OBJECT_OR_COLUMN' AND t.is_ms_shipped = 0
ORDER BY s.name, t.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT 'dbo' AS schema_name, pf.name AS object_name, 'PF' AS type
FROM sys.partition_functions pf
ORDER BY pf.name;", MapObjectType),

            () => RunQuery(options, @"
SELECT 'dbo' AS schema_name, ps.name AS object_name, 'PS' AS type
FROM sys.partition_schemes ps
ORDER BY ps.name;", MapObjectType),
        };

        var bag = new ConcurrentBag<DbObjectInfo>();
        Parallel.ForEach(queries, new ParallelOptions { MaxDegreeOfParallelism = dop }, query =>
        {
            foreach (var item in query())
            {
                bag.Add(item);
            }
        });

        return bag.ToList();
    }

    public virtual IReadOnlyList<DbObjectInfo> ListMatchingObjects(
        SqlConnectionOptions options,
        IReadOnlyList<string> objectTypes,
        string schema,
        string name,
        int maxParallelism = 0)
    {
        var candidates = objectTypes
            .Where(type => SupportedSqlObjectTypes.IsActiveInSync(type))
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (candidates.Length == 0)
        {
            return [];
        }

        var dop = Math.Min(ResolveParallelism(maxParallelism), candidates.Length);
        var bag = new ConcurrentBag<DbObjectInfo>();

        Parallel.ForEach(candidates, new ParallelOptions { MaxDegreeOfParallelism = dop }, objectType =>
        {
            foreach (var item in RunMatchingQuery(options, objectType, schema, name))
            {
                bag.Add(item);
            }
        });

        return bag
            .OrderBy(item => item.Schema, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.ObjectType, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    public virtual string? GetTableCompatibleOmittedTextImageOnDataSpaceName(
        SqlConnectionOptions options,
        string schema,
        string name)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = """
SELECT CASE
           WHEN t.lob_data_space_id <> 0
            AND default_ds.data_space_id IS NOT NULL
            AND t.lob_data_space_id = default_ds.data_space_id
           THEN lob_ds.name
           ELSE NULL
       END
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
LEFT JOIN sys.data_spaces lob_ds ON lob_ds.data_space_id = t.lob_data_space_id
OUTER APPLY (
    SELECT TOP (1) ds.data_space_id
    FROM sys.data_spaces ds
    WHERE ds.is_default = 1
    ORDER BY ds.data_space_id
) default_ds
WHERE s.name = @schema
  AND t.name = @name;
""";
        command.Parameters.AddWithValue("@schema", schema);
        command.Parameters.AddWithValue("@name", name);

        return command.ExecuteScalar() as string;
    }

    internal static int ResolveParallelism(int configured)
        => configured > 0 ? configured : Environment.ProcessorCount;

    private static IEnumerable<DbObjectInfo> RunQuery(
        SqlConnectionOptions options,
        string sql,
        Func<string, string> typeMapper)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();
        // .ToList() materializes results before the connection is disposed.
        return QueryObjects(connection, sql, typeMapper).ToList();
    }

    private static IEnumerable<DbObjectInfo> RunMatchingQuery(
        SqlConnectionOptions options,
        string objectType,
        string schema,
        string name)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();
        return QueryMatchingObjects(connection, objectType, schema, name).ToList();
    }

    private static IEnumerable<DbObjectInfo> RunQueryIfExists(
        SqlConnectionOptions options,
        string objectName,
        string sql,
        Func<string, string> typeMapper)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        if (!ObjectExists(connection, objectName))
        {
            return [];
        }

        // .ToList() materializes results before the connection is disposed.
        return QueryObjects(connection, sql, typeMapper).ToList();
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
            var schema = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
            var name = reader.GetString(1);
            var type = reader.GetString(2);
            var objectType = typeMapper(type);
            var userDefinedTypeKind = MapUserDefinedTypeKind(type);
            if (SupportedSqlObjectTypes.IsSchemaLess(objectType))
            {
                schema = string.Empty;
            }
            else if (string.IsNullOrWhiteSpace(schema))
            {
                schema = "dbo";
            }

            yield return new DbObjectInfo(schema, name, objectType, userDefinedTypeKind);
        }
    }

    private static IEnumerable<DbObjectInfo> QueryMatchingObjects(
        SqlConnection connection,
        string objectType,
        string schema,
        string name)
    {
        using var command = connection.CreateCommand();

        switch (objectType)
        {
            case "Assembly":
                command.CommandText = """
SELECT '' AS schema_name, a.name AS object_name
FROM sys.assemblies a
WHERE a.is_user_defined = 1
  AND a.name = @name
ORDER BY a.name;
""";
                command.Parameters.AddWithValue("@name", name);
                break;

            case "Table":
                command.CommandText = """
SELECT s.name AS schema_name, t.name AS object_name
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE t.is_ms_shipped = 0
  AND s.name = @schema
  AND t.name = @name
ORDER BY s.name, t.name;
""";
                command.Parameters.AddWithValue("@schema", schema);
                command.Parameters.AddWithValue("@name", name);
                break;

            case "View":
                command.CommandText = """
SELECT s.name AS schema_name, o.name AS object_name
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE o.is_ms_shipped = 0
  AND o.type = 'V'
  AND s.name = @schema
  AND o.name = @name
ORDER BY s.name, o.name;
""";
                command.Parameters.AddWithValue("@schema", schema);
                command.Parameters.AddWithValue("@name", name);
                break;

            case "StoredProcedure":
                command.CommandText = """
SELECT s.name AS schema_name, o.name AS object_name
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE o.is_ms_shipped = 0
  AND o.type IN ('P','PC')
  AND s.name = @schema
  AND o.name = @name
ORDER BY s.name, o.name;
""";
                command.Parameters.AddWithValue("@schema", schema);
                command.Parameters.AddWithValue("@name", name);
                break;

            case "Function":
                command.CommandText = """
SELECT s.name AS schema_name, o.name AS object_name
FROM sys.objects o
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE o.is_ms_shipped = 0
  AND o.type IN ('FN','TF','IF','FS','FT')
  AND s.name = @schema
  AND o.name = @name
ORDER BY s.name, o.name;
""";
                command.Parameters.AddWithValue("@schema", schema);
                command.Parameters.AddWithValue("@name", name);
                break;

            case "Sequence":
                command.CommandText = """
SELECT s.name AS schema_name, seq.name AS object_name
FROM sys.sequences seq
JOIN sys.schemas s ON s.schema_id = seq.schema_id
WHERE s.name = @schema
  AND seq.name = @name
ORDER BY s.name, seq.name;
""";
                command.Parameters.AddWithValue("@schema", schema);
                command.Parameters.AddWithValue("@name", name);
                break;

            case "Schema":
                command.CommandText = """
SELECT '' AS schema_name, s.name AS object_name
FROM sys.schemas s
WHERE s.name = @name
  AND (
        s.name NOT IN (
            'dbo',
            'guest',
            'sys',
            'INFORMATION_SCHEMA',
            'db_accessadmin',
            'db_backupoperator',
            'db_datareader',
            'db_datawriter',
            'db_ddladmin',
            'db_denydatareader',
            'db_denydatawriter',
            'db_owner',
            'db_securityadmin')
        OR (
            s.name = 'dbo'
            AND (
                EXISTS (
                    SELECT 1
                    FROM sys.database_permissions dp
                    WHERE dp.class_desc = 'SCHEMA'
                      AND dp.major_id = s.schema_id)
                OR EXISTS (
                    SELECT 1
                    FROM sys.extended_properties ep
                    WHERE ep.class_desc = 'SCHEMA'
                      AND ep.major_id = s.schema_id))
        )
      )
ORDER BY s.name;
""";
                command.Parameters.AddWithValue("@name", name);
                break;

            case "Synonym":
                command.CommandText = """
SELECT s.name AS schema_name, syn.name AS object_name
FROM sys.synonyms syn
JOIN sys.schemas s ON s.schema_id = syn.schema_id
WHERE s.name = @schema
  AND syn.name = @name
ORDER BY s.name, syn.name;
""";
                command.Parameters.AddWithValue("@schema", schema);
                command.Parameters.AddWithValue("@name", name);
                break;

            case "UserDefinedType":
                command.CommandText = """
SELECT s.name AS schema_name, t.name AS object_name, 'UDT_SCALAR' AS type
FROM sys.types t
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE t.is_user_defined = 1
  AND t.is_table_type = 0
  AND s.name = @schema
  AND t.name = @name
UNION ALL
SELECT s.name AS schema_name, tt.name AS object_name, 'UDT_TABLE' AS type
FROM sys.table_types tt
JOIN sys.schemas s ON s.schema_id = tt.schema_id
WHERE s.name = @schema
  AND tt.name = @name
ORDER BY schema_name, object_name;
""";
                command.Parameters.AddWithValue("@schema", schema);
                command.Parameters.AddWithValue("@name", name);
                break;

            case "XmlSchemaCollection":
                command.CommandText = """
SELECT s.name AS schema_name, x.name AS object_name
FROM sys.xml_schema_collections x
JOIN sys.schemas s ON s.schema_id = x.schema_id
WHERE s.name = @schema
  AND x.name = @name
  AND s.name NOT IN ('sys','INFORMATION_SCHEMA')
ORDER BY s.name, x.name;
""";
                command.Parameters.AddWithValue("@schema", schema);
                command.Parameters.AddWithValue("@name", name);
                break;

            case "MessageType":
                command.CommandText = """
SELECT '' AS schema_name, mt.name AS object_name
FROM sys.service_message_types mt
WHERE mt.name = @name
  AND mt.name <> 'DEFAULT'
  AND mt.name NOT LIKE 'http://schemas.microsoft.com/SQL/%'
ORDER BY mt.name;
""";
                command.Parameters.AddWithValue("@name", name);
                break;

            case "Contract":
                command.CommandText = """
SELECT '' AS schema_name, c.name AS object_name
FROM sys.service_contracts c
WHERE c.name = @name
  AND c.name <> 'DEFAULT'
  AND c.name NOT LIKE 'http://schemas.microsoft.com/SQL/%'
ORDER BY c.name;
""";
                command.Parameters.AddWithValue("@name", name);
                break;

            case "Queue":
                command.CommandText = """
SELECT s.name AS schema_name, q.name AS object_name
FROM sys.service_queues q
JOIN sys.objects o ON o.object_id = q.object_id
JOIN sys.schemas s ON s.schema_id = o.schema_id
WHERE s.name = @schema
  AND q.name = @name
  AND q.name NOT IN ('ServiceBrokerQueue', 'QueryNotificationErrorsQueue', 'EventNotificationErrorsQueue')
ORDER BY s.name, q.name;
""";
                command.Parameters.AddWithValue("@schema", schema);
                command.Parameters.AddWithValue("@name", name);
                break;

            case "Service":
                command.CommandText = """
SELECT '' AS schema_name, sv.name AS object_name
FROM sys.services sv
WHERE sv.name = @name
  AND sv.name NOT LIKE 'http://schemas.microsoft.com/SQL/%'
ORDER BY sv.name;
""";
                command.Parameters.AddWithValue("@name", name);
                break;

            case "Route":
                command.CommandText = """
SELECT '' AS schema_name, r.name AS object_name
FROM sys.routes r
WHERE r.name = @name
  AND r.name <> 'AutoCreatedLocal'
ORDER BY r.name;
""";
                command.Parameters.AddWithValue("@name", name);
                break;

            case "EventNotification":
                command.CommandText = """
SELECT '' AS schema_name, en.name AS object_name
FROM sys.event_notifications en
WHERE en.name = @name
ORDER BY en.name;
""";
                command.Parameters.AddWithValue("@name", name);
                break;

            case "Role":
                command.CommandText = """
SELECT '' AS schema_name, dp.name AS object_name
FROM sys.database_principals dp
WHERE dp.type = 'R'
  AND dp.name <> 'public'
  AND dp.name = @name
  AND (
    dp.is_fixed_role = 0
    OR EXISTS (
      SELECT 1
      FROM sys.database_role_members drm
      JOIN sys.database_principals member_principal ON member_principal.principal_id = drm.member_principal_id
      WHERE drm.role_principal_id = dp.principal_id
        AND member_principal.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys')
    )
  )
ORDER BY dp.name;
""";
                command.Parameters.AddWithValue("@name", name);
                break;

            case "ServiceBinding":
                command.CommandText = """
SELECT '' AS schema_name, rsb.name AS object_name
FROM sys.remote_service_bindings rsb
WHERE rsb.name = @name
ORDER BY rsb.name;
""";
                command.Parameters.AddWithValue("@name", name);
                break;

            case "User":
                command.CommandText = """
SELECT '' AS schema_name, dp.name AS object_name
FROM sys.database_principals dp
WHERE dp.type IN ('S','U','G','E','X','C','K')
  AND dp.name NOT IN ('dbo','guest','INFORMATION_SCHEMA','sys')
  AND dp.name = @name
ORDER BY dp.name;
""";
                command.Parameters.AddWithValue("@name", name);
                break;

            case "PartitionFunction":
                command.CommandText = """
SELECT '' AS schema_name, pf.name AS object_name
FROM sys.partition_functions pf
WHERE pf.name = @name
ORDER BY pf.name;
""";
                command.Parameters.AddWithValue("@name", name);
                break;

            case "PartitionScheme":
                command.CommandText = """
SELECT '' AS schema_name, ps.name AS object_name
FROM sys.partition_schemes ps
WHERE ps.name = @name
ORDER BY ps.name;
""";
                command.Parameters.AddWithValue("@name", name);
                break;

            case "FullTextCatalog":
                command.CommandText = """
SELECT '' AS schema_name, fc.name AS object_name
FROM sys.fulltext_catalogs fc
WHERE fc.name = @name
ORDER BY fc.name;
""";
                command.Parameters.AddWithValue("@name", name);
                break;

            case "FullTextStoplist":
                command.CommandText = """
SELECT '' AS schema_name, fs.name AS object_name
FROM sys.fulltext_stoplists fs
WHERE fs.name = @name
  AND fs.stoplist_id > 0
ORDER BY fs.name;
""";
                command.Parameters.AddWithValue("@name", name);
                break;

            case "SearchPropertyList":
                if (!ObjectExists(connection, "sys.registered_search_property_lists"))
                {
                    yield break;
                }

                command.CommandText = """
SELECT '' AS schema_name, sp.name AS object_name
FROM sys.registered_search_property_lists sp
WHERE sp.name = @name
ORDER BY sp.name;
""";
                command.Parameters.AddWithValue("@name", name);
                break;

            default:
                yield break;
        }

        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            var matchedSchema = reader.IsDBNull(0) ? string.Empty : reader.GetString(0);
            var matchedName = reader.GetString(1);
            var userDefinedTypeKind = string.Equals(objectType, "UserDefinedType", StringComparison.OrdinalIgnoreCase)
                && reader.FieldCount > 2
                ? MapUserDefinedTypeKind(reader.GetString(2))
                : null;
            yield return new DbObjectInfo(matchedSchema, matchedName, objectType, userDefinedTypeKind);
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
            "ASSEMBLY" => "Assembly",
            "U" => "Table",
            "V" => "View",
            "P" or "PC" => "StoredProcedure",
            "FN" or "TF" or "IF" or "FS" or "FT" => "Function",
            "SQ" => "Sequence",
            "SC" => "Schema",
            "SY" => "Synonym",
            "UDT" or "UDT_SCALAR" or "UDT_TABLE" => "UserDefinedType",
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

    private static UserDefinedTypeKind? MapUserDefinedTypeKind(string type)
        => type.Trim() switch
        {
            "UDT_SCALAR" => UserDefinedTypeKind.Scalar,
            "UDT_TABLE" => UserDefinedTypeKind.Table,
            _ => null
        };
}
