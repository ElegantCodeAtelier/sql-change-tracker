namespace SqlChangeTracker.Sql;

internal sealed record DbObjectInfo(
    string Schema,
    string Name,
    string ObjectType);
