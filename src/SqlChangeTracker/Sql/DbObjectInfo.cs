namespace SqlChangeTracker.Sql;

internal enum UserDefinedTypeKind
{
    Scalar,
    Table
}

internal sealed record DbObjectInfo(
    string Schema,
    string Name,
    string ObjectType,
    UserDefinedTypeKind? UserDefinedTypeKind = null);
