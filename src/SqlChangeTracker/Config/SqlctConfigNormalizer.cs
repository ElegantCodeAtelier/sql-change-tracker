namespace SqlChangeTracker.Config;

internal static class SqlctConfigNormalizer
{
    public static void Normalize(SqlctConfig config)
    {
        config.Database ??= new DatabaseConfig();
        config.Options ??= new OptionsConfig();
        config.Data ??= new DataConfig();
        config.Data.TrackedTables = NormalizeTrackedTables(config.Data.TrackedTables);
    }

    public static List<string> NormalizeTrackedTables(IEnumerable<string>? trackedTables)
    {
        if (trackedTables == null)
        {
            return [];
        }

        return trackedTables
            .Where(value => !string.IsNullOrWhiteSpace(value))
            .Select(value => value.Trim())
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(value => value, StringComparer.OrdinalIgnoreCase)
            .ToList();
    }
}
