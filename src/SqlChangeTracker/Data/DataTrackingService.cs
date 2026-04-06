using Microsoft.Data.SqlClient;
using SqlChangeTracker.Config;
using SqlChangeTracker.Sql;
using SqlChangeTracker.Sync;

namespace SqlChangeTracker.Data;

internal sealed record DataTrackPlan(
    string ProjectDir,
    string ProjectDisplayPath,
    string ConfigPath,
    string Pattern,
    IReadOnlyList<string> MatchedTables,
    IReadOnlyList<string> CurrentTrackedTables,
    IReadOnlyList<string> NextTrackedTables,
    bool WouldChange);

internal sealed record DataUntrackPlan(
    string ProjectDir,
    string ProjectDisplayPath,
    string ConfigPath,
    string Pattern,
    IReadOnlyList<string> MatchedTables,
    IReadOnlyList<string> CurrentTrackedTables,
    IReadOnlyList<string> NextTrackedTables,
    bool WouldChange);

internal interface IDataTrackingService
{
    CommandExecutionResult<DataTrackPlan> PrepareTrack(string? projectDir, string pattern);

    CommandExecutionResult<DataTrackResult> ApplyTrack(DataTrackPlan plan);

    CommandExecutionResult<DataUntrackPlan> PrepareUntrack(string? projectDir, string pattern);

    CommandExecutionResult<DataUntrackResult> ApplyUntrack(DataUntrackPlan plan);

    CommandExecutionResult<DataListResult> RunList(string? projectDir);
}

internal sealed class DataTrackingService : IDataTrackingService
{
    private readonly SqlctConfigReader _configReader;
    private readonly SqlctConfigWriter _configWriter;

    public DataTrackingService()
        : this(new SqlctConfigReader(), new SqlctConfigWriter())
    {
    }

    internal DataTrackingService(SqlctConfigReader configReader, SqlctConfigWriter configWriter)
    {
        _configReader = configReader;
        _configWriter = configWriter;
    }

    public CommandExecutionResult<DataTrackPlan> PrepareTrack(string? projectDir, string pattern)
    {
        var patternResult = TryParsePattern(pattern);
        if (!patternResult.Success)
        {
            return CommandExecutionResult<DataTrackPlan>.Failure(patternResult.Error!, patternResult.ExitCode);
        }

        var projectResult = LoadProject(projectDir, requireDatabase: true);
        if (!projectResult.Success)
        {
            return CommandExecutionResult<DataTrackPlan>.Failure(projectResult.Error!, projectResult.ExitCode);
        }

        IReadOnlyList<string> matchedTables;
        try
        {
            matchedTables = ListUserTables(projectResult.Payload!.ConnectionOptions!)
                .Where(table => patternResult.Payload!.Matches(table.Schema, table.Name))
                .Select(table => $"{table.Schema}.{table.Name}")
                .OrderBy(value => value, StringComparer.OrdinalIgnoreCase)
                .ToArray();
        }
        catch (SqlException sqlEx)
        {
            var code = IsDatabaseNotFound(sqlEx) ? ErrorCodes.DatabaseNotFound : ErrorCodes.ConnectionFailed;
            var message = code == ErrorCodes.DatabaseNotFound
                ? "database not found."
                : "failed to connect to SQL Server.";
            return CommandExecutionResult<DataTrackPlan>.Failure(
                new ErrorInfo(code, message, Detail: sqlEx.Message),
                ExitCodes.ConnectionFailure);
        }
        catch (Exception ex) when (ex is IOException || ex is InvalidOperationException)
        {
            return CommandExecutionResult<DataTrackPlan>.Failure(
                new ErrorInfo(ErrorCodes.ExecutionFailed, "failed to resolve tracked tables.", Detail: ex.Message),
                ExitCodes.ExecutionFailure);
        }

        var currentTracked = projectResult.Payload.Config.Data.TrackedTables.ToArray();
        var nextTracked = SqlctConfigNormalizer.NormalizeTrackedTables(currentTracked.Concat(matchedTables));
        var changed = !currentTracked.SequenceEqual(nextTracked, StringComparer.OrdinalIgnoreCase);

        return CommandExecutionResult<DataTrackPlan>.Ok(
            new DataTrackPlan(
                projectResult.Payload.ProjectDir,
                projectResult.Payload.DisplayPath,
                projectResult.Payload.ConfigPath,
                pattern.Trim(),
                matchedTables,
                currentTracked,
                nextTracked,
                changed),
            ExitCodes.Success);
    }

    public CommandExecutionResult<DataTrackResult> ApplyTrack(DataTrackPlan plan)
    {
        var readResult = _configReader.Read(plan.ConfigPath);
        if (!readResult.Success)
        {
            return CommandExecutionResult<DataTrackResult>.Failure(readResult.Error!, readResult.ExitCode);
        }

        var config = readResult.Config!;
        config.Data.TrackedTables = plan.NextTrackedTables.ToList();
        var writeResult = _configWriter.Write(plan.ConfigPath, config, overwriteExisting: true);
        if (!writeResult.Success)
        {
            return CommandExecutionResult<DataTrackResult>.Failure(writeResult.Error!, writeResult.ExitCode);
        }

        return CommandExecutionResult<DataTrackResult>.Ok(
            new DataTrackResult(
                "data track",
                plan.ProjectDisplayPath,
                plan.Pattern,
                plan.WouldChange,
                false,
                plan.MatchedTables,
                config.Data.TrackedTables),
            ExitCodes.Success);
    }

    public CommandExecutionResult<DataUntrackPlan> PrepareUntrack(string? projectDir, string pattern)
    {
        var patternResult = TryParsePattern(pattern);
        if (!patternResult.Success)
        {
            return CommandExecutionResult<DataUntrackPlan>.Failure(patternResult.Error!, patternResult.ExitCode);
        }

        var projectResult = LoadProject(projectDir, requireDatabase: false);
        if (!projectResult.Success)
        {
            return CommandExecutionResult<DataUntrackPlan>.Failure(projectResult.Error!, projectResult.ExitCode);
        }

        var currentTracked = projectResult.Payload!.Config.Data.TrackedTables.ToArray();
        var matchedTables = currentTracked
            .Select(value =>
            {
                Sync.SyncCommandService.TryParseSchemaAndName(value, out var schema, out var name);
                return new { Value = value, Schema = schema, Name = name };
            })
            .Where(table => patternResult.Payload!.Matches(table.Schema, table.Name))
            .Select(table => table.Value)
            .OrderBy(value => value, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var nextTracked = currentTracked
            .Where(value => !matchedTables.Contains(value, StringComparer.OrdinalIgnoreCase))
            .ToList();
        var changed = matchedTables.Length > 0;

        return CommandExecutionResult<DataUntrackPlan>.Ok(
            new DataUntrackPlan(
                projectResult.Payload.ProjectDir,
                projectResult.Payload.DisplayPath,
                projectResult.Payload.ConfigPath,
                pattern.Trim(),
                matchedTables,
                currentTracked,
                nextTracked,
                changed),
            ExitCodes.Success);
    }

    public CommandExecutionResult<DataUntrackResult> ApplyUntrack(DataUntrackPlan plan)
    {
        var readResult = _configReader.Read(plan.ConfigPath);
        if (!readResult.Success)
        {
            return CommandExecutionResult<DataUntrackResult>.Failure(readResult.Error!, readResult.ExitCode);
        }

        var config = readResult.Config!;
        config.Data.TrackedTables = plan.NextTrackedTables.ToList();
        var writeResult = _configWriter.Write(plan.ConfigPath, config, overwriteExisting: true);
        if (!writeResult.Success)
        {
            return CommandExecutionResult<DataUntrackResult>.Failure(writeResult.Error!, writeResult.ExitCode);
        }

        return CommandExecutionResult<DataUntrackResult>.Ok(
            new DataUntrackResult(
                "data untrack",
                plan.ProjectDisplayPath,
                plan.Pattern,
                plan.WouldChange,
                false,
                plan.MatchedTables,
                config.Data.TrackedTables),
            ExitCodes.Success);
    }

    public CommandExecutionResult<DataListResult> RunList(string? projectDir)
    {
        var projectResult = LoadProject(projectDir, requireDatabase: false);
        if (!projectResult.Success)
        {
            return CommandExecutionResult<DataListResult>.Failure(projectResult.Error!, projectResult.ExitCode);
        }

        return CommandExecutionResult<DataListResult>.Ok(
            new DataListResult(
                "data list",
                projectResult.Payload!.DisplayPath,
                projectResult.Payload.Config.Data.TrackedTables.ToArray()),
            ExitCodes.Success);
    }

    private CommandExecutionResult<ProjectContext> LoadProject(string? projectDir, bool requireDatabase)
    {
        var resolved = ResolveProjectDir(projectDir);
        var configPath = SqlctConfigWriter.GetDefaultPath(resolved.FullPath);

        var readResult = _configReader.Read(configPath);
        if (!readResult.Success)
        {
            return CommandExecutionResult<ProjectContext>.Failure(readResult.Error!, readResult.ExitCode);
        }

        var config = readResult.Config!;
        SqlConnectionOptions? options = null;
        if (requireDatabase)
        {
            var validation = ValidateDatabaseConfig(config);
            if (!validation.Success)
            {
                return CommandExecutionResult<ProjectContext>.Failure(validation.Error!, validation.ExitCode);
            }

            options = new SqlConnectionOptions(
                config.Database.Server,
                config.Database.Name,
                config.Database.Auth,
                string.IsNullOrWhiteSpace(config.Database.User) ? null : config.Database.User,
                string.IsNullOrWhiteSpace(config.Database.Password) ? null : config.Database.Password,
                config.Database.TrustServerCertificate);
        }

        return CommandExecutionResult<ProjectContext>.Ok(
            new ProjectContext(
                resolved.FullPath,
                resolved.DisplayPath,
                configPath,
                config,
                options),
            ExitCodes.Success);
    }

    private static CommandExecutionResult<TablePattern> TryParsePattern(string pattern)
    {
        var trimmed = pattern?.Trim() ?? string.Empty;
        if (!Sync.SyncCommandService.TryParseSchemaAndName(trimmed, out var schemaToken, out var nameToken))
        {
            return CommandExecutionResult<TablePattern>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid table pattern.", Detail: "expected format: schema.name, schema.*, or *.name"),
                ExitCodes.InvalidConfig);
        }

        if (string.Equals(schemaToken, "*", StringComparison.Ordinal) &&
            string.Equals(nameToken, "*", StringComparison.Ordinal))
        {
            return CommandExecutionResult<TablePattern>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid table pattern.", Detail: "pattern '*.*' is not supported."),
                ExitCodes.InvalidConfig);
        }

        return CommandExecutionResult<TablePattern>.Ok(new TablePattern(schemaToken, nameToken), ExitCodes.Success);
    }

    private static IReadOnlyList<(string Schema, string Name)> ListUserTables(SqlConnectionOptions options)
    {
        using var connection = SqlConnectionFactory.Create(options);
        connection.Open();

        using var command = connection.CreateCommand();
        command.CommandText = @"
SELECT s.name AS schema_name, t.name AS table_name
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE t.is_ms_shipped = 0
ORDER BY s.name, t.name;";

        using var reader = command.ExecuteReader();
        var tables = new List<(string Schema, string Name)>();
        while (reader.Read())
        {
            tables.Add((reader.GetString(0), reader.GetString(1)));
        }

        return tables;
    }

    private static bool IsDatabaseNotFound(SqlException ex)
        => ex.Number == 4060;

    private static CommandExecutionResult<bool> ValidateDatabaseConfig(SqlctConfig config)
    {
        if (string.IsNullOrWhiteSpace(config.Database.Server))
        {
            return CommandExecutionResult<bool>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid config file.", Detail: "missing required field: database.server."),
                ExitCodes.InvalidConfig);
        }

        if (string.IsNullOrWhiteSpace(config.Database.Name))
        {
            return CommandExecutionResult<bool>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid config file.", Detail: "missing required field: database.name."),
                ExitCodes.InvalidConfig);
        }

        if (!string.Equals(config.Database.Auth, "integrated", StringComparison.OrdinalIgnoreCase) &&
            !string.Equals(config.Database.Auth, "sql", StringComparison.OrdinalIgnoreCase))
        {
            return CommandExecutionResult<bool>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid config file.", Detail: "database.auth must be 'integrated' or 'sql'."),
                ExitCodes.InvalidConfig);
        }

        if (string.Equals(config.Database.Auth, "sql", StringComparison.OrdinalIgnoreCase) &&
            string.IsNullOrWhiteSpace(config.Database.User))
        {
            return CommandExecutionResult<bool>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid config file.", Detail: "missing required field: database.user for sql authentication."),
                ExitCodes.InvalidConfig);
        }

        return CommandExecutionResult<bool>.Ok(true, ExitCodes.Success);
    }

    private static ResolvedPath ResolveProjectDir(string? projectDir)
    {
        var input = string.IsNullOrWhiteSpace(projectDir) ? Environment.CurrentDirectory : projectDir!;
        var fullPath = Path.GetFullPath(input, Environment.CurrentDirectory);
        var displayPath = NormalizeDisplayPath(fullPath, input);
        return new ResolvedPath(fullPath, displayPath);
    }

    private static string NormalizeDisplayPath(string fullPath, string originalInput)
    {
        if (Path.IsPathRooted(originalInput))
        {
            return fullPath;
        }

        var relative = Path.GetRelativePath(Environment.CurrentDirectory, fullPath);
        if (relative.StartsWith(".", StringComparison.Ordinal))
        {
            return relative;
        }

        var prefix = Path.DirectorySeparatorChar == '\\' ? ".\\" : "./";
        return prefix + relative;
    }

    private sealed record ProjectContext(
        string ProjectDir,
        string DisplayPath,
        string ConfigPath,
        SqlctConfig Config,
        SqlConnectionOptions? ConnectionOptions);

    private sealed record ResolvedPath(string FullPath, string DisplayPath);

    private sealed record TablePattern(string SchemaToken, string NameToken)
    {
        public bool Matches(string schema, string name)
        {
            var schemaMatches = string.Equals(SchemaToken, "*", StringComparison.Ordinal)
                || string.Equals(SchemaToken, schema, StringComparison.OrdinalIgnoreCase);
            var nameMatches = string.Equals(NameToken, "*", StringComparison.Ordinal)
                || string.Equals(NameToken, name, StringComparison.OrdinalIgnoreCase);
            return schemaMatches && nameMatches;
        }
    }
}
