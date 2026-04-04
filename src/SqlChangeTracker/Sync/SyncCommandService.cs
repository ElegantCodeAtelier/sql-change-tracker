using Microsoft.Data.SqlClient;
using SqlChangeTracker.Config;
using SqlChangeTracker.Schema;
using SqlChangeTracker.Sql;
using System.Text;

namespace SqlChangeTracker.Sync;

internal interface ISyncCommandService
{
    CommandExecutionResult<StatusResult> RunStatus(string? projectDir, string? target, Action<string>? progress = null);

    CommandExecutionResult<DiffResult> RunDiff(string? projectDir, string? target, string? objectName, Action<string>? progress = null);

    CommandExecutionResult<PullResult> RunPull(string? projectDir, Action<string>? progress = null);
}

internal sealed record CommandExecutionResult<T>(
    bool Success,
    T? Payload,
    ErrorInfo? Error,
    int ExitCode)
{
    public static CommandExecutionResult<T> Ok(T payload, int exitCode)
        => new(true, payload, null, exitCode);

    public static CommandExecutionResult<T> Failure(ErrorInfo error, int exitCode)
        => new(false, default, error, exitCode);
}

internal enum ComparisonTarget
{
    Db,
    Folder
}

internal sealed class SyncCommandService : ISyncCommandService
{
    private static readonly IReadOnlyList<FolderMapEntry> CoreFolderMap =
    [
        new("Table", "Tables"),
        new("View", "Views"),
        new("StoredProcedure", "Stored Procedures"),
        new("Function", "Functions"),
        new("Sequence", "Sequences"),
        new("Default", "Other")
    ];

    private static readonly IReadOnlyList<string> CoreObjectTypes =
    [
        "Function",
        "Sequence",
        "StoredProcedure",
        "Table",
        "View"
    ];

    private static readonly IReadOnlyDictionary<string, string> CoreTypeFolders =
        new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase)
        {
            ["Table"] = "Tables",
            ["View"] = "Views",
            ["StoredProcedure"] = "Stored Procedures",
            ["Function"] = "Functions",
            ["Sequence"] = "Sequences"
        };

    private readonly SqlctConfigReader _configReader;
    private readonly SqlServerIntrospector _introspector;
    private readonly SqlServerScripter _scripter;
    private readonly SchemaFolderMapper _mapper;

    public SyncCommandService()
        : this(new SqlctConfigReader(), new SqlServerIntrospector(), new SqlServerScripter(), new SchemaFolderMapper(CoreFolderMap, dataWriteAllFilesInOneDirectory: true))
    {
    }

    internal SyncCommandService(
        SqlctConfigReader configReader,
        SqlServerIntrospector introspector,
        SqlServerScripter scripter,
        SchemaFolderMapper mapper)
    {
        _configReader = configReader;
        _introspector = introspector;
        _scripter = scripter;
        _mapper = mapper;
    }

    public CommandExecutionResult<StatusResult> RunStatus(string? projectDir, string? target, Action<string>? progress = null)
    {
        if (!TryParseTarget(target, out var comparisonTarget))
        {
            return CommandExecutionResult<StatusResult>.Failure(
                new ErrorInfo(
                    ErrorCodes.InvalidConfig,
                    "invalid target value.",
                    Detail: "allowed values: db, folder."),
                ExitCodes.InvalidConfig);
        }

        var projectResult = LoadProject(projectDir);
        if (!projectResult.Success)
        {
            return CommandExecutionResult<StatusResult>.Failure(projectResult.Error!, projectResult.ExitCode);
        }

        var snapshotResult = BuildSnapshot(projectResult.Payload!, progress);
        if (!snapshotResult.Success)
        {
            return CommandExecutionResult<StatusResult>.Failure(snapshotResult.Error!, snapshotResult.ExitCode);
        }

        var changes = ComputeChanges(snapshotResult.Payload!, comparisonTarget);
        var summary = new StatusSummary(
            changes.Count(entry => string.Equals(entry.Change, "added", StringComparison.OrdinalIgnoreCase)),
            changes.Count(entry => string.Equals(entry.Change, "changed", StringComparison.OrdinalIgnoreCase)),
            changes.Count(entry => string.Equals(entry.Change, "deleted", StringComparison.OrdinalIgnoreCase)));

        var status = new StatusResult(
            "status",
            projectResult.Payload!.DisplayPath,
            comparisonTarget == ComparisonTarget.Db ? "db" : "folder",
            summary,
            changes.Select(entry => new StatusObject(entry.Object.DisplayName, entry.Object.ObjectType, entry.Change)).ToArray(),
            snapshotResult.Payload!.Warnings);

        var exitCode = summary.Added + summary.Changed + summary.Deleted > 0
            ? ExitCodes.DiffExists
            : ExitCodes.Success;

        return CommandExecutionResult<StatusResult>.Ok(status, exitCode);
    }

    public CommandExecutionResult<DiffResult> RunDiff(string? projectDir, string? target, string? objectName, Action<string>? progress = null)
    {
        if (!TryParseTarget(target, out var comparisonTarget))
        {
            return CommandExecutionResult<DiffResult>.Failure(
                new ErrorInfo(
                    ErrorCodes.InvalidConfig,
                    "invalid target value.",
                    Detail: "allowed values: db, folder."),
                ExitCodes.InvalidConfig);
        }

        var projectResult = LoadProject(projectDir);
        if (!projectResult.Success)
        {
            return CommandExecutionResult<DiffResult>.Failure(projectResult.Error!, projectResult.ExitCode);
        }

        var snapshotResult = BuildSnapshot(projectResult.Payload!, progress);
        if (!snapshotResult.Success)
        {
            return CommandExecutionResult<DiffResult>.Failure(snapshotResult.Error!, snapshotResult.ExitCode);
        }

        var changes = ComputeChanges(snapshotResult.Payload!, comparisonTarget);
        var sourceLabel = comparisonTarget == ComparisonTarget.Db ? "db" : "folder";
        var targetLabel = comparisonTarget == ComparisonTarget.Db ? "folder" : "db";

        if (!string.IsNullOrWhiteSpace(objectName))
        {
            var parsedObject = ParseObjectSelector(objectName!);
            if (!parsedObject.Success)
            {
                return CommandExecutionResult<DiffResult>.Failure(parsedObject.Error!, parsedObject.ExitCode);
            }

            var selected = SelectObject(snapshotResult.Payload!, parsedObject.Payload!.Schema, parsedObject.Payload.Name);
            if (!selected.Success)
            {
                return CommandExecutionResult<DiffResult>.Failure(selected.Error!, selected.ExitCode);
            }

            var entry = BuildChangeEntry(snapshotResult.Payload!, comparisonTarget, selected.Payload!);
            var diff = BuildDiffText(entry, sourceLabel, targetLabel);

            var result = new DiffResult(
                "diff",
                projectResult.Payload!.DisplayPath,
                comparisonTarget == ComparisonTarget.Db ? "db" : "folder",
                selected.Payload!.DisplayName,
                diff,
                snapshotResult.Payload!.Warnings);

            var exitCode = string.IsNullOrWhiteSpace(diff) ? ExitCodes.Success : ExitCodes.DiffExists;
            return CommandExecutionResult<DiffResult>.Ok(result, exitCode);
        }

        var diffSections = changes
            .Select(change => BuildDiffSection(change, sourceLabel, targetLabel))
            .Where(section => !string.IsNullOrWhiteSpace(section))
            .ToArray();

        var combinedDiff = string.Join(Environment.NewLine + Environment.NewLine, diffSections);

        var aggregate = new DiffResult(
            "diff",
            projectResult.Payload!.DisplayPath,
            comparisonTarget == ComparisonTarget.Db ? "db" : "folder",
            null,
            combinedDiff,
            snapshotResult.Payload!.Warnings);

        return CommandExecutionResult<DiffResult>.Ok(
            aggregate,
            string.IsNullOrWhiteSpace(combinedDiff) ? ExitCodes.Success : ExitCodes.DiffExists);
    }

    public CommandExecutionResult<PullResult> RunPull(string? projectDir, Action<string>? progress = null)
    {
        var projectResult = LoadProject(projectDir);
        if (!projectResult.Success)
        {
            return CommandExecutionResult<PullResult>.Failure(projectResult.Error!, projectResult.ExitCode);
        }

        var snapshotResult = BuildSnapshot(projectResult.Payload!, progress);
        if (!snapshotResult.Success)
        {
            return CommandExecutionResult<PullResult>.Failure(snapshotResult.Error!, snapshotResult.ExitCode);
        }

        var snapshot = snapshotResult.Payload!;
        var keys = snapshot.DbObjects.Keys
            .Concat(snapshot.FolderObjects.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(key => snapshot.DbObjects.TryGetValue(key, out var dbObj) ? dbObj : snapshot.FolderObjects[key])
            .OrderBy(item => item.Schema, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.ObjectType, StringComparer.OrdinalIgnoreCase)
            .Select(item => item.Key)
            .ToArray();

        var changes = new List<PullObject>();
        var created = 0;
        var updated = 0;
        var deleted = 0;
        var unchanged = 0;
        var writeIndex = 0;
        var writeTotal = keys.Length;

        foreach (var key in keys)
        {
            writeIndex++;
            snapshot.DbObjects.TryGetValue(key, out var dbObject);
            snapshot.FolderObjects.TryGetValue(key, out var folderObject);
            var displayName = (dbObject ?? folderObject)?.DisplayName ?? key;
            progress?.Invoke($"Writing objects ({writeIndex}/{writeTotal}): {displayName}");

            if (dbObject is not null && folderObject is null)
            {
                var write = WriteScriptFile(dbObject.FullPath, dbObject.Script, existingFile: false);
                if (!write.Success)
                {
                    return CommandExecutionResult<PullResult>.Failure(write.Error!, write.ExitCode);
                }

                created++;
                changes.Add(new PullObject(dbObject.DisplayName, dbObject.ObjectType, "created", dbObject.RelativePath));
                continue;
            }

            if (dbObject is null && folderObject is not null)
            {
                try
                {
                    File.Delete(folderObject.FullPath);
                }
                catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
                {
                    return CommandExecutionResult<PullResult>.Failure(
                        new ErrorInfo(ErrorCodes.IoFailed, "failed to delete schema script.", File: folderObject.RelativePath, Detail: ex.Message),
                        ExitCodes.ExecutionFailure);
                }

                deleted++;
                changes.Add(new PullObject(folderObject.DisplayName, folderObject.ObjectType, "deleted", folderObject.RelativePath));
                continue;
            }

            if (dbObject is null || folderObject is null)
            {
                continue;
            }

            if (ScriptsEqualForComparison(dbObject.Script, folderObject.Script))
            {
                unchanged++;
                continue;
            }

            var update = WriteScriptFile(dbObject.FullPath, dbObject.Script, existingFile: true);
            if (!update.Success)
            {
                return CommandExecutionResult<PullResult>.Failure(update.Error!, update.ExitCode);
            }

            if (update.Payload)
            {
                updated++;
                changes.Add(new PullObject(dbObject.DisplayName, dbObject.ObjectType, "updated", dbObject.RelativePath));
            }
            else
            {
                unchanged++;
            }
        }

        var result = new PullResult(
            "pull",
            projectResult.Payload!.DisplayPath,
            new PullSummary(created, updated, deleted, unchanged),
            changes,
            snapshot.Warnings);

        return CommandExecutionResult<PullResult>.Ok(result, ExitCodes.Success);
    }

    private CommandExecutionResult<bool> WriteScriptFile(string fullPath, string script, bool existingFile)
    {
        try
        {
            Directory.CreateDirectory(Path.GetDirectoryName(fullPath)!);

            var style = existingFile
                ? DetectExistingStyle(fullPath)
                : FileContentStyle.Default;
            var styledScript = ApplyStyle(script, style);

            if (existingFile)
            {
                var currentContent = File.ReadAllText(fullPath, style.Encoding);
                if (string.Equals(currentContent, styledScript, StringComparison.Ordinal))
                {
                    return CommandExecutionResult<bool>.Ok(false, ExitCodes.Success);
                }
            }

            File.WriteAllText(fullPath, styledScript, style.Encoding);
            return CommandExecutionResult<bool>.Ok(true, ExitCodes.Success);
        }
        catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
        {
            return CommandExecutionResult<bool>.Failure(
                new ErrorInfo(ErrorCodes.IoFailed, "failed to write schema script.", File: fullPath, Detail: ex.Message),
                ExitCodes.ExecutionFailure);
        }
    }

    private static bool TryParseTarget(string? value, out ComparisonTarget target)
    {
        if (string.IsNullOrWhiteSpace(value) || string.Equals(value, "db", StringComparison.OrdinalIgnoreCase))
        {
            target = ComparisonTarget.Db;
            return true;
        }

        if (string.Equals(value, "folder", StringComparison.OrdinalIgnoreCase))
        {
            target = ComparisonTarget.Folder;
            return true;
        }

        target = default;
        return false;
    }

    private CommandExecutionResult<ProjectContext> LoadProject(string? projectDir)
    {
        var resolved = ResolveProjectDir(projectDir);
        var configPath = SqlctConfigWriter.GetDefaultPath(resolved.FullPath);

        var readResult = _configReader.Read(configPath);
        if (!readResult.Success)
        {
            return CommandExecutionResult<ProjectContext>.Failure(readResult.Error!, readResult.ExitCode);
        }

        var config = readResult.Config!;
        var validation = ValidateDatabaseConfig(config);
        if (!validation.Success)
        {
            return CommandExecutionResult<ProjectContext>.Failure(validation.Error!, validation.ExitCode);
        }

        var options = new SqlConnectionOptions(
            config.Database.Server,
            config.Database.Name,
            config.Database.Auth,
            string.IsNullOrWhiteSpace(config.Database.User) ? null : config.Database.User,
            string.IsNullOrWhiteSpace(config.Database.Password) ? null : config.Database.Password,
            config.Database.TrustServerCertificate);

        return CommandExecutionResult<ProjectContext>.Ok(new ProjectContext(
            resolved.FullPath,
            resolved.DisplayPath,
            config,
            options),
            ExitCodes.Success);
    }

    private CommandExecutionResult<ComparisonSnapshot> BuildSnapshot(ProjectContext context, Action<string>? progress = null)
    {
        progress?.Invoke("Scanning schema folder...");
        var folderResult = ScanFolder(context.ProjectDir);
        if (!folderResult.Success)
        {
            return CommandExecutionResult<ComparisonSnapshot>.Failure(folderResult.Error!, folderResult.ExitCode);
        }

        var dbResult = ScanDatabase(context, progress);
        if (!dbResult.Success)
        {
            return CommandExecutionResult<ComparisonSnapshot>.Failure(dbResult.Error!, dbResult.ExitCode);
        }

        var warnings = folderResult.Payload!.Warnings.Concat(dbResult.Payload!.Warnings)
            .OrderBy(entry => entry.Code, StringComparer.OrdinalIgnoreCase)
            .ThenBy(entry => entry.Message, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        return CommandExecutionResult<ComparisonSnapshot>.Ok(
            new ComparisonSnapshot(dbResult.Payload!.Objects, folderResult.Payload!.Objects, warnings),
            ExitCodes.Success);
    }
    private CommandExecutionResult<ScanResult> ScanFolder(string projectDir)
    {
        var objects = new Dictionary<string, InternalObject>(StringComparer.OrdinalIgnoreCase);
        var warnings = new List<CommandWarning>();
        var scannedSqlFiles = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var entry in CoreTypeFolders.OrderBy(item => item.Key, StringComparer.OrdinalIgnoreCase))
        {
            var objectType = entry.Key;
            var folder = entry.Value;
            var fullFolderPath = Path.Combine(projectDir, folder);
            if (!Directory.Exists(fullFolderPath))
            {
                continue;
            }

            string[] files;
            try
            {
                files = Directory.EnumerateFiles(fullFolderPath, "*.sql", SearchOption.TopDirectoryOnly)
                    .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
                    .ToArray();
            }
            catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
            {
                return CommandExecutionResult<ScanResult>.Failure(
                    new ErrorInfo(ErrorCodes.IoFailed, "failed to scan schema folder.", File: fullFolderPath, Detail: ex.Message),
                    ExitCodes.ExecutionFailure);
            }

            foreach (var file in files)
            {
                var normalizedFilePath = Path.GetFullPath(file);
                scannedSqlFiles.Add(normalizedFilePath);

                var fileName = Path.GetFileNameWithoutExtension(file);
                if (!TryParseSchemaAndName(fileName, out var schema, out var name))
                {
                    warnings.Add(new CommandWarning(
                        "invalid_script_name",
                        $"skipped '{Path.Combine(folder, Path.GetFileName(file))}' because it does not match 'Schema.Object.sql'."));
                    continue;
                }
                var key = BuildObjectKey(objectType, schema, name);
                if (objects.ContainsKey(key))
                {
                    warnings.Add(new CommandWarning(
                        "duplicate_script",
                        $"skipped duplicate folder object '{schema}.{name}' of type '{objectType}'."));
                    continue;
                }

                string script;
                try
                {
                    script = File.ReadAllText(file);
                }
                catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
                {
                    return CommandExecutionResult<ScanResult>.Failure(
                        new ErrorInfo(ErrorCodes.IoFailed, "failed to read schema script.", File: file, Detail: ex.Message),
                        ExitCodes.ExecutionFailure);
                }

                var relativePath = Path.Combine(folder, Path.GetFileName(file));
                objects[key] = new InternalObject(
                    key,
                    schema,
                    name,
                    objectType,
                    script,
                    relativePath,
                    normalizedFilePath);
            }
        }

        try
        {
            warnings.AddRange(CollectUnsupportedFolderWarnings(projectDir, scannedSqlFiles));
        }
        catch (Exception ex) when (ex is IOException || ex is UnauthorizedAccessException)
        {
            return CommandExecutionResult<ScanResult>.Failure(
                new ErrorInfo(ErrorCodes.IoFailed, "failed to scan schema folder.", File: projectDir, Detail: ex.Message),
                ExitCodes.ExecutionFailure);
        }

        return CommandExecutionResult<ScanResult>.Ok(new ScanResult(objects, warnings), ExitCodes.Success);
    }

    private CommandExecutionResult<ScanResult> ScanDatabase(ProjectContext context, Action<string>? progress = null)
    {
        progress?.Invoke("Connecting to database...");
        IReadOnlyList<DbObjectInfo> listedObjects;
        try
        {
            listedObjects = _introspector.ListObjects(context.ConnectionOptions);
        }
        catch (Exception ex)
        {
            return ToRuntimeFailure<ScanResult>(ex, "failed to read database objects.");
        }

        var warnings = listedObjects
            .Where(item => !CoreObjectTypes.Contains(item.ObjectType, StringComparer.OrdinalIgnoreCase))
            .Select(item => item.ObjectType)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .Select(item => new CommandWarning("unsupported_object_type", $"skipped unsupported object type '{item}'."))
            .ToList();

        var activeObjects = listedObjects
            .Where(item => CoreObjectTypes.Contains(item.ObjectType, StringComparer.OrdinalIgnoreCase))
            .OrderBy(item => item.Schema, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.ObjectType, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var total = activeObjects.Length;
        var scriptIndex = 0;
        var objects = new Dictionary<string, InternalObject>(StringComparer.OrdinalIgnoreCase);
        foreach (var dbObject in activeObjects)
        {
            scriptIndex++;
            progress?.Invoke($"Scripting objects ({scriptIndex}/{total}): {dbObject.Schema}.{dbObject.Name}");
            string relativePath;
            try
            {
                relativePath = _mapper.GetObjectPath(
                    dbObject.ObjectType,
                    new ObjectIdentifier(dbObject.Schema, dbObject.Name),
                    isData: false);
            }
            catch (Exception ex)
            {
                return ToRuntimeFailure<ScanResult>(ex, "failed to map object path.");
            }

            var fullPath = Path.Combine(context.ProjectDir, relativePath);
            var referencePath = File.Exists(fullPath) ? fullPath : null;

            string script;
            try
            {
                script = _scripter.ScriptObject(context.ConnectionOptions, dbObject, referencePath);
            }
            catch (Exception ex)
            {
                return ToRuntimeFailure<ScanResult>(ex, "failed to script object from database.");
            }

            var key = BuildObjectKey(dbObject.ObjectType, dbObject.Schema, dbObject.Name);
            if (!objects.ContainsKey(key))
            {
                objects[key] = new InternalObject(
                    key,
                    dbObject.Schema,
                    dbObject.Name,
                    dbObject.ObjectType,
                    script,
                    relativePath,
                    fullPath);
            }
        }

        return CommandExecutionResult<ScanResult>.Ok(new ScanResult(objects, warnings), ExitCodes.Success);
    }

    private static CommandExecutionResult<T> ToRuntimeFailure<T>(Exception exception, string fallbackMessage)
    {
        if (exception is SqlException sqlEx)
        {
            var code = IsDatabaseNotFound(sqlEx) ? ErrorCodes.DatabaseNotFound : ErrorCodes.ConnectionFailed;
            var message = code == ErrorCodes.DatabaseNotFound
                ? "database not found."
                : "failed to connect to SQL Server.";
            return CommandExecutionResult<T>.Failure(
                new ErrorInfo(code, message, Detail: sqlEx.Message),
                ExitCodes.ConnectionFailure);
        }

        if (exception is IOException || exception is UnauthorizedAccessException)
        {
            return CommandExecutionResult<T>.Failure(
                new ErrorInfo(ErrorCodes.IoFailed, fallbackMessage, Detail: exception.Message),
                ExitCodes.ExecutionFailure);
        }

        return CommandExecutionResult<T>.Failure(
            new ErrorInfo(ErrorCodes.ExecutionFailed, fallbackMessage, Detail: exception.Message),
            ExitCodes.ExecutionFailure);
    }

    private static bool IsDatabaseNotFound(SqlException sqlEx)
    {
        foreach (SqlError error in sqlEx.Errors)
        {
            if (error.Number == 4060)
            {
                return true;
            }
        }

        return false;
    }

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

        if (!string.Equals(config.Database.Auth, "integrated", StringComparison.OrdinalIgnoreCase)
            && !string.Equals(config.Database.Auth, "sql", StringComparison.OrdinalIgnoreCase))
        {
            return CommandExecutionResult<bool>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid config file.", Detail: "database.auth must be 'integrated' or 'sql'."),
                ExitCodes.InvalidConfig);
        }

        if (string.Equals(config.Database.Auth, "sql", StringComparison.OrdinalIgnoreCase)
            && string.IsNullOrWhiteSpace(config.Database.User))
        {
            return CommandExecutionResult<bool>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid config file.", Detail: "missing required field: database.user for sql authentication."),
                ExitCodes.InvalidConfig);
        }

        return CommandExecutionResult<bool>.Ok(true, ExitCodes.Success);
    }

    private static CommandExecutionResult<(string Schema, string Name)> ParseObjectSelector(string selector)
    {
        var dotIndex = selector.IndexOf('.', StringComparison.Ordinal);
        if (dotIndex <= 0 || dotIndex >= selector.Length - 1)
        {
            return CommandExecutionResult<(string Schema, string Name)>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid object selector.", Detail: "expected format: schema.name"),
                ExitCodes.InvalidConfig);
        }

        var schema = selector[..dotIndex].Trim();
        var name = selector[(dotIndex + 1)..].Trim();
        if (schema.Length == 0 || name.Length == 0)
        {
            return CommandExecutionResult<(string Schema, string Name)>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid object selector.", Detail: "expected format: schema.name"),
                ExitCodes.InvalidConfig);
        }

        return CommandExecutionResult<(string Schema, string Name)>.Ok((schema, name), ExitCodes.Success);
    }
    private static CommandExecutionResult<InternalObject> SelectObject(ComparisonSnapshot snapshot, string schema, string name)
    {
        var matches = snapshot.DbObjects.Values
            .Concat(snapshot.FolderObjects.Values)
            .Where(item => string.Equals(item.Schema, schema, StringComparison.OrdinalIgnoreCase)
                && string.Equals(item.Name, name, StringComparison.OrdinalIgnoreCase))
            .GroupBy(item => item.Key, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .OrderBy(item => item.ObjectType, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (matches.Length == 0)
        {
            return CommandExecutionResult<InternalObject>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "object not found for diff.", Detail: $"no object named '{schema}.{name}' exists in db/folder comparison set."),
                ExitCodes.InvalidConfig);
        }

        if (matches.Length > 1)
        {
            return CommandExecutionResult<InternalObject>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "object selector is ambiguous.", Detail: $"'{schema}.{name}' matches multiple object types."),
                ExitCodes.InvalidConfig);
        }

        return CommandExecutionResult<InternalObject>.Ok(matches[0], ExitCodes.Success);
    }

    private static IReadOnlyList<ChangeEntry> ComputeChanges(ComparisonSnapshot snapshot, ComparisonTarget target)
    {
        var sourceObjects = target == ComparisonTarget.Db ? snapshot.DbObjects : snapshot.FolderObjects;
        var targetObjects = target == ComparisonTarget.Db ? snapshot.FolderObjects : snapshot.DbObjects;

        var keys = sourceObjects.Keys
            .Concat(targetObjects.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(key => sourceObjects.TryGetValue(key, out var sourceObject) ? sourceObject : targetObjects[key])
            .OrderBy(item => item.Schema, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.ObjectType, StringComparer.OrdinalIgnoreCase)
            .Select(item => item.Key)
            .ToArray();

        var entries = new List<ChangeEntry>();
        foreach (var key in keys)
        {
            sourceObjects.TryGetValue(key, out var sourceObject);
            targetObjects.TryGetValue(key, out var targetObject);

            if (sourceObject is not null && targetObject is null)
            {
                entries.Add(new ChangeEntry(sourceObject, sourceObject, null, "added"));
                continue;
            }

            if (sourceObject is null && targetObject is not null)
            {
                entries.Add(new ChangeEntry(targetObject, null, targetObject, "deleted"));
                continue;
            }

            if (sourceObject is null || targetObject is null)
            {
                continue;
            }

            if (ScriptsEqualForComparison(sourceObject.Script, targetObject.Script))
            {
                continue;
            }

            entries.Add(new ChangeEntry(sourceObject, sourceObject, targetObject, "changed"));
        }

        return entries;
    }

    private static ChangeEntry BuildChangeEntry(ComparisonSnapshot snapshot, ComparisonTarget target, InternalObject selected)
    {
        var sourceObjects = target == ComparisonTarget.Db ? snapshot.DbObjects : snapshot.FolderObjects;
        var targetObjects = target == ComparisonTarget.Db ? snapshot.FolderObjects : snapshot.DbObjects;

        sourceObjects.TryGetValue(selected.Key, out var sourceObject);
        targetObjects.TryGetValue(selected.Key, out var targetObject);

        if (sourceObject is not null && targetObject is null)
        {
            return new ChangeEntry(sourceObject, sourceObject, null, "added");
        }

        if (sourceObject is null && targetObject is not null)
        {
            return new ChangeEntry(targetObject, null, targetObject, "deleted");
        }

        if (sourceObject is null || targetObject is null)
        {
            return new ChangeEntry(selected, null, null, "unchanged");
        }

        if (ScriptsEqualForComparison(sourceObject.Script, targetObject.Script))
        {
            return new ChangeEntry(sourceObject, sourceObject, targetObject, "unchanged");
        }

        return new ChangeEntry(sourceObject, sourceObject, targetObject, "changed");
    }

    private static string BuildDiffSection(ChangeEntry entry, string sourceLabel, string targetLabel)
    {
        var diff = BuildDiffText(entry, sourceLabel, targetLabel);
        if (string.IsNullOrWhiteSpace(diff))
        {
            return string.Empty;
        }

        return $"Object: {entry.Object.DisplayName} ({entry.Object.ObjectType}){Environment.NewLine}{diff}";
    }

    private static string BuildDiffText(ChangeEntry entry, string sourceLabel, string targetLabel)
    {
        var sourceScript = entry.SourceObject?.Script ?? string.Empty;
        var targetScript = entry.TargetObject?.Script ?? string.Empty;

        if (entry.Change == "unchanged")
        {
            return string.Empty;
        }

        return BuildUnifiedDiff(sourceLabel, targetLabel, sourceScript, targetScript);
    }

    internal static string BuildUnifiedDiff(string sourceLabel, string targetLabel, string sourceScript, string targetScript)
    {
        var normalizedSource = NormalizeForComparison(sourceScript);
        var normalizedTarget = NormalizeForComparison(targetScript);
        if (string.Equals(normalizedSource, normalizedTarget, StringComparison.Ordinal))
        {
            return string.Empty;
        }

        var sourceLines = normalizedSource.Length == 0
            ? Array.Empty<string>()
            : normalizedSource.Split('\n');
        var targetLines = normalizedTarget.Length == 0
            ? Array.Empty<string>()
            : normalizedTarget.Split('\n');

        var lines = new List<string>
        {
            $"--- {sourceLabel}",
            $"+++ {targetLabel}",
            "@@"
        };

        foreach (var line in sourceLines)
        {
            lines.Add($"-{line}");
        }

        foreach (var line in targetLines)
        {
            lines.Add($"+{line}");
        }

        return string.Join(Environment.NewLine, lines);
    }

    private static string BuildObjectKey(string objectType, string schema, string name)
        => $"{objectType}|{schema}|{name}";

    internal static IReadOnlyList<CommandWarning> CollectUnsupportedFolderWarnings(
        string projectDir,
        IReadOnlyCollection<string> supportedSqlFiles)
    {
        if (!Directory.Exists(projectDir))
        {
            return Array.Empty<CommandWarning>();
        }

        var normalizedSupportedFiles = new HashSet<string>(
            supportedSqlFiles.Select(Path.GetFullPath),
            StringComparer.OrdinalIgnoreCase);

        return Directory.EnumerateFiles(projectDir, "*.sql", SearchOption.AllDirectories)
            .Select(Path.GetFullPath)
            .Where(path => !normalizedSupportedFiles.Contains(path))
            .Select(path => Path.GetRelativePath(projectDir, path))
            .OrderBy(path => path, StringComparer.OrdinalIgnoreCase)
            .Select(path => new CommandWarning(
                "unsupported_folder_entry",
                $"skipped unsupported folder entry '{path}'."))
            .ToArray();
    }

    internal static bool TryParseSchemaAndName(string fileNameWithoutExtension, out string schema, out string name)
    {
        var separatorIndex = fileNameWithoutExtension.IndexOf('.', StringComparison.Ordinal);
        if (separatorIndex <= 0 || separatorIndex >= fileNameWithoutExtension.Length - 1)
        {
            schema = string.Empty;
            name = string.Empty;
            return false;
        }

        schema = fileNameWithoutExtension[..separatorIndex];
        name = fileNameWithoutExtension[(separatorIndex + 1)..];
        return schema.Length > 0 && name.Length > 0;
    }

    internal static IReadOnlyList<ComparableChange> ComputeChangesForComparison(
        IReadOnlyList<ComparableObject> source,
        IReadOnlyList<ComparableObject> target)
    {
        var sourceMap = source.ToDictionary(
            item => BuildObjectKey(item.ObjectType, item.Schema, item.Name),
            StringComparer.OrdinalIgnoreCase);
        var targetMap = target.ToDictionary(
            item => BuildObjectKey(item.ObjectType, item.Schema, item.Name),
            StringComparer.OrdinalIgnoreCase);

        var keys = sourceMap.Keys
            .Concat(targetMap.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(key => sourceMap.TryGetValue(key, out var sourceObject) ? sourceObject : targetMap[key])
            .OrderBy(item => item.Schema, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.ObjectType, StringComparer.OrdinalIgnoreCase)
            .Select(item => BuildObjectKey(item.ObjectType, item.Schema, item.Name))
            .ToArray();

        var results = new List<ComparableChange>();
        foreach (var key in keys)
        {
            sourceMap.TryGetValue(key, out var sourceObject);
            targetMap.TryGetValue(key, out var targetObject);

            if (sourceObject is not null && targetObject is null)
            {
                results.Add(new ComparableChange(sourceObject, "added"));
                continue;
            }

            if (sourceObject is null && targetObject is not null)
            {
                results.Add(new ComparableChange(targetObject, "deleted"));
                continue;
            }

            if (sourceObject is null || targetObject is null)
            {
                continue;
            }

            if (!ScriptsEqualForComparison(sourceObject.Script, targetObject.Script))
            {
                results.Add(new ComparableChange(sourceObject, "changed"));
            }
        }

        return results;
    }

    private static bool ScriptsEqualForComparison(string left, string right)
        => string.Equals(NormalizeForComparison(left), NormalizeForComparison(right), StringComparison.Ordinal);

    internal static string NormalizeForComparison(string script)
    {
        var normalized = script
            .Replace("\r\n", "\n", StringComparison.Ordinal)
            .Replace("\r", "\n", StringComparison.Ordinal);
        return normalized.TrimEnd('\n');
    }

    internal static FileContentStyle DetectExistingStyle(string path)
    {
        var bytes = File.ReadAllBytes(path);
        if (bytes.Length == 0)
        {
            return FileContentStyle.Default;
        }

        var encoding = DetectEncoding(bytes);
        var content = encoding.GetString(bytes);
        var newLine = content.Contains("\r\n", StringComparison.Ordinal) ? "\r\n" : "\n";
        var hasTrailingNewline = bytes[^1] == 0x0A;

        return new FileContentStyle(encoding, newLine, hasTrailingNewline);
    }

    internal static string ApplyStyle(string script, FileContentStyle style)
    {
        var normalized = script
            .Replace("\r\n", "\n", StringComparison.Ordinal)
            .Replace("\r", "\n", StringComparison.Ordinal);
        var withTargetNewline = normalized.Replace("\n", style.NewLine, StringComparison.Ordinal);

        if (style.HasTrailingNewline)
        {
            if (!withTargetNewline.EndsWith(style.NewLine, StringComparison.Ordinal))
            {
                withTargetNewline += style.NewLine;
            }

            return withTargetNewline;
        }

        while (withTargetNewline.EndsWith("\r\n", StringComparison.Ordinal) || withTargetNewline.EndsWith("\n", StringComparison.Ordinal))
        {
            withTargetNewline = withTargetNewline.EndsWith("\r\n", StringComparison.Ordinal)
                ? withTargetNewline[..^2]
                : withTargetNewline[..^1];
        }

        return withTargetNewline;
    }

    private static Encoding DetectEncoding(byte[] bytes)
    {
        if (bytes.Length >= 3 && bytes[0] == 0xEF && bytes[1] == 0xBB && bytes[2] == 0xBF)
        {
            return new UTF8Encoding(true);
        }

        if (bytes.Length >= 2 && bytes[0] == 0xFF && bytes[1] == 0xFE)
        {
            return new UnicodeEncoding(false, true);
        }

        if (bytes.Length >= 2 && bytes[0] == 0xFE && bytes[1] == 0xFF)
        {
            return new UnicodeEncoding(true, true);
        }

        return new UTF8Encoding(false);
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

    private sealed record ResolvedPath(string FullPath, string DisplayPath);

    private sealed record ProjectContext(
        string ProjectDir,
        string DisplayPath,
        SqlctConfig Config,
        SqlConnectionOptions ConnectionOptions);

    private sealed record ScanResult(
        IReadOnlyDictionary<string, InternalObject> Objects,
        IReadOnlyList<CommandWarning> Warnings);

    private sealed record ComparisonSnapshot(
        IReadOnlyDictionary<string, InternalObject> DbObjects,
        IReadOnlyDictionary<string, InternalObject> FolderObjects,
        IReadOnlyList<CommandWarning> Warnings);

    private sealed record InternalObject(
        string Key,
        string Schema,
        string Name,
        string ObjectType,
        string Script,
        string RelativePath,
        string FullPath)
    {
        public string DisplayName => $"{Schema}.{Name}";
    }

    private sealed record ChangeEntry(
        InternalObject Object,
        InternalObject? SourceObject,
        InternalObject? TargetObject,
        string Change);

    internal sealed record ComparableObject(
        string Schema,
        string Name,
        string ObjectType,
        string Script);

    internal sealed record ComparableChange(
        ComparableObject Object,
        string Change);

    internal sealed record FileContentStyle(
        Encoding Encoding,
        string NewLine,
        bool HasTrailingNewline)
    {
        public static FileContentStyle Default { get; } = new(new UTF8Encoding(false), "\r\n", true);
    }
}
