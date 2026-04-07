using System.Collections.Concurrent;
using System.Text.RegularExpressions;
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

    CommandExecutionResult<PullResult> RunPull(string? projectDir, string[]? objectPatterns = null, Action<string>? progress = null);
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
    internal const string TableDataObjectType = "TableData";
    private static readonly IReadOnlyList<SupportedSqlObjectType> ActiveObjectTypes = SupportedSqlObjectTypes.ActiveSync;

    private readonly SqlctConfigReader _configReader;
    private readonly SqlServerIntrospector _introspector;
    private readonly SqlServerScripter _scripter;
    private readonly SchemaFolderMapper _mapper;

    public SyncCommandService()
        : this(
            new SqlctConfigReader(),
            new SqlServerIntrospector(),
            new SqlServerScripter(),
            new SchemaFolderMapper(SupportedSqlObjectTypes.DefaultFolderMap, dataWriteAllFilesInOneDirectory: true))
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
        var summary = BuildStatusSummary(changes);

        var status = new StatusResult(
            "status",
            projectResult.Payload!.DisplayPath,
            comparisonTarget == ComparisonTarget.Db ? "db" : "folder",
            summary,
            changes.Select(entry => new StatusObject(entry.Object.DisplayName, entry.Object.ObjectType, entry.Change)).ToArray(),
            snapshotResult.Payload!.Warnings);

        var exitCode = summary.Schema.Added + summary.Schema.Changed + summary.Schema.Deleted
            + summary.Data.Added + summary.Data.Changed + summary.Data.Deleted > 0
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

            var selected = SelectObject(snapshotResult.Payload!, parsedObject.Payload!);
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
                selected.Payload!.SelectorDisplayName,
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

    public CommandExecutionResult<PullResult> RunPull(string? projectDir, string[]? objectPatterns = null, Action<string>? progress = null)
    {
        var projectResult = LoadProject(projectDir);
        if (!projectResult.Success)
        {
            return CommandExecutionResult<PullResult>.Failure(projectResult.Error!, projectResult.ExitCode);
        }

        IReadOnlyList<Regex>? compiledPatterns = null;
        if (objectPatterns is { Length: > 0 })
        {
            var patternList = new List<Regex>(objectPatterns.Length);
            foreach (var pattern in objectPatterns)
            {
                try
                {
                    patternList.Add(new Regex(pattern, RegexOptions.IgnoreCase, TimeSpan.FromSeconds(1)));
                }
                catch (ArgumentException)
                {
                    return CommandExecutionResult<PullResult>.Failure(
                        new ErrorInfo(ErrorCodes.InvalidConfig, "invalid object pattern.", Detail: $"'{pattern}' is not a valid regular expression."),
                        ExitCodes.InvalidConfig);
                }
            }

            compiledPatterns = patternList;
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
            .Where(item => compiledPatterns is null || MatchesObjectPatterns(item!.DisplayName, compiledPatterns))
            .OrderBy(item => item.Schema, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.ObjectType, StringComparer.OrdinalIgnoreCase)
            .Select(item => item.Key)
            .ToArray();

        var changes = new List<PullObject>();
        var schemaCreated = 0;
        var schemaUpdated = 0;
        var schemaDeleted = 0;
        var schemaUnchanged = 0;
        var dataCreated = 0;
        var dataUpdated = 0;
        var dataDeleted = 0;
        var dataUnchanged = 0;
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

                IncrementPullCounter(dbObject.ObjectType, ref schemaCreated, ref dataCreated);
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
                        new ErrorInfo(ErrorCodes.IoFailed, "failed to delete script.", File: folderObject.RelativePath, Detail: ex.Message),
                        ExitCodes.ExecutionFailure);
                }

                IncrementPullCounter(folderObject.ObjectType, ref schemaDeleted, ref dataDeleted);
                changes.Add(new PullObject(folderObject.DisplayName, folderObject.ObjectType, "deleted", folderObject.RelativePath));
                continue;
            }

            if (dbObject is null || folderObject is null)
            {
                continue;
            }

            if (ScriptsEqualForComparison(dbObject.Script, folderObject.Script))
            {
                IncrementPullCounter(dbObject.ObjectType, ref schemaUnchanged, ref dataUnchanged);
                continue;
            }

            var update = WriteScriptFile(dbObject.FullPath, dbObject.Script, existingFile: true);
            if (!update.Success)
            {
                return CommandExecutionResult<PullResult>.Failure(update.Error!, update.ExitCode);
            }

            if (update.Payload)
            {
                IncrementPullCounter(dbObject.ObjectType, ref schemaUpdated, ref dataUpdated);
                changes.Add(new PullObject(dbObject.DisplayName, dbObject.ObjectType, "updated", dbObject.RelativePath));
            }
            else
            {
                IncrementPullCounter(dbObject.ObjectType, ref schemaUnchanged, ref dataUnchanged);
            }
        }

        var result = new PullResult(
            "pull",
            projectResult.Payload!.DisplayPath,
            new PullSummary(
                new PullChangeSummary(schemaCreated, schemaUpdated, schemaDeleted, schemaUnchanged),
                new PullChangeSummary(dataCreated, dataUpdated, dataDeleted, dataUnchanged)),
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
                new ErrorInfo(ErrorCodes.IoFailed, "failed to write script.", File: fullPath, Detail: ex.Message),
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

        foreach (var entry in ActiveObjectTypes)
        {
            var objectType = entry.ObjectType;
            var folder = entry.RelativeFolder;
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
                if (!TryParseObjectFileName(fileName, entry.IsSchemaLess, out var schema, out var name))
                {
                    warnings.Add(new CommandWarning(
                        "invalid_script_name",
                        $"skipped '{Path.Combine(folder, Path.GetFileName(file))}' because it does not match '{GetExpectedFileNamePattern(entry.IsSchemaLess)}'."));
                    continue;
                }

                var key = BuildObjectKey(objectType, schema, name);
                if (objects.ContainsKey(key))
                {
                    var displayName = FormatDisplayName(schema, name);
                    warnings.Add(new CommandWarning(
                        "duplicate_script",
                        $"skipped duplicate folder object '{displayName}' of type '{objectType}'."));
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

        var dataFolderResult = ScanDataFolder(projectDir, objects, scannedSqlFiles, warnings);
        if (!dataFolderResult.Success)
        {
            return dataFolderResult;
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
        var dop = SqlServerIntrospector.ResolveParallelism(context.Config.Options.Parallelism);

        progress?.Invoke("Connecting to database...");
        IReadOnlyList<DbObjectInfo> listedObjects;
        try
        {
            listedObjects = _introspector.ListObjects(context.ConnectionOptions, dop);
        }
        catch (Exception ex)
        {
            return ToRuntimeFailure<ScanResult>(ex, "failed to read database objects.");
        }

        var warnings = listedObjects
            .Where(item => !SupportedSqlObjectTypes.IsActiveInSync(item.ObjectType))
            .Select(item => item.ObjectType)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .Select(item => new CommandWarning("unsupported_object_type", $"skipped unsupported object type '{item}'."))
            .ToList();

        var activeObjects = listedObjects
            .Where(item => SupportedSqlObjectTypes.IsActiveInSync(item.ObjectType))
            .OrderBy(item => item.Schema, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.ObjectType, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var total = activeObjects.Length;
        var scriptIndex = 0;
        var objects = new ConcurrentDictionary<string, InternalObject>(StringComparer.OrdinalIgnoreCase);
        CommandExecutionResult<ScanResult>? firstFailure = null;

        Parallel.ForEach(activeObjects, new ParallelOptions { MaxDegreeOfParallelism = dop }, (dbObject, loopState) =>
        {
            if (firstFailure != null)
            {
                loopState.Stop();
                return;
            }

            var index = Interlocked.Increment(ref scriptIndex);
            progress?.Invoke($"Scripting objects ({index}/{total}): {FormatDisplayName(dbObject.Schema, dbObject.Name)}");

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
                Interlocked.CompareExchange(ref firstFailure, ToRuntimeFailure<ScanResult>(ex, "failed to map object path."), null);
                loopState.Stop();
                return;
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
                Interlocked.CompareExchange(ref firstFailure, ToRuntimeFailure<ScanResult>(ex, "failed to script object from database."), null);
                loopState.Stop();
                return;
            }

            var key = BuildObjectKey(dbObject.ObjectType, dbObject.Schema, dbObject.Name);
            // TryAdd is a no-op for duplicate keys; duplicates are not expected since each catalog query
            // targets distinct object types, and this matches the original ContainsKey guard behavior.
            objects.TryAdd(key, new InternalObject(
                key,
                dbObject.Schema,
                dbObject.Name,
                dbObject.ObjectType,
                script,
                relativePath,
                fullPath));
        });

        if (firstFailure != null)
        {
            return firstFailure;
        }

        var trackedTableKeys = new HashSet<string>(
            context.Config.Data.TrackedTables,
            StringComparer.OrdinalIgnoreCase);
        var trackedTables = activeObjects
            .Where(item => string.Equals(item.ObjectType, "Table", StringComparison.OrdinalIgnoreCase))
            .Where(item => trackedTableKeys.Contains(FormatDisplayName(item.Schema, item.Name)))
            .OrderBy(item => item.Schema, StringComparer.OrdinalIgnoreCase)
            .ThenBy(item => item.Name, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        var dataScriptIndex = 0;
        var dataScriptTotal = trackedTables.Length;
        Parallel.ForEach(trackedTables, new ParallelOptions { MaxDegreeOfParallelism = dop }, (table, loopState) =>
        {
            if (firstFailure != null)
            {
                loopState.Stop();
                return;
            }

            var index = Interlocked.Increment(ref dataScriptIndex);
            progress?.Invoke($"Scripting data ({index}/{dataScriptTotal}): {FormatDisplayName(table.Schema, table.Name)}");

            var relativePath = _mapper.GetObjectPath(
                table.ObjectType,
                new ObjectIdentifier(table.Schema, table.Name),
                isData: true);
            var fullPath = Path.Combine(context.ProjectDir, relativePath);

            string script;
            try
            {
                script = _scripter.ScriptTableData(context.ConnectionOptions, new ObjectIdentifier(table.Schema, table.Name));
            }
            catch (Exception ex)
            {
                Interlocked.CompareExchange(ref firstFailure, ToRuntimeFailure<ScanResult>(ex, "failed to script data from database."), null);
                loopState.Stop();
                return;
            }

            var key = BuildObjectKey(TableDataObjectType, table.Schema, table.Name);
            objects.TryAdd(key, new InternalObject(
                key,
                table.Schema,
                table.Name,
                TableDataObjectType,
                script,
                relativePath,
                fullPath));
        });

        if (firstFailure != null)
        {
            return firstFailure;
        }

        return CommandExecutionResult<ScanResult>.Ok(new ScanResult(objects, warnings), ExitCodes.Success);
    }

    private CommandExecutionResult<ScanResult> ScanDataFolder(
        string projectDir,
        IDictionary<string, InternalObject> objects,
        ISet<string> scannedSqlFiles,
        IList<CommandWarning> warnings)
    {
        var fullFolderPath = Path.Combine(projectDir, "Data");
        if (!Directory.Exists(fullFolderPath))
        {
            return CommandExecutionResult<ScanResult>.Ok(new ScanResult((IReadOnlyDictionary<string, InternalObject>)objects, warnings.ToArray()), ExitCodes.Success);
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
            if (!TryParseDataFileName(fileName, out var schema, out var name))
            {
                warnings.Add(new CommandWarning(
                    "invalid_script_name",
                    $"skipped '{Path.Combine("Data", Path.GetFileName(file))}' because it does not match '{GetExpectedDataFileNamePattern()}'.")); 
                continue;
            }

            var key = BuildObjectKey(TableDataObjectType, schema, name);
            if (objects.ContainsKey(key))
            {
                warnings.Add(new CommandWarning(
                    "duplicate_script",
                    $"skipped duplicate folder object '{FormatDisplayName(schema, name)}' of type '{TableDataObjectType}'."));
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

            var relativePath = Path.Combine("Data", Path.GetFileName(file));
            objects[key] = new InternalObject(
                key,
                schema,
                name,
                TableDataObjectType,
                script,
                relativePath,
                normalizedFilePath);
        }

        return CommandExecutionResult<ScanResult>.Ok(new ScanResult((IReadOnlyDictionary<string, InternalObject>)objects, warnings.ToArray()), ExitCodes.Success);
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

    internal static CommandExecutionResult<ObjectSelector> ParseObjectSelector(string selector)
    {
        var trimmed = selector.Trim();
        if (trimmed.Length == 0)
        {
            return CommandExecutionResult<ObjectSelector>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid object selector.", Detail: "expected format: schema.name, name, type:name, or type:schema.name"),
                ExitCodes.InvalidConfig);
        }

        var typeSeparatorIndex = trimmed.IndexOf(':', StringComparison.Ordinal);
        if (typeSeparatorIndex >= 0)
        {
            var objectTypeToken = trimmed[..typeSeparatorIndex].Trim();
            var nameToken = trimmed[(typeSeparatorIndex + 1)..].Trim();
            if (objectTypeToken.Length == 0 || nameToken.Length == 0)
            {
                return CommandExecutionResult<ObjectSelector>.Failure(
                    new ErrorInfo(ErrorCodes.InvalidConfig, "invalid object selector.", Detail: "expected format: schema.name, name, type:name, or type:schema.name"),
                    ExitCodes.InvalidConfig);
            }

            if (!SupportedSqlObjectTypes.TryGet(objectTypeToken, out var entry))
            {
                if (string.Equals(objectTypeToken, "data", StringComparison.OrdinalIgnoreCase))
                {
                    if (!TryParseObjectFileName(nameToken, isSchemaLess: false, out var dataSchema, out var dataName))
                    {
                        return CommandExecutionResult<ObjectSelector>.Failure(
                            new ErrorInfo(ErrorCodes.InvalidConfig, "invalid object selector.", Detail: "expected format: data:schema.name"),
                            ExitCodes.InvalidConfig);
                    }

                    return CommandExecutionResult<ObjectSelector>.Ok(
                        new ObjectSelector(TableDataObjectType, dataSchema, dataName, false, trimmed),
                        ExitCodes.Success);
                }

                return CommandExecutionResult<ObjectSelector>.Failure(
                    new ErrorInfo(ErrorCodes.InvalidConfig, "invalid object selector.", Detail: $"unsupported object type '{objectTypeToken}'."),
                    ExitCodes.InvalidConfig);
            }

            if (!TryParseObjectFileName(nameToken, entry.IsSchemaLess, out var schema, out var name))
            {
                var expectedFormat = entry.IsSchemaLess
                    ? $"expected format: {entry.ObjectType}:name"
                    : $"expected format: {entry.ObjectType}:schema.name";
                return CommandExecutionResult<ObjectSelector>.Failure(
                    new ErrorInfo(ErrorCodes.InvalidConfig, "invalid object selector.", Detail: expectedFormat),
                    ExitCodes.InvalidConfig);
            }

            return CommandExecutionResult<ObjectSelector>.Ok(
                new ObjectSelector(entry.ObjectType, schema, name, entry.IsSchemaLess, trimmed),
                ExitCodes.Success);
        }

        if (trimmed.Contains('.', StringComparison.Ordinal))
        {
            if (TryParseSchemaAndName(trimmed, out var parsedSchema, out var parsedName))
            {
                return CommandExecutionResult<ObjectSelector>.Ok(
                    new ObjectSelector(null, parsedSchema, parsedName, false, trimmed),
                    ExitCodes.Success);
            }

            return CommandExecutionResult<ObjectSelector>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "invalid object selector.", Detail: "expected format: schema.name, name, type:name, or type:schema.name"),
                ExitCodes.InvalidConfig);
        }

        if (TryParseObjectFileName(trimmed, isSchemaLess: true, out _, out var bareName))
        {
            return CommandExecutionResult<ObjectSelector>.Ok(
                new ObjectSelector(null, string.Empty, bareName, true, trimmed),
                ExitCodes.Success);
        }

        return CommandExecutionResult<ObjectSelector>.Failure(
            new ErrorInfo(ErrorCodes.InvalidConfig, "invalid object selector.", Detail: "expected format: schema.name, name, type:name, or type:schema.name"),
            ExitCodes.InvalidConfig);
    }

    private static CommandExecutionResult<InternalObject> SelectObject(ComparisonSnapshot snapshot, ObjectSelector selector)
    {
        var matches = snapshot.DbObjects.Values
            .Concat(snapshot.FolderObjects.Values)
            .Where(item => MatchesSelector(item.ObjectType, item.Schema, item.Name, selector))
            .GroupBy(item => item.Key, StringComparer.OrdinalIgnoreCase)
            .Select(group => group.First())
            .OrderBy(item => item.ObjectType, StringComparer.OrdinalIgnoreCase)
            .ToArray();

        if (matches.Length == 0)
        {
            return CommandExecutionResult<InternalObject>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "object not found for diff.", Detail: $"no object matching '{selector.Raw}' exists in db/folder comparison set."),
                ExitCodes.InvalidConfig);
        }

        if (matches.Length > 1)
        {
            var disambiguationHint = selector.IsSchemaLess
                ? "use a type-qualified selector such as type:name."
                : "use a type-qualified selector such as type:schema.name.";
            return CommandExecutionResult<InternalObject>.Failure(
                new ErrorInfo(ErrorCodes.InvalidConfig, "object selector is ambiguous.", Detail: $"'{selector.Raw}' matches multiple object types; {disambiguationHint}"),
                ExitCodes.InvalidConfig);
        }

        return CommandExecutionResult<InternalObject>.Ok(matches[0], ExitCodes.Success);
    }

    internal static bool MatchesSelector(string objectType, string schema, string name, ObjectSelector selector)
    {
        if (selector.ObjectType is not null &&
            !string.Equals(selector.ObjectType, objectType, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        if (selector.ObjectType is null)
        {
            if (string.Equals(objectType, TableDataObjectType, StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            if (selector.IsSchemaLess && !SupportedSqlObjectTypes.IsSchemaLess(objectType))
            {
                return false;
            }

            if (!selector.IsSchemaLess && SupportedSqlObjectTypes.IsSchemaLess(objectType))
            {
                return false;
            }
        }

        if (selector.IsSchemaLess)
        {
            return string.IsNullOrWhiteSpace(schema)
                && string.Equals(name, selector.Name, StringComparison.OrdinalIgnoreCase);
        }

        return string.Equals(schema, selector.Schema, StringComparison.OrdinalIgnoreCase)
            && string.Equals(name, selector.Name, StringComparison.OrdinalIgnoreCase);
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

        return $"Object: {entry.Object.SelectorDisplayName} ({entry.Object.ObjectType}){Environment.NewLine}{diff}";
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

    private static StatusSummary BuildStatusSummary(IReadOnlyList<ChangeEntry> changes)
    {
        var schema = new ChangeSummary(
            changes.Count(entry => !IsDataObjectType(entry.Object.ObjectType) && string.Equals(entry.Change, "added", StringComparison.OrdinalIgnoreCase)),
            changes.Count(entry => !IsDataObjectType(entry.Object.ObjectType) && string.Equals(entry.Change, "changed", StringComparison.OrdinalIgnoreCase)),
            changes.Count(entry => !IsDataObjectType(entry.Object.ObjectType) && string.Equals(entry.Change, "deleted", StringComparison.OrdinalIgnoreCase)));
        var data = new ChangeSummary(
            changes.Count(entry => IsDataObjectType(entry.Object.ObjectType) && string.Equals(entry.Change, "added", StringComparison.OrdinalIgnoreCase)),
            changes.Count(entry => IsDataObjectType(entry.Object.ObjectType) && string.Equals(entry.Change, "changed", StringComparison.OrdinalIgnoreCase)),
            changes.Count(entry => IsDataObjectType(entry.Object.ObjectType) && string.Equals(entry.Change, "deleted", StringComparison.OrdinalIgnoreCase)));
        return new StatusSummary(schema, data);
    }

    private static void IncrementPullCounter(string objectType, ref int schemaCount, ref int dataCount)
    {
        if (IsDataObjectType(objectType))
        {
            dataCount++;
        }
        else
        {
            schemaCount++;
        }
    }

    private static bool IsDataObjectType(string objectType)
        => string.Equals(objectType, TableDataObjectType, StringComparison.OrdinalIgnoreCase);

    internal static bool MatchesObjectPatterns(string displayName, IReadOnlyList<Regex> patterns)
    {
        foreach (var pattern in patterns)
        {
            var match = pattern.Match(displayName);
            if (match.Success && match.Index == 0 && match.Length == displayName.Length)
            {
                return true;
            }
        }

        return false;
    }

    private static string GetExpectedFileNamePattern(bool isSchemaLess)
        => isSchemaLess ? "Name.sql" : "Schema.Object.sql";

    private static string GetExpectedDataFileNamePattern()
        => "Schema.Object_Data.sql";

    private static string FormatDisplayName(string schema, string name)
        => string.IsNullOrWhiteSpace(schema) ? name : $"{schema}.{name}";

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

    internal static bool TryParseObjectFileName(string fileNameWithoutExtension, bool isSchemaLess, out string schema, out string name)
    {
        if (isSchemaLess)
        {
            schema = string.Empty;
            name = fileNameWithoutExtension.Trim();
            return name.Length > 0;
        }

        return TryParseSchemaAndName(fileNameWithoutExtension, out schema, out name);
    }

    internal static bool TryParseDataFileName(string fileNameWithoutExtension, out string schema, out string name)
    {
        const string suffix = "_Data";
        if (!fileNameWithoutExtension.EndsWith(suffix, StringComparison.Ordinal))
        {
            schema = string.Empty;
            name = string.Empty;
            return false;
        }

        return TryParseSchemaAndName(fileNameWithoutExtension[..^suffix.Length], out schema, out name);
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
        => ProjectPathResolver.Resolve(projectDir);

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

    internal sealed record ObjectSelector(
        string? ObjectType,
        string Schema,
        string Name,
        bool IsSchemaLess,
        string Raw);

    private sealed record InternalObject(
        string Key,
        string Schema,
        string Name,
        string ObjectType,
        string Script,
        string RelativePath,
        string FullPath)
    {
        public string DisplayName => FormatDisplayName(Schema, Name);

        public string SelectorDisplayName => IsDataObjectType(ObjectType)
            ? $"data:{DisplayName}"
            : DisplayName;
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
