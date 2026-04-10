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

    CommandExecutionResult<DiffResult> RunDiff(string? projectDir, string? target, string? objectSelector, string[]? filterPatterns = null, int contextLines = 3, bool normalizedDiff = false, Action<string>? progress = null);

    CommandExecutionResult<PullResult> RunPull(string? projectDir, string? objectSelector = null, string[]? filterPatterns = null, Action<string>? progress = null);
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
    private static readonly Regex ScalarUserDefinedTypeScriptRegex = new(
        @"\bCREATE\s+TYPE\b.*\bFROM\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline | RegexOptions.Compiled);
    private static readonly Regex TableUserDefinedTypeScriptRegex = new(
        @"\bCREATE\s+TYPE\b.*\bAS\s+TABLE\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Singleline | RegexOptions.Compiled);
    private static readonly Regex SqlIdentifierRegex = new(
        """\G\s*(?<identifier>\[(?:[^\]]|\]\])+\]|"(?:""|[^"])+"|[^\s(]+)""",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex ClrTableValuedFunctionReturnColumnNullRegex = new(
        @"^(?<prefix>\s*(?:\[[^\]]+\]|[A-Za-z_][\w@#$]*)\s+(?:(?:\[[^\]]+\]|[A-Za-z_][\w@#$]*)(?:\.(?:\[[^\]]+\]|[A-Za-z_][\w@#$]*))?)(?:\s*\([^)]*\))?)\s+NULL(?<suffix>\s*,?\s*)$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex ClrTableValuedFunctionReturnColumnNullWithCloseParenRegex = new(
        @"^(?<prefix>\s*(?:\[[^\]]+\]|[A-Za-z_][\w@#$]*)\s+(?:(?:\[[^\]]+\]|[A-Za-z_][\w@#$]*)(?:\.(?:\[[^\]]+\]|[A-Za-z_][\w@#$]*))?)(?:\s*\([^)]*\))?)\s+NULL(?<suffix>\s*\)\s*)$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex RoleMembershipLegacySyntaxRegex = new(
        @"^\s*EXEC(?:UTE)?\s+sp_addrolemember\s+N'(?<role>(?:''|[^'])*)'\s*,\s*N'(?<member>(?:''|[^'])*)'\s*;?\s*$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex RoleMembershipAlterRoleSyntaxRegex = new(
        @"^\s*ALTER\s+ROLE\s+(?<role>\[[^\]]+(?:\]\])*\]|""(?:""""|[^""])+""|[^\s;]+)\s+ADD\s+MEMBER\s+(?<member>\[[^\]]+(?:\]\])*\]|""(?:""""|[^""])+""|[^\s;]+)\s*;?\s*$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex ExtendedPropertyStatementRegex = new(
        @"^\s*EXEC(?:UTE)?\s+(?:sys\.)?sp_addextendedproperty\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex SsmsObjectHeaderCommentRegex = new(
        @"^\s*/\*{5,}\s*Object:\s+(?:StoredProcedure|Procedure|View|Function|Trigger)\b.*Script Date:.*\*+/\s*$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
    private static readonly Regex CompatibleTextImageOnRegex = new(
        @"^(?<prefix>\)\s*(?:ON\s+(?:\[[^\]]+(?:\]\])*\]|""(?:""""|[^""])+""|[^\s]+)(?:\s*\([^)]+\))?)?)\s+TEXTIMAGE_ON\s+(?<dataSpace>\[[^\]]+(?:\]\])*\]|""(?:""""|[^""])+""|[^\s]+)(?<suffix>\s*)$",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);
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

        var snapshotResult = BuildSnapshot(projectResult.Payload!, progress: progress);
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

    public CommandExecutionResult<DiffResult> RunDiff(string? projectDir, string? target, string? objectSelector, string[]? filterPatterns = null, int contextLines = 3, bool normalizedDiff = false, Action<string>? progress = null)
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

        IReadOnlyList<Regex>? compiledPatterns = null;
        if (filterPatterns is { Length: > 0 })
        {
            var patternList = new List<Regex>(filterPatterns.Length);
            foreach (var pattern in filterPatterns)
            {
                try
                {
                    patternList.Add(new Regex(pattern, RegexOptions.IgnoreCase, TimeSpan.FromSeconds(1)));
                }
                catch (ArgumentException)
                {
                    return CommandExecutionResult<DiffResult>.Failure(
                        new ErrorInfo(ErrorCodes.InvalidConfig, "invalid filter pattern.", Detail: $"'{pattern}' is not a valid regular expression."),
                        ExitCodes.InvalidConfig);
                }
            }

            compiledPatterns = patternList;
        }

        var sourceLabel = comparisonTarget == ComparisonTarget.Db ? "db" : "folder";
        var targetLabel = comparisonTarget == ComparisonTarget.Db ? "folder" : "db";

        if (!string.IsNullOrWhiteSpace(objectSelector))
        {
            var parsedObject = ParseObjectSelector(objectSelector!);
            if (!parsedObject.Success)
            {
                return CommandExecutionResult<DiffResult>.Failure(parsedObject.Error!, parsedObject.ExitCode);
            }

            var selectedSnapshotResult = BuildSelectedObjectSnapshot(projectResult.Payload!, parsedObject.Payload!, progress);
            if (!selectedSnapshotResult.Success)
            {
                return CommandExecutionResult<DiffResult>.Failure(selectedSnapshotResult.Error!, selectedSnapshotResult.ExitCode);
            }

            var selected = SelectObject(selectedSnapshotResult.Payload!, parsedObject.Payload!);
            if (!selected.Success)
            {
                return CommandExecutionResult<DiffResult>.Failure(selected.Error!, selected.ExitCode);
            }

            if (compiledPatterns is not null && !MatchesObjectPatterns(selected.Payload!.DisplayName, compiledPatterns))
            {
                var emptyResult = new DiffResult(
                    "diff",
                    projectResult.Payload!.DisplayPath,
                    comparisonTarget == ComparisonTarget.Db ? "db" : "folder",
                    selected.Payload!.SelectorDisplayName,
                    string.Empty,
                    selectedSnapshotResult.Payload!.Warnings);

                return CommandExecutionResult<DiffResult>.Ok(emptyResult, ExitCodes.Success);
            }

            var entry = BuildChangeEntry(selectedSnapshotResult.Payload!, comparisonTarget, selected.Payload!);
            var diff = BuildDiffText(entry, sourceLabel, targetLabel, contextLines, normalizedDiff);

            var result = new DiffResult(
                "diff",
                projectResult.Payload!.DisplayPath,
                comparisonTarget == ComparisonTarget.Db ? "db" : "folder",
                selected.Payload!.SelectorDisplayName,
                diff,
                selectedSnapshotResult.Payload!.Warnings);

            var exitCode = string.IsNullOrWhiteSpace(diff) ? ExitCodes.Success : ExitCodes.DiffExists;
            return CommandExecutionResult<DiffResult>.Ok(result, exitCode);
        }

        var snapshotResult = BuildSnapshot(projectResult.Payload!, compiledPatterns, progress);
        if (!snapshotResult.Success)
        {
            return CommandExecutionResult<DiffResult>.Failure(snapshotResult.Error!, snapshotResult.ExitCode);
        }

        var changes = ComputeChanges(snapshotResult.Payload!, comparisonTarget);
        var filteredChanges = compiledPatterns is null
            ? changes
            : changes.Where(change => MatchesObjectPatterns(change.Object.DisplayName, compiledPatterns)).ToArray();

        var diffSections = filteredChanges
            .Select(change => BuildDiffSection(change, sourceLabel, targetLabel, contextLines, normalizedDiff))
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

    public CommandExecutionResult<PullResult> RunPull(string? projectDir, string? objectSelector = null, string[]? filterPatterns = null, Action<string>? progress = null)
    {
        var projectResult = LoadProject(projectDir);
        if (!projectResult.Success)
        {
            return CommandExecutionResult<PullResult>.Failure(projectResult.Error!, projectResult.ExitCode);
        }

        ObjectSelector? parsedSelector = null;
        if (objectSelector is not null)
        {
            var selectorResult = ParseObjectSelector(objectSelector);
            if (!selectorResult.Success)
            {
                return CommandExecutionResult<PullResult>.Failure(selectorResult.Error!, selectorResult.ExitCode);
            }

            parsedSelector = selectorResult.Payload!;
        }

        IReadOnlyList<Regex>? compiledPatterns = null;
        if (filterPatterns is { Length: > 0 })
        {
            var patternList = new List<Regex>(filterPatterns.Length);
            foreach (var pattern in filterPatterns)
            {
                try
                {
                    patternList.Add(new Regex(pattern, RegexOptions.IgnoreCase, TimeSpan.FromSeconds(1)));
                }
                catch (ArgumentException)
                {
                    return CommandExecutionResult<PullResult>.Failure(
                        new ErrorInfo(ErrorCodes.InvalidConfig, "invalid filter pattern.", Detail: $"'{pattern}' is not a valid regular expression."),
                        ExitCodes.InvalidConfig);
                }
            }

            compiledPatterns = patternList;
        }

        var snapshotResult = parsedSelector is not null
            ? BuildSelectedObjectSnapshot(projectResult.Payload!, parsedSelector, progress)
            : BuildSnapshot(projectResult.Payload!, compiledPatterns, progress);
        if (!snapshotResult.Success)
        {
            return CommandExecutionResult<PullResult>.Failure(snapshotResult.Error!, snapshotResult.ExitCode);
        }

        var snapshot = snapshotResult.Payload!;
        var keys = snapshot.DbObjects.Keys
            .Concat(snapshot.FolderObjects.Keys)
            .Distinct(StringComparer.OrdinalIgnoreCase)
            .Select(key => snapshot.DbObjects.TryGetValue(key, out var dbObj) ? dbObj : snapshot.FolderObjects[key])
            .Where(item => parsedSelector is null || MatchesSelector(item.ObjectType, item.Schema, item.Name, parsedSelector))
            .Where(item => compiledPatterns is null || MatchesObjectPatterns(item.DisplayName, compiledPatterns))
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

            if (ScriptsEqualForComparison(dbObject, folderObject))
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

    private CommandExecutionResult<ComparisonSnapshot> BuildSnapshot(
        ProjectContext context,
        IReadOnlyList<Regex>? filterPatterns = null,
        Action<string>? progress = null)
    {
        progress?.Invoke("Scanning schema folder...");
        var folderResult = ScanFolder(context.ProjectDir);
        if (!folderResult.Success)
        {
            return CommandExecutionResult<ComparisonSnapshot>.Failure(folderResult.Error!, folderResult.ExitCode);
        }

        var dbResult = ScanDatabase(context, filterPatterns, progress);
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

    private CommandExecutionResult<ComparisonSnapshot> BuildSelectedObjectSnapshot(
        ProjectContext context,
        ObjectSelector selector,
        Action<string>? progress = null)
    {
        progress?.Invoke("Scanning schema folder...");
        var folderResult = ScanFolder(context.ProjectDir);
        if (!folderResult.Success)
        {
            return CommandExecutionResult<ComparisonSnapshot>.Failure(folderResult.Error!, folderResult.ExitCode);
        }

        var dbResult = ScanDatabaseForSelector(context, selector, progress);
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

                if (string.Equals(objectType, "UserDefinedType", StringComparison.OrdinalIgnoreCase)
                    && !TryClassifyUserDefinedTypeScript(script, out _))
                {
                    warnings.Add(new CommandWarning(
                        "invalid_user_defined_type_script",
                        $"skipped '{Path.Combine(folder, Path.GetFileName(file))}' because it is not a recognized user-defined type script."));
                    continue;
                }

                if (TryResolveSchemaLessFolderIdentityFromScript(objectType, fileName, script, name, out var scriptName))
                {
                    schema = string.Empty;
                    name = scriptName;
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

    private CommandExecutionResult<ScanResult> ScanDatabase(
        ProjectContext context,
        IReadOnlyList<Regex>? filterPatterns = null,
        Action<string>? progress = null)
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

        if (filterPatterns is not null)
        {
            activeObjects = activeObjects
                .Where(obj => MatchesObjectPatterns(FormatDisplayName(obj.Schema, obj.Name), filterPatterns))
                .ToArray();
        }

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

            string? compatibleOmittedTextImageOnDataSpaceName = null;
            if (string.Equals(dbObject.ObjectType, "Table", StringComparison.OrdinalIgnoreCase))
            {
                try
                {
                    compatibleOmittedTextImageOnDataSpaceName =
                        _introspector.GetTableCompatibleOmittedTextImageOnDataSpaceName(
                            context.ConnectionOptions,
                            dbObject.Schema,
                            dbObject.Name);
                }
                catch (Exception ex)
                {
                    Interlocked.CompareExchange(
                        ref firstFailure,
                        ToRuntimeFailure<ScanResult>(ex, "failed to read table comparison metadata."),
                        null);
                    loopState.Stop();
                    return;
                }
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
                fullPath,
                compatibleOmittedTextImageOnDataSpaceName));
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

    private CommandExecutionResult<ScanResult> ScanDatabaseForSelector(
        ProjectContext context,
        ObjectSelector selector,
        Action<string>? progress = null)
    {
        var dop = SqlServerIntrospector.ResolveParallelism(context.Config.Options.Parallelism);

        progress?.Invoke("Connecting to database...");
        IReadOnlyList<DbObjectInfo> matchedObjects;
        try
        {
            matchedObjects = _introspector.ListMatchingObjects(
                context.ConnectionOptions,
                GetCandidateDbObjectTypes(selector),
                selector.Schema,
                selector.Name,
                dop);
        }
        catch (Exception ex)
        {
            return ToRuntimeFailure<ScanResult>(ex, "failed to read database objects.");
        }

        var objects = new ConcurrentDictionary<string, InternalObject>(StringComparer.OrdinalIgnoreCase);
        CommandExecutionResult<ScanResult>? firstFailure = null;

        if (string.Equals(selector.ObjectType, TableDataObjectType, StringComparison.OrdinalIgnoreCase))
        {
            var trackedTableKeys = new HashSet<string>(
                context.Config.Data.TrackedTables,
                StringComparer.OrdinalIgnoreCase);
            var trackedName = FormatDisplayName(selector.Schema, selector.Name);
            if (!trackedTableKeys.Contains(trackedName))
            {
                return CommandExecutionResult<ScanResult>.Ok(new ScanResult(objects, []), ExitCodes.Success);
            }

            var trackedTables = matchedObjects
                .Where(item => string.Equals(item.ObjectType, "Table", StringComparison.OrdinalIgnoreCase))
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
        }
        else
        {
            var total = matchedObjects.Count;
            var scriptIndex = 0;

            Parallel.ForEach(matchedObjects, new ParallelOptions { MaxDegreeOfParallelism = dop }, (dbObject, loopState) =>
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

                string? compatibleOmittedTextImageOnDataSpaceName = null;
                if (string.Equals(dbObject.ObjectType, "Table", StringComparison.OrdinalIgnoreCase))
                {
                    try
                    {
                        compatibleOmittedTextImageOnDataSpaceName =
                            _introspector.GetTableCompatibleOmittedTextImageOnDataSpaceName(
                                context.ConnectionOptions,
                                dbObject.Schema,
                                dbObject.Name);
                    }
                    catch (Exception ex)
                    {
                        Interlocked.CompareExchange(
                            ref firstFailure,
                            ToRuntimeFailure<ScanResult>(ex, "failed to read table comparison metadata."),
                            null);
                        loopState.Stop();
                        return;
                    }
                }

                var key = BuildObjectKey(dbObject.ObjectType, dbObject.Schema, dbObject.Name);
                objects.TryAdd(key, new InternalObject(
                    key,
                    dbObject.Schema,
                    dbObject.Name,
                    dbObject.ObjectType,
                    script,
                    relativePath,
                    fullPath,
                    compatibleOmittedTextImageOnDataSpaceName));
            });
        }

        if (firstFailure != null)
        {
            return firstFailure;
        }

        return CommandExecutionResult<ScanResult>.Ok(new ScanResult(objects, []), ExitCodes.Success);
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
                if (string.Equals(objectTypeToken, "TableType", StringComparison.OrdinalIgnoreCase))
                {
                    return CommandExecutionResult<ObjectSelector>.Failure(
                        new ErrorInfo(
                            ErrorCodes.InvalidConfig,
                            "invalid object selector.",
                            Detail: "`TableType` selectors are no longer supported; use `UserDefinedType:schema.name`."),
                        ExitCodes.InvalidConfig);
                }

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
            var dotCount = 0;
            foreach (var character in trimmed)
            {
                if (character == '.')
                {
                    dotCount++;
                }
            }

            if (dotCount > 1 &&
                TryParseObjectFileName(trimmed, isSchemaLess: true, out _, out var dottedSchemaLessName))
            {
                return CommandExecutionResult<ObjectSelector>.Ok(
                    new ObjectSelector(null, string.Empty, dottedSchemaLessName, true, trimmed),
                    ExitCodes.Success);
            }

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

            if (ScriptsEqualForComparison(sourceObject, targetObject))
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

        if (ScriptsEqualForComparison(sourceObject, targetObject))
        {
            return new ChangeEntry(sourceObject, sourceObject, targetObject, "unchanged");
        }

        return new ChangeEntry(sourceObject, sourceObject, targetObject, "changed");
    }

    private static string BuildDiffSection(ChangeEntry entry, string sourceLabel, string targetLabel, int contextLines, bool normalizedDiff)
    {
        var diff = BuildDiffText(entry, sourceLabel, targetLabel, contextLines, normalizedDiff);
        if (string.IsNullOrWhiteSpace(diff))
        {
            return string.Empty;
        }

        return $"Object: {entry.Object.SelectorDisplayName} ({entry.Object.ObjectType}){Environment.NewLine}{diff}";
    }

    private static string BuildDiffText(ChangeEntry entry, string sourceLabel, string targetLabel, int contextLines, bool normalizedDiff)
    {
        var sourceScript = entry.SourceObject?.Script ?? string.Empty;
        var targetScript = entry.TargetObject?.Script ?? string.Empty;

        if (entry.Change == "unchanged")
        {
            return string.Empty;
        }

        return BuildUnifiedDiff(entry.SourceObject, entry.TargetObject, sourceLabel, targetLabel, contextLines, normalizedDiff);
    }

    internal static string BuildUnifiedDiff(string sourceLabel, string targetLabel, string sourceScript, string targetScript, int contextLines = 3, bool normalizedDiff = false)
        => BuildUnifiedDiff(null, sourceLabel, targetLabel, sourceScript, targetScript, contextLines, normalizedDiff);

    internal static string BuildUnifiedDiff(string? objectType, string sourceLabel, string targetLabel, string sourceScript, string targetScript, int contextLines = 3, bool normalizedDiff = false)
        => BuildUnifiedDiffCore(objectType, null, sourceLabel, targetLabel, sourceScript, targetScript, contextLines, normalizedDiff);

    private static string BuildUnifiedDiff(
        InternalObject? sourceObject,
        InternalObject? targetObject,
        string sourceLabel,
        string targetLabel,
        int contextLines = 3,
        bool normalizedDiff = false)
    {
        var objectType = sourceObject?.ObjectType ?? targetObject?.ObjectType;
        var sourceScript = sourceObject?.Script ?? string.Empty;
        var targetScript = targetObject?.Script ?? string.Empty;
        var compatibleOmittedTextImageOnDataSpaceName =
            GetCompatibleOmittedTextImageOnDataSpaceName(sourceObject, targetObject);

        return BuildUnifiedDiffCore(
            objectType,
            compatibleOmittedTextImageOnDataSpaceName,
            sourceLabel,
            targetLabel,
            sourceScript,
            targetScript,
            contextLines,
            normalizedDiff);
    }

    private static string BuildUnifiedDiffCore(
        string? objectType,
        string? compatibleOmittedTextImageOnDataSpaceName,
        string sourceLabel,
        string targetLabel,
        string sourceScript,
        string targetScript,
        int contextLines = 3,
        bool normalizedDiff = false)
    {
        var comparableSourceLines = BuildComparableLinesForDiff(
            sourceScript,
            objectType,
            compatibleOmittedTextImageOnDataSpaceName,
            normalizedDiff);
        var comparableTargetLines = BuildComparableLinesForDiff(
            targetScript,
            objectType,
            compatibleOmittedTextImageOnDataSpaceName,
            normalizedDiff);

        if (ComparableLinesEqual(comparableSourceLines, comparableTargetLines))
        {
            return string.Empty;
        }

        var diffLines = ComputeDiffLines(comparableSourceLines, comparableTargetLines);
        var hunks = BuildDiffHunks(diffLines, Math.Max(0, contextLines));

        if (hunks.Count == 0)
        {
            return string.Empty;
        }

        var lines = new List<string>
        {
            $"--- {sourceLabel}",
            $"+++ {targetLabel}"
        };

        foreach (var hunk in hunks)
        {
            lines.Add($"@@ -{hunk.SrcStart},{hunk.SrcCount} +{hunk.TgtStart},{hunk.TgtCount} @@");
            lines.AddRange(hunk.Lines);
        }

        return string.Join(Environment.NewLine, lines);
    }

    private enum DiffLineKind { Equal, Removed, Added }

    private readonly record struct ComparableLine(string Key, string Display);

    private readonly record struct TableLikeBodyItem(string Text, bool HasTrailingComma);

    private sealed record TableLikeStatementParts(
        string Prefix,
        IReadOnlyList<TableLikeBodyItem> Items,
        string Suffix);

    private readonly record struct DiffEntry(DiffLineKind Kind, int SrcLine, int TgtLine, string Content);

    private sealed record DiffHunk(IReadOnlyList<string> Lines, int SrcStart, int SrcCount, int TgtStart, int TgtCount);

    private static IReadOnlyList<DiffEntry> ComputeDiffLines(string[] source, string[] target)
    {
        int m = source.Length, n = target.Length;

        // dp[i,j] = length of LCS of source[i..m-1] and target[j..n-1]
        var dp = new int[m + 1, n + 1];
        for (int i = m - 1; i >= 0; i--)
        {
            for (int j = n - 1; j >= 0; j--)
            {
                dp[i, j] = string.Equals(source[i], target[j], StringComparison.Ordinal)
                    ? 1 + dp[i + 1, j + 1]
                    : Math.Max(dp[i + 1, j], dp[i, j + 1]);
            }
        }

        var result = new List<DiffEntry>(m + n);
        int si = 0, ti = 0, srcLine = 1, tgtLine = 1;
        while (si < m || ti < n)
        {
            if (si < m && ti < n && string.Equals(source[si], target[ti], StringComparison.Ordinal))
            {
                result.Add(new DiffEntry(DiffLineKind.Equal, srcLine++, tgtLine++, source[si]));
                si++; ti++;
            }
            else if (si < m && (ti >= n || dp[si + 1, ti] >= dp[si, ti + 1]))
            {
                result.Add(new DiffEntry(DiffLineKind.Removed, srcLine++, 0, source[si]));
                si++;
            }
            else
            {
                result.Add(new DiffEntry(DiffLineKind.Added, 0, tgtLine++, target[ti]));
                ti++;
            }
        }

        return result;
    }

    private static IReadOnlyList<DiffEntry> ComputeDiffLines(ComparableLine[] source, ComparableLine[] target)
    {
        int m = source.Length, n = target.Length;

        var dp = new int[m + 1, n + 1];
        for (int i = m - 1; i >= 0; i--)
        {
            for (int j = n - 1; j >= 0; j--)
            {
                dp[i, j] = string.Equals(source[i].Key, target[j].Key, StringComparison.Ordinal)
                    ? 1 + dp[i + 1, j + 1]
                    : Math.Max(dp[i + 1, j], dp[i, j + 1]);
            }
        }

        var result = new List<DiffEntry>(m + n);
        int si = 0, ti = 0, srcLine = 1, tgtLine = 1;
        while (si < m || ti < n)
        {
            if (si < m && ti < n && string.Equals(source[si].Key, target[ti].Key, StringComparison.Ordinal))
            {
                result.Add(new DiffEntry(DiffLineKind.Equal, srcLine++, tgtLine++, source[si].Display));
                si++;
                ti++;
            }
            else if (si < m && (ti >= n || dp[si + 1, ti] >= dp[si, ti + 1]))
            {
                result.Add(new DiffEntry(DiffLineKind.Removed, srcLine++, 0, source[si].Display));
                si++;
            }
            else
            {
                result.Add(new DiffEntry(DiffLineKind.Added, 0, tgtLine++, target[ti].Display));
                ti++;
            }
        }

        return result;
    }

    private static ComparableLine[] BuildComparableLinesForDiff(
        string script,
        string? objectType,
        string? compatibleOmittedTextImageOnDataSpaceName,
        bool normalizedDiff)
    {
        var keyScript = NormalizeForComparison(script, objectType, compatibleOmittedTextImageOnDataSpaceName);
        var keyLines = keyScript.Length == 0
            ? Array.Empty<string>()
            : keyScript.Split('\n');

        if (normalizedDiff || !NeedsReadableDiffDisplayNormalization(objectType))
        {
            return keyLines.Select(line => new ComparableLine(line, line)).ToArray();
        }

        var structuredComparableLines = BuildStructuredComparableLinesForDiff(
            script,
            objectType,
            compatibleOmittedTextImageOnDataSpaceName);
        if (structuredComparableLines is not null)
        {
            return structuredComparableLines;
        }

        var displayScript = NormalizeForDiffDisplay(script, objectType, compatibleOmittedTextImageOnDataSpaceName);
        var displayLines = displayScript.Length == 0
            ? Array.Empty<string>()
            : displayScript.Split('\n');

        if (keyLines.Length != displayLines.Length)
        {
            if (TryBuildGroupedComparableLines(keyLines, displayLines, out var groupedComparableLines))
            {
                return groupedComparableLines;
            }

            return keyLines.Select(line => new ComparableLine(line, line)).ToArray();
        }

        var comparableLines = new ComparableLine[keyLines.Length];
        for (var i = 0; i < keyLines.Length; i++)
        {
            comparableLines[i] = new ComparableLine(keyLines[i], displayLines[i]);
        }

        return comparableLines;
    }

    private static ComparableLine[]? BuildStructuredComparableLinesForDiff(
        string script,
        string? objectType,
        string? compatibleOmittedTextImageOnDataSpaceName)
    {
        if (string.Equals(objectType, "Table", StringComparison.OrdinalIgnoreCase))
        {
            var normalized = PrepareScriptForReadableDiffDisplay(script, objectType);
            normalized = NormalizeCompatibleOmittedTextImageOnForComparison(
                normalized,
                compatibleOmittedTextImageOnDataSpaceName);
            return BuildTableComparableLinesForDiff(normalized);
        }

        if (string.Equals(objectType, "UserDefinedType", StringComparison.OrdinalIgnoreCase))
        {
            var normalized = PrepareScriptForReadableDiffDisplay(script, objectType);
            return BuildUserDefinedTypeComparableLinesForDiff(normalized);
        }

        return null;
    }

    private static string PrepareScriptForReadableDiffDisplay(string script, string? objectType)
    {
        var normalized = script
            .Replace("\r\n", "\n", StringComparison.Ordinal)
            .Replace("\r", "\n", StringComparison.Ordinal)
            .TrimEnd('\n');

        var lines = normalized.Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            if (string.IsNullOrWhiteSpace(lines[i]))
            {
                lines[i] = string.Empty;
            }
            else if (IsProgrammableObjectTypeForHeaderCommentCompatibility(objectType) &&
                     SsmsObjectHeaderCommentRegex.IsMatch(lines[i]))
            {
                lines[i] = string.Empty;
            }
        }

        if (IsProgrammableObjectTypeForHeaderCommentCompatibility(objectType))
        {
            lines = TrimLeadingEmptyLinesForComparison(lines);
        }

        lines = RemoveEmptyLinesForComparison(lines);

        var joined = string.Join("\n", lines);
        joined = NormalizeEmptyGoBatchesForComparison(joined);

        if (joined.Contains("sp_addextendedproperty", StringComparison.OrdinalIgnoreCase))
        {
            joined = NormalizeExtendedPropertyBlocksForComparison(joined);
        }

        return joined;
    }

    private static string[] BuildTableDisplayUnitsForDiff(string script)
    {
        var blocks = SplitGoDelimitedBlocks(script);
        if (blocks.Count == 0)
        {
            return script.Length == 0 ? Array.Empty<string>() : script.Split('\n');
        }

        var createTableIndex = blocks.FindIndex(BlockContainsCreateTable);
        if (createTableIndex < 0)
        {
            return script.Split('\n');
        }

        var units = new List<string>();
        foreach (var block in blocks.Take(createTableIndex))
        {
            units.AddRange(GetDisplayUnitsForBlock(block));
        }

        units.AddRange(GetDisplayUnitsForTableLikeBlock(blocks[createTableIndex]));

        var postCreatePackages = BuildTablePostCreatePackages(blocks, createTableIndex + 1);
        foreach (var package in postCreatePackages.OrderBy(NormalizeTablePostCreatePackageForComparison, StringComparer.Ordinal))
        {
            units.AddRange(GetDisplayUnitsForTablePostCreatePackage(package));
        }

        return units.ToArray();
    }

    private static ComparableLine[]? BuildTableComparableLinesForDiff(string script)
    {
        var blocks = SplitGoDelimitedBlocks(script);
        if (blocks.Count == 0)
        {
            return Array.Empty<ComparableLine>();
        }

        var createTableIndex = blocks.FindIndex(BlockContainsCreateTable);
        if (createTableIndex < 0)
        {
            return null;
        }

        var comparableLines = new List<ComparableLine>();
        foreach (var block in blocks.Take(createTableIndex))
        {
            comparableLines.Add(BuildComparableLineForStructuredBlock(
                NormalizeTableBlockForComparison(block),
                NormalizeTableBlockForDiffDisplay(block)));
        }

        if (!TryBuildComparableLinesForTableLikeBlock(blocks[createTableIndex], out var createTableComparableLines))
        {
            return null;
        }

        comparableLines.AddRange(createTableComparableLines);

        var postCreatePackages = BuildTablePostCreatePackages(blocks, createTableIndex + 1);
        foreach (var package in postCreatePackages.OrderBy(NormalizeTablePostCreatePackageForComparison, StringComparer.Ordinal))
        {
            comparableLines.Add(BuildComparableLineForStructuredBlock(
                NormalizeTablePostCreatePackageForComparison(package),
                NormalizeTablePostCreatePackageForDiffDisplay(package)));
        }

        return comparableLines.ToArray();
    }

    private static string[] BuildUserDefinedTypeDisplayUnitsForDiff(string script)
    {
        var blocks = SplitGoDelimitedBlocks(script);
        if (blocks.Count == 0)
        {
            return script.Length == 0 ? Array.Empty<string>() : script.Split('\n');
        }

        var units = new List<string>();
        foreach (var block in blocks)
        {
            var firstLine = GetFirstMeaningfulLine(block);
            if (firstLine is not null &&
                firstLine.StartsWith("CREATE TYPE", StringComparison.OrdinalIgnoreCase))
            {
                units.AddRange(GetDisplayUnitsForTableLikeBlock(block));
            }
            else
            {
                units.AddRange(GetDisplayUnitsForBlock(block));
            }
        }

        return units.ToArray();
    }

    private static ComparableLine[]? BuildUserDefinedTypeComparableLinesForDiff(string script)
    {
        var blocks = SplitGoDelimitedBlocks(script);
        if (blocks.Count == 0)
        {
            return Array.Empty<ComparableLine>();
        }

        var comparableLines = new List<ComparableLine>();
        foreach (var block in blocks)
        {
            var firstLine = GetFirstMeaningfulLine(block);
            if (!string.IsNullOrEmpty(firstLine) &&
                firstLine.StartsWith("CREATE TYPE", StringComparison.OrdinalIgnoreCase))
            {
                if (!TryBuildComparableLinesForTableLikeBlock(block, out var createTypeComparableLines))
                {
                    return null;
                }

                comparableLines.AddRange(createTypeComparableLines);
                continue;
            }

            comparableLines.Add(BuildComparableLineForStructuredBlock(
                JoinBlockLines(block),
                JoinBlockLines(block)));
        }

        return comparableLines.ToArray();
    }

    private static IReadOnlyList<string> GetDisplayUnitsForTablePostCreatePackage(string package)
    {
        var blocks = SplitGoDelimitedBlocks(package);
        if (blocks.Count != 1)
        {
            return package.Split('\n');
        }

        var firstLine = GetFirstMeaningfulLine(blocks[0]);
        if (!string.IsNullOrEmpty(firstLine) &&
            (firstLine.StartsWith("ALTER TABLE", StringComparison.OrdinalIgnoreCase) ||
             (firstLine.StartsWith("CREATE", StringComparison.OrdinalIgnoreCase) &&
              firstLine.IndexOf(" INDEX ", StringComparison.OrdinalIgnoreCase) >= 0)))
        {
            return GetDisplayUnitsForTableLikeBlock(blocks[0]);
        }

        return GetDisplayUnitsForBlock(blocks[0]);
    }

    private static IReadOnlyList<string> GetDisplayUnitsForTableLikeBlock(IEnumerable<string> block)
        => NormalizeLegacyTableStatementBlockForDiffDisplay(block).Split('\n');

    private static IReadOnlyList<string> GetDisplayUnitsForBlock(IEnumerable<string> block)
        => JoinBlockLines(block).Split('\n');

    private static ComparableLine BuildComparableLineForStructuredBlock(string key, string display)
        => new(key, display);

    private static bool TryBuildComparableLinesForTableLikeBlock(
        IEnumerable<string> block,
        out IReadOnlyList<ComparableLine> comparableLines)
    {
        var statement = string.Join(
            " ",
            block.Where(line => !string.Equals(line.Trim(), "GO", StringComparison.OrdinalIgnoreCase))
                 .Select(line => line.Trim())
                 .Where(line => line.Length > 0));

        if (statement.Length == 0)
        {
            comparableLines = [new ComparableLine("GO", "GO")];
            return true;
        }

        if (!TryParseTableLikeStatementForDiffDisplay(statement, out var parts))
        {
            comparableLines = Array.Empty<ComparableLine>();
            return false;
        }

        var lines = new List<ComparableLine>(parts.Items.Count + 4)
        {
            new ComparableLine(
                NormalizeLegacyTableStatementTextForComparison(parts.Prefix),
                parts.Prefix),
            new ComparableLine("(", "(")
        };

        foreach (var item in parts.Items)
        {
            var display = item.HasTrailingComma
                ? $"    {item.Text},"
                : $"    {item.Text}";
            var key = NormalizeLegacyTableStatementTextForComparison(
                item.HasTrailingComma ? item.Text + "," : item.Text);
            lines.Add(new ComparableLine(key, display));
        }

        var closeDisplay = string.IsNullOrWhiteSpace(parts.Suffix)
            ? ")"
            : $") {parts.Suffix}";
        lines.Add(new ComparableLine(
            NormalizeLegacyTableStatementTextForComparison(closeDisplay),
            closeDisplay));
        lines.Add(new ComparableLine("GO", "GO"));

        comparableLines = lines;
        return true;
    }

    private static bool TryBuildGroupedComparableLines(
        IReadOnlyList<string> keyLines,
        IReadOnlyList<string> displayLines,
        out ComparableLine[] comparableLines)
    {
        var grouped = new List<ComparableLine>(keyLines.Count);
        var displayIndex = 0;

        for (var keyIndex = 0; keyIndex < keyLines.Count; keyIndex++)
        {
            var keyLine = keyLines[keyIndex];
            if (string.Equals(keyLine, "GO", StringComparison.OrdinalIgnoreCase))
            {
                if (displayIndex >= displayLines.Count ||
                    !string.Equals(displayLines[displayIndex].Trim(), "GO", StringComparison.OrdinalIgnoreCase))
                {
                    comparableLines = Array.Empty<ComparableLine>();
                    return false;
                }

                grouped.Add(new ComparableLine(keyLine, displayLines[displayIndex]));
                displayIndex++;
                continue;
            }

            if (displayIndex >= displayLines.Count)
            {
                comparableLines = Array.Empty<ComparableLine>();
                return false;
            }

            var statementLines = new List<string>();
            while (displayIndex < displayLines.Count &&
                   !string.Equals(displayLines[displayIndex].Trim(), "GO", StringComparison.OrdinalIgnoreCase))
            {
                statementLines.Add(displayLines[displayIndex]);
                displayIndex++;
            }

            if (statementLines.Count == 0)
            {
                comparableLines = Array.Empty<ComparableLine>();
                return false;
            }

            grouped.Add(new ComparableLine(keyLine, string.Join("\n", statementLines)));
        }

        if (displayIndex != displayLines.Count)
        {
            comparableLines = Array.Empty<ComparableLine>();
            return false;
        }

        comparableLines = grouped.ToArray();
        return true;
    }

    private static bool ComparableLinesEqual(ComparableLine[] source, ComparableLine[] target)
    {
        if (source.Length != target.Length)
        {
            return false;
        }

        for (var i = 0; i < source.Length; i++)
        {
            if (!string.Equals(source[i].Key, target[i].Key, StringComparison.Ordinal))
            {
                return false;
            }
        }

        return true;
    }

    private static IReadOnlyList<DiffHunk> BuildDiffHunks(IReadOnlyList<DiffEntry> diffLines, int contextLines)
    {
        var changeIndices = new List<int>();
        for (int i = 0; i < diffLines.Count; i++)
        {
            if (diffLines[i].Kind != DiffLineKind.Equal)
                changeIndices.Add(i);
        }

        if (changeIndices.Count == 0)
            return Array.Empty<DiffHunk>();

        // Compute merged hunk ranges with context extension
        var hunkRanges = new List<(int Start, int End)>();
        int hunkStart = Math.Max(0, changeIndices[0] - contextLines);
        int hunkEnd = Math.Min(diffLines.Count - 1, changeIndices[0] + contextLines);

        for (int i = 1; i < changeIndices.Count; i++)
        {
            int newStart = Math.Max(0, changeIndices[i] - contextLines);
            int newEnd = Math.Min(diffLines.Count - 1, changeIndices[i] + contextLines);

            if (newStart <= hunkEnd + 1)
            {
                hunkEnd = Math.Max(hunkEnd, newEnd);
            }
            else
            {
                hunkRanges.Add((hunkStart, hunkEnd));
                hunkStart = newStart;
                hunkEnd = newEnd;
            }
        }
        hunkRanges.Add((hunkStart, hunkEnd));

        var hunks = new List<DiffHunk>(hunkRanges.Count);
        foreach (var (start, end) in hunkRanges)
        {
            var hunkLines = new List<string>();
            int srcStart = 0, srcCount = 0, tgtStart = 0, tgtCount = 0;

            for (int i = start; i <= end; i++)
            {
                var entry = diffLines[i];
                switch (entry.Kind)
                {
                    case DiffLineKind.Equal:
                        AppendDiffDisplayLines(hunkLines, " ", entry.Content);
                        if (srcStart == 0) srcStart = entry.SrcLine;
                        if (tgtStart == 0) tgtStart = entry.TgtLine;
                        srcCount++;
                        tgtCount++;
                        break;
                    case DiffLineKind.Removed:
                        AppendDiffDisplayLines(hunkLines, "-", entry.Content);
                        if (srcStart == 0) srcStart = entry.SrcLine;
                        srcCount++;
                        break;
                    case DiffLineKind.Added:
                        AppendDiffDisplayLines(hunkLines, "+", entry.Content);
                        if (tgtStart == 0) tgtStart = entry.TgtLine;
                        tgtCount++;
                        break;
                }
            }

            hunks.Add(new DiffHunk(hunkLines, srcStart, srcCount, tgtStart, tgtCount));
        }

        return hunks;
    }

    private static void AppendDiffDisplayLines(List<string> lines, string prefix, string content)
    {
        var displayLines = content.Split('\n');
        foreach (var line in displayLines)
        {
            lines.Add(prefix + line);
        }
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
            .Select(path => IsLegacyTableTypePath(path)
                ? new CommandWarning(
                    "legacy_folder_entry",
                    $"skipped legacy folder entry '{path}'; move it to '{Path.Combine("Types", "User-defined Data Types")}'.")
                : new CommandWarning(
                    "unsupported_folder_entry",
                    $"skipped unsupported folder entry '{path}'."))
            .ToArray();
    }

    internal static bool TryClassifyUserDefinedTypeScript(string script, out UserDefinedTypeKind kind)
    {
        var matchesScalar = ScalarUserDefinedTypeScriptRegex.IsMatch(script);
        var matchesTable = TableUserDefinedTypeScriptRegex.IsMatch(script);

        if (matchesScalar == matchesTable)
        {
            kind = default;
            return false;
        }

        kind = matchesTable ? UserDefinedTypeKind.Table : UserDefinedTypeKind.Scalar;
        return true;
    }

    internal static bool TryParseObjectFileName(string fileNameWithoutExtension, bool isSchemaLess, out string schema, out string name)
    {
        if (isSchemaLess)
        {
            schema = string.Empty;
            name = UnescapeFileNamePart(fileNameWithoutExtension.Trim());
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

        schema = UnescapeFileNamePart(fileNameWithoutExtension[..separatorIndex]);
        name = UnescapeFileNamePart(fileNameWithoutExtension[(separatorIndex + 1)..]);
        return schema.Length > 0 && name.Length > 0;
    }

    internal static string UnescapeFileNamePart(string value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return value;
        }

        var builder = new StringBuilder(value.Length);
        for (var i = 0; i < value.Length; i++)
        {
            if (value[i] == '%' &&
                i + 2 < value.Length &&
                IsHexDigit(value[i + 1]) &&
                IsHexDigit(value[i + 2]))
            {
                var decoded = Convert.ToInt32(value.Substring(i + 1, 2), 16);
                builder.Append((char)decoded);
                i += 2;
                continue;
            }

            builder.Append(value[i]);
        }

        return builder.ToString();
    }

    internal static bool TryResolveSchemaLessFolderIdentityFromScript(
        string objectType,
        string fileNameWithoutExtension,
        string script,
        string parsedFileName,
        out string name)
    {
        name = string.Empty;
        if (!SupportedSqlObjectTypes.IsSchemaLess(objectType) ||
            !TryExtractSchemaLessCreateName(objectType, script, out var scriptName))
        {
            return false;
        }

        var canonicalFileName = SchemaFolderMapper.EscapeFileNamePart(scriptName);
        if (string.Equals(canonicalFileName, scriptName, StringComparison.Ordinal) ||
            string.Equals(canonicalFileName, fileNameWithoutExtension.Trim(), StringComparison.OrdinalIgnoreCase) ||
            string.Equals(scriptName, parsedFileName, StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        name = scriptName;
        return true;
    }

    private static bool TryExtractSchemaLessCreateName(string objectType, string script, out string name)
    {
        name = string.Empty;
        var prefixPattern = objectType switch
        {
            "Assembly" => @"\bCREATE\s+ASSEMBLY\b",
            "Schema" => @"\bCREATE\s+SCHEMA\b",
            "Role" => @"\bCREATE\s+ROLE\b",
            "User" => @"\bCREATE\s+USER\b",
            "MessageType" => @"\bCREATE\s+MESSAGE\s+TYPE\b",
            "Contract" => @"\bCREATE\s+CONTRACT\b",
            "EventNotification" => @"\bCREATE\s+EVENT\s+NOTIFICATION\b",
            "ServiceBinding" => @"\bCREATE\s+REMOTE\s+SERVICE\s+BINDING\b",
            "Service" => @"\bCREATE\s+SERVICE\b",
            "Route" => @"\bCREATE\s+ROUTE\b",
            "PartitionFunction" => @"\bCREATE\s+PARTITION\s+FUNCTION\b",
            "PartitionScheme" => @"\bCREATE\s+PARTITION\s+SCHEME\b",
            "FullTextCatalog" => @"\bCREATE\s+FULLTEXT\s+CATALOG\b",
            "FullTextStoplist" => @"\bCREATE\s+FULLTEXT\s+STOPLIST\b",
            "SearchPropertyList" => @"\bCREATE\s+SEARCH\s+PROPERTY\s+LIST\b",
            _ => null
        };

        if (prefixPattern is null)
        {
            return false;
        }

        var prefixMatch = Regex.Match(
            script,
            prefixPattern,
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        if (!prefixMatch.Success)
        {
            return false;
        }

        var identifierMatch = SqlIdentifierRegex.Match(script, prefixMatch.Index + prefixMatch.Length);
        if (!identifierMatch.Success)
        {
            return false;
        }

        name = UnquoteSqlIdentifier(identifierMatch.Groups["identifier"].Value);
        return name.Length > 0;
    }

    private static string UnquoteSqlIdentifier(string value)
    {
        if (value.Length >= 2 && value[0] == '[' && value[^1] == ']')
        {
            return value[1..^1].Replace("]]", "]", StringComparison.Ordinal);
        }

        if (value.Length >= 2 && value[0] == '"' && value[^1] == '"')
        {
            return value[1..^1].Replace("\"\"", "\"", StringComparison.Ordinal);
        }

        return value;
    }

    private static bool IsHexDigit(char value)
        => (value >= '0' && value <= '9')
           || (value >= 'A' && value <= 'F')
           || (value >= 'a' && value <= 'f');

    private static bool IsLegacyTableTypePath(string relativePath)
    {
        var normalized = relativePath.Replace(Path.AltDirectorySeparatorChar, Path.DirectorySeparatorChar);
        var legacyPrefix = Path.Combine("Types", "Table Types") + Path.DirectorySeparatorChar;
        return normalized.StartsWith(legacyPrefix, StringComparison.OrdinalIgnoreCase);
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

            if (!ScriptsEqualForComparison(sourceObject.ObjectType, sourceObject.Script, targetObject.Script))
            {
                results.Add(new ComparableChange(sourceObject, "changed"));
            }
        }

        return results;
    }

    private static bool ScriptsEqualForComparison(InternalObject left, InternalObject right)
        => string.Equals(
            NormalizeForComparison(
                left.Script,
                left.ObjectType,
                GetCompatibleOmittedTextImageOnDataSpaceName(left, right)),
            NormalizeForComparison(
                right.Script,
                right.ObjectType,
                GetCompatibleOmittedTextImageOnDataSpaceName(left, right)),
            StringComparison.Ordinal);

    private static bool ScriptsEqualForComparison(string? objectType, string left, string right)
        => string.Equals(NormalizeForComparison(left, objectType), NormalizeForComparison(right, objectType), StringComparison.Ordinal);

    private static string? GetCompatibleOmittedTextImageOnDataSpaceName(InternalObject? left, InternalObject? right)
    {
        var objectType = left?.ObjectType ?? right?.ObjectType;
        if (!string.Equals(objectType, "Table", StringComparison.OrdinalIgnoreCase))
        {
            return null;
        }

        var leftValue = left?.CompatibleOmittedTextImageOnDataSpaceName;
        var rightValue = right?.CompatibleOmittedTextImageOnDataSpaceName;
        if (!string.IsNullOrWhiteSpace(leftValue) && !string.IsNullOrWhiteSpace(rightValue))
        {
            return string.Equals(leftValue, rightValue, StringComparison.OrdinalIgnoreCase)
                ? leftValue
                : null;
        }

        return !string.IsNullOrWhiteSpace(leftValue)
            ? leftValue
            : rightValue;
    }

    private static IReadOnlyList<string> GetCandidateDbObjectTypes(ObjectSelector selector)
    {
        if (string.Equals(selector.ObjectType, TableDataObjectType, StringComparison.OrdinalIgnoreCase))
        {
            return ["Table"];
        }

        if (selector.ObjectType is not null)
        {
            return [selector.ObjectType];
        }

        return SupportedSqlObjectTypes.ActiveSync
            .Where(entry => entry.IsSchemaLess == selector.IsSchemaLess)
            .Select(entry => entry.ObjectType)
            .ToArray();
    }

    internal static string NormalizeForComparison(string script)
        => NormalizeForComparison(script, null);

    internal static string NormalizeForComparison(string script, string? objectType)
        => NormalizeForComparison(script, objectType, null);

    internal static string NormalizeForComparison(
        string script,
        string? objectType,
        string? compatibleOmittedTextImageOnDataSpaceName)
    {
        var normalized = script
            .Replace("\r\n", "\n", StringComparison.Ordinal)
            .Replace("\r", "\n", StringComparison.Ordinal)
            .TrimEnd('\n');

        var lines = normalized.Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            if (string.IsNullOrWhiteSpace(lines[i]))
            {
                lines[i] = string.Empty;
            }
            else if (IsProgrammableObjectTypeForHeaderCommentCompatibility(objectType) &&
                     SsmsObjectHeaderCommentRegex.IsMatch(lines[i]))
            {
                lines[i] = string.Empty;
            }
        }

        if (IsProgrammableObjectTypeForHeaderCommentCompatibility(objectType))
        {
            lines = TrimLeadingEmptyLinesForComparison(lines);
        }

        lines = RemoveEmptyLinesForComparison(lines);

        var isTableData = string.Equals(objectType, TableDataObjectType, StringComparison.OrdinalIgnoreCase);
        var joined = string.Join("\n", lines);
        joined = NormalizeEmptyGoBatchesForComparison(joined);
        if (isTableData)
        {
            return !joined.Contains("INSERT ", StringComparison.OrdinalIgnoreCase) &&
                   !joined.Contains("SET IDENTITY_INSERT ", StringComparison.OrdinalIgnoreCase)
                ? joined
                : NormalizeLegacyTableDataScript(joined);
        }

        if (string.Equals(objectType, "Queue", StringComparison.OrdinalIgnoreCase))
        {
            return NormalizeQueueScriptForComparison(joined);
        }

        if (string.Equals(objectType, "Role", StringComparison.OrdinalIgnoreCase))
        {
            return NormalizeRoleScriptForComparison(joined);
        }

        if (string.Equals(objectType, "MessageType", StringComparison.OrdinalIgnoreCase))
        {
            return NormalizeServiceBrokerScriptForComparison(joined, NormalizeMessageTypeBaseBlockForComparison);
        }

        if (string.Equals(objectType, "Contract", StringComparison.OrdinalIgnoreCase))
        {
            return NormalizeServiceBrokerScriptForComparison(joined, NormalizeContractBaseBlockForComparison);
        }

        if (string.Equals(objectType, "Service", StringComparison.OrdinalIgnoreCase))
        {
            return NormalizeServiceBrokerScriptForComparison(joined, NormalizeServiceBaseBlockForComparison);
        }

        if (string.Equals(objectType, "Function", StringComparison.OrdinalIgnoreCase))
        {
            joined = NormalizeClrTableValuedFunctionScriptForComparison(joined);
        }

        if (joined.Contains("sp_addextendedproperty", StringComparison.OrdinalIgnoreCase))
        {
            joined = NormalizeExtendedPropertyBlocksForComparison(joined);
        }

        if (string.Equals(objectType, "Table", StringComparison.OrdinalIgnoreCase))
        {
            joined = NormalizeCompatibleOmittedTextImageOnForComparison(
                joined,
                compatibleOmittedTextImageOnDataSpaceName);
            joined = NormalizeTableScriptForComparison(joined);
        }
        else if (string.Equals(objectType, "UserDefinedType", StringComparison.OrdinalIgnoreCase))
        {
            joined = NormalizeUserDefinedTypeScriptForComparison(joined);
        }

        if (!joined.Contains("INSERT ", StringComparison.OrdinalIgnoreCase))
        {
            return joined;
        }

        var joinedLines = joined.Split('\n');
        for (var i = 0; i < joinedLines.Length; i++)
        {
            var line = joinedLines[i];
            if (line.EndsWith(';') && LineStartsWithInsert(line))
            {
                line = line[..^1];
            }

            joinedLines[i] = line;
        }

        return string.Join("\n", joinedLines);
    }

    private static string NormalizeEmptyGoBatchesForComparison(string script)
    {
        var lines = script.Split('\n');
        var normalizedLines = new List<string>(lines.Length);
        var pendingBatchLines = new List<string>();
        var batchHasContent = false;

        foreach (var line in lines)
        {
            if (string.Equals(line.Trim(), "GO", StringComparison.OrdinalIgnoreCase))
            {
                if (batchHasContent)
                {
                    normalizedLines.AddRange(pendingBatchLines);
                    normalizedLines.Add("GO");
                }

                pendingBatchLines.Clear();
                batchHasContent = false;
                continue;
            }

            pendingBatchLines.Add(line);
            if (!IsIgnorableNoOpBatchLine(line))
            {
                batchHasContent = true;
            }
        }

        normalizedLines.AddRange(pendingBatchLines);
        return string.Join("\n", normalizedLines);
    }

    private static string NormalizeCompatibleOmittedTextImageOnForComparison(
        string script,
        string? compatibleOmittedTextImageOnDataSpaceName)
    {
        if (string.IsNullOrWhiteSpace(compatibleOmittedTextImageOnDataSpaceName))
        {
            return script;
        }

        var lines = script.Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            var match = CompatibleTextImageOnRegex.Match(lines[i]);
            if (!match.Success)
            {
                continue;
            }

            var dataSpaceName = UnquoteSqlIdentifier(match.Groups["dataSpace"].Value);
            if (!string.Equals(
                    dataSpaceName,
                    compatibleOmittedTextImageOnDataSpaceName,
                    StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            lines[i] = match.Groups["prefix"].Value + match.Groups["suffix"].Value;
        }

        return string.Join("\n", lines);
    }

    private static bool IsIgnorableNoOpBatchLine(string line)
    {
        if (string.IsNullOrWhiteSpace(line))
        {
            return true;
        }

        var trimmed = line.Trim();
        return trimmed.All(ch => ch == ';');
    }

    private static string[] TrimLeadingEmptyLinesForComparison(string[] lines)
    {
        var startIndex = 0;
        while (startIndex < lines.Length && lines[startIndex].Length == 0)
        {
            startIndex++;
        }

        return startIndex == 0 ? lines : lines[startIndex..];
    }

    private static string[] RemoveEmptyLinesForComparison(string[] lines)
        => lines.Where(line => line.Length > 0).ToArray();

    private static bool NeedsReadableDiffDisplayNormalization(string? objectType)
        => string.Equals(objectType, "Table", StringComparison.OrdinalIgnoreCase)
           || string.Equals(objectType, "UserDefinedType", StringComparison.OrdinalIgnoreCase);

    private static string NormalizeForDiffDisplay(
        string script,
        string? objectType,
        string? compatibleOmittedTextImageOnDataSpaceName)
    {
        var normalized = script
            .Replace("\r\n", "\n", StringComparison.Ordinal)
            .Replace("\r", "\n", StringComparison.Ordinal)
            .TrimEnd('\n');

        var lines = normalized.Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            if (string.IsNullOrWhiteSpace(lines[i]))
            {
                lines[i] = string.Empty;
            }
            else if (IsProgrammableObjectTypeForHeaderCommentCompatibility(objectType) &&
                     SsmsObjectHeaderCommentRegex.IsMatch(lines[i]))
            {
                lines[i] = string.Empty;
            }
        }

        if (IsProgrammableObjectTypeForHeaderCommentCompatibility(objectType))
        {
            lines = TrimLeadingEmptyLinesForComparison(lines);
        }

        lines = RemoveEmptyLinesForComparison(lines);

        var joined = string.Join("\n", lines);
        joined = NormalizeEmptyGoBatchesForComparison(joined);

        if (joined.Contains("sp_addextendedproperty", StringComparison.OrdinalIgnoreCase))
        {
            joined = NormalizeExtendedPropertyBlocksForComparison(joined);
        }

        if (string.Equals(objectType, "Table", StringComparison.OrdinalIgnoreCase))
        {
            joined = NormalizeCompatibleOmittedTextImageOnForComparison(
                joined,
                compatibleOmittedTextImageOnDataSpaceName);
            return NormalizeTableScriptForDiffDisplay(joined);
        }

        if (string.Equals(objectType, "UserDefinedType", StringComparison.OrdinalIgnoreCase))
        {
            return NormalizeUserDefinedTypeScriptForDiffDisplay(joined);
        }

        return joined;
    }

    private static string NormalizeQueueScriptForComparison(string script)
    {
        var normalized = Regex.Replace(
            script,
            @"(?im)^\s*ON\s+\[PRIMARY\]\s*$\n?",
            string.Empty,
            RegexOptions.CultureInvariant);
        normalized = Regex.Replace(normalized, @"\s*=\s*", "=", RegexOptions.CultureInvariant);
        normalized = Regex.Replace(normalized, @"\s*,\s*", ",", RegexOptions.CultureInvariant);
        normalized = Regex.Replace(normalized, @"\s+\(", "(", RegexOptions.CultureInvariant);
        normalized = Regex.Replace(normalized, @"\(\s*", "(", RegexOptions.CultureInvariant);
        normalized = Regex.Replace(normalized, @"\s*\)", ")", RegexOptions.CultureInvariant);
        normalized = Regex.Replace(normalized, @"\s+", " ", RegexOptions.CultureInvariant).Trim();
        normalized = Regex.Replace(
            normalized,
            @"\s+ON \[PRIMARY\](?=\s+GO\b|$)",
            string.Empty,
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        normalized = Regex.Replace(
            normalized,
            @",ACTIVATION\(STATUS=OFF,EXECUTE AS (?:'dbo'|\[dbo\])\)",
            string.Empty,
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        return normalized;
    }

    private static string NormalizeRoleScriptForComparison(string script)
    {
        var lines = script.Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            lines[i] = NormalizeRoleMembershipLineForComparison(lines[i]);
        }

        return string.Join("\n", lines);
    }

    private static string NormalizeRoleMembershipLineForComparison(string line)
    {
        var legacyMatch = RoleMembershipLegacySyntaxRegex.Match(line);
        if (legacyMatch.Success)
        {
            var roleName = UnescapeSqlStringLiteral(legacyMatch.Groups["role"].Value);
            var memberName = UnescapeSqlStringLiteral(legacyMatch.Groups["member"].Value);
            return $"ALTER ROLE {QuoteIdentifierForComparison(roleName)} ADD MEMBER {QuoteIdentifierForComparison(memberName)}";
        }

        var alterRoleMatch = RoleMembershipAlterRoleSyntaxRegex.Match(line);
        if (alterRoleMatch.Success)
        {
            var roleName = UnquoteSqlIdentifier(alterRoleMatch.Groups["role"].Value);
            var memberName = UnquoteSqlIdentifier(alterRoleMatch.Groups["member"].Value);
            return $"ALTER ROLE {QuoteIdentifierForComparison(roleName)} ADD MEMBER {QuoteIdentifierForComparison(memberName)}";
        }

        return line;
    }

    private static string UnescapeSqlStringLiteral(string value)
        => value.Replace("''", "'", StringComparison.Ordinal);

    private static string QuoteIdentifierForComparison(string value)
        => $"[{value.Replace("]", "]]", StringComparison.Ordinal)}]";

    private static string NormalizeServiceBrokerScriptForComparison(
        string script,
        Func<string, string> normalizeBaseBlock)
    {
        var lines = script.Split('\n');
        var goIndex = Array.FindIndex(
            lines,
            line => string.Equals(line.Trim(), "GO", StringComparison.OrdinalIgnoreCase));

        if (goIndex < 0)
        {
            return normalizeBaseBlock(script);
        }

        var baseBlock = string.Join("\n", lines.Take(goIndex));
        var normalizedBaseBlock = normalizeBaseBlock(baseBlock);
        var remainder = string.Join("\n", lines.Skip(goIndex));
        return string.IsNullOrEmpty(normalizedBaseBlock)
            ? remainder
            : normalizedBaseBlock + "\n" + remainder;
    }

    private static string NormalizeMessageTypeBaseBlockForComparison(string baseBlock)
    {
        var normalized = CollapseServiceBrokerWhitespace(baseBlock);
        normalized = Regex.Replace(
            normalized,
            @"(?i)\bVALIDATION\s*=\s*XML\b",
            "VALIDATION=WELL_FORMED_XML",
            RegexOptions.CultureInvariant);
        normalized = Regex.Replace(
            normalized,
            @"(?i)\bVALIDATION\s*=\s*WELL_FORMED_XML\b",
            "VALIDATION=WELL_FORMED_XML",
            RegexOptions.CultureInvariant);
        normalized = Regex.Replace(
            normalized,
            @"(?i)\bVALIDATION\s*=\s*VALID_XML\s+WITH\s+SCHEMA\s+COLLECTION\s+",
            "VALIDATION=VALID_XML WITH SCHEMA COLLECTION ",
            RegexOptions.CultureInvariant);
        normalized = Regex.Replace(
            normalized,
            @"(?i)\bVALIDATION\s*=\s*NONE\b",
            "VALIDATION=NONE",
            RegexOptions.CultureInvariant);
        normalized = Regex.Replace(
            normalized,
            @"(?i)\bVALIDATION\s*=\s*EMPTY\b",
            "VALIDATION=EMPTY",
            RegexOptions.CultureInvariant);
        return normalized;
    }

    private static string NormalizeContractBaseBlockForComparison(string baseBlock)
    {
        var normalized = CollapseServiceBrokerWhitespace(baseBlock);
        var openParenIndex = normalized.IndexOf('(');
        var closeParenIndex = normalized.LastIndexOf(')');
        if (openParenIndex < 0 || closeParenIndex < openParenIndex)
        {
            return normalized;
        }

        var prefix = normalized[..openParenIndex].TrimEnd();
        var suffix = normalized[(closeParenIndex + 1)..].Trim();
        var body = normalized[(openParenIndex + 1)..closeParenIndex];
        var items = SplitNormalizedServiceBrokerList(body);

        var rebuilt = prefix + "(" + string.Join(",", items) + ")";
        return string.IsNullOrWhiteSpace(suffix)
            ? rebuilt
            : rebuilt + " " + suffix;
    }

    private static string NormalizeServiceBaseBlockForComparison(string baseBlock)
    {
        var normalized = CollapseServiceBrokerWhitespace(baseBlock);
        if (!normalized.Contains("ON QUEUE", StringComparison.OrdinalIgnoreCase))
        {
            return normalized;
        }

        var openParenIndex = normalized.IndexOf('(');
        var closeParenIndex = normalized.LastIndexOf(')');
        if (openParenIndex < 0 || closeParenIndex < openParenIndex)
        {
            return normalized;
        }

        var prefix = normalized[..openParenIndex].TrimEnd();
        var suffix = normalized[(closeParenIndex + 1)..].Trim();
        var body = normalized[(openParenIndex + 1)..closeParenIndex];
        var items = SplitNormalizedServiceBrokerList(body);

        var rebuilt = prefix + "(" + string.Join(",", items) + ")";
        return string.IsNullOrWhiteSpace(suffix)
            ? rebuilt
            : rebuilt + " " + suffix;
    }

    private static string CollapseServiceBrokerWhitespace(string text)
        => Regex.Replace(text, @"\s+", " ", RegexOptions.CultureInvariant).Trim();

    private static IReadOnlyList<string> SplitNormalizedServiceBrokerList(string body)
    {
        if (string.IsNullOrWhiteSpace(body))
        {
            return Array.Empty<string>();
        }

        return body
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(item => Regex.Replace(item, @"\s+", " ", RegexOptions.CultureInvariant).Trim())
            .OrderBy(item => item, StringComparer.OrdinalIgnoreCase)
            .ToArray();
    }

    private static string NormalizeClrTableValuedFunctionScriptForComparison(string script)
    {
        if (!script.Contains("EXTERNAL NAME", StringComparison.OrdinalIgnoreCase) ||
            !script.Contains("RETURNS TABLE", StringComparison.OrdinalIgnoreCase))
        {
            return script;
        }

        var inputLines = script.Split('\n');
        var outputLines = new List<string>(inputLines.Length);
        for (var i = 0; i < inputLines.Length; i++)
        {
            var line = inputLines[i];
            var splitMatch = ClrTableValuedFunctionReturnColumnNullWithCloseParenRegex.Match(line);
            if (splitMatch.Success)
            {
                outputLines.Add(splitMatch.Groups["prefix"].Value);
                outputLines.Add(")");
                continue;
            }

            var match = ClrTableValuedFunctionReturnColumnNullRegex.Match(line);
            outputLines.Add(match.Success
                ? match.Groups["prefix"].Value + match.Groups["suffix"].Value
                : line);
        }

        return string.Join("\n", outputLines);
    }

    // Checks whether a line begins with "INSERT " (ignoring leading whitespace) without
    // allocating a trimmed string.
    private static bool LineStartsWithInsert(string line)
    {
        var pos = 0;
        while (pos < line.Length && line[pos] is ' ' or '\t') pos++;
        const int insertPrefixLength = 7; // "INSERT ".Length
        return line.Length - pos >= insertPrefixLength &&
               line.AsSpan(pos, insertPrefixLength).Equals("INSERT ", StringComparison.OrdinalIgnoreCase);
    }

    private static bool LineStartsWithIdentityInsert(string line)
    {
        var pos = 0;
        while (pos < line.Length && line[pos] is ' ' or '\t') pos++;
        const string prefix = "SET IDENTITY_INSERT ";
        return line.Length - pos >= prefix.Length &&
               line.AsSpan(pos, prefix.Length).Equals(prefix, StringComparison.OrdinalIgnoreCase);
    }

    private static string NormalizeLegacyTableDataScript(string script)
    {
        var normalizedSegments = new List<string>();
        var bufferedInsertStatements = new List<string>();

        static void FlushBufferedInsertStatements(List<string> segments, List<string> inserts)
        {
            if (inserts.Count == 0)
            {
                return;
            }

            foreach (var statement in inserts.OrderBy(item => item, StringComparer.Ordinal))
            {
                segments.Add(statement);
            }

            inserts.Clear();
        }

        var position = 0;
        while (position < script.Length)
        {
            var lineEnd = script.IndexOf('\n', position);
            var segmentEnd = lineEnd >= 0 ? lineEnd : script.Length;
            var line = script.Substring(position, segmentEnd - position);

            if (LineStartsWithIdentityInsert(line))
            {
                FlushBufferedInsertStatements(normalizedSegments, bufferedInsertStatements);
                normalizedSegments.Add(StripTrailingSemicolon(line));
                position = lineEnd >= 0 ? lineEnd + 1 : script.Length;
                continue;
            }

            if (LineStartsWithInsert(line))
            {
                var insertRange = TryFindInsertValuesStatementRange(script, position);
                if (insertRange.HasValue)
                {
                    var statement = script.Substring(
                        position,
                        insertRange.Value.StatementEndExclusive - position);
                    bufferedInsertStatements.Add(NormalizeLegacyTableDataInsertStatement(statement));
                    position = insertRange.Value.ConsumedEndExclusive;
                    continue;
                }
            }

            FlushBufferedInsertStatements(normalizedSegments, bufferedInsertStatements);
            normalizedSegments.Add(line);
            position = lineEnd >= 0 ? lineEnd + 1 : script.Length;
        }

        FlushBufferedInsertStatements(normalizedSegments, bufferedInsertStatements);
        return string.Join("\n", normalizedSegments);
    }

    private static string NormalizeExtendedPropertyBlocksForComparison(string script)
    {
        var lines = script.Split('\n');
        var normalizedLines = new List<string>(lines.Length);

        for (var i = 0; i < lines.Length; i++)
        {
            if (IsExtendedPropertyStatementLine(lines[i]) &&
                i + 1 < lines.Length &&
                string.Equals(lines[i + 1].Trim(), "GO", StringComparison.OrdinalIgnoreCase))
            {
                var statements = new List<string>();
                while (i < lines.Length &&
                       IsExtendedPropertyStatementLine(lines[i]) &&
                       i + 1 < lines.Length &&
                       string.Equals(lines[i + 1].Trim(), "GO", StringComparison.OrdinalIgnoreCase))
                {
                    statements.Add(NormalizeExtendedPropertyStatementForComparison(lines[i]));
                    i += 2;
                }

                foreach (var statement in statements.OrderBy(item => item, StringComparer.Ordinal))
                {
                    normalizedLines.Add(statement);
                    normalizedLines.Add("GO");
                }

                i--;
                continue;
            }

            normalizedLines.Add(lines[i]);
        }

        return string.Join("\n", normalizedLines);
    }

    private static string NormalizeTableScriptForComparison(string script)
    {
        var blocks = SplitGoDelimitedBlocks(script);
        if (blocks.Count == 0)
        {
            return script;
        }

        var createTableIndex = blocks.FindIndex(BlockContainsCreateTable);
        if (createTableIndex < 0)
        {
            return script;
        }

        var normalizedBlocks = new List<string>(blocks.Count);
        normalizedBlocks.AddRange(blocks.Take(createTableIndex).Select(NormalizeTableBlockForComparison));
        normalizedBlocks.Add(NormalizeTableBlockForComparison(blocks[createTableIndex]));

        var postCreatePackages = BuildTablePostCreatePackages(blocks, createTableIndex + 1);
        foreach (var package in postCreatePackages
                     .Select(NormalizeTablePostCreatePackageForComparison)
                     .OrderBy(NormalizeTablePostCreatePackageKey, StringComparer.Ordinal))
        {
            normalizedBlocks.Add(package);
        }

        return string.Join("\n", normalizedBlocks);
    }

    private static List<string[]> SplitGoDelimitedBlocks(string script)
    {
        var lines = script.Split('\n');
        var blocks = new List<string[]>();
        var current = new List<string>();

        foreach (var line in lines)
        {
            current.Add(line);
            if (string.Equals(line.Trim(), "GO", StringComparison.OrdinalIgnoreCase))
            {
                blocks.Add(current.ToArray());
                current.Clear();
            }
        }

        if (current.Count > 0)
        {
            blocks.Add(current.ToArray());
        }

        return blocks;
    }

    private static bool BlockContainsCreateTable(string[] block)
        => block.Any(line => line.TrimStart().StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase));

    private static bool BlockContainsCreateTrigger(string[] block)
        => block.Any(line =>
            line.TrimStart().StartsWith("CREATE TRIGGER", StringComparison.OrdinalIgnoreCase) ||
            line.TrimStart().StartsWith("ALTER TRIGGER", StringComparison.OrdinalIgnoreCase));

    private static bool IsSetOnlyBlock(string[] block)
    {
        var contentLines = block
            .TakeWhile(line => !string.Equals(line.Trim(), "GO", StringComparison.OrdinalIgnoreCase))
            .Where(line => line.Trim().Length > 0)
            .ToArray();

        return contentLines.Length == 1 &&
               contentLines[0].TrimStart().StartsWith("SET ", StringComparison.OrdinalIgnoreCase);
    }

    private static List<string> BuildTablePostCreatePackages(IReadOnlyList<string[]> blocks, int startIndex)
    {
        var packages = new List<string>();
        for (var i = startIndex; i < blocks.Count; i++)
        {
            if (IsSetOnlyBlock(blocks[i]))
            {
                var setStart = i;
                while (i < blocks.Count && IsSetOnlyBlock(blocks[i]))
                {
                    i++;
                }

                if (i < blocks.Count && BlockContainsCreateTrigger(blocks[i]))
                {
                    var packageLines = new List<string>();
                    for (var blockIndex = setStart; blockIndex <= i; blockIndex++)
                    {
                        packageLines.AddRange(blocks[blockIndex]);
                    }

                    packages.Add(string.Join("\n", packageLines));
                    continue;
                }

                for (var blockIndex = setStart; blockIndex < i; blockIndex++)
                {
                    packages.Add(JoinBlockLines(blocks[blockIndex]));
                }

                i--;
                continue;
            }

            packages.Add(JoinBlockLines(blocks[i]));
        }

        return packages;
    }

    private static string NormalizeTablePostCreatePackageKey(string package)
        => Regex.Replace(package, @"\s+", " ", RegexOptions.CultureInvariant).Trim();

    private static string NormalizeTableBlockForComparison(string[] block)
    {
        var firstLine = GetFirstMeaningfulLine(block);
        if (string.IsNullOrEmpty(firstLine))
        {
            return JoinBlockLines(block);
        }

        if (firstLine.StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase) ||
            firstLine.StartsWith("ALTER TABLE", StringComparison.OrdinalIgnoreCase) ||
            (firstLine.StartsWith("CREATE", StringComparison.OrdinalIgnoreCase) &&
             firstLine.IndexOf(" INDEX ", StringComparison.OrdinalIgnoreCase) >= 0))
        {
            return NormalizeLegacyTableStatementBlockForComparison(block);
        }

        return JoinBlockLines(block);
    }

    private static string NormalizeTablePostCreatePackageForComparison(string package)
    {
        var blocks = SplitGoDelimitedBlocks(package);
        if (blocks.Count != 1)
        {
            return package;
        }

        return NormalizeTableBlockForComparison(blocks[0]);
    }

    private static string NormalizeTableScriptForDiffDisplay(string script)
    {
        var blocks = SplitGoDelimitedBlocks(script);
        if (blocks.Count == 0)
        {
            return script;
        }

        var createTableIndex = blocks.FindIndex(BlockContainsCreateTable);
        if (createTableIndex < 0)
        {
            return script;
        }

        var normalizedBlocks = new List<string>(blocks.Count);
        normalizedBlocks.AddRange(blocks.Take(createTableIndex).Select(NormalizeTableBlockForDiffDisplay));
        normalizedBlocks.Add(NormalizeTableBlockForDiffDisplay(blocks[createTableIndex]));

        var postCreatePackages = BuildTablePostCreatePackages(blocks, createTableIndex + 1);
        foreach (var package in postCreatePackages
                     .OrderBy(NormalizeTablePostCreatePackageForComparison, StringComparer.Ordinal))
        {
            normalizedBlocks.Add(NormalizeTablePostCreatePackageForDiffDisplay(package));
        }

        return string.Join("\n", normalizedBlocks);
    }

    private static string NormalizeTableBlockForDiffDisplay(string[] block)
    {
        var firstLine = GetFirstMeaningfulLine(block);
        if (string.IsNullOrEmpty(firstLine))
        {
            return JoinBlockLines(block);
        }

        if (firstLine.StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase) ||
            firstLine.StartsWith("ALTER TABLE", StringComparison.OrdinalIgnoreCase) ||
            (firstLine.StartsWith("CREATE", StringComparison.OrdinalIgnoreCase) &&
             firstLine.IndexOf(" INDEX ", StringComparison.OrdinalIgnoreCase) >= 0))
        {
            return NormalizeLegacyTableStatementBlockForDiffDisplay(block);
        }

        return JoinBlockLines(block);
    }

    private static string NormalizeTablePostCreatePackageForDiffDisplay(string package)
    {
        var blocks = SplitGoDelimitedBlocks(package);
        if (blocks.Count != 1)
        {
            return package;
        }

        return NormalizeTableBlockForDiffDisplay(blocks[0]);
    }

    private static string NormalizeUserDefinedTypeScriptForComparison(string script)
    {
        var blocks = SplitGoDelimitedBlocks(script);
        for (var i = 0; i < blocks.Count; i++)
        {
            var firstLine = GetFirstMeaningfulLine(blocks[i]);
            if (firstLine is null ||
                !firstLine.StartsWith("CREATE TYPE", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            blocks[i] = [NormalizeLegacyTableStatementBlockForComparison(blocks[i])];
        }

        return string.Join("\n", blocks.SelectMany(block => block));
    }

    private static string NormalizeUserDefinedTypeScriptForDiffDisplay(string script)
    {
        var blocks = SplitGoDelimitedBlocks(script);
        for (var i = 0; i < blocks.Count; i++)
        {
            var firstLine = GetFirstMeaningfulLine(blocks[i]);
            if (firstLine is null ||
                !firstLine.StartsWith("CREATE TYPE", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            blocks[i] = [NormalizeLegacyTableStatementBlockForDiffDisplay(blocks[i])];
        }

        return string.Join("\n", blocks.SelectMany(block => block));
    }

    private static string NormalizeLegacyTableStatementBlockForComparison(IEnumerable<string> block)
    {
        var statement = string.Join(
            " ",
            block.Where(line => !string.Equals(line.Trim(), "GO", StringComparison.OrdinalIgnoreCase))
                 .Select(line => line.Trim())
                 .Where(line => line.Length > 0));

        if (statement.Length == 0)
        {
            return "GO";
        }

        return NormalizeLegacyTableStatementTextForComparison(statement) + "\nGO";
    }

    private static string NormalizeLegacyTableStatementBlockForDiffDisplay(IEnumerable<string> block)
    {
        var statement = string.Join(
            " ",
            block.Where(line => !string.Equals(line.Trim(), "GO", StringComparison.OrdinalIgnoreCase))
                 .Select(line => line.Trim())
                 .Where(line => line.Length > 0));

        if (statement.Length == 0)
        {
            return "GO";
        }

        return NormalizeLegacyTableStatementTextForDiffDisplay(statement) + "\nGO";
    }

    private static string NormalizeLegacyTableStatementTextForDiffDisplay(string statement)
    {
        var normalized = Regex.Replace(statement, @"\s+", " ", RegexOptions.CultureInvariant).Trim();
        while (normalized.EndsWith(";", StringComparison.Ordinal))
        {
            normalized = normalized[..^1].TrimEnd();
        }

        if (TryFormatCreateTableLikeStatementForDiffDisplay(normalized, out var formatted))
        {
            return formatted;
        }

        return normalized;
    }

    private static bool TryFormatCreateTableLikeStatementForDiffDisplay(string statement, out string formatted)
    {
        formatted = string.Empty;

        if (!TryParseTableLikeStatementForDiffDisplay(statement, out var parts))
        {
            return false;
        }

        var lines = new List<string>(parts.Items.Count + 3)
        {
            parts.Prefix,
            "("
        };

        foreach (var item in parts.Items)
        {
            lines.Add(item.HasTrailingComma ? $"    {item.Text}," : $"    {item.Text}");
        }

        lines.Add(string.IsNullOrWhiteSpace(parts.Suffix) ? ")" : $") {parts.Suffix}");
        formatted = string.Join("\n", lines);
        return true;
    }

    private static bool TryParseTableLikeStatementForDiffDisplay(string statement, out TableLikeStatementParts parts)
    {
        parts = null!;

        if (!statement.StartsWith("CREATE TABLE", StringComparison.OrdinalIgnoreCase) &&
            !statement.StartsWith("CREATE TYPE", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var openParenIndex = statement.IndexOf('(');
        if (openParenIndex < 0)
        {
            return false;
        }

        var closeParenIndex = FindMatchingCloseParenthesis(statement, openParenIndex);
        if (closeParenIndex < 0)
        {
            return false;
        }

        var prefix = statement[..openParenIndex].TrimEnd();
        var body = statement[(openParenIndex + 1)..closeParenIndex];
        var suffix = statement[(closeParenIndex + 1)..].Trim();
        var rawItems = SplitTopLevelSqlList(body);
        var items = new List<TableLikeBodyItem>(rawItems.Count);
        for (var i = 0; i < rawItems.Count; i++)
        {
            var item = rawItems[i].Trim();
            if (item.Length == 0)
            {
                continue;
            }

            items.Add(new TableLikeBodyItem(item, i < rawItems.Count - 1));
        }

        if (items.Count == 0)
        {
            return false;
        }

        parts = new TableLikeStatementParts(prefix, items, suffix);
        return true;
    }

    private static int FindMatchingCloseParenthesis(string text, int openParenIndex)
    {
        var depth = 0;
        var inSingleQuotedString = false;

        for (var i = openParenIndex; i < text.Length; i++)
        {
            var ch = text[i];
            if (inSingleQuotedString)
            {
                if (ch == '\'')
                {
                    if (i + 1 < text.Length && text[i + 1] == '\'')
                    {
                        i++;
                    }
                    else
                    {
                        inSingleQuotedString = false;
                    }
                }

                continue;
            }

            if (ch == '\'')
            {
                inSingleQuotedString = true;
                continue;
            }

            if (ch == '(')
            {
                depth++;
                continue;
            }

            if (ch == ')')
            {
                depth--;
                if (depth == 0)
                {
                    return i;
                }
            }
        }

        return -1;
    }

    private static IReadOnlyList<string> SplitTopLevelSqlList(string body)
    {
        var items = new List<string>();
        var current = new StringBuilder();
        var depth = 0;
        var inSingleQuotedString = false;

        for (var i = 0; i < body.Length; i++)
        {
            var ch = body[i];
            if (inSingleQuotedString)
            {
                current.Append(ch);
                if (ch == '\'')
                {
                    if (i + 1 < body.Length && body[i + 1] == '\'')
                    {
                        current.Append(body[++i]);
                    }
                    else
                    {
                        inSingleQuotedString = false;
                    }
                }

                continue;
            }

            if (ch == '\'')
            {
                inSingleQuotedString = true;
                current.Append(ch);
                continue;
            }

            if (ch == '(')
            {
                depth++;
                current.Append(ch);
                continue;
            }

            if (ch == ')')
            {
                depth--;
                current.Append(ch);
                continue;
            }

            if (ch == ',' && depth == 0)
            {
                items.Add(current.ToString());
                current.Clear();
                continue;
            }

            current.Append(ch);
        }

        items.Add(current.ToString());
        return items;
    }

    private static string NormalizeLegacyTableStatementTextForComparison(string statement)
    {
        var normalized = NormalizeSqlStatementTokensForComparison(statement);
        string previous;
        do
        {
            previous = normalized;
            normalized = Regex.Replace(
                normalized,
                @"\bdefault\(\((?<expr>[-+]?\d+(?:\.\d+)?)\)\)",
                "default(${expr})",
                RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        }
        while (!string.Equals(previous, normalized, StringComparison.Ordinal));

        return normalized;
    }

    private static string NormalizeSqlStatementTokensForComparison(string statement)
    {
        var builder = new StringBuilder(statement.Length);
        var pendingSpace = false;

        for (var i = 0; i < statement.Length; i++)
        {
            var ch = statement[i];
            if (char.IsWhiteSpace(ch))
            {
                pendingSpace = builder.Length > 0;
                continue;
            }

            if (ch == '\'')
            {
                if (pendingSpace &&
                    builder.Length > 0 &&
                    builder[^1] is not '(' and not '.' and not ',' and not '=')
                {
                    builder.Append(' ');
                }

                pendingSpace = false;
                builder.Append(ch);
                while (++i < statement.Length)
                {
                    builder.Append(statement[i]);
                    if (statement[i] == '\'')
                    {
                        if (i + 1 < statement.Length && statement[i + 1] == '\'')
                        {
                            builder.Append(statement[++i]);
                            continue;
                        }

                        break;
                    }
                }

                continue;
            }

            if (ch == '[' || ch == '"')
            {
                var quote = ch;
                var start = i;
                var tokenBuilder = new StringBuilder();
                while (++i < statement.Length)
                {
                    var current = statement[i];
                    if (current == (quote == '[' ? ']' : '"'))
                    {
                        if (i + 1 < statement.Length && statement[i + 1] == current)
                        {
                            tokenBuilder.Append(current);
                            i++;
                            continue;
                        }

                        break;
                    }

                    tokenBuilder.Append(current);
                }

                if (pendingSpace &&
                    builder.Length > 0 &&
                    builder[^1] is not '(' and not '.' and not ',' and not '=')
                {
                    builder.Append(' ');
                }

                pendingSpace = false;
                builder.Append(tokenBuilder.ToString().ToLowerInvariant());
                continue;
            }

            if (ch == ';')
            {
                pendingSpace = false;
                continue;
            }

            if (ch is '(' or ')' or ',' or '=' or '.')
            {
                TrimTrailingSpaces(builder);
                builder.Append(ch);
                pendingSpace = false;
                continue;
            }

            if (pendingSpace &&
                builder.Length > 0 &&
                builder[^1] is not '(' and not '.' and not ',' and not '=')
            {
                builder.Append(' ');
            }

            pendingSpace = false;
            builder.Append(char.ToLowerInvariant(ch));
        }

        return builder.ToString().Trim();
    }

    private static string? GetFirstMeaningfulLine(IEnumerable<string> lines)
        => lines
            .Select(line => line.TrimStart())
            .FirstOrDefault(line => line.Length > 0 && !string.Equals(line, "GO", StringComparison.OrdinalIgnoreCase));

    private static string JoinBlockLines(IEnumerable<string> block)
        => string.Join("\n", block);

    private static bool IsProgrammableObjectTypeForHeaderCommentCompatibility(string? objectType)
        => string.Equals(objectType, "StoredProcedure", StringComparison.OrdinalIgnoreCase)
           || string.Equals(objectType, "View", StringComparison.OrdinalIgnoreCase)
           || string.Equals(objectType, "Function", StringComparison.OrdinalIgnoreCase)
           || string.Equals(objectType, "Trigger", StringComparison.OrdinalIgnoreCase);

    private static bool IsExtendedPropertyStatementLine(string line)
        => ExtendedPropertyStatementRegex.IsMatch(line);

    private static string NormalizeExtendedPropertyStatementForComparison(string statement)
    {
        var normalized = NormalizeExtendedPropertyStatementWhitespace(statement);
        if (TryNormalizeExtendedPropertyStatementArguments(normalized, out var canonical))
        {
            return canonical;
        }

        return normalized;
    }

    private static string NormalizeExtendedPropertyStatementWhitespace(string statement)
    {
        var builder = new StringBuilder(statement.Length);
        var inSingleQuotedString = false;
        var inBracketedIdentifier = false;
        var pendingSpace = false;

        for (var i = 0; i < statement.Length; i++)
        {
            var ch = statement[i];
            if (inSingleQuotedString)
            {
                builder.Append(ch);
                if (ch == '\'')
                {
                    if (i + 1 < statement.Length && statement[i + 1] == '\'')
                    {
                        builder.Append(statement[i + 1]);
                        i++;
                    }
                    else
                    {
                        inSingleQuotedString = false;
                    }
                }

                continue;
            }

            if (inBracketedIdentifier)
            {
                builder.Append(ch);
                if (ch == ']')
                {
                    inBracketedIdentifier = false;
                }

                continue;
            }

            if (ch == '\'')
            {
                if (pendingSpace && builder.Length > 0 && builder[^1] is not '(' and not ',')
                {
                    builder.Append(' ');
                }

                pendingSpace = false;
                inSingleQuotedString = true;
                builder.Append(ch);
                continue;
            }

            if (ch == '[')
            {
                if (pendingSpace && builder.Length > 0 && builder[^1] is not '(' and not ',')
                {
                    builder.Append(' ');
                }

                pendingSpace = false;
                inBracketedIdentifier = true;
                builder.Append(ch);
                continue;
            }

            if (char.IsWhiteSpace(ch))
            {
                pendingSpace = builder.Length > 0;
                continue;
            }

            if (ch is ',' or '(' or ')')
            {
                TrimTrailingSpaces(builder);
                builder.Append(ch);
                pendingSpace = false;
                continue;
            }

            if (pendingSpace && builder.Length > 0 && builder[^1] is not '(' and not ',')
            {
                builder.Append(' ');
            }

            pendingSpace = false;
            builder.Append(ch);
        }

        var normalized = builder.ToString().Trim();
        return Regex.Replace(
            normalized,
            @"(?i)^EXEC(?:UTE)?\s+SYS\.SP_ADDEXTENDEDPROPERTY\b",
            "EXEC sp_addextendedproperty",
            RegexOptions.CultureInvariant);
    }

    private static bool TryNormalizeExtendedPropertyStatementArguments(string statement, out string canonical)
    {
        const string parameterOrderList = "name,value,level0type,level0name,level1type,level1name,level2type,level2name";
        var prefixMatch = Regex.Match(
            statement,
            @"^\s*EXEC(?:UTE)?\s+(?:sys\.)?sp_addextendedproperty\b",
            RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        if (!prefixMatch.Success)
        {
            canonical = string.Empty;
            return false;
        }

        var argumentsText = statement[prefixMatch.Length..].Trim();
        if (argumentsText.Length == 0)
        {
            canonical = "EXEC sp_addextendedproperty";
            return true;
        }

        var parameterOrder = parameterOrderList.Split(',');
        var parameterValues = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var nextPositionalIndex = 0;

        foreach (var token in SplitSqlArgumentList(argumentsText))
        {
            var trimmedToken = token.Trim();
            if (trimmedToken.Length == 0)
            {
                canonical = string.Empty;
                return false;
            }

            var equalsIndex = FindTopLevelEquals(trimmedToken);
            if (equalsIndex >= 0)
            {
                var parameterName = trimmedToken[..equalsIndex].Trim().TrimStart('@');
                var parameterValue = trimmedToken[(equalsIndex + 1)..].Trim();
                if (parameterName.Length == 0 ||
                    parameterValue.Length == 0 ||
                    !parameterOrder.Contains(parameterName, StringComparer.OrdinalIgnoreCase))
                {
                    canonical = string.Empty;
                    return false;
                }

                parameterValues[parameterName] = NormalizeExtendedPropertyArgumentValueForComparison(parameterName, parameterValue);
                continue;
            }

            while (nextPositionalIndex < parameterOrder.Length &&
                   parameterValues.ContainsKey(parameterOrder[nextPositionalIndex]))
            {
                nextPositionalIndex++;
            }

            if (nextPositionalIndex >= parameterOrder.Length)
            {
                canonical = string.Empty;
                return false;
            }

            parameterValues[parameterOrder[nextPositionalIndex]] =
                NormalizeExtendedPropertyArgumentValueForComparison(parameterOrder[nextPositionalIndex], trimmedToken);
            nextPositionalIndex++;
        }

        var canonicalArguments = parameterOrder
            .Select(name => parameterValues.TryGetValue(name, out var value) ? value : "NULL")
            .ToArray();
        canonical = "EXEC sp_addextendedproperty " + string.Join(", ", canonicalArguments);
        return true;
    }

    private static IEnumerable<string> SplitSqlArgumentList(string argumentsText)
    {
        var current = new StringBuilder(argumentsText.Length);
        var inSingleQuotedString = false;
        var inBracketedIdentifier = false;
        var parenthesisDepth = 0;

        for (var i = 0; i < argumentsText.Length; i++)
        {
            var ch = argumentsText[i];

            if (inSingleQuotedString)
            {
                current.Append(ch);
                if (ch == '\'')
                {
                    if (i + 1 < argumentsText.Length && argumentsText[i + 1] == '\'')
                    {
                        current.Append(argumentsText[i + 1]);
                        i++;
                    }
                    else
                    {
                        inSingleQuotedString = false;
                    }
                }

                continue;
            }

            if (inBracketedIdentifier)
            {
                current.Append(ch);
                if (ch == ']')
                {
                    inBracketedIdentifier = false;
                }

                continue;
            }

            switch (ch)
            {
                case '\'':
                    inSingleQuotedString = true;
                    current.Append(ch);
                    break;
                case '[':
                    inBracketedIdentifier = true;
                    current.Append(ch);
                    break;
                case '(':
                    parenthesisDepth++;
                    current.Append(ch);
                    break;
                case ')':
                    if (parenthesisDepth > 0)
                    {
                        parenthesisDepth--;
                    }

                    current.Append(ch);
                    break;
                case ',' when parenthesisDepth == 0:
                    yield return current.ToString();
                    current.Clear();
                    break;
                default:
                    current.Append(ch);
                    break;
            }
        }

        if (current.Length > 0)
        {
            yield return current.ToString();
        }
    }

    private static int FindTopLevelEquals(string text)
    {
        var inSingleQuotedString = false;
        var inBracketedIdentifier = false;

        for (var i = 0; i < text.Length; i++)
        {
            var ch = text[i];
            if (inSingleQuotedString)
            {
                if (ch == '\'')
                {
                    if (i + 1 < text.Length && text[i + 1] == '\'')
                    {
                        i++;
                    }
                    else
                    {
                        inSingleQuotedString = false;
                    }
                }

                continue;
            }

            if (inBracketedIdentifier)
            {
                if (ch == ']')
                {
                    inBracketedIdentifier = false;
                }

                continue;
            }

            if (ch == '\'')
            {
                inSingleQuotedString = true;
                continue;
            }

            if (ch == '[')
            {
                inBracketedIdentifier = true;
                continue;
            }

            if (ch == '=')
            {
                return i;
            }
        }

        return -1;
    }

    private static string NormalizeExtendedPropertyArgumentValueForComparison(string parameterName, string value)
    {
        var trimmed = value.Trim();
        if (string.Equals(trimmed, "NULL", StringComparison.OrdinalIgnoreCase))
        {
            return "NULL";
        }

        if (!string.Equals(parameterName, "value", StringComparison.OrdinalIgnoreCase) &&
            trimmed.Length >= 3 &&
            (trimmed[0] == 'N' || trimmed[0] == 'n') &&
            trimmed[1] == '\'' &&
            trimmed[^1] == '\'')
        {
            return trimmed[1..];
        }

        return trimmed;
    }

    private static void TrimTrailingSpaces(StringBuilder builder)
    {
        while (builder.Length > 0 && builder[^1] == ' ')
        {
            builder.Length--;
        }
    }

    private static string StripTrailingSemicolon(string line)
        => line.EndsWith(';') ? line[..^1] : line;

    private static (int StatementEndExclusive, int ConsumedEndExclusive)? TryFindInsertValuesStatementRange(
        string script,
        int start)
    {
        var valuesKeywordIndex = script.IndexOf("VALUES", start, StringComparison.OrdinalIgnoreCase);
        if (valuesKeywordIndex < 0)
        {
            return null;
        }

        var valuesOpenParenIndex = script.IndexOf('(', valuesKeywordIndex);
        if (valuesOpenParenIndex < 0)
        {
            return null;
        }

        var inSingleQuotedString = false;
        var inBracketedIdentifier = false;
        var parenDepth = 0;
        for (var i = valuesOpenParenIndex; i < script.Length; i++)
        {
            var ch = script[i];
            if (inSingleQuotedString)
            {
                if (ch == '\'')
                {
                    if (i + 1 < script.Length && script[i + 1] == '\'')
                    {
                        i++;
                    }
                    else
                    {
                        inSingleQuotedString = false;
                    }
                }

                continue;
            }

            if (inBracketedIdentifier)
            {
                if (ch == ']')
                {
                    inBracketedIdentifier = false;
                }

                continue;
            }

            if (ch == '[')
            {
                inBracketedIdentifier = true;
                continue;
            }

            if (ch == '\'')
            {
                inSingleQuotedString = true;
                continue;
            }

            if (ch == '(')
            {
                parenDepth++;
                continue;
            }

            if (ch != ')')
            {
                continue;
            }

            parenDepth--;
            if (parenDepth != 0)
            {
                continue;
            }

            var statementEndExclusive = i + 1;
            var consumedEndExclusive = statementEndExclusive;
            while (consumedEndExclusive < script.Length && script[consumedEndExclusive] is ' ' or '\t')
            {
                consumedEndExclusive++;
            }

            if (consumedEndExclusive < script.Length && script[consumedEndExclusive] == ';')
            {
                consumedEndExclusive++;
            }
            else
            {
                consumedEndExclusive = statementEndExclusive;
            }

            if (consumedEndExclusive < script.Length && script[consumedEndExclusive] == '\n')
            {
                consumedEndExclusive++;
            }

            return (statementEndExclusive, consumedEndExclusive);
        }

        return null;
    }

    private static string NormalizeLegacyTableDataInsertStatement(string line)
    {
        var valuesKeywordIndex = line.IndexOf("VALUES", StringComparison.OrdinalIgnoreCase);
        if (valuesKeywordIndex < 0)
        {
            return line;
        }

        var valuesOpenParenIndex = line.IndexOf('(', valuesKeywordIndex);
        if (valuesOpenParenIndex < 0)
        {
            return line;
        }

        var builder = new StringBuilder(line.Length);
        var inSingleQuotedString = false;
        var inBracketedIdentifier = false;
        var parenDepth = 0;

        for (var i = 0; i < line.Length; i++)
        {
            var ch = line[i];
            if (inSingleQuotedString)
            {
                builder.Append(ch);
                if (ch == '\'')
                {
                    if (i + 1 < line.Length && line[i + 1] == '\'')
                    {
                        builder.Append(line[i + 1]);
                        i++;
                    }
                    else
                    {
                        inSingleQuotedString = false;
                    }
                }

                continue;
            }

            if (inBracketedIdentifier)
            {
                builder.Append(ch);
                if (ch == ']')
                {
                    inBracketedIdentifier = false;
                }

                continue;
            }

            if (ch == '[')
            {
                inBracketedIdentifier = true;
                builder.Append(ch);
                continue;
            }

            if (ch == '(')
            {
                parenDepth++;
                builder.Append(ch);
                continue;
            }

            if (ch == ')')
            {
                parenDepth--;
                builder.Append(ch);
                continue;
            }

            if (ch == '\'')
            {
                inSingleQuotedString = true;
                builder.Append(ch);
                continue;
            }

            if ((ch == 'N' || ch == 'n') &&
                i > valuesOpenParenIndex &&
                parenDepth == 1 &&
                i + 1 < line.Length &&
                line[i + 1] == '\'' &&
                IsTopLevelInsertStringPrefixBoundary(line, valuesOpenParenIndex, i))
            {
                continue;
            }

            builder.Append(ch);
        }

        return builder.ToString();
    }

    private static bool IsTopLevelInsertStringPrefixBoundary(string line, int valuesOpenParenIndex, int prefixIndex)
    {
        for (var i = prefixIndex - 1; i > valuesOpenParenIndex; i--)
        {
            var ch = line[i];
            if (char.IsWhiteSpace(ch))
            {
                continue;
            }

            return ch is '(' or ',';
        }

        return true;
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
        string FullPath,
        string? CompatibleOmittedTextImageOnDataSpaceName = null)
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
