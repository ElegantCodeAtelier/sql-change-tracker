using System.Collections.Generic;

namespace SqlChangeTracker.Config;

internal sealed record ConfigShowResult(
    string Command,
    string ProjectDir,
    string ConfigPath,
    SqlctConfig Config,
    CompatibilityScan Compatibility);

internal sealed record InitResult(
    string Command,
    string ProjectDir,
    IReadOnlyList<string> Created,
    IReadOnlyList<string> Skipped);

internal sealed record ConfigValidateResult(
    string Command,
    string ProjectDir,
    bool Valid,
    IReadOnlyList<ConfigError> Errors);

internal sealed record ConfigResult(
    string Command,
    string ProjectDir,
    bool Valid,
    IReadOnlyList<ConfigError> Errors,
    string ConfigPath,
    SqlctConfig Config,
    CompatibilityScan Compatibility);

internal sealed record CommandWarning(
    string Code,
    string Message);

internal sealed record StatusSummary(
    int Added,
    int Changed,
    int Deleted);

internal sealed record StatusObject(
    string Name,
    string Type,
    string Change);

internal sealed record StatusResult(
    string Command,
    string ProjectDir,
    string Target,
    StatusSummary Summary,
    IReadOnlyList<StatusObject> Objects,
    IReadOnlyList<CommandWarning> Warnings);

internal sealed record DiffResult(
    string Command,
    string ProjectDir,
    string Target,
    string? Object,
    string Diff,
    IReadOnlyList<CommandWarning> Warnings);

internal sealed record PullSummary(
    int Created,
    int Updated,
    int Deleted,
    int Unchanged);

internal sealed record PullObject(
    string Name,
    string Type,
    string Change,
    string Path);

internal sealed record PullResult(
    string Command,
    string ProjectDir,
    PullSummary Summary,
    IReadOnlyList<PullObject> Objects,
    IReadOnlyList<CommandWarning> Warnings);

internal sealed record ErrorResult(
    string Command,
    ErrorInfo Error);

internal sealed record ErrorInfo(
    string Code,
    string Message,
    string? File = null,
    string? Detail = null,
    string? Hint = null);

internal sealed record ConfigError(
    string Code,
    string File,
    string Message);

internal sealed record FolderMapEntry(
    string Key,
    string Value);
