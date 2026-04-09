# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Fixed
- Trailing semicolon differences on `INSERT` statement lines in data scripts are now suppressed during comparison normalization; scripts emitted with and without statement terminators compare as compatible (#47).
- Legacy `TableData` scripts now compare as compatible when they differ from canonical output only by `SET IDENTITY_INSERT` semicolons or top-level `N'...'` string literal prefixes.
- Whitespace-only separator lines now compare as compatible with empty blank lines during `status` and `diff`.
- Preserve reference banner-comment formatting and module-declaration identifier quoting during programmable-object compatibility reconciliation.
- Preserve compatible computed-column arithmetic grouping parentheses during table compatibility reconciliation.

### Added
- `sqlct init` now prompts interactively for connection details (server, database, auth, credentials, trust-server-certificate) when run without flags in a new project directory (#36).
- Connection flags (`--server`, `--database`, `--auth`, `--user`, `--password`, `--trust-server-certificate`) for non-interactive/scripted `init` use (#36).
- `--skip-connection-test` flag for `sqlct init` to bypass the connection test step (#36).
- After `sqlct init`, a connection test is attempted before creating any files, reporting success or failure with troubleshooting tips; in interactive mode, a failed test prompts the user to proceed or abort (#36).
- Next-steps suggestions are printed after `sqlct init` to guide users toward `pull`, `status`, and `diff` (#36).
- Add `--object <selector>` to `sqlct pull` for exact-match filtering using the same selector forms as `diff --object` (#35).
- Add `--filter <pattern>` to `sqlct pull` for regex-based filtering; multiple patterns may be provided and matching is case-insensitive (#35).
- Add `--filter <pattern>` to `sqlct diff` for regex-based filtering; without `--object` filters the output to matching objects, with `--object` additionally constrains the single-object result (#35).
- SQL Authentication support: set `database.auth` to `"sql"` and supply `database.user` (and optionally `database.password`) in `sqlct.config.json` to connect using SQL Server Authentication (#30).
- Support active object type `Assembly`, with deterministic scripting to `Assemblies/*.sql` for user-defined SQL Server assemblies.
- Support additional active object types: `TableType`, `XmlSchemaCollection`, `MessageType`, `Contract`, `Queue`, `Service`, `Route`, `EventNotification`, `ServiceBinding`, `FullTextCatalog`, `FullTextStoplist`, and `SearchPropertyList`.
- Script standalone user-created table statistics as deterministic post-create table statements, including filtered, effective sampling, persisted-sample, incremental, and auto-drop metadata when available.
- Add `--object <pattern>` to `sqlct data track` and `sqlct data untrack` as a flag alias for the positional pattern argument.
- Add `--filter <regex>` to `sqlct data track` and `sqlct data untrack` for regex-based table matching; matched case-insensitively against the full `schema.table` display name. Exactly one of the positional pattern, `--object`, or `--filter` must be provided; combining any two returns exit code 2.
- `sqlct diff` now uses a chunked diff format: only changed segments and configurable surrounding context lines are shown instead of the full file. Use `--context <N>` to control the number of context lines (default: 3) (#39).

### Changed
- In `diff --object` mode, database discovery and scripting are now limited to the selector-matching candidate set instead of scanning the full active object set, improving performance for targeted diffs (#28).
- In `pull --object` mode, database discovery and scripting are now limited to the selector-matching candidate object set instead of scanning the full active object set (#38).
- In `diff --filter` mode (without `--object`), database scripting is now limited to objects whose display name matches at least one pattern, avoiding unnecessary reads (#38).
- In `pull --filter` mode, database scripting is now limited to objects whose display name matches at least one pattern, avoiding unnecessary reads (#38).
- Collapse table-valued types into `UserDefinedType`, writing all UDT scripts to `Types/User-defined Data Types/`; `TableType:` selectors and `Types/Table Types/` are no longer supported.
- Extend scripting for the newly supported securables to emit permissions and extended properties where SQL Server exposes them, with platform-limited cases documented in specs and package docs.

### Fixed
- Close additional `SqlDataReader` scopes in object-type scripting paths to avoid `There is already an open DataReader associated with this Connection` failures during `status`, `diff`, and `pull`.

## [0.2.1] - 2026-04-07

### Added
- Support for .NET 8 alongside .NET 10 (multi-targeted); CI validates both frameworks (#16).

### Fixed
- Ensure `SqlDataReader` instances are closed after use in `SqlServerScripter` to prevent "There is already an open DataReader" errors (#25).

## [0.2.0] - 2026-04-07

### Added
- Support for additional active object types: `Sequence`, `Schema`, `Role`, `User`, `Synonym`, `UserDefinedType`, `PartitionFunction`, `PartitionScheme` (#4).
- Add `sqlct data track`, `sqlct data untrack`, and `sqlct data list` commands for managing explicit tracked tables used for data scripting (#15).
- Add tracked-table `TableData` scripting to `status`, `diff`, and `pull`, including `Data/Schema.Table_Data.sql` output and the `data:schema.name` diff selector (#15).
- Parallelism option (`options.parallelism`) for database introspection; defaults to processor count when set to `0` (#9).
- Add a progress bar for long-running commands (#3).
- Add --version / -v to print the installed sqlct semantic version (#2).

### Changed
- Change `status` and `pull` output to report schema and data summaries separately when tracked-table data scripting is configured (#15).

### Removed
- Remove unused `options.orderByDependencies` config option; legacy configs containing this field continue to load without error but the field is stripped on save.

### Fixed
- Parameter-level MS_Description extended properties are not scripted for procedures and functions (#5).
- `sqlct config` should fail with a clear error if executed in a non-initialized project directory (#1).

## [0.1.0] - 2026-03-22

### Added
- `sqlct init` command — scaffolds a new schema-folder project with `sqlct.config.json` and folder structure.
- `sqlct config` command — validates and displays the current project configuration.
- `sqlct status` command — compares the live database against the local schema folder and reports added, changed, and deleted objects.
- `sqlct diff` command — shows unified diffs for individual objects or all changed objects.
- `sqlct pull` command — reconciles the local schema folder from the live database (create, update, delete files).
- Support for active object types: `Table`, `View`, `StoredProcedure`, `Function`.
- Human-readable and JSON output modes for `status`, `diff`, and `pull`.
- Deterministic script ordering (dependency-aware where applicable).
- Extended property scripting support for database objects.
- Exit codes aligned with spec: `0` (no diffs), `1` (diffs present), `2`/`3`/`4` (failure categories).
- NuGet CI publish workflow triggered on version tag push.

[Unreleased]: https://github.com/ElegantCodeAtelier/sql-change-tracker/compare/v0.2.1...HEAD
[0.2.1]: https://github.com/ElegantCodeAtelier/sql-change-tracker/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/ElegantCodeAtelier/sql-change-tracker/releases/tag/v0.2.0
[0.1.0]: https://github.com/ElegantCodeAtelier/sql-change-tracker/releases/tag/v0.1.0
