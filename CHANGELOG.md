# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Changed
- Replace `sqlct.config.json` with `sqlct.config.yaml` as the project configuration file; config files are now written in YAML with a header comment block containing a tool introduction, installation instructions, usage instructions, and a link to the GitHub repository.

### Removed
- `sqlct.config.json` is no longer created by `sqlct init`; existing users should rename the file to `sqlct.config.yaml` and run `sqlct config` to rewrite it in the new format.

## [0.3.0] - 2026-04-12

### Fixed
- Script schema-level permissions after schema creation and before schema extended properties.
- Match legacy non-canonical schema-less object filenames to the scripted object name when the canonical name requires percent escaping.
- Allow bare `--object` selectors with dotted schema-less names (for example assemblies) to resolve correctly during targeted `status`, `diff`, and `pull`.
- Treat equivalent queue option formatting, explicit default `ON [PRIMARY]`, and disabled default activation as compatible during comparison.
- Treat equivalent role-membership statements written as `EXEC sp_addrolemember ...` or `ALTER ROLE ... ADD MEMBER ...` as compatible during comparison.
- Treat legacy Service Broker message-type validation synonyms and equivalent contract/service body formatting and item ordering as compatible during comparison.
- Treat equivalent `TableData` scripts as compatible during comparison when the normalized `INSERT` statements differ only by row ordering within the same contiguous data block.
- Treat equivalent `TableData` scripts as compatible during comparison when single-row `INSERT` column lists and corresponding value tuples are reordered consistently.
- Treat equivalent `Table` scripts as compatible during comparison when post-create statement packages differ only by ordering after the base `CREATE TABLE` block.
- Treat equivalent legacy `Table` statement formatting as compatible during comparison when normalized table definitions, post-create table statements, and persisted option values are otherwise identical.
- Treat equivalent legacy `UserDefinedType` `CREATE TYPE` formatting as compatible during comparison when the normalized type definition is otherwise identical.
- Treat omitted `TEXTIMAGE_ON` on `Table` scripts as compatible during comparison only when DB metadata shows the table LOB data space matches the current default data space.
- Treat equivalent extended-property blocks as compatible during comparison when the normalized `sp_addextendedproperty` statements differ only by ordering, argument spacing, or named-vs-positional argument forms within the same contiguous block.
- Treat equivalent extended-property blocks as compatible during comparison when the normalized `sp_addextendedproperty` statements differ only by ordering, argument spacing, named-vs-positional forms, or top-level `N'...'` string literal prefixes.
- Treat leading SSMS-generated banner comments on programmable objects as compatible during comparison.
- Treat redundant empty or otherwise no-op `GO` batches as compatible during comparison.
- Treat an omitted terminal `GO` after the final batch as compatible during comparison with an explicit final `GO`.
- Keep `diff` output readable by rendering compatible `Table` and `UserDefinedType` changes from readable script text instead of opaque comparison-normalized text.
- Keep normal `diff` output readable for non-table objects such as users and roles by rendering permission changes from readable script text instead of lowercase comparison-normalized tokens.
- Keep readable `diff` output for `Table` and table-valued `UserDefinedType` bodies at per-entry granularity instead of collapsing the entire body into one changed line.
- Align readable `Table` and table-valued `UserDefinedType` diffs by individual body entries so a single changed column or inline constraint does not mark the entire body as changed.
- Exclude SSMS database-diagram support stored procedures from discovery and scripting even when SQL Server does not mark them as system-shipped.
- Exclude SSMS database-diagram support table and function objects from discovery and scripting even when SQL Server does not mark them as system-shipped.
- Script `TYPE::` permissions for scalar and table-valued `UserDefinedType` objects.
- Script database-level permissions granted directly to `Role` and `User` principals, and emit `CREATE USER ... WITHOUT LOGIN` when no server-login metadata is available.
- Treat equivalent contiguous permission statement ordering as compatible during comparison.
- Treat legacy standalone table-level inline `PRIMARY KEY` and `UNIQUE` constraints as compatible during comparison with canonical post-create key-constraint statements.
- Treat legacy CLR table-valued function return-column collation clauses as compatible during comparison when SQL Server ignores them in the effective return shape.
- Treat legacy explicit `NULL` tokens on CLR table-valued function return columns as compatible during comparison and preserve them during compatibility reconciliation when the rest of the definition matches.
- Treat equivalent legacy `Assembly` scripts as compatible during comparison when they differ only by banner comments, wrapped or case-varied hex payload formatting, `PERMISSION_SET` spacing, or quoted versus bracketed `ADD FILE` names.
- Rewrite programmable-object declaration lines to the current metadata name when SQL Server stores stale module text after an object rename.
- Fix table-trigger scripting after the declaration rewrite change by resolving trigger schema without referencing a non-existent `sys.triggers.schema_id` column.
- Trailing semicolon differences on `INSERT` statement lines in data scripts are now suppressed during comparison normalization; scripts emitted with and without statement terminators compare as compatible (#47).
- Legacy `TableData` scripts now compare as compatible when they differ from canonical output only by `SET IDENTITY_INSERT` semicolons or top-level `N'...'` string literal prefixes, including inside multi-line `INSERT ... VALUES (...)` statements.
- Empty separator lines are now ignored during `status` and `diff`, and whitespace-only separator lines compare as compatible after normalization.
- Preserve reference banner-comment formatting and module-declaration identifier quoting during programmable-object compatibility reconciliation.
- Preserve compatible computed-column arithmetic grouping parentheses during table compatibility reconciliation.

### Added
- `sqlct init` now scans the target project directory for existing `Data/*.sql` files, extracts table names from the file names, and proposes a `trackedTables` list: in interactive mode the user is prompted to confirm before the tables are written to config; in non-interactive mode (with `--project-dir`) the tables are added automatically.
- Discover and script SQL CLR scalar functions as `Function` objects.
- Discover and script SQL CLR table-valued functions as `Function` objects.
- Discover and script SQL CLR stored procedures as `StoredProcedure` objects.
- Discover and script SQL CLR aggregates as `Aggregate` objects in `Functions/`.
- Discover and script built-in `dbo` as a `Schema` object when it has explicit schema permissions or schema-level extended properties, without emitting `CREATE SCHEMA`.
- `sqlct init` now prompts interactively for connection details (server, database, auth, credentials, trust-server-certificate) when run without flags in a new project directory (#36).
- Connection flags (`--server`, `--database`, `--auth`, `--user`, `--password`, `--trust-server-certificate`) for non-interactive/scripted `init` use (#36).
- `--skip-connection-test` flag for `sqlct init` to bypass the connection test step (#36).
- After `sqlct init`, a connection test is attempted before creating any files, reporting success or failure with troubleshooting tips; in interactive mode, a failed test prompts the user to proceed or abort (#36).
- Next-steps suggestions are printed after `sqlct init` to guide users toward `pull`, `status`, and `diff` (#36).
- Add `--object <selector>` to `sqlct pull` for exact-match filtering using the same selector forms as `diff --object` (#35).
- Add `--filter <pattern>` to `sqlct pull` for regex-based filtering; multiple patterns may be provided and matching is case-insensitive (#35).
- Add `--normalized-diff` to `sqlct diff` to render comparison-normalized diff text for debugging while preserving readable diff output by default.
- Add `--filter <pattern>` to `sqlct diff` for regex-based filtering; without `--object` filters the output to matching objects, with `--object` additionally constrains the single-object result (#35).
- SQL Authentication support: set `database.auth` to `"sql"` and supply `database.user` (and optionally `database.password`) in `sqlct.config.json` to connect using SQL Server Authentication (#30).
- Support active object type `Assembly`, with deterministic scripting to `Assemblies/*.sql` for user-defined SQL Server assemblies.
- Support additional active object types: `TableType`, `XmlSchemaCollection`, `MessageType`, `Contract`, `Queue`, `Service`, `Route`, `EventNotification`, `ServiceBinding`, `FullTextCatalog`, `FullTextStoplist`, and `SearchPropertyList`.
- Script standalone user-created table statistics as deterministic post-create table statements, including filtered, effective sampling, persisted-sample, incremental, and auto-drop metadata when available.
- Script persisted key and index storage options such as fill factor, pad index, duplicate-key handling, and row/page locking when those options differ from defaults.
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

[Unreleased]: https://github.com/ElegantCodeAtelier/sql-change-tracker/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/ElegantCodeAtelier/sql-change-tracker/compare/v0.2.1...v0.3.0
[0.2.1]: https://github.com/ElegantCodeAtelier/sql-change-tracker/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/ElegantCodeAtelier/sql-change-tracker/releases/tag/v0.2.0
[0.1.0]: https://github.com/ElegantCodeAtelier/sql-change-tracker/releases/tag/v0.1.0
