# CLI

Status: draft
Last updated: 2026-04-08

## Command Overview
Binary: sqlct.

Global flags:
- --version, -v: print the installed sqlct semantic version and exit.
- --project-dir <path>: project directory location (default current working directory).
- --verbose.
- --json: machine-readable output.
- --no-progress: disable the progress spinner (see Flag Semantics).

## Version Flag
`
sqlct --version
sqlct -v
`
Behavior:
- Prints the installed semantic version string (e.g. `0.1.0`) to stdout and exits with code 0.
- Works without any project files or initialization.
- Output is stable and suitable for scripting/CI use.
- No JSON output is produced for this flag.

## CLI Shape (Selected)
Use git-style verbs: short, task-oriented commands with clear intent.

## Commands
- init
- config
- data
- status
- diff
- pull

## v1 Scope
- Active schema object types: `Assembly`, `Table`, `View`, `StoredProcedure`, `Function`, `Sequence`, `Schema`, `Role`, `User`, `Synonym`, `UserDefinedType`, `TableType`, `XmlSchemaCollection`, `PartitionFunction`, `PartitionScheme`, `MessageType`, `Contract`, `Queue`, `Service`, `Route`, `EventNotification`, `ServiceBinding`, `FullTextCatalog`, `FullTextStoplist`, `SearchPropertyList`.
- `status`, `diff`, and `pull` process the active schema object types.
- When `data.trackedTables` is configured, `status`, `diff`, and `pull` also process `TableData` artifacts for those explicit tracked tables.
- Unsupported object types discovered in DB introspection are skipped with warnings.
- Include/exclude filters are deferred to vNext.
- Comparison ignore options are deferred to vNext.

## Flag Semantics
Common flag behavior across commands.

### --project-dir
- Overrides the project directory for the current command.
- `--project-dir` values wrapped in matching single or double quotes MUST be unwrapped before path resolution.
- On Windows, stray double-quote artifacts introduced by native argument parsing for quoted paths with a trailing backslash MUST be ignored before path resolution.
- When omitted, default to current working directory.
- For `init`, if omitted, prompt for confirmation before initializing the current working directory.

### --target <db|folder>
- `db`: compare DB against folder; results show what would be pulled.
- `folder`: compare folder against DB; results show what would be committed in folder state.
- Default: `db`.

### --object <selector>
- Supported selector forms:
  - `schema.name` for schema-scoped active object types.
  - `name` for schema-less active object types.
  - `type:name` for explicit schema-less selection.
  - `type:schema.name` for explicit schema-scoped selection.
  - `data:schema.name` for tracked table-data scripts.
- Bare `name` selectors search only schema-less active object types.
- Bare-name or `schema.name` collisions across object types return an error that instructs the user to use the type-qualified form.

### --filter <pattern>
- Patterns are .NET regular expressions matched against the full object display name (full-string match, not substring).
- Display name format: `schema.name` (e.g. `dbo.Customer`) or bare `name` for schema-less types (e.g. `AppReader`).
- Multiple `--filter` values may be provided; an object is included if any pattern matches (OR semantics).
- Matching is case-insensitive.
- Examples:
  - `dbo\.Customer` — exact match on `dbo.Customer`.
  - `dbo\..*` — all objects in the `dbo` schema.
  - `.*Order.*` — all objects with "Order" anywhere in the name.
  - `AppReader` — schema-less object named `AppReader`.

### --no-progress
- Disables the progress spinner for `status`, `diff`, and `pull`.
- The progress spinner is enabled by default in interactive terminals.
- The progress spinner is automatically suppressed when:
  - `--json` is used.
  - `--no-progress` is used.
  - The output is not an interactive terminal (e.g., redirected output, CI pipelines).

### init
Initialize project configuration and schema folder structure.
`
sqlct init [--project-dir <path>]
`
Behavior:
- Initialize configuration in an empty directory or existing project structure.
- Requires only schema directory context.
- If `--project-dir` is omitted, assume current working directory and prompt for confirmation.
- On first-time setup (when `sqlct.config.json` does not yet exist in the target directory), prompt step-by-step for connection details: server, database, auth mode, credentials (when auth is `sql`), and trust-server-certificate.
- If `sqlct.config.json` already exists in the target directory, exit with an error and suggest removing the file to re-initialize the project.
- Auth accepts `integrated` (Windows/Entra Authentication, default) or `sql` (SQL Server Authentication).
- Password input during interactive prompts is masked.
- When connection details are collected, attempt a connection test **before** creating any project files (5-second timeout).
- If the connection test fails, print troubleshooting hints and prompt `"Proceed anyway? [y/N]:"`. If declined, exit without creating any files. If confirmed, proceed to create the directory structure and write config.
- After init completes, print context-aware next-steps: `pull`, `status`, `diff` on success; edit config and run `sqlct config` on failure.
- Exit codes:
  - `0` success.
  - `2` invalid config (e.g., `sqlct.config.json` already exists, or connection test declined).


Parse, validate, and write configuration from the project directory.
`
sqlct config [--project-dir <path>]
`
Behavior:
- Read project configuration and schema metadata files from project directory.
- Validate required files and required fields.
- Rewrite normalized configuration outputs if needed.
- Print a summary of parsed values and validation status.

### data
Manage tracked tables for selective data scripting.

Tracked-table rules:
- `data.trackedTables` stores explicit tracked tables in `schema.table` form.
- Tracked tables MUST be unique case-insensitively and persisted in stable sorted order.
- Top-level `status`, `diff`, and `pull` process `TableData` only for tables explicitly listed in `data.trackedTables`.

Selector forms for `track` and `untrack`:
- Positional `<pattern>` or `--object <pattern>`: glob-style table selector.
  - exact table: `schema.table`
  - schema wildcard: `schema.*`
  - name wildcard: `*.table`
  - Matching is case-insensitive.
  - Bare table names without schema are not supported.
- `--filter <regex>`: .NET regular expression matched case-insensitively against the full display name (`schema.table`).
  - Use `.*` for substring matching.
  - An invalid regular expression returns exit code 2 (invalid config).
- Exactly one of: positional `<pattern>`, `--object`, or `--filter` must be provided.
  - Combining any two or omitting all three returns exit code 2 (invalid config).

#### track
`
sqlct data track [<pattern>] [--object <pattern>] [--filter <regex>] [--project-dir <path>]
`

Behavior:
- Match user tables in the current database against the provided selector.
- List matched tables in stable sorted order before any config change is made.
- When the selector matches no user tables, return success with an informational message and leave config unchanged.
- When one or more tables match, prompt for confirmation before updating `sqlct.config.json`.
- If confirmed, add matched tables to `data.trackedTables` in `sqlct.config.json` as explicit `schema.table` entries.
- Normalize ordering and deduplicate tracked tables case-insensitively.
- If confirmation is declined, return success with an informational message and leave config unchanged.
- If confirmation is required but cannot be obtained in non-interactive execution, return execution failure.
- Do not perform synchronization by itself.

#### untrack
`
sqlct data untrack [<pattern>] [--object <pattern>] [--filter <regex>] [--project-dir <path>]
`

Behavior:
- Match against existing tracked entries using the provided selector.
- List matched tracked tables in stable sorted order before any config change is made.
- When the selector matches no tracked tables, return success with an informational message and leave config unchanged.
- When one or more tracked tables match, prompt for confirmation before updating `sqlct.config.json`.
- If confirmed, remove matched tracked tables from `data.trackedTables` in `sqlct.config.json`.
- If confirmation is declined, return success with an informational message and leave config unchanged.
- If confirmation is required but cannot be obtained in non-interactive execution, return execution failure.
- Do not perform synchronization by itself.

#### list
`
sqlct data list [--project-dir <path>]
`

Behavior:
- Show tracked tables from config in stable sorted order.
- Do not connect to the database.

### status
Show object-level differences.
`
sqlct status [--project-dir <path>] [--target <db|folder>]
`
Behavior:
- List adds/changes/deletes for objects.
- Summary counts and details.
- Stable object ordering by schema, object name, and object type.
- Change classification rules:
  - Added: object exists only in source.
  - Deleted: object exists only in target.
  - Changed: normalized script content differs.
  - Suppress changes when scripts are identical after normalization.
- Normalization in v1 is limited to line-ending/trailing-newline stability for deterministic comparison.
- Trailing semicolons on `INSERT` statement lines are stripped during normalization; scripts emitted with and without statement terminators compare as compatible.
- When `data.trackedTables` is configured, `status` also reports data-script differences for tracked tables.
- Status output MUST report schema and data summaries separately.
- Exit codes:
  - `0` no differences.
  - `1` differences found.
  - `2/3/4` invalid config / connection failure / execution failure.

### diff
Show textual diffs.
`
sqlct diff [--project-dir <path>] [--target <db|folder>] [--object <selector>] [--filter <pattern>...] [--context <N>]
`
Behavior:
- Compare object script from DB vs folder.
- `--object` mode diffs one object and validates uniqueness across active object types.
- In `--object` mode, database discovery and scripting MUST be limited to the selector-matching candidate object set rather than scanning the full active database object set.
- Without `--object`, output concatenated per-object diffs in stable order.
- Changed objects use DB-vs-folder unified diff.
- Added/deleted objects use empty-side vs script-side unified diff.
- Normalization in v1 is limited to line-ending/trailing-newline stability for deterministic comparison.
- Trailing semicolons on `INSERT` statement lines are stripped during normalization; scripts emitted with and without statement terminators compare as compatible.
- Diff output uses a chunked format: only changed lines and their surrounding context are shown, not the entire file.
- `--context <N>` controls the number of unchanged context lines shown before and after each changed segment (default: 3). Negative values are treated as 0.
- When two change segments are close enough that their context regions overlap, they are merged into a single hunk.
- Each hunk is prefixed with a `@@ -l,s +l,s @@` header indicating the source and target line ranges.
- When `data.trackedTables` is configured, `diff` also supports data-script diffs for tracked tables.
- When `--filter` is specified without `--object`, only objects whose display name matches at least one regex pattern are included in the diff output; database scripting is limited to matching objects to avoid unnecessary reads.
- When `--filter` is specified with `--object`, the filter is also applied to the single selected object; if it does not match, an empty diff is returned.
- An invalid regular expression in `--filter` returns exit code 2 (invalid config).
- Exit codes follow `status`.

### pull
Write DB changes into folder.
`
sqlct pull [--project-dir <path>] [--object <selector>] [--filter <pattern>...]
`
Behavior:
- Materialize DB state into schema folder for active object types.
- Reconcile folder by deterministic create/update/delete behavior.
- Preserve existing file encoding/BOM and trailing-newline style when updating.
- New files default to UTF-8 (no BOM), CRLF, trailing newline.
- Do not rewrite unchanged files.
- Output deterministic summary and changed object list.
- When `data.trackedTables` is configured, `pull` also synchronizes `Data/*.sql` scripts for tracked tables.
- `pull` MUST delete `Data/*.sql` files for tables that are no longer present in `data.trackedTables`.
- Pull output MUST report schema and data summaries separately.
- When `--object` is specified, only objects matching the selector exactly are considered for reconciliation; database discovery and scripting is limited to the selector-matching candidate object set.
- When `--filter` is specified, only objects whose display name matches at least one regex pattern are considered for reconciliation; database scripting is limited to matching objects to avoid unnecessary reads.
- `--object` and `--filter` may be combined; both filters are applied (AND semantics).
- An invalid selector in `--object` returns exit code 2 (invalid config).
- An invalid regular expression in `--filter` returns exit code 2 (invalid config).
- Exit codes:
  - `0` success.
  - `2/3/4` invalid config / connection failure / execution failure.

## Usage
Common flows using the simplified CLI.

### First-time setup (interactive)
`
sqlct init
`
Result:
- Prompts for directory confirmation.
- On first-time setup (no existing config), prompts for connection details, runs a connection test, and on success creates the project directory structure and writes config; prints next steps.
- On connection failure, prints troubleshooting hints and prompts to proceed or abort.
- If `sqlct.config.json` already exists, exits with an error and suggests removing it to re-initialize.

### Validate and normalize configuration
`
sqlct config --project-dir ./schema
`
Result:
- Parses and validates project configuration.
- Writes normalized configuration outputs when needed.

### Manage tracked data tables
`
sqlct data track Sales.* --project-dir ./schema
sqlct data track --object Sales.Orders --project-dir ./schema
sqlct data track --filter '^Sales\.' --project-dir ./schema
sqlct data list --project-dir ./schema
`
Result:
- Lists matching tables and asks for confirmation before updating `data.trackedTables` in config.
- Shows tracked tables.

### Review changes before update
`
sqlct status --project-dir ./schema
sqlct diff --project-dir ./schema --object dbo.Customer
sqlct diff --project-dir ./schema --object data:dbo.Customer
sqlct diff --project-dir ./schema --object ServiceUser
sqlct diff --project-dir ./schema --object Role:AppReader
`
Result:
- Status shows separate schema and data add/change/delete counts.
- Diff shows object-level schema or data-script differences.

### Pull DB state into folder
`
sqlct pull --project-dir ./schema
`
Result:
- Updates schema folder from DB.
- Writes a change summary to stdout.

## Output Conventions
- Human output by default.
- --json returns structured results with status, counts, and objects.
- `status`, `diff`, and `pull` include warning entries for skipped unsupported object types or invalid script names.
- Exit codes:
  - 0 success
  - 1 diff exists / non-empty status
  - 2 invalid config or missing link
  - 3 connection failure
  - 4 execution failure
