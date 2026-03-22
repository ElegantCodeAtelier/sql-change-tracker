# CLI

Status: draft
Last updated: 2026-03-11

## Command Overview
Binary: sqlct.

Global flags:
- --project-dir <path>: project directory location (default current working directory).
- --verbose.
- --json: machine-readable output.

## CLI Shape (Selected)
Use git-style verbs: short, task-oriented commands with clear intent.

## Commands (MVP)
- init
- config
- status
- diff
- pull

## v1 Scope
- Active object types: `Table`, `View`, `StoredProcedure`, `Function`, `Sequence`.
- `status`, `diff`, and `pull` process only the active object types.
- Unsupported object types discovered in DB introspection are skipped with warnings.
- Include/exclude filters are deferred to vNext.
- Comparison ignore options are deferred to vNext.

## Flag Semantics
Common flag behavior across commands.

### --project-dir
- Overrides the project directory for the current command.
- When omitted, default to current working directory.
- For `init`, if omitted, prompt for confirmation before initializing the current working directory.

### --target <db|folder>
- `db`: compare DB against folder; results show what would be pulled.
- `folder`: compare folder against DB; results show what would be committed in folder state.
- Default: `db`.

### --object <schema.name>
- Identifies a single object for diff output (`schema.name` format).
- Must be unique across object types; if ambiguous, return an error.

### init
Initialize project configuration and schema folder structure.
`
sqlct init [--project-dir <path>]
`
Behavior:
- Initialize configuration in an empty directory or existing project structure.
- Requires only schema directory context.
- If `--project-dir` is omitted, assume current working directory and prompt for confirmation.

### config
Parse, validate, and write configuration from the project directory.
`
sqlct config [--project-dir <path>]
`
Behavior:
- Read project configuration and schema metadata files from project directory.
- Validate required files and required fields.
- Rewrite normalized configuration outputs if needed.
- Print a summary of parsed values and validation status.

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
- Exit codes:
  - `0` no differences.
  - `1` differences found.
  - `2/3/4` invalid config / connection failure / execution failure.

### diff
Show textual diffs.
`
sqlct diff [--project-dir <path>] [--target <db|folder>] [--object <schema.name>]
`
Behavior:
- Compare object script from DB vs folder.
- `--object` mode diffs one object and validates uniqueness across active object types.
- Without `--object`, output concatenated per-object diffs in stable order.
- Changed objects use DB-vs-folder unified diff.
- Added/deleted objects use empty-side vs script-side unified diff.
- Normalization in v1 is limited to line-ending/trailing-newline stability for deterministic comparison.
- Exit codes follow `status`.

### pull
Write DB changes into folder.
`
sqlct pull [--project-dir <path>]
`
Behavior:
- Materialize DB state into schema folder for active object types.
- Reconcile folder by deterministic create/update/delete behavior.
- Preserve existing file encoding/BOM and trailing-newline style when updating.
- New files default to UTF-8 (no BOM), CRLF, trailing newline.
- Do not rewrite unchanged files.
- Output deterministic summary and changed object list.
- Exit codes:
  - `0` success.
  - `2/3/4` invalid config / connection failure / execution failure.

## Usage
Common flows using the simplified CLI.

### First-time setup
`
sqlct init --project-dir ./schema
`
Result:
- Initializes config and schema folder structure in the target directory.

### Initialize in current directory
`
sqlct init
`
Result:
- Prompts for confirmation.
- Initializes config and schema folder structure in the current directory if confirmed.

### Validate and normalize configuration
`
sqlct config --project-dir ./schema
`
Result:
- Parses and validates project configuration.
- Writes normalized configuration outputs when needed.

### Review changes before update
`
sqlct status --project-dir ./schema
sqlct diff --project-dir ./schema --object dbo.Customer
`
Result:
- Status shows add/change/delete counts.
- Diff shows object-level script differences.

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
