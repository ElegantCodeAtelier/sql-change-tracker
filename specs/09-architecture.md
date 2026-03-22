# Architecture

Status: draft
Last updated: 2026-03-11

## Goals
- Deterministic, reproducible scripting.
- Clear separation of concerns and testability.
- Keep runtime behavior driven by repository specs.
- Use `sqlct.config.json` as the primary runtime configuration source.

## Directory Structure
- `src/SqlChangeTracker/` main CLI application.
  - Subfolders are organized by responsibility (commands, configuration, schema mapping, SQL access, normalization, diffing, output).
  - Prefer shallow, stable folders that map to the major modules; avoid deep nesting and feature-first slices for now.
  - Keep cross-cutting helpers near the module that owns them; avoid a global `Utils` dump.
  - Suggested subfolders (non-binding, adjust as needed):
    - `Commands/`
    - `Config/`
    - `Schema/`
    - `Sql/`
    - `Normalization/`
    - `Diff/`
    - `Data/`
    - `Output/`
- `tests/SqlChangeTracker.Tests/` unit and integration tests.
  - Mirror the `src/` structure loosely to keep discovery easy.
  - Separate slow or local-only tests from fast unit tests.
- `specs/` specifications and fixtures.
- `tools/` local tooling and helper scripts.

## Modules
- CLI
  - Command parsing, human/JSON output.
  - No business logic beyond orchestration.
  - Use Spectre.Console for CLI parsing and output styling.
- Config
  - Owns `sqlct.config.json` read/write behavior.
  - Configuration schema and behavior are defined in `specs/02-config.md`.
  - Compatibility-file presence detection is part of current v1 behavior; compatibility-driven sync remains deferred.
- Folder Mapping
  - Maps object types and file names using rules in `specs/03-schema-folder.md`.
- SQL Server Adapter
  - Introspection and scripting.
  - Returns raw scripts for normalization/persistence.
- Sync Runtime Service
  - Internal orchestration for `status`, `diff`, and `pull`.
  - Responsibilities:
    - load project/config and validate DB config
    - scan core schema folders (`Tables`, `Views`, `Stored Procedures`, `Functions`, `Sequences`)
    - snapshot DB objects for the same core types
    - classify objects (`added`, `changed`, `deleted`) in deterministic order
    - render unified diff text for single-object and aggregate modes
    - reconcile pull writes/deletes with idempotent update behavior
    - preserve existing encoding/newline style for updated files
  - v1 comparison normalization is limited to line-ending/trailing-newline stability.
- Script Normalizer
  - Applies deterministic text handling required for comparison and write stability in v1.

## Code Architecture
- CLI is a thin orchestration layer. It resolves the project directory (`--project-dir` or `sqlct.config.json`), dispatches to domain services, and renders output (human or JSON).
- Config owns `sqlct.config.json` parsing/validation and normalization behavior (`specs/02-config.md`).
- Schema defines object types, folder mapping, and file naming rules (`specs/03-schema-folder.md`). This is pure mapping logic with no DB calls or filesystem I/O.
- SQL Server (SqlClient) handles connectivity, metadata discovery, and scripting. It returns raw scripts and metadata for downstream normalization and persistence.
- Normalization and formatting follow `specs/04-scripting.md` and v1 runtime constraints.
- Sync runtime compares normalized scripts between DB and folder sources, producing structured status/diff/pull results.
- Active v1 comparison scope is core object types only: `Table`, `View`, `StoredProcedure`, `Function`, `Sequence`.
- Include/exclude filters and comparison ignore options are deferred to vNext.

## Data Flow (MVP)
1) CLI resolves project directory via `sqlct.config.json` or `--project-dir`.
2) Config module reads baseline config.
3) CLI prints or validates parsed values.

## Data Flow (Full)
1) Load `sqlct.config.json` and effective folder mapping.
2) Introspect DB and script objects.
3) Normalize/persist scripts into the schema layout defined in `specs/03-schema-folder.md`.
4) Compare DB vs folder for status/diff.
5) For `pull`, reconcile create/update/delete and keep unchanged files untouched.

## Key Interfaces (Draft)
- `IConfigStore`
  - `ReadSqlctConfig()`
  - `WriteSqlctConfig()`
- `IFolderMapper`
  - `GetFolderForType(ObjectType type)`
  - `GetFileName(ObjectName name, ObjectType type)`
- `ISchemaReader`
  - `ListObjects()`
  - `GetScript(ObjectRef)`
- `IScriptNormalizer`
  - `Normalize(string script, ObjectType type)`
- `IDiffEngine`
  - `Compare(source, target, options)`

## Testing Strategy
- Unit tests:
  - Config parsing and normalization for `sqlct.config.json`.
  - Folder mapping and object naming rules.
  - Diff normalization and ordering for core object types.
  - Encoding/newline style detection and preservation for pull updates.
- Integration tests (local-only):
  - `config` parsing/normalization against baseline config.
  - SqlClient scripting against a small local DB with known outputs.
- Golden fixtures:
  - Use sanitized, intentionally authored samples in tracked specs when needed.

## Tech Stack
- .NET 10
- Spectre.Console (CLI parsing + output)
- System.Text.Json (config)
- System.Xml.Linq + raw file preservation (XML files)
- Microsoft.Data.SqlClient (SQL Server connectivity, later)
- DiffPlex (optional, diff output)

## Open Questions
- Should normalization be a no-op pass-through to avoid any drift?
