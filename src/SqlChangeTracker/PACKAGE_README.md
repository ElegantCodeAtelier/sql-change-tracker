# SQL Change Tracker (sqlct)

`sqlct` is a cross-platform CLI for state-based SQL Server schema change tracking.
It focuses on deterministic output and schema-folder workflows that are Git and CI/CD friendly.

## Purpose
- Manage SQL schema changes as code using a schema folder as the source of truth.
- Keep scripting output deterministic for stable diffs and repeatable automation.
- Support compatibility-oriented workflows for teams with existing schema-folder conventions.

## Principles
- State-based workflow.
- Deterministic scripting.
- Explicit, reversible commands.
- Cross-platform usage on Windows, macOS, and Linux.
- Git-agnostic operation.

## Current Commands
```text
sqlct init [--project-dir <path>]
sqlct config [--project-dir <path>]
sqlct status [--project-dir <path>] [--target <db|folder>] [--no-progress]
sqlct diff [--project-dir <path>] [--target <db|folder>] [--object <selector>] [--no-progress]
sqlct pull [--project-dir <path>] [--no-progress]
```

Current v1 runtime scope for `status`, `diff`, and `pull` covers:
- `Table`
- `View`
- `StoredProcedure`
- `Function`
- `Sequence`
- `Schema`
- `Role`
- `User`
- `Synonym`
- `UserDefinedType`
- `PartitionFunction`
- `PartitionScheme`

`--object` selectors support:
- `schema.name` for schema-scoped objects
- `name` for schema-less objects
- `type:name` and `type:schema.name` for explicit selection

## Install
Global:
```bash
dotnet tool install --global sqlct
```

Local (tool manifest):
```bash
dotnet new tool-manifest
dotnet tool install --local sqlct
```

