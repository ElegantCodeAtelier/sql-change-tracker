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
sqlct data track [<pattern>] [--object <pattern>] [--filter <regex>] [--project-dir <path>]
sqlct data untrack [<pattern>] [--object <pattern>] [--filter <regex>] [--project-dir <path>]
sqlct data list [--project-dir <path>]
sqlct status [--project-dir <path>] [--target <db|folder>] [--no-progress]
sqlct diff [--project-dir <path>] [--target <db|folder>] [--object <selector>] [--filter <pattern>...] [--context <N>] [--no-progress]
sqlct pull [--project-dir <path>] [--object <selector>] [--filter <pattern>...] [--no-progress]
```

Current runtime scope for `status`, `diff`, and `pull` covers:
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
- `TableType`
- `XmlSchemaCollection`
- `PartitionFunction`
- `PartitionScheme`
- `MessageType`
- `Contract`
- `Queue`
- `Service`
- `Route`
- `EventNotification`
- `ServiceBinding`
- `FullTextCatalog`
- `FullTextStoplist`
- `SearchPropertyList`

When `data.trackedTables` is configured, `status`, `diff`, and `pull` also process `TableData` artifacts for those explicit tracked tables.

Feature-backed object types are included when the target database exposes them, such as `FullTextCatalog`, `FullTextStoplist`, and `SearchPropertyList`.

`--object` selectors support:
- `schema.name` for schema-scoped objects
- `name` for schema-less objects
- `type:name` and `type:schema.name` for explicit selection
- `data:schema.name` for tracked table-data scripts

`--filter` accepts one or more .NET regex patterns matched against the full display name (`schema.name` or bare `name`); an object is included if any pattern matches. Matching is case-insensitive and full-string. Use `.*` for substring matching.

## Selective Data Scripting
Tracked-table data scripting is configuration-driven.

- `sqlct data track` matches user tables in the current database against a positional pattern (`schema.*`, `*.name`, `schema.name`), `--object <pattern>`, or `--filter <regex>`, and asks for confirmation before updating `data.trackedTables`. Exactly one selector must be provided.
- `sqlct data untrack` previews tracked matches (same selector forms) and asks for confirmation before removing them.
- `sqlct data list` shows the currently tracked tables from config.
- `sqlct pull` creates, updates, and deletes `Data/Schema.Table_Data.sql` files for tracked tables.
- `sqlct status` and `sqlct diff` include `TableData` alongside schema artifacts, with separate schema/data summaries in `status`.

Example:
```text
sqlct data track Sales.* --project-dir ./schema
sqlct data track --object Sales.Orders --project-dir ./schema
sqlct data track --filter '^Sales\.' --project-dir ./schema
sqlct data list --project-dir ./schema
sqlct diff --project-dir ./schema --object data:Sales.Customer
sqlct pull --project-dir ./schema
```

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

