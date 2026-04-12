# SQL Change Tracker (sqlct)

`sqlct` helps teams manage SQL schema changes like code: initialize a project,
validate configuration, and generate reproducible scripts for Git and CI/CD.

## Design goals
- Deterministic schema scripting
- Git-friendly schema representation
- CI/CD friendly workflows
- Tooling built entirely on publicly documented SQL Server metadata

## What this tool will do
- Initialize a schema-folder project with required configuration and templates.
- Validate project configuration before scripting or automation runs.
- Generate deterministic SQL scripts from live database metadata.
- Preserve schema-folder conventions used in existing SQL source control workflows.
- Support tracked-table data scripting alongside schema workflows.

## What it does today
- `sqlct init [--project-dir <path>]`
- `sqlct config [--project-dir <path>]`
- `sqlct data track [<pattern>] [--object <pattern>] [--filter <regex>] [--project-dir <path>]`
- `sqlct data untrack [<pattern>] [--object <pattern>] [--filter <regex>] [--project-dir <path>]`
- `sqlct data list [--project-dir <path>]`
- `sqlct status [--project-dir <path>] [--target <db|folder>]`
- `sqlct diff [--project-dir <path>] [--target <db|folder>] [--object <selector>] [--filter <pattern>...] [--context <N>] [--normalized-diff]`
- `sqlct pull [--project-dir <path>] [--object <selector>] [--filter <pattern>...]`

Current runtime scope for `status`, `diff`, and `pull` covers:
- `Assembly`
- `Table`
- `View`
- `StoredProcedure`
- `Function`
- `Aggregate`
- `Sequence`
- `Schema`
- `Role`
- `User`
- `Synonym`
- `UserDefinedType`
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

Schema scripting covers user-defined schemas and also emits built-in `dbo` when it has explicit schema permissions or schema-level extended properties; `dbo` is scripted without `CREATE SCHEMA`.

Stored procedure scripting covers T-SQL procedures and SQL CLR stored procedures (`sys.objects.type = 'P'` and `PC`).

Role and user scripting includes database-level permissions granted directly to those principals, and loginless users are scripted as `CREATE USER ... WITHOUT LOGIN`.

Table scripting also includes standalone user-created table statistics (`CREATE STATISTICS`) as post-create table statements. Current statistics option coverage includes effective sampling (`FULLSCAN` or `SAMPLE <n> PERCENT`), `PERSIST_SAMPLE_PERCENT = ON`, `NORECOMPUTE`, `INCREMENTAL=ON`, and `AUTO_DROP = ON|OFF` when the source server exposes the required metadata. `MAXDOP`, `STATS_STREAM`, `ROWCOUNT`, and `PAGECOUNT` remain deferred.

Function scripting covers T-SQL scalar/table functions and SQL CLR scalar/table-valued functions (`sys.objects.type = 'FS'` and `FT`), including `EXTERNAL NAME` assembly bindings.

Aggregate scripting covers SQL CLR aggregates (`sys.objects.type = 'AF'`), including `CREATE AGGREGATE ... RETURNS ... EXTERNAL NAME` bindings.

When `data.trackedTables` is configured, `status`, `diff`, and `pull` also process `TableData` artifacts for those explicit tracked tables.

`--object` selectors support:
- `schema.name` for schema-scoped objects
- `name` for schema-less objects
- `type:name` and `type:schema.name` for explicit selection
- `data:schema.name` for tracked table-data scripts

`--filter` accepts one or more .NET regex patterns matched against the full display name (`schema.name` or bare `name`); an object is included if any pattern matches. Matching is case-insensitive and full-string. Use `.*` for substring matching.

## Selective data scripting
Tracked-table data scripting is configuration-driven.

- `sqlct init` scans the project directory for existing `Data/*.sql` files and proposes their table names as initial `trackedTables`; in interactive mode you are prompted to confirm, in non-interactive mode (`--project-dir`) they are added automatically.
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

## Build
```bash
dotnet build
```

## Tests
```bash
dotnet test tests/SqlChangeTracker.Tests/SqlChangeTracker.Tests.csproj
```

## Releases (NuGet)
Publish is handled by GitHub Actions on tag push.

1. Add `NUGET_API_KEY` to the repo GitHub Actions secrets.
2. Tag and push a release:
```bash
git tag v0.1.1
git push origin v0.1.1
```
This triggers `.github/workflows/nuget-publish.yml` to pack and publish the tool to NuGet using version `0.1.1`.

## Changelog
See [CHANGELOG.md](CHANGELOG.md) for a history of notable changes.
