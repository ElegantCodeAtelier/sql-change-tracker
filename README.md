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
- `sqlct init [--project-dir <path>] [--server <host>] [--database <name>] [--auth <integrated|sql>] [--user <name>] [--password <secret>] [--trust-server-certificate] [--skip-connection-test]`
- `sqlct config [--project-dir <path>]`
- `sqlct data track <pattern> [--project-dir <path>]`
- `sqlct data untrack <pattern> [--project-dir <path>]`
- `sqlct data list [--project-dir <path>]`
- `sqlct status [--project-dir <path>] [--target <db|folder>]`
- `sqlct diff [--project-dir <path>] [--target <db|folder>] [--object <selector>] [--filter <pattern>...]`
- `sqlct pull [--project-dir <path>] [--object <selector>] [--filter <pattern>...]`

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
- `PartitionFunction`
- `PartitionScheme`

When `data.trackedTables` is configured, `status`, `diff`, and `pull` also process `TableData` artifacts for those explicit tracked tables.

`--object` selectors support:
- `schema.name` for schema-scoped objects
- `name` for schema-less objects
- `type:name` and `type:schema.name` for explicit selection
- `data:schema.name` for tracked table-data scripts

`--filter` accepts one or more .NET regex patterns matched against the full display name (`schema.name` or bare `name`); an object is included if any pattern matches. Matching is case-insensitive and full-string. Use `.*` for substring matching.

## Selective data scripting
Tracked-table data scripting is configuration-driven.

- `sqlct data track` matches user tables in the current database and asks for confirmation before updating `data.trackedTables`.
- `sqlct data untrack` previews tracked matches and asks for confirmation before removing them.
- `sqlct data list` shows the currently tracked tables from config.
- `sqlct pull` creates, updates, and deletes `Data/Schema.Table_Data.sql` files for tracked tables.
- `sqlct status` and `sqlct diff` include `TableData` alongside schema artifacts, with separate schema/data summaries in `status`.

Example:
```text
sqlct data track Sales.* --project-dir ./schema
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
