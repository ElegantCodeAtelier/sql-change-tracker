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

## What it does today
- `sqlct init [--project-dir <path>]`
- `sqlct config [--project-dir <path>]`
- `sqlct status [--project-dir <path>] [--target <db|folder>]`
- `sqlct diff [--project-dir <path>] [--target <db|folder>] [--object <schema.name>]`
- `sqlct pull [--project-dir <path>]`

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
