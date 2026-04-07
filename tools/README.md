# Tools

## Local Source Config
All tooling can resolve local reference inputs from `local/fixtures.local.json`.

```json
{
  "defaultSource": "adventureworks",
  "sources": {
    "adventureworks": {
      "referencePath": "C:/AdventureWorks",
      "objectListPath": "local/fixtures/outputs/poc-objects-adventureworks.md"
    },
    ...
  },
  "notes": "Local compatibility exports; do not commit."
}
```

- Legacy `compatExportPath` is not supported.
- Explicit path arguments (`-Ref`, `-CompatPath`, `--compat`) override source-derived paths.

## POC Runners

### SqlClient
```
powershell -File tools/poc-sqlclient/poc.ps1 `
  -Server "localhost" `
  -Database "SampleDatabase" `
  -Source "adventureworks" `
  -Output "local/fixtures/outputs/poc-out"
```

### SMO
```
dotnet run --project tools/poc-smo/POC.Smo/POC.Smo.csproj -- `
  --server "localhost" `
  --database "SampleDatabase" `
  --source "adventureworks"
```
- Default output path: `local/fixtures/outputs/poc-out-smo-<source>`.

### DacFx
```
dotnet run --project tools/poc-dacfx/POC.DacFx/POC.DacFx.csproj -- `
  --server "localhost" `
  --database "SampleDatabase" `
  --source "adventureworks"
```
- Default output path: `local/fixtures/outputs/poc-out-dacfx-<source>`.

## Compare
```
powershell -File tools/compare-poc.ps1 `
  -Source "adventureworks" `
  -Out "local/fixtures/outputs/poc-out-smo" `
  -ObjectList "local/fixtures/outputs/poc-objects-adventureworks.md"
```

## sqlct Export + Compare Harness (Local Only)
```
dotnet run --project tools/sqlct-export/Sqlct.Export.csproj -- `
  --server "localhost" `
  --database "SampleDatabase" `
  --out "local/fixtures/outputs/sqlct-out" `
  --source "adventureworks" `
  --trust-server-certificate
```

- This helper now stages the reference active object files and invokes the real `sqlct pull` command path.
- Scope therefore matches `pull` v1 behavior:
  - `Tables`
  - `Views`
  - `Stored Procedures`
  - `Functions`
  - `Sequences`
  - `Security/Schemas`
  - `Security/Roles`
  - `Security/Users`
  - `Service Broker/Contracts`
  - `Service Broker/Event Notifications`
  - `Service Broker/Message Types`
  - `Service Broker/Queues`
  - `Service Broker/Remote Service Bindings`
  - `Service Broker/Routes`
  - `Service Broker/Services`
  - `Storage/Full Text Catalogs`
  - `Storage/Full Text Stoplists`
  - `Storage/Partition Functions`
  - `Storage/Partition Schemes`
  - `Storage/Search Property Lists`
  - `Synonyms`
  - `Types/Table Types`
  - `Types/XML Schema Collections`
  - `Types/User-defined Data Types`

```
powershell -File tools/compat-harness.ps1 `
  -Source "adventureworks" `
  -Out "local/fixtures/outputs/sqlct-out" `
  -ReportPath "local/fixtures/outputs/reports/compat.json"
```
