# Schema Folder

Status: draft
Last updated: 2026-04-08

## Scope
Defines the baseline `sqlct` schema-folder structure and naming rules.

## Baseline Rules
- The project root must contain `sqlct.config.json` (see `specs/02-config.md` for schema/defaults).
- Object scripts are organized by object-type folders.
- One object per file.
- Object matching is case-insensitive, while output preserves original casing.
- Keep script formatting and line endings stable for deterministic diffs.
- Scripting rules are specified in `specs/04-scripting.md`.

## Baseline Layout
```text
<project>/
  sqlct.config.json
  Data/
  Assemblies/
  Functions/
  Security/
    Roles/
    Schemas/
    Users/
  Sequences/
  Service Broker/
    Contracts/
    Event Notifications/
    Message Types/
    Queues/
    Remote Service Bindings/
    Routes/
    Services/
  Storage/
    Full Text Catalogs/
    Full Text Stoplists/
    Partition Functions/
    Partition Schemes/
    Search Property Lists/
  Stored Procedures/
  Synonyms/
  Tables/
  Types/
    XML Schema Collections/
    User-defined Data Types/
  Views/
```

## Naming Rules
- Schema-scoped object scripts use `Schema.Object.sql` in their object-type folder (for example `Tables/`, `Views/`, `Functions/`, `Stored Procedures/`, `Sequences/`, `Synonyms/`, `Service Broker/Queues/`, `Types/XML Schema Collections/`, and `Types/User-defined Data Types/`).
- `Functions/` stores both `Function` and `Aggregate` scripts.
- `Types/User-defined Data Types/` stores both scalar alias types and table-valued types.
- Data scripts use `Schema.Object_Data.sql` in `Data/`.
- Data tracking uses `Data/` for scripts derived from tables explicitly listed in `data.trackedTables`.
- Schema-less objects omit the schema prefix (e.g., `Assemblies/AppClr.sql`, `Security/Schemas/AppSecurity.sql`, `Security/Roles/AppReader.sql`, `Storage/Partition Functions/FiscalYear_PF.sql`, `Storage/Full Text Catalogs/DocumentCatalog.sql`, `Storage/Search Property Lists/DocumentProperties.sql`, `Service Broker/Contracts/%2F%2FApp%2FMessaging%2FContract.sql`, `Service Broker/Services/AppInitiatorService.sql`, `Service Broker/Event Notifications/NotifySchemaChanges.sql`, `Service Broker/Remote Service Bindings/AppRemoteBinding.sql`).
- Replace invalid file name characters in `Schema` or `Object` with percent-encoded hex (e.g., `:` -> `%3A`, `/` -> `%2F`).
- Folder readers MAY recover object identity for legacy schema-less files whose names are not canonical percent-encoded paths by reading the top-level `CREATE` statement when the scripted object name contains characters that require escaping; writers MUST continue to use canonical percent-encoded file names.
- Folder names and casing must remain stable within a project.
- Line endings must match existing output (typically CRLF); do not force-normalize.
- Object matching is case-insensitive.

## Pattern Examples
Object identifiers:
- `dbo.Customer`
- `Reporting.QuarterlyRollup`
- `//App/Messaging/Request`

Relative paths:
- `Tables/dbo.Customer.sql`
- `Views/Reporting.QuarterlyRollup.sql`
- `Assemblies/AppClr.sql`
- `Service Broker/Message Types/%2F%2FApp%2FMessaging%2FRequest.sql`
- `Storage/Full Text Catalogs/DocumentCatalog.sql`
- `Storage/Search Property Lists/DocumentProperties.sql`
- `Data/dbo.Customer_Data.sql`

Filter examples:
- Include all tables: `Tables/**`
- Include objects under a schema: `dbo.*`
- Include only one object: `dbo.Customer`
- Exclude archive objects: `**/*_Archive.sql` or `dbo.*_Archive`
- Exclude data scripts: `Data/**`

Notes:
- Folder names with spaces must be matched literally (e.g., `Stored Procedures/**`, `Event Notifications/**`, `Remote Service Bindings/**`, `Partition Functions/**`, `Full Text Catalogs/**`, `Full Text Stoplists/**`, `Search Property Lists/**`, `Message Types/**`, `XML Schema Collections/**`, `User-defined Data Types/**`).
- For schema-scoped object identifiers, use `Schema.Object` rather than a path.
