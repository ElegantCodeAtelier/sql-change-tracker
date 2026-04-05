# Schema Folder

Status: draft
Last updated: 2026-04-04

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
  Functions/
  Security/
    Roles/
    Schemas/
    Users/
  Sequences/
  Storage/
    Partition Functions/
    Partition Schemes/
  Stored Procedures/
  Synonyms/
  Tables/
  Types/
    User-defined Data Types/
  Views/
```

## Naming Rules
- Schema-scoped object scripts use `Schema.Object.sql` in their object-type folder (for example `Tables/`, `Views/`, `Functions/`, `Stored Procedures/`, `Sequences/`, `Synonyms/`, and `Types/User-defined Data Types/`).
- Data scripts use `Schema.Object_Data.sql` in `Data/`.
- Schema-less objects omit the schema prefix (e.g., `Security/Schemas/CORE.sql`, `Security/Roles/datareader.sql`, `Storage/Partition Functions/Years_PF.sql`).
- Replace invalid file name characters in `Schema` or `Object` with percent-encoded hex (e.g., `:` -> `%3A`, `/` -> `%2F`).
- Folder names and casing must remain stable within a project.
- Line endings must match existing output (typically CRLF); do not force-normalize.
- Object matching is case-insensitive.

## Pattern Examples
Object identifiers:
- `dbo.Customer`
- `Reporting.QuarterlyRollup`

Relative paths:
- `Tables/dbo.Customer.sql`
- `Views/Reporting.QuarterlyRollup.sql`
- `Data/dbo.Customer_Data.sql`

Filter examples:
- Include all tables: `Tables/**`
- Include objects under a schema: `dbo.*`
- Include only one object: `dbo.Customer`
- Exclude archive objects: `**/*_Archive.sql` or `dbo.*_Archive`
- Exclude data scripts: `Data/**`

Notes:
- Folder names with spaces must be matched literally (e.g., `Stored Procedures/**`, `Partition Functions/**`).
- For object identifiers, use `Schema.Object` rather than a path.
