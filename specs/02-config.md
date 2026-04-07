# Config

Status: draft
Last updated: 2026-04-06

## Authoritative Configuration
`sqlct.config.json` is the authoritative configuration file for `sqlct` projects.

- Required for baseline `sqlct` workflows.
- Stores database connection settings and tool options.
- All command behavior is resolved from this file (with CLI overrides where supported).

## Schema
The `sqlct.config.json` structure defines the current contract.
Current shape:

```json
{
  "database": {
    "server": "localhost",
    "name": "MyDb",
    "auth": "integrated",
    "user": "",
    "password": "",
    "trustServerCertificate": false
  },
  "options": {
    "parallelism": 0
  },
  "data": {
    "trackedTables": [
      "dbo.Customer",
      "Sales.SalesOrderHeader"
    ]
  }
}
```

## Init Behavior
`sqlct init` initializes `sqlct.config.json` in the target project directory.
- Writes default config when missing.
- Preserves existing compatibility files if present.

## Config Command Behavior
`sqlct config` parses, validates, and writes configuration from the project directory.

- Requires `sqlct.config.json` to already exist in the project directory. If the file is missing, `config` exits with code `2` (`invalid config`) and prints: `Error: project directory is not initialized.` with hint `run \`sqlct init\` first.` No file is created.
- Validates `sqlct.config.json` as the primary source.
- Detects optional compatibility file presence for summary output only.
- Writes normalized configuration back to `sqlct.config.json`.
- Deprecated fields are tolerated on read and omitted on rewrite.

Deprecated runtime fields removed from v1 contract:
- `options.orderByDependencies`
- `options.includeSchemas`
- `options.excludeObjects`
- `options.comparison.*`

## File Handling Policy
- `sqlct.config.json`: required; read/write by `init` and `config`.
- External compatibility files: optional; presence may be scanned/reported; files are preserved as-is.
- Do not fail baseline workflows solely because optional external compatibility files are missing.

## Notes
- Plaintext passwords are acceptable for MVP; future integration with OS secret store.
- Config versioning planned for migrations.
- `trustServerCertificate` mirrors TLS environments with self-signed certs.
- `options.parallelism`: maximum number of concurrent SQL connections used during catalog discovery and per-object scripting. `0` (default) resolves to `Environment.ProcessorCount`. Set a positive integer to cap DOP on shared SQL Server instances.
- Include/exclude filters and comparison ignore options are deferred to vNext.
- `data.trackedTables`: array of explicit tracked tables used for selective data scripting.
- Entries in `data.trackedTables` MUST use `schema.table` form.
- Entries in `data.trackedTables` MUST be unique case-insensitively and persisted in stable sorted order.
- When omitted, `data.trackedTables` defaults to an empty array.

## External interoperability
- Compatibility-file presence may be detected for summary/reporting purposes.
- Any future mapping from compatibility files into runtime config is a vNext item.
