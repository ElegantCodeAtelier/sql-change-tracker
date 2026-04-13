# Config

Status: draft
Last updated: 2026-04-13

## Authoritative Configuration
`sqlct.config.yaml` is the authoritative configuration file for `sqlct` projects.

- Required for baseline `sqlct` workflows.
- Stores database connection settings and tool options.
- All command behavior is resolved from this file (with CLI overrides where supported).

## Schema
The `sqlct.config.yaml` structure defines the current contract.
Current shape:

```yaml
database:
  server: localhost
  name: MyDb
  auth: integrated
  user: ''
  password: ''
  trustServerCertificate: false
options:
  parallelism: 0
data:
  trackedTables:
    - dbo.Customer
    - Sales.SalesOrderHeader
```

## Init Behavior
`sqlct init` initializes `sqlct.config.yaml` in the target project directory.
- Writes default config when missing. The written file begins with a header comment block containing a short intro, installation instructions, usage instructions, and a link to the GitHub repository.
- Preserves existing compatibility files if present.

## Config Command Behavior
`sqlct config` parses, validates, and writes configuration from the project directory.

- Requires `sqlct.config.yaml` to already exist in the project directory. If the file is missing, `config` exits with code `2` (`invalid config`) and prints: `Error: project directory is not initialized.` with hint `run \`sqlct init\` first.` No file is created.
- Validates `sqlct.config.yaml` as the primary source.
- Detects optional compatibility file presence for summary output only.
- Writes normalized configuration back to `sqlct.config.yaml`. The header comment block is regenerated on every write; any user-added comments inside the file are not preserved.
- Deprecated fields are tolerated on read and omitted on rewrite.

Deprecated runtime fields removed from v1 contract:
- `options.includeSchemas`
- `options.excludeObjects`
- `options.comparison.*`

## File Handling Policy
- `sqlct.config.yaml`: required; read/write by `init` and `config`.
- External compatibility files: optional; presence may be scanned/reported; files are preserved as-is.
- Do not fail baseline workflows solely because optional external compatibility files are missing.

## Authentication Modes
`database.auth` controls how `sqlct` authenticates to SQL Server. Supported values:

- `integrated` (default): Uses Windows Integrated Security (Trusted Connection). `user` and `password` are ignored. Suitable for domain environments and local development.
- `sql`: Uses SQL Server Authentication. `user` is required; `password` is optional (empty string is accepted).

### SQL Authentication example
```yaml
database:
  server: my-server.example.com
  name: MyDb
  auth: sql
  user: my_login
  password: my_password
  trustServerCertificate: false
```

Validation rules:
- `database.auth` MUST be `"integrated"` or `"sql"` (case-insensitive). Any other value is a configuration error.
- When `database.auth` is `"sql"`, `database.user` MUST be non-empty.
- When `database.auth` is `"integrated"`, `database.user` and `database.password` are ignored.

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

## Migration from JSON config
Projects initialized with `sqlct` v0.3.0 or earlier use `sqlct.config.json`. To migrate:

1. Rename `sqlct.config.json` to `sqlct.config.yaml`.
2. Translate the JSON content to YAML (the field names are identical; the schema is the same).
3. Run `sqlct config` to validate and normalize the migrated file.

If `sqlct.config.yaml` is absent but `sqlct.config.json` is present in the project directory, `sqlct` exits with a targeted error and migration hint instead of a generic "not initialized" message.

## External interoperability
- Compatibility-file presence may be detected for summary/reporting purposes.
- Any future mapping from compatibility files into runtime config is a vNext item.
