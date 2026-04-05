# Output Formats

Status: draft
Last updated: 2026-04-04

## Human Output
- Concise summaries with change lists.
- Command headers are single-line; details follow in stable order.
- Report only `sqlct` project state (`sqlct.config.json`, schema folder state, DB comparison results).
- Do not list optional compatibility input files as output entities.
- `status`, `diff`, and `pull` may include warnings for skipped unsupported object types or invalid script names.
- Schema-less active object types use bare names in human and JSON payloads (for example `ServiceUser`, `AppReader`, `Years_PF`).

## Progress Spinner
- `status`, `diff`, and `pull` display a progress spinner on stdout while the command is running, with per-step status updates (e.g. "Scripting objects (3/42): dbo.Customer" or "Scripting objects (7/42): ServiceUser").
- The spinner is cleared before results are printed; it does not appear in final output.
- The spinner is suppressed when:
  - `--json` is used (machine-readable mode must not include spinner characters).
  - `--no-progress` is explicitly passed.
  - The output is not an interactive terminal (e.g., redirected stdout, CI environments without TTY).

### Example: status
```text
Status: target=db
Added: 2  Changed: 1  Deleted: 0

Added:
  dbo.NewTable
  dbo.NewView
Changed:
  dbo.Customer
```

### Example: diff (single object)
```text
Diff: dbo.Customer
--- db
+++ folder
@@
-ALTER TABLE [dbo].[Customer] ADD [IsActive] bit NOT NULL;
+ALTER TABLE [dbo].[Customer] ADD [IsActive] bit NOT NULL CONSTRAINT [DF_Customer_IsActive] DEFAULT (1);
```

### Example: diff (single schema-less object)
```text
Diff: AppReader
--- db
+++ folder
@@
-EXEC sp_addrolemember N'AppReader', N'ServiceUser'
+EXEC sp_addrolemember N'AppReader', N'ServiceUser'
+EXEC sp_addrolemember N'AppReader', N'AuditUser'
```

### Example: diff (all objects)
```text
Diff: target=db
Object: dbo.Customer (Table)
--- db
+++ folder
@@
-ALTER TABLE [dbo].[Customer] ADD [IsActive] bit NOT NULL;
+ALTER TABLE [dbo].[Customer] ADD [IsActive] bit NOT NULL CONSTRAINT [DF_Customer_IsActive] DEFAULT (1);

Object: dbo.NewView (View)
--- db
+++ folder
@@
+CREATE VIEW [dbo].[NewView] AS SELECT 1;
```

### Example: config
```text
Config: project-dir=.\schema
Config file: .\schema\sqlct.config.json
Database: server=localhost; name=MyDb; auth=integrated
Options: orderByDependencies=true
Compatibility: detected=false
Config validation: ok
```

### Example: init
```text
Init: project-dir=.\schema
Created:
  sqlct.config.json
Skipped:
  none
```

## JSON Output
- Structured results with status, counts, and objects.

## JSON Schema Outline
Common fields:
- `command` (string)
- `projectDir` (string, when resolved)
- `warnings` (array, optional)

Status fields:
- `target` (string)
- `summary` (object with counts)
- `objects` (array of `{ name, type, change }`)

Diff fields:
- `target` (string)
- `object` (string, nullable)
- `diff` (string, for `diff`)

Notes:
- `name` and `object` values use `schema.name` for schema-scoped objects and bare names for schema-less objects.
- `type` remains the discriminator for selectors and JSON consumers; no additional schema-less marker field is added.

Pull fields:
- `summary` (object with `created`, `updated`, `deleted`, `unchanged`)
- `objects` (array of `{ name, type, change, path }`)

Config fields:
- `valid` (bool)
- `errors` (array of `{ code, file, message }`)
- `configPath` (string)
- `config` (object, mirrors `sqlct.config.json`)
- `compatibility` (object with `hasAny`, `hasDatabaseInfo`, `hasScpf`)

Error fields:
- `error` (object with `{ code, message, ...context }`)

### Example: status
```json
{
  "command": "status",
  "projectDir": ".\\schema",
  "target": "db",
  "summary": { "added": 2, "changed": 1, "deleted": 0 },
  "objects": [
    { "name": "dbo.NewTable", "type": "Table", "change": "added" },
    { "name": "dbo.NewView", "type": "View", "change": "added" },
    { "name": "dbo.Customer", "type": "Table", "change": "changed" }
  ],
  "warnings": []
}
```

### Example: diff
```json
{
  "command": "diff",
  "projectDir": ".\\schema",
  "target": "db",
  "object": "dbo.Customer",
  "diff": "--- db\\n+++ folder\\n@@\\n-ALTER TABLE...\\n+ALTER TABLE...",
  "warnings": []
}
```

### Example: status (schema-less object)
```json
{
  "command": "status",
  "projectDir": ".\\schema",
  "target": "db",
  "summary": { "added": 1, "changed": 0, "deleted": 0 },
  "objects": [
    { "name": "ServiceUser", "type": "User", "change": "added" }
  ],
  "warnings": []
}
```

### Example: config
```json
{
  "command": "config",
  "projectDir": ".\\schema",
  "valid": true,
  "errors": [],
  "configPath": ".\\schema\\sqlct.config.json",
  "config": {
    "database": {
      "server": "localhost",
      "name": "MyDb",
      "auth": "integrated",
      "user": "",
      "password": "",
      "trustServerCertificate": false
    },
    "options": {
      "orderByDependencies": true
    }
  },
  "compatibility": {
    "hasAny": false,
    "hasDatabaseInfo": false,
    "hasScpf": false
  }
}
```

### Example: init
```json
{
  "command": "init",
  "projectDir": ".\\schema",
  "created": ["sqlct.config.json"],
  "skipped": []
}
```

### Example: error (config)
```json
{
  "command": "config",
  "projectDir": ".\\schema",
  "valid": false,
  "errors": [
    {
      "code": "invalid_config",
      "file": "sqlct.config.json",
      "message": "missing required field: database.name"
    }
  ]
}
```

### Example: error (connection)
```json
{
  "command": "status",
  "error": {
    "code": "connection_failed",
    "message": "failed to connect to SQL Server.",
    "detail": "login failed for user 'sa'."
  }
}
```
