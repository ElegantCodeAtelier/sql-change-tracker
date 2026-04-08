# Output Formats

Status: draft
Last updated: 2026-04-07

## Human Output
- Concise summaries with change lists.
- Command headers are single-line; details follow in stable order.
- Report only `sqlct` project state (`sqlct.config.json`, schema folder state, DB comparison results).
- Do not list optional compatibility input files as output entities.
- `status`, `diff`, and `pull` may include warnings for skipped unsupported object types or invalid script names.
- Schema-less active object types use bare names in human and JSON payloads (for example `ServiceUser`, `AppReader`, `FiscalYear_PF`, `DocumentCatalog`, `//App/Messaging/Request`, `NotifySchemaChanges`).
- Tracked table-data artifacts use `schema.name` for display names and `TableData` as the output `type`.
- `data track` and `data untrack` list matched tables before asking for confirmation.
- `data track` and `data untrack` report one of `Status: updated`, `Status: no changes`, or `Status: cancelled` in final human output.

## Progress Spinner
- `status`, `diff`, and `pull` display a progress spinner on stdout while the command is running, with per-step status updates (e.g. "Scripting objects (3/42): dbo.Customer" or "Scripting objects (7/42): ServiceUser").
- The spinner is cleared before results are printed; it does not appear in final output.
- The spinner is suppressed when:
  - `--json` is used (machine-readable mode must not include spinner characters).
  - `--no-progress` is explicitly passed.
  - The output is not an interactive terminal (e.g., redirected stdout, CI environments without TTY).

### Example: data track
```text
Matching tables:
  Sales.Customer
  Sales.SalesOrderHeader
Track these tables? [y/N]: y
Data track: pattern=Sales.*
Matched tables:
  Sales.Customer
  Sales.SalesOrderHeader
Tracked tables:
  Sales.Customer
  Sales.SalesOrderHeader
Status: updated
```

### Example: data track (no-op)
```text
Data track: pattern=Archive.*
Matched tables:
  none
Tracked tables:
  dbo.Customer
  Sales.SalesOrderHeader
Status: no changes
```

### Example: data untrack (cancelled)
```text
Matching tracked tables:
  Sales.Customer
  Sales.SalesOrderHeader
Untrack these tables? [y/N]: n
Data untrack: pattern=Sales.*
Matched tables:
  Sales.Customer
  Sales.SalesOrderHeader
Tracked tables:
  dbo.Customer
  Sales.Customer
  Sales.SalesOrderHeader
Status: cancelled
```

### Example: data list
```text
Data list: project-dir=.\schema
Tracked tables:
  dbo.Customer
  Sales.SalesOrderHeader
```

### Example: status
```text
Status: target=db
Schema: Added=2  Changed=1  Deleted=0
Data:   Added=0  Changed=1  Deleted=0

Added:
  dbo.NewTable
  dbo.NewView
Changed:
  dbo.Customer
  data:dbo.Customer
```

### Example: diff (single object)
```text
Diff: dbo.Customer
--- db
+++ folder
@@ -1,1 +1,1 @@
-ALTER TABLE [dbo].[Customer] ADD [IsActive] bit NOT NULL;
+ALTER TABLE [dbo].[Customer] ADD [IsActive] bit NOT NULL CONSTRAINT [DF_Customer_IsActive] DEFAULT (1);
```

### Example: diff (single data object)
```text
Diff: data:dbo.Customer
--- db
+++ folder
@@ -1,1 +1,1 @@
-INSERT INTO [dbo].[Customer] ([CustomerID], [Name]) VALUES (1, N'Acme');
+INSERT INTO [dbo].[Customer] ([CustomerID], [Name]) VALUES (1, N'Acme Ltd');
```

### Example: diff (single schema-less object)
```text
Diff: AppReader
--- db
+++ folder
@@ -1,1 +1,2 @@
 EXEC sp_addrolemember N'AppReader', N'ServiceUser'
+EXEC sp_addrolemember N'AppReader', N'AuditUser'
```

### Example: diff (all objects)
```text
Diff: target=db
Object: dbo.Customer (Table)
--- db
+++ folder
@@ -1,1 +1,1 @@
-ALTER TABLE [dbo].[Customer] ADD [IsActive] bit NOT NULL;
+ALTER TABLE [dbo].[Customer] ADD [IsActive] bit NOT NULL CONSTRAINT [DF_Customer_IsActive] DEFAULT (1);

Object: dbo.NewView (View)
--- db
+++ folder
@@ -0,0 +1,1 @@
+CREATE VIEW [dbo].[NewView] AS SELECT 1;
```

### Example: config
```text
Config: project-dir=.\schema
Config file: .\schema\sqlct.config.json
Database: server=localhost; name=MyDb; auth=integrated
Options: parallelism=0
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

Prompt behavior:
- `data track` and `data untrack` require interactive confirmation before mutating `sqlct.config.json`.
- In human mode, the confirmation prompt is written to the terminal before final output is printed.
- When `--json` is used, any confirmation prompt MUST be written to stderr only; stdout MUST remain valid JSON.
- If confirmation is required but cannot be obtained in non-interactive execution, the command MUST fail with an execution error.

Data command fields:
- `pattern` (string, for `data track` / `data untrack`)
- `changed` (bool, for `data track` / `data untrack`)
- `cancelled` (bool, for `data track` / `data untrack`)
- `matchedTables` (array of `schema.table`, for `data track` / `data untrack`)
- `trackedTables` (array of `schema.table`, for `data track`, `data untrack`, and `data list`)

Status fields:
- `target` (string)
- `summary` (object with `schema` and `data` count objects; each count object contains `added`, `changed`, `deleted`)
- `objects` (array of `{ name, type, change }`)

Diff fields:
- `target` (string)
- `object` (string, nullable)
- `diff` (string, for `diff`)

Notes:
- `name` and `object` values use `schema.name` for schema-scoped objects and bare names for schema-less objects.
- `type` remains the discriminator for selectors and JSON consumers; no additional schema-less marker field is added.
- Data-script objects use `type = "TableData"` and use `schema.name` as the object name; `diff --object` addresses them via the reserved `data:schema.name` selector form.

Pull fields:
- `summary` (object with `schema` and `data` count objects; each count object contains `created`, `updated`, `deleted`, `unchanged`)
- `objects` (array of `{ name, type, change, path }`)

Config fields:
- `valid` (bool)
- `errors` (array of `{ code, file, message }`)
- `configPath` (string)
- `config` (object, mirrors `sqlct.config.json`)

Error fields:
- `error` (object with `{ code, message, ...context }`)

### Example: status
```json
{
  "command": "status",
  "projectDir": ".\\schema",
  "target": "db",
  "summary": {
    "schema": { "added": 2, "changed": 1, "deleted": 0 },
    "data": { "added": 0, "changed": 1, "deleted": 0 }
  },
  "objects": [
    { "name": "dbo.NewTable", "type": "Table", "change": "added" },
    { "name": "dbo.NewView", "type": "View", "change": "added" },
    { "name": "dbo.Customer", "type": "Table", "change": "changed" },
    { "name": "dbo.Customer", "type": "TableData", "change": "changed" }
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
  "object": "data:dbo.Customer",
  "diff": "--- db\n+++ folder\n@@ -1,1 +1,1 @@\n-INSERT INTO [dbo].[Customer] ([CustomerID], [Name]) VALUES (1, N'Acme')\n+INSERT INTO [dbo].[Customer] ([CustomerID], [Name]) VALUES (1, N'Acme Ltd')",
  "warnings": []
}
```

### Example: data list
```json
{
  "command": "data list",
  "projectDir": ".\\schema",
  "trackedTables": [
    "dbo.Customer",
    "Sales.SalesOrderHeader"
  ]
}
```

### Example: data track (no-op)
```json
{
  "command": "data track",
  "projectDir": ".\\schema",
  "pattern": "Archive.*",
  "changed": false,
  "cancelled": false,
  "matchedTables": [],
  "trackedTables": [
    "dbo.Customer",
    "Sales.SalesOrderHeader"
  ]
}
```

### Example: data untrack (cancelled)
```json
{
  "command": "data untrack",
  "projectDir": ".\\schema",
  "pattern": "Sales.*",
  "changed": false,
  "cancelled": true,
  "matchedTables": [
    "Sales.Customer",
    "Sales.SalesOrderHeader"
  ],
  "trackedTables": [
    "dbo.Customer",
    "Sales.Customer",
    "Sales.SalesOrderHeader"
  ]
}
```

### Example: status (schema-less object)
```json
{
  "command": "status",
  "projectDir": ".\\schema",
  "target": "db",
  "summary": {
    "schema": { "added": 1, "changed": 0, "deleted": 0 },
    "data": { "added": 0, "changed": 0, "deleted": 0 }
  },
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
      "parallelism": 0
    },
    "data": {
      "trackedTables": [
        "dbo.Customer",
        "Sales.SalesOrderHeader"
      ]
    }
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
