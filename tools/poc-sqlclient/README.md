# POC SqlClient Scripting

This folder contains a local-only POC script that uses SqlClient to script selected
objects from a sample database and write them into a compatibility folder layout.

## Usage
```
pwsh .\tools\poc-sqlclient\poc.ps1 `
  -Server "localhost" `
  -Database "SampleDatabase" `
  -Source "adventureworks" `
  -Output "local/fixtures/outputs/poc-out" `
  -OverridesPath "local/fixtures/outputs/poc-overrides.json"
```

## Notes
- This is a POC; scripts may not fully match compatibility output yet.
- Input and output paths are local-only and should not be committed.
- Use a full server name like `HOST\INSTANCE` if instance resolution fails.
- `local/fixtures/outputs/poc-overrides.json` is optional and local-only.
- Source settings are resolved from `local/fixtures.local.json` (`defaultSource` + `sources`).
- `-CompatPath` and `-ObjectList` override source-derived paths when explicitly provided.
