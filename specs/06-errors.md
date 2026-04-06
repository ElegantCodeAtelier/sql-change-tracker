# Errors

Status: draft
Last updated: 2026-04-06

## Exit Codes
- 0 success
- 1 diff exists / non-empty status
- 2 invalid config
- 3 connection failure
- 4 execution failure

## Error Categories
- Config errors
- Connection errors
- Execution errors

## Diff vs Error
Exit code 1 indicates differences were found (non-empty status/diff). It is not an error and should not include an `error` payload in JSON output.

## Error Codes (JSON)
Config:
- invalid_config: config file missing or malformed.
- missing_link: project directory/config link could not be resolved.
- missing_required_field: required setting is missing in `sqlct.config.json`.
- unsupported_config_version: config version is not supported by this build.

Connection:
- connection_failed: connection attempt failed (auth/network/timeout).
- database_not_found: server reachable but database missing.

Execution:
- script_failed: SQL execution failed.
- io_failed: failed to read/write a file.
- not_implemented: command or option not implemented in current build.

## Exit Code Matrix
| Command | 0 | 1 | 2 | 3 | 4 |
| --- | --- | --- | --- | --- | --- |
| init | success | n/a | invalid config/path | n/a | execution failure |
| config | success | n/a | invalid config | n/a | execution failure |
| status | no diffs | diffs found | invalid config | connection failure | execution failure |
| diff | no diffs | diffs found | invalid config | connection failure | execution failure |
| pull | success | n/a | invalid config | connection failure | execution failure |

## Example Outputs
### Invalid config (exit code 2)
```text
Error: invalid config file.
Detail: config was empty.
```

### Missing project config (exit code 2)
```text
Error: no linked schema folder found.
File: C:\path\to\project\sqlct.config.json
Detail: expected config file at 'C:\path\to\project\sqlct.config.json'.
Hint: run `sqlct init` or `sqlct config`.
```

### Connection failure (exit code 3)
```text
Error: failed to connect to SQL Server.
Detail: login failed for user 'sa'.
```

### Execution failure (exit code 4)
```text
Error: script execution failed.
Detail: cannot drop table because it is referenced by a foreign key.
```
