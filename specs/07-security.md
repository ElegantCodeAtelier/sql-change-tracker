# Security

Status: draft
Last updated: 2026-04-07

## Authentication Modes
`sqlct` supports two SQL Server authentication modes, controlled by `database.auth` in `sqlct.config.json`.

### Integrated Authentication (default)
- `"auth": "integrated"` uses Windows Integrated Security (Trusted Connection).
- No credentials are stored or transmitted; the OS identity is used.
- `user` and `password` fields are ignored.

### SQL Server Authentication
- `"auth": "sql"` uses SQL Server Authentication with an explicit login and password.
- `database.user` must be set to a non-empty login name.
- `database.password` holds the plaintext password (see Credential Handling below).

## Credential Handling
- Plaintext passwords are acceptable for MVP.
- Do not log or expose passwords in output, progress messages, or error details.
- Future: integration with OS secret store.

## TLS / Certificate Trust
- `database.trustServerCertificate` controls whether the TLS server certificate is validated.
- Set to `true` in development environments with self-signed certificates.
- Default is `false` (certificate validation enforced).

## Logging
- Do not log secrets.
