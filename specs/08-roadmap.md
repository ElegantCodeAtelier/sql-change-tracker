# Roadmap

Status: draft
Last updated: 2026-04-04

## MVP
- init, config, status, diff, pull
- `sqlct.config.json` schema and baseline schema folder layout
- Active object types for status/diff/pull: `Table`, `View`, `StoredProcedure`, `Function`, `Sequence`, `Schema`, `Role`, `User`, `Synonym`, `UserDefinedType`, `PartitionFunction`, `PartitionScheme`
- Warnings for skipped unsupported object types

## vNext (not-planned yet)
- Improve dependency ordering and diff accuracy
- Enhanced JSON outputs
- Include/exclude object patterns for status/diff/pull
- Comparison ignore options (whitespace/comments/permissions/etc.)
- Compatibility option sync from other tools into `sqlct.config.json`
- Data sync
- Migrations support
- Create public, sanitized, intentionally authored samples for regression testing
