# Project Plan

Status: draft
Last updated: 2026-04-06

This plan reflects the current repository state and defines execution order to reach v1.

## Current State Summary
- CLI entrypoint exists with commands: `init`, `config`, `status`, `diff`, `pull`.
- `init`, `config`, `status`, `diff`, and `pull` are implemented.
- `status`, `diff`, and `pull` are active for: `Table`, `View`, `StoredProcedure`, `Function`, `Sequence`, `Schema`, `Role`, `User`, `Synonym`, `UserDefinedType`, `PartitionFunction`, `PartitionScheme`.
- Config runtime contract is simplified to:
  - `database` (existing fields)
  - `options.orderByDependencies`
  - `options.parallelism`
- Deprecated runtime fields are removed from current contract:
  - `options.includeSchemas`
  - `options.excludeObjects`
  - `options.comparison.*`
- Comparison/filter capabilities are deferred to vNext.
- Compatibility logic is presence detection only (no comparison-option sync surface).

## Stream Status
Status values: `not_started`, `in_progress`, `blocked`, `done`.

| Stream | Scope | Status | Notes |
| --- | --- | --- | --- |
| S1 | CLI foundation and command wiring | done | Command registration and global settings are in place. |
| S2 | Config and init flows | done | `sqlct.config.json` read/write and project seeding are functional. |
| S3 | SQL adapter and schema mapping | done | Active object-type services are integrated into command runtime. |
| S4 | Status/diff engine | done | End-to-end behavior implemented for active object types. |
| S5 | Pull workflow | done | End-to-end reconciliation implemented for active object types. |
| S6 | Test and quality hardening | in_progress | Fast unit tests added; DB-backed local-only coverage still to expand. |
| S7 | Packaging and release readiness | in_progress | Packaging baseline exists; release gates need completion. |
| S8 | Selective tracked-table data scripting | not_started | Concept integrated into CLI/config/scripting/architecture specs; no runtime implementation yet. |

## Execution Order
1. S6 Test and quality hardening.
2. S7 Packaging and release readiness.
3. S8 Selective tracked-table data scripting.

## S4: Status/Diff Engine
### Specs to read
- `specs/01-cli.md`
- `specs/02-config.md`
- `specs/03-schema-folder.md`
- `specs/04-scripting.md`
- `specs/05-output-formats.md`
- `specs/06-errors.md`
- `specs/09-architecture.md`

### Tasks
- Implemented folder scan model for active schema folders.
- Implemented DB-vs-folder comparison/classification (`added`, `changed`, `deleted`) with deterministic ordering.
- Implemented script normalization hooks for line-ending/trailing-newline stability.
- Implemented `status` command output (human + JSON).
- Implemented `diff` command output for single-object and aggregate concatenated mode.
- Implemented exit code behavior (`0` no diffs, `1` diffs, `2/3/4` failures).

### Acceptance Criteria
- `sqlct status` reports added/changed/deleted consistently for active object types.
- `sqlct diff` shows deterministic unified diffs (single object and aggregate modes).
- JSON output contracts are stable and aligned with `specs/05-output-formats.md`.

## S5: Pull Workflow
### Specs to read
- `specs/01-cli.md`
- `specs/02-config.md`
- `specs/03-schema-folder.md`
- `specs/04-scripting.md`
- `specs/06-errors.md`
- `specs/09-architecture.md`

### Tasks
- Connected introspector + scripter + folder mapper into `pull`.
- Implemented deterministic create/update/delete reconciliation.
- Preserved existing encoding/BOM and trailing-newline style on updates.
- Defaulted new files to UTF-8 (no BOM), CRLF, trailing newline.
- Implemented idempotent behavior by skipping unchanged rewrites.
- Produced pull summary output (human + JSON).

### Acceptance Criteria
- `sqlct pull` materializes DB state into expected active folder structure.
- Output is deterministic across repeated runs.
- Command returns success/failure codes aligned with spec.

## S6: Test and Quality Hardening
### Tasks
- Add/maintain unit tests for command handlers (`init`, `config`, `status`, `diff`, `pull`).
- Add/maintain unit tests for normalization, diff classification, ordering, and style preservation.
- Expand local-only DB-backed tests for pull/status/diff scenarios (env-gated).
- Add local-only smoke script for end-to-end validation.
- Add multi-source local POC fixture configuration (`defaultSource` + named `sources`) and validate AdventureWorks source workflow.
- Remove stale naming in test artifacts and project metadata where applicable.

### Acceptance Criteria
- Stable test suite for active command paths.
- Clear separation of fast tests vs local-only integration tests.
- Reproducible local validation steps documented.
- Local POC tooling can switch between named sources and defaults to AdventureWorks.

## S7: Packaging and Release Readiness
### Tasks
- Verify package metadata, command name, and docs consistency.
- Finalize package README and command examples.
- Add pre-release checklist execution record.
- Validate install/update/uninstall flow for local tool and global tool modes.

### Acceptance Criteria
- Package can be installed and executed as documented.
- Release checklist is complete and traceable.

## S8: Selective Tracked-Table Data Scripting
### Specs to read
- `specs/01-cli.md`
- `specs/02-config.md`
- `specs/03-schema-folder.md`
- `specs/04-scripting.md`
- `specs/09-architecture.md`

### Tasks
- Add `data track`, `data untrack`, and `data list` command wiring.
- Extend config handling for explicit tracked tables in `data.trackedTables`.
- Implement deterministic pattern matching for `data track` / `data untrack`.
- Implement explicit tracked-table management with matched-table preview and confirmation before config writes.
- Implement no-op informational handling for unmatched `track` / `untrack` operations.
- Extend top-level `status`, `diff`, and `pull` to synchronize `Data/*.sql` artifacts for tracked tables only.
- Define and implement deterministic data-script output.
- Define and implement separate schema/data summaries for `status` and `pull`.
- Ensure `pull` deletes `Data/*.sql` files for tables no longer tracked.
- Document local validation steps and usage examples.

### Acceptance Criteria
- Users can track and untrack tables through the CLI, with explicit tracked tables persisted in config.
- `track` and `untrack` preview matched tables and require confirmation before changing config.
- Users can inspect tracked tables before synchronization.
- Top-level `status`, `diff`, and `pull` synchronize data scripts for tracked tables.
- The config contract remains simple and based on explicit tracked tables in `data.trackedTables`.

## Cross-Cutting Rules
- Specs are authoritative over inferred behavior.
- `sqlct.config.json` is the primary runtime configuration source.
- Local artifacts under `local/` remain untracked.
- Keep naming compatibility-neutral in code and docs.
- Keep v1 runtime scope constrained to the active object types and simplified config contract.

## Done Criteria for v1
- `init`, `config`, `status`, `diff`, and `pull` are fully implemented.
- Active behavior is covered by automated tests.
- Deferred features are explicitly documented:
  - include/exclude filters
  - comparison ignore options
- Tool package is validated with release checklist completed.

