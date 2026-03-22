# Compatibility Harness

Status: draft
Last updated: 2026-03-10

## Purpose
Define a local-only harness to compare this tool’s output against a reference export without committing production data.

## Document Role
- Normative product scripting rules are defined in `specs/04-scripting.md`.
- This document defines local validation workflow and strict comparison policy.
- Implementation traceability and current POC deltas are tracked in `tools/poc-evaluation.md`.

## Inputs
- Local source config from `local/fixtures.local.json` (`defaultSource` + `sources` map).
- Generated output folder (from this tool).
- Optional allowlist of sample objects (for spot checks).

## Outputs
- Tree diff: missing/extra folders and files.
- Script diff: byte-for-byte comparisons for selected objects.
- Summary report with pass/fail status and counts.

## Workflow
1) Resolve source settings from `local/fixtures.local.json`.
   - Use `-Source <name>` when provided.
   - Otherwise use `defaultSource`.
   - Explicit `-Ref` overrides source-derived reference path.
2) Generate output into a temp folder outside the repo (default under `local/fixtures/outputs/`).
   - When using `tools/sqlct-export`, generation SHOULD go through the actual `sqlct pull` command path rather than direct scripting services.
   - Command-based local export is limited to the active `pull` scope (`Table`, `View`, `StoredProcedure`, `Function`, `Sequence`).
3) Compare:
   - Folder tree: names, casing, and path separators.
   - Object scripts: exact byte comparison for selected objects.
4) Emit a summary report (text + JSON).

## Local Source Config Shape
```json
{
  "defaultSource": "adventureworks",
  "sources": {
    "adventureworks": {
      "referencePath": "C:/AdventureWorks",
      "objectListPath": "local/fixtures/outputs/poc-objects-adventureworks.md"
    }
  }
}
```

- Legacy `compatExportPath` is not supported.

## Comparison Rules
- Exact path and filename matching (case-preserving, case-insensitive compare).
- Ignore timestamps and filesystem metadata.
- No content normalization: reference output must match byte-for-byte.
- Exceptions/overrides/ignores are allowed only when explicitly defined and reviewed.
- Exception handling is allowlist-based (never blanket ignore patterns).

## Decisions
- Local run reports are stored under `local/fixtures/outputs/reports/`.
- Manual exception rules are local-only and must be recorded with reason, scope, owner, and review date.
- MVP target includes adding `specs/fixtures/standard` as a repository baseline fixture set.

## Exception Recording Guidance
- Keep exceptions local-only.
- Record each exception with:
  - rule id or file/object scope,
  - rationale,
  - owner,
  - approval date,
  - planned removal/review date.
- Exceptions must be allowlisted and narrowly scoped.

## Open Questions
- Should exception rules be represented as a structured local JSON file (recommended) or markdown table?
