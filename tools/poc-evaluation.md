# POC Evaluation Plan

Status: draft
Last updated: 2026-03-10

## Goal
Evaluate scripting approaches for reference output fidelity.

## Candidates
1) SqlClient + custom scripting (catalog queries + manual formatting).
2) SMO scripting (Microsoft.SqlServer.Smo).
3) DacFx extract + script (Microsoft.SqlServer.Dac).

## Inputs
- Gold standard fixtures (MVP planned): `specs/fixtures/standard` (future, not yet present).
- Local reference export: from `local/fixtures.local.json` (`defaultSource` + `sources`, not committed).
- Representative object set (choose locally):
  - Tables: 3 with constraints, extended properties, and grants.
  - Views: 2.
  - Stored procedures: 2.
  - Functions: 1.
  - Sequence: 1.

## How to Pick/Rotate Objects
- Prefer objects that exercise constraints, permissions, and extended properties.
- Rotate a subset each run to catch formatting drift on different object types.
- Keep the chosen list in a local-only file under `local/fixtures/outputs/`.
- Use `local/fixtures/outputs/poc-objects-adventureworks.md` for AdventureWorks runs.

## Success Criteria
- Folder layout matches reference.
- Script content matches reference byte-for-byte for selected objects.
- SET options, GO separators, GRANTs, and extended properties match order.
- Any exception is explicit, manually approved, and documented as an allowlisted rule (never implicit normalization).

## Evaluation Steps
1) Copy the selected reference scripts into a local comparison set (outside Git).
2) Optional preflight: verify DB connectivity and module settings using `sqlcmd` or SqlClient.
3) Run each candidate approach to generate scripts for the same objects.
4) Compare with strict byte-for-byte rules using the compatibility harness (`tools/compat-harness.ps1`).
5) For approved deviations, record manual exceptions/overrides/ignores as explicit allowlist entries and keep them local-only.
6) Record differences and the minimal rule changes required.

## Scoring
- Exact match rate (% objects matching byte-for-byte).
- Systematic diffs (e.g., missing SET options).
- Effort estimate to fix (low/medium/high), including whether exceptions are temporary or acceptable for MVP.

## Decision Rule
- If SqlClient + custom scripting achieves >= 90% exact matches with low/medium effort fixes, proceed.
- If SMO/DacFx significantly outperforms with lower effort, consider switching.

## Recording Template
```
Candidate:
Objects tested:
Exact matches:
Systematic diffs:
Exceptions/overrides/ignores:
Effort estimate:
Decision:
Notes:
```
