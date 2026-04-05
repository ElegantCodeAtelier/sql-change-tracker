# Agents

## Purpose
This file defines how automation and AI agents should operate in this repository.

## Project Focus
- Build and maintain a cross-platform .NET CLI (`sqlct`, SQL Change Tracker).
- Keep behavior aligned with repository specs and clean-room constraints.

## Spec-First Workflow
- Before coding, read `specs/README.md` and the specs relevant to the task.
- Use `specs/12-project-plan.md` for stream-specific tasks and acceptance criteria.
- Treat specs as authoritative over inferred behavior.

## Spec Authoring Style
- For files under `specs/`, prefer inline contextual references near the rule or statement they support.
- Avoid terminal `References` sections in specs unless an external standard/compliance requirement explicitly requires one.

## Local-Only Data
- Do not commit production database exports or artifacts.
- Keep local files under `local/fixtures/outputs/` and `local/fixtures.local.json` out of Git.
- Do not hardcode object names discovered from local compatibility or validation databases into tracked code, tests, docs, or specs.
- In tracked tests and docs, use neutral placeholder names or runtime discovery instead of names copied from local-only databases.

## POC and Validation Workflows
- Use local-only outputs for compatibility and POC runs.
- Record results in `local/fixtures/notes/poc-results.md` (local details stay in `local/fixtures/outputs/`).

## Safety
- Avoid destructive commands unless explicitly requested.
- Do not revert unrelated changes in the repository.

