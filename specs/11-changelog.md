# Changelog Strategy

Status: draft
Last updated: 2026-04-06

## Goal

Define a simple, consistent approach for maintaining a changelog that supports
release traceability, contributor guidance, and future release-readiness work.

## Chosen Approach

This repository uses a **manually curated `CHANGELOG.md`** at the repository
root, following the [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
format and [Semantic Versioning](https://semver.org/).

Rationale:
- Automated release-note generation requires structured commit conventions
  (e.g., Conventional Commits) that are not yet enforced; manual curation is
  more reliable at current maturity.
- A single human-readable file is easy to review, search, and link from GitHub
  Releases.
- Keep a Changelog is widely recognized and requires no tooling to adopt.
- The approach can be upgraded to hybrid automation later without restructuring
  the file format.

## File Location

`CHANGELOG.md` at the repository root.

## Format

```
# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
### Changed
### Deprecated
### Removed
### Fixed
### Security

## [X.Y.Z] - YYYY-MM-DD

### Added
- …
```

## Categories

Use only the categories that have entries. Omit empty headings.

| Category | Use for |
| --- | --- |
| **Added** | New features or commands visible to users |
| **Changed** | Changes to existing behavior, flags, or output formats |
| **Deprecated** | Features or flags that will be removed in a future version |
| **Removed** | Features or flags removed in this version |
| **Fixed** | Bug fixes |
| **Security** | Security-relevant fixes or mitigations |

## When to Update

| Event | Action |
| --- | --- |
| Pull request merged that adds/changes/fixes user-facing behavior | Add entry to `[Unreleased]` |
| Release tagged and published | Promote `[Unreleased]` to a versioned heading |
| Pre-release or internal-only changes (no user impact) | No entry required |

The author of the PR is responsible for adding the changelog entry. Reviewers
should verify an entry is present for user-facing changes.

## Entry Style

- Write entries in the imperative mood: "Add `--json` flag to `status` command."
- One line per change is preferred; use sub-bullets only when extra context is
  essential.
- Reference issue or PR numbers where useful: "(#42)"
- Focus on user-observable impact, not internal implementation details.

## Release Workflow

1. Review `[Unreleased]` and verify all merged user-facing changes are captured.
2. Replace `[Unreleased]` with the new version heading and today's date:
   `## [X.Y.Z] - YYYY-MM-DD`
3. Add a new empty `## [Unreleased]` section above it.
4. Commit the updated `CHANGELOG.md` as part of the release commit.
5. Add a link anchor at the bottom of the file for the new version (see
   Keep a Changelog link-reference section). After the first release the footer
   should look like:

   ```
   [Unreleased]: https://github.com/ElegantCodeAtelier/sql-change-tracker/compare/vX.Y.Z...HEAD
   [X.Y.Z]: https://github.com/ElegantCodeAtelier/sql-change-tracker/releases/tag/vX.Y.Z
   ```

## Backfill Recommendation

Because the repository is pre-v1 with limited public release history, a full
backfill is not required. The initial `CHANGELOG.md` captures current
`[Unreleased]` work that will become the first versioned entry at v1 release.
