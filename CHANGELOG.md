# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- `sqlct init` command — scaffolds a new schema-folder project with `sqlct.config.json` and folder structure.
- `sqlct config` command — validates and displays the current project configuration.
- `sqlct status` command — compares the live database against the local schema folder and reports added, changed, and deleted objects.
- `sqlct diff` command — shows unified diffs for individual objects or all changed objects.
- `sqlct pull` command — reconciles the local schema folder from the live database (create, update, delete files).
- Support for active object types: `Table`, `View`, `StoredProcedure`, `Function`, `Sequence`, `Schema`, `Role`, `User`, `Synonym`, `UserDefinedType`, `PartitionFunction`, `PartitionScheme`.
- Human-readable and JSON output modes for `status`, `diff`, and `pull`.
- Deterministic script ordering (dependency-aware where applicable).
- Parallelism option (`options.parallelism`) for database introspection; defaults to processor count when set to `0`.
- Extended property scripting support for database objects.
- Exit codes aligned with spec: `0` (no diffs), `1` (diffs present), `2`/`3`/`4` (failure categories).
- NuGet CI publish workflow triggered on version tag push.

### Changed
- Simplified runtime config contract to `database` and `options` (`orderByDependencies`, `parallelism`); removed `options.includeSchemas`, `options.excludeObjects`, and `options.comparison.*` (deferred to vNext).
