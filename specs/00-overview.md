# Overview

Status: draft
Last updated: 2026-03-08

## Purpose
Define an evolvable specification for a cross-platform CLI that provides state-based SQL Server schema change tracking.

## Principles
- State-based: schema folder is the source of truth.
- Compatible with existing project structures and scripting conventions.
- Deterministic: stable output for object scripts and diffs.
- Minimal surprises: commands are explicit and reversible.
- Verb-first CLI: git-style commands (init/status/diff/pull).
- Cross-platform: works on Windows, macOS, Linux.
- Git-agnostic: integrates with any Git tooling, no embedded Git operations.

## Goals
- Link a SQL Server database to a schema folder in a repo.
- Show status and diffs between DB and folder.
- Generate scripts to apply changes in either direction.
- Pull DB state into folder and push folder state to DB.
- Produce a schema folder structure compatible with existing source-control workflows.

## Non-Goals (Initial)
- Migration-based workflows.
- Data compare/sync.
- Cross-DB support beyond SQL Server.
- UI beyond CLI.

## Terminology
- DB: SQL Server database instance.
- Folder: local schema folder containing object scripts.
- Link: config binding DB to folder.
- Source/Target: direction of change or comparison.

