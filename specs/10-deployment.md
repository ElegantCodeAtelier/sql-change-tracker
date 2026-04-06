# Deployment

Status: draft
Last updated: 2026-01-08

## Goal
Distribute the CLI as a .NET tool for cross-platform usage.

## Package Type
- .NET global tool and local tool (tool manifest).

## Install
Global:
`
dotnet tool install --global sqlct
`

Local (repo scoped):
`
dotnet new tool-manifest
dotnet tool install --local sqlct
`

## Update
`
dotnet tool update --global sqlct
`

## Uninstall
`
dotnet tool uninstall --global sqlct
`

## Notes
- Package as a NuGet tool package with a stable command name (`sqlct`).
- Keep versioning aligned with CLI changes and specs.

## Local Packaging Test
1) `dotnet pack src/SqlChangeTracker/SqlChangeTracker.csproj -c Release -o ./artifacts`
2) `dotnet new tool-manifest` (if not already present)
3) `dotnet tool install --local sqlct --add-source ./artifacts`
4) `dotnet tool list --local` to confirm `sqlct`
5) `dotnet tool uninstall --local sqlct` when finished

Keep generated artifacts out of Git.

## CI Publish (NuGet)
- Workflow: `.github/workflows/nuget-publish.yml`.
- Trigger: push a tag like `v1.2.3` or run `workflow_dispatch`.
- Secrets: set `NUGET_API_KEY` in GitHub repository secrets.
- Version: workflow strips the leading `v` from the tag and passes it to `dotnet pack`.

## Release Checklist
- [ ] Bump `Version` in `src/SqlChangeTracker/SqlChangeTracker.csproj`.
- [ ] Promote `[Unreleased]` in `CHANGELOG.md` to `[X.Y.Z] - YYYY-MM-DD` and add a new empty `[Unreleased]` section above it.
- [ ] Run `dotnet test tests/SqlChangeTracker.Tests/SqlChangeTracker.Tests.csproj`.
- [ ] Confirm `NUGET_API_KEY` is set in GitHub Actions secrets.
- [ ] Tag and push: `git tag vX.Y.Z` then `git push origin vX.Y.Z`.
- [ ] Verify NuGet package published and installable.
