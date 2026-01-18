# README — main branch

Purpose
This is the canonical/main branch for the repository. It should contain the stable, production-ready combination of the project’s artifacts (either the canonical DWH approach, or a pointer to the chosen implementation branch), plus high-level documentation and release/artifact information.

Current status
At inspection time the repository contains only a single-line README (`# Tp_DWH`). Use this README to present project overview and direct contributors to feature branches.

Suggested structure
- README.md (this file) — high-level overview and links
- docs/ — detailed architecture, onboarding, runbooks
- ci/ — CI workflows and templates
- changelog.md — versioning and release notes

Recommended content for this README
- Project purpose and summary of branches:
  - main — stable releases / overall project documentation
  - aws_tp — AWS-native implementation (infrastructure & pipelines)
  - lakeHouse — Lakehouse implementation (Delta/Iceberg on object storage)
  - pentahoTp — Pentaho-specific artifacts and ETL jobs
- Quickstart — how to get started locally and how to pick a branch
- Contributing — branching, PR, and review guidelines
- Contacts — owners and maintainers
- License & code of conduct (if any)

Actions to make main useful
- Consolidate cross-branch docs under `docs/`.
- Add a contribution guide and a release process.
- Provide links to architecture diagrams and runbooks.

Next steps
- Populate `docs/` with architecture.md and onboarding.md.
- Add CI to protect main and automatically validate PRs.
