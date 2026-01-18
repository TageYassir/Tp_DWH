# README — pentahoTp branch

Purpose
This branch is intended for Pentaho-specific ETL artifacts (Transformations and Jobs from PDI / Pentaho Data Integration) and any Pentaho deployment/configuration needed to run the DWH pipelines.

Current status
No Pentaho artifacts were present in the inspected branches; this README is a template to help structure and document Pentaho deliverables.

Recommended structure
- pentaho/
  - transformations/ — .ktr files (transformations)
  - jobs/ — .kjb files (jobs)
  - resources/ — kettle.properties example, connection configs (example only, no secrets)
- deploy/
  - scripts/ — deployment scripts to push packages to the Pentaho server
- docs/
  - pentaho_setup.md — how to install/configure PDI and related server components
  - conventions.md — naming conventions for jobs and transformations

How to use
1. Store Pentaho XML artifacts under `pentaho/`.
2. Use `kettle.properties.example` for connection placeholders.
3. Include packaging scripts (zip or war) if you deploy to a Pentaho server.

Development and testing
- Use local PDI client to test transformations and jobs.
- Keep small sample datasets for reproducible testing.
- Document expected versions of PDI and plugins.

Operational notes
- Provide runbook for monitoring and restart procedures.
- Keep backups of production .ktr and .kjb files.

Next steps
- Add a sample job and transformation.
- Add a `pentaho_setup.md` with installation steps and configuration examples.
