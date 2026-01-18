# README — lakeHouse branch

Purpose
This branch should host the Lakehouse-based implementation of the warehouse (Delta Lake / Iceberg / Hive on S3 or similar). It is intended for code and configuration that focus on a lakehouse architecture rather than a classic DWH.

Current status
Repository branches currently contain only a minimal README; this is a template README to guide adding lakehouse artifacts.

Recommended structure
- storage/
  - s3/ — bucket layout & lifecycle rules
  - partitioning.md — partition and layout strategy
- processing/
  - spark_jobs/ — Spark/Databricks notebooks or jobs (Python/Scala)
  - delta/ or iceberg/ — table creation scripts and schema management
- orchestration/
  - airflow/ or dagster/ — DAGs that orchestrate ingestion/transforms
- metadata/
  - hive_metastore/ or glue_catalog/ — catalog migration and registration scripts
- docs/
  - architecture.md — design rationale for lakehouse
  - data_contracts/ — schemas and producer/consumer contracts

How to use (suggested)
1. Storage layout
   - Document S3 prefixes for raw, bronze, silver, and gold zones.
   - Provide lifecycle and access policies.
2. Jobs
   - Keep modular Spark jobs under `processing/spark_jobs/`.
   - Use reproducible dependency management (poetry/requirements).
3. Catalog
   - Provide scripts to register Delta/Iceberg tables in the metastore.

Development and testing
- Local Spark testing with `pyspark` or `spark-local`.
- Use unit tests and job integration tests using small sample datasets.
- CI: run static checks, unit tests, and minimal job smoke tests.

Operational guidance
- Document refresh cadence and retention for each zone.
- Provide a rollback plan for metadata schema changes.

Next steps
- Add sample Spark job and a small dataset for integration testing.
- Document how to register tables in the chosen catalog (Glue/Hive).
