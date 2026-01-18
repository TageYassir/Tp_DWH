# README — aws_tp branch

Purpose
This branch is intended for the AWS-based Data Warehouse implementation (infrastructure and pipelines that run on AWS). It should contain infrastructure-as-code, deployment scripts, and any AWS-specific ETL/ingestion and orchestration code.

Current status
During inspection this repository's branches had minimal contents (only a top-level README). Use this README as a starting template to document and organize AWS-specific work.

Recommended structure
- infra/
  - terraform/ or cloudformation/ — IaC for AWS resources (S3, Glue, EMR, RDS/Aurora, Redshift, IAM, VPC)
  - scripts/ — helper deployment scripts, bootstrap scripts
- pipelines/
  - glue_jobs/ or airflow/ — ETL code, job definitions, DAGs
  - lambda/ — small processing functions if used
- data_models/
  - sql/ — table DDL and view definitions for the DWH in Redshift/Athena
- monitoring/
  - cloudwatch/ — dashboards and alarm definitions
- docs/
  - design.md — architecture diagrams and decisions
  - runbooks.md — deployment/runbook instructions

How to use (suggested)
1. Infrastructure
   - Store IaC modules under `infra/terraform`.
   - Use remote state (S3 + DynamoDB) for Terraform locking.
   - Provide a `terraform.tfvars.example` with required variables.
2. Pipelines
   - Keep Glue job code or Lambda code in `pipelines/`.
   - Provide a requirements.txt or pyproject.toml for Python dependencies.
3. Deploy
   - Provide a `deploy/` script or Makefile that bootstraps environment, runs Terraform, and deploys pipeline packages.

Development and testing
- Local test: use `localstack` for lightweight AWS integration testing.
- Unit tests: place Python tests under `pipelines/tests/` and run with pytest.
- CI: add GitHub Actions workflows to run terraform fmt/validate, python tests, and linting.

Security & best practices
- Do not commit secrets — use AWS Secrets Manager, SSM, or CI secrets.
- Use least-privilege IAM roles for services.
- Tag AWS resources consistently.

Next steps
- Add concrete IaC and pipeline code to the recommended directories.
- Add environment-specific example variables and a deployment walkthrough.
