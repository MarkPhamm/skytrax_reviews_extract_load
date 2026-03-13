# Skytrax Reviews Extract-Load Pipeline — Claude Instructions

## Project Overview

Apache Airflow (Astronomer) EL pipeline: scrapes airline reviews from AirlineQuality.com,
stages to S3 (multi-directory), loads into Snowflake.

## Architecture

- **Orchestrator**: Airflow on Astronomer Runtime (Docker)
- **Scraper output**: `/landing/` directory in project root (persisted, not cleaned between runs)
- **Local landing layout**:
  - `landing/raw/YYYY/MM/raw_data_YYYYMMDD.csv`
  - `landing/processed/YYYY/MM/clean_data_YYYYMMDD.csv`
- **S3 staging mirrors the same layout**:
  - `raw/YYYY/MM/raw_data_YYYYMMDD.csv`
  - `processed/YYYY/MM/clean_data_YYYYMMDD.csv`
- **Warehouse**: Snowflake — `SKYTRAX_REVIEWS_DB.RAW.AIRLINE_REVIEWS`
- **IaC**: Terraform in `terraform/` (S3 bucket + IAM role for Airflow)

## Code Conventions

- Use **TaskFlow API** (`@task` decorator) — no BashOperators
- All task functions must be in `include/tasks/`, imported into DAG
- SQL goes in `include/sql/*.sql` — never inline SQL in Python
- S3 paths must use `YYYY/MM/DD` partition scheme keyed on `{{ ds }}`
- Airflow connections: `aws_s3_connection`, `snowflake_default`
- No hardcoded credentials — always pull from Airflow connections or env vars

## Directory Layout

```text
.claude/          ← Claude project instructions
landing/          ← persisted raw scraper output (gitkeep committed, CSVs gitignored)
dags/             ← DAG definitions only (no business logic)
include/
  tasks/
    extract/      ← scraper writes to /landing
    transform/    ← reads from /landing, outputs to /landing/processed
    load/         ← S3 upload + Snowflake load
  sql/            ← SQL templates
terraform/        ← S3 bucket, IAM role/policy
tests/
```

## Terraform

- State: local (dev), remote backend TBD for prod
- Resources: `aws_s3_bucket`, `aws_iam_role`, `aws_iam_policy`, `aws_iam_role_policy_attachment`
- Variables file: `terraform/variables.tf` — override with `terraform.tfvars` (gitignored)

## What NOT to do

- Do not use `BashOperator` to run Python scripts — use `PythonOperator` or `@task`
- Do not use `cd` in any scripts — use absolute paths or `pathlib`
- Do not commit CSV files or `terraform.tfvars`
- Do not hardcode bucket names — use Airflow Variables or Terraform outputs
