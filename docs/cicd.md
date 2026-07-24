# CI Pipeline

This project uses GitHub Actions for continuous integration only. There is no CD — deployment is manual via `astro deploy` or Terraform.

## What runs on CI

On every push to `main` and on pull requests:

1. **Lint** — `flake8` restricted to fatal error codes (`E9,F63,F7,F82`): syntax errors and undefined names, not style nits (style is enforced locally by the pre-commit hook)
1. **Tests** — `uv run pytest tests/ -v`, all task logic mocked (no live Airflow/S3/Snowflake)
1. **DAG validation** — part of the pytest run: `tests/dags/test_dag_integrity.py` builds a `DagBag` from `dags/` with Airflow installed via the dev extra (pinned to the Astro Runtime 3.0-x line), so a DAG import error fails CI instead of surfacing in the Airflow UI after deploy
1. **Terraform** — parallel job over `terraform/aws` and `terraform/snowflake`: `terraform fmt -check` + `terraform init -backend=false` + `terraform validate` (no apply; no cloud credentials)

## Secrets

CI needs **no cloud credentials** — every test mocks its AWS/Snowflake boundary, and nothing in the pipeline deploys from CI. If a future workflow step needs secrets, add them under **Settings → Secrets and variables → Actions**.

## Deployment (manual)

```bash
# Deploy Airflow DAGs to Astronomer
astro deploy

# Provision / update infrastructure (two independent root modules)
cd terraform/aws && terraform apply
cd terraform/snowflake && terraform apply
```
