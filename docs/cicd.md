# CI Pipeline

This project uses GitHub Actions for continuous integration only. There is no CD — deployment is manual via `astro deploy` or Terraform.

## What runs on CI

On every push to `main` and on pull requests:

1. **Lint** — flake8 / ruff
1. **Tests** — `uv run pytest tests/ -v`
1. **DAG validation** — Airflow parses all DAGs to catch import errors

## Required GitHub Secrets

| Secret | Description |
| ------ | ----------- |
| `AWS_ACCESS_KEY_ID` | For any AWS calls in tests |
| `AWS_SECRET_ACCESS_KEY` | For any AWS calls in tests |
| `SNOWFLAKE_ACCOUNT` | Snowflake account identifier (`ORG-ACCOUNT`) |
| `SNOWFLAKE_USER` | Snowflake user |
| `SNOWFLAKE_PASSWORD` | Snowflake password |

Add secrets under **Settings → Secrets and variables → Actions**.

## Deployment (manual)

```bash
# Deploy Airflow DAGs to Astronomer
astro deploy

# Provision / update infrastructure
cd terraform
terraform apply
```
