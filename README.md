# Skytrax Reviews Extract-Load Pipeline

Production-grade EL pipeline that scrapes 60,000+ airline reviews from [AirlineQuality.com](https://www.airlinequality.com/), stages partitioned data to S3, and loads into Snowflake for analytics.

- **26 parallel scraping tasks** (A-Z) with dynamic task mapping and dataset-driven DAG triggers
- **Infrastructure as Code** — S3 bucket (versioning, encryption, lifecycle policies), IAM roles/users with least-privilege access, Snowflake database/schema/table/S3 external stage — all managed with Terraform
- **Three-DAG pipeline** — crawl, process, load — chained via Airflow Datasets

## Architecture

```text
                    ┌──────────────────────────────┐
                    │   airlinequality.com          │
                    └──────────────┬───────────────┘
                                   │ scrape (26 A-Z tasks)
                                   ▼
                    ┌──────────────────────────────┐
                    │   S3: raw/YYYY/MM/           │
                    │   raw_data_YYYYMMDD.csv      │
                    └──────────────┬───────────────┘
                                   │ clean + transform
                                   ▼
                    ┌──────────────────────────────┐
                    │   S3: processed/YYYY/MM/     │
                    │   clean_data_YYYYMMDD.csv    │
                    └──────────────┬───────────────┘
                                   │ COPY INTO
                                   ▼
                    ┌──────────────────────────────┐
                    │   Snowflake                   │
                    │   SKYTRAX_REVIEWS_DB.RAW      │
                    │   .AIRLINE_REVIEWS             │
                    └──────────────────────────────┘
```

## Stack

| Layer | Technology |
| ----- | ---------- |
| Orchestrator | Apache Airflow (Astronomer Runtime, Docker) |
| Storage | AWS S3 (landing zone) → Snowflake |
| IaC | Terraform (AWS + Snowflake) |
| Language | Python 3.12, pandas, BeautifulSoup |

## DAGs

| DAG | Trigger | What it does |
| --- | ------- | ------------ |
| `skytrax_crawl` | Daily 02:00 UTC | Scrapes reviews, splits by date, uploads raw CSVs to S3 |
| `skytrax_process` | Dataset (raw) | Downloads raw CSVs, cleans/transforms, uploads processed CSVs |
| `skytrax_snowflake` | Dataset (processed) | Runs COPY INTO Snowflake for each review date |

## Getting Started

Follow these guides in order:

### 1. [Local Development](docs/local-dev.md)

Run the scraper and processing pipeline locally without any cloud services. Good for testing and development.

### 2. [Terraform & AWS Setup](docs/terraform.md)

Provision the S3 bucket, IAM role/user, and Snowflake resources with Terraform. Required before running the full pipeline.

### 3. [Airflow Setup](docs/airflow.md)

Configure Airflow connections, start the Astronomer environment, and run the full pipeline end-to-end.

### 4. [Snowflake](docs/snowflake.md)

Load data into Snowflake — both incremental (via DAG) and bulk backfill.

## Quick Reference

```bash
# Local smoke test (no AWS/Snowflake needed)
uv sync
STORAGE_MODE=local make scrape-smoke

# Run tests
make test

# Start Airflow
astro dev start

# Full infrastructure setup
cd terraform && terraform init && terraform apply
```

## Directory Layout

```text
dags/                  DAG definitions (no business logic)
include/
  tasks/
    extract/           Scraper → landing/raw/
    transform/         Cleaning pipeline → landing/processed/
    load/              S3 upload + Snowflake COPY INTO
  sql/                 SQL templates
terraform/             S3 bucket, IAM, Snowflake resources
tests/                 Unit tests
docs/                  Setup guides
landing/               Local data directory (CSVs gitignored)
```
