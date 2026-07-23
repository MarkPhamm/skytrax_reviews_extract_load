# Skytrax Reviews Extract-Load Pipeline (Part 1)

![Airline](assets/images/airline.png)

At Insurify, I work with Airflow, Terraform, and AWS every day. Two years ago, I signed up for an AWS account and accidentally racked up a $21,000 bill just from spinning up Amazon QuickSight — thankfully the Billing team sorted it out. That experience taught me how easy it is to get burned by cloud services if you don't understand what you're provisioning.

This project is my attempt to break down the tools I use at work into something anyone can follow. It's an EL pipeline that scrapes 160,000+ reviews across four review types from [AirlineQuality.com](https://www.airlinequality.com/) — airlines, seats, lounges, and airports — stages type- and date-partitioned data to S3, and loads each type into its own Snowflake table, with every piece of infrastructure defined in Terraform so you know exactly what you're spinning up (and what it costs).

- **Four review types scraped in parallel** (airline, seat, lounge, airport) via Airflow dynamic task mapping — each type fans out into its own set of entities (airlines, seat classes, lounges, airports), scraped with entity-level thread-pool parallelism inside each task
- **Dataset-driven DAG chaining** — no cron guesswork between stages; each DAG triggers the next the moment its upstream data actually lands
- **Data quality gates** — every file is processed and uploaded first, *then* validated (schema / null-rate / rating-range); a date that fails is excluded from the Snowflake load rather than blocking the pipeline, and every `COPY INTO` is reconciled (rows parsed vs. rows loaded) into an auditable `LOAD_AUDIT` table
- **PII masking** — a tag-based Snowflake masking policy on `CUSTOMER_NAME`/`NATIONALITY`, so raw reviewer PII is masked for every role except an explicit `PII_READER`
- **Infrastructure as Code** — S3 bucket (versioning, encryption, lifecycle policies), IAM roles/users with least-privilege access, and every Snowflake object (database, schema, 4 review tables, `LOAD_AUDIT`, external stage, masking policy) — all managed with Terraform, nothing created ad hoc at runtime
- **Three-DAG pipeline** — crawl, process, load — chained via Airflow Datasets

## Architecture

```text
                    ┌──────────────────────────────┐
                    │   airlinequality.com         │
                    └──────────────┬───────────────┘
                                   │ scrape (4 types × per-entity parallelism)
                                   ▼
                    ┌──────────────────────────────┐
                    │   S3: raw/<type>/YYYY/MM/    │
                    │   raw_data_YYYYMMDD.csv      │
                    └──────────────┬───────────────┘
                                   │ clean/transform → upload → validate
                                   ▼
                    ┌──────────────────────────────┐
                    │  S3: processed/<type>/YYYY/MM/│
                    │   clean_data_YYYYMMDD.csv    │
                    └──────────────┬───────────────┘
                                   │ COPY INTO (skips quality-rejected dates)
                                   │ + post-load reconciliation
                                   ▼
                    ┌──────────────────────────────┐
                    │   Snowflake                  │
                    │   SKYTRAX_REVIEWS_DB.RAW     │
                    │   .AIRLINE_REVIEWS           │
                    │   .SEAT_REVIEWS              │
                    │   .LOUNGE_REVIEWS            │
                    │   .AIRPORT_REVIEWS           │
                    │   .LOAD_AUDIT                │
                    └──────────────────────────────┘
```

`<type>` is one of `airlines` / `seats` / `lounges` / `airports` — every review type flows through the same shape, one dedicated table each.

## Stack

| Layer | Technology |
| ----- | ---------- |
| Orchestrator | Apache Airflow (Astronomer Runtime, Docker) |
| Extraction | Python 3.12 — `requests` + BeautifulSoup, custom scraper (no API to hit) |
| Storage | AWS S3 (type/date-partitioned landing zone) → Snowflake |
| Data Governance | Post-upload quality validation + post-load reconciliation + tag-based PII masking policy |
| IaC | Terraform (two independent root modules: AWS, Snowflake) |
| CI | GitHub Actions — lint + pytest on every push/PR to `main` (see [docs/cicd.md](docs/cicd.md)) |

## S3 Bucket Structure

Data is partitioned **type-first, then by review date**:

```text
s3://skytrax-reviews-landing-<account-id>/
  raw/
    airlines/
      2024/
        01/
          raw_data_20240101.csv
          raw_data_20240102.csv
    seats/
      2024/01/raw_data_20240101.csv
    lounges/
      2024/01/raw_data_20240101.csv
    airports/
      2024/01/raw_data_20240101.csv
  processed/
    airlines/
      2024/01/clean_data_20240101.csv
    ...
```

The bucket contains two top-level prefixes — `raw/` for scraped data and `processed/` for cleaned data — each split into the four type sub-prefixes above:

![S3 Bucket](assets/aws/s3_dir.png)

Raw CSVs are written directly by the scraper, one file per (type, review date):

![S3 Raw](assets/aws/s3_raw.png)

Processed CSVs are the cleaned output, ready for Snowflake ingestion. Each is validated after upload (schema match, null-rate thresholds, star-rating range); a file that fails is left in S3 but its date is skipped at the Snowflake load step:

![S3 Processed](assets/aws/s3_processed.png)

- **Versioning** enabled — protects against accidental overwrites
- **AES256 encryption** — server-side encryption on all objects
- **Lifecycle rules** — transitions to Standard-IA after 30 days, expires old versions after 90 days
- **Public access blocked** — all public access is denied at the bucket level

Why stage to S3 at all instead of loading straight into Snowflake:

| Reason | Why it matters |
| ------ | --------------- |
| Reliability | If Snowflake is unavailable, data is safely stored in S3 instead of being lost. |
| Replayability | If a load fails or a transformation has a bug, you can reload the original files without re-scraping the source site. |
| Cost | S3 storage is far cheaper than storing raw historical files in Snowflake. |
| Decoupling | The scraper only writes to S3; the load step reads independently — a load-side bug never risks re-hitting the live site. |
| Auditability | The original raw files are always available for debugging or compliance. |

## Loading Strategy

**Incremental (daily)**: The `skytrax_crawl` DAG runs at **16:00 UTC**, scraping each of the 4 review types for reviews posted since the day before *that run's own logical date* (not wall-clock "yesterday" — so clearing and rerunning a specific past run recomputes the correct target day instead of silently re-fetching today's yesterday). Each (type, review date) maps to exactly one CSV file, so re-runs are idempotent — and `COPY INTO` at the Snowflake end additionally dedupes by file, so nothing double-loads.

**Bulk backfill**: Trigger `skytrax_crawl` with `full_scrape=True` to scrape each type's full review history. The scraper writes every row to the S3/local partition matching **that review's own date**, not the run date — so a multi-year backfill lands every file in the correct `YYYY/MM` partition regardless of when the scrape actually ran.

## Data Governance

- **Quality validation, non-blocking** (`include/tasks/common/quality.py`) — the pipeline processes and uploads *every* file unconditionally, then validates each one for schema drift, empty files, null-rate thresholds on required columns, and star ratings within `[1, 5]`. A file that fails isn't deleted or blocked — its date is recorded in a `QUALITY_REJECTED__<category>` Airflow Variable and excluded from the Snowflake load, so one bad day never stalls processing or holds up the other days. Only a genuine processing error (download/clean/upload) fails a task.
- **Post-load reconciliation** — `copy_into_bulk()` runs one `COPY INTO` per category (over the whole `processed/<type>/` prefix, or an explicit file list when excluding quality-rejected dates), then compares Snowflake's own result (`rows_parsed` vs. `rows_loaded`, `errors_seen`) and writes every load outcome to `RAW.LOAD_AUDIT`, a Terraform-managed table. A partial or rejected load fails the task loudly instead of silently under-loading.
- **PII masking** — `terraform/snowflake/masking.tf` tags `CUSTOMER_NAME`/`NATIONALITY` across all 4 tables with a `PII` tag bound to a masking policy; only `PII_READER` (or `ACCOUNTADMIN`) sees unmasked values.
- **Secrets hygiene** — `.env`, `terraform.tfvars`, and all `.tfstate` files are gitignored; no credentials are ever committed.

## DAGs

| DAG | Trigger | What it does |
| --- | ------- | ------------ |
| `skytrax_crawl` | Daily 16:00 UTC (or manual, `full_scrape=True` for a backfill) | Scrapes all 4 review types (dynamic task mapping over `airline`/`seat`/`lounge`/`airport`), splits by review date, uploads raw CSVs to S3 |
| `skytrax_process` | Dataset (raw) | One mapped task per category: downloads, cleans, and uploads every queued date to S3 in a thread pool, then validates each file and records any quality-rejected dates |
| `skytrax_snowflake` | Dataset (processed) | One `COPY INTO` per category (skipping quality-rejected dates), reconciles the result, and records every load in `RAW.LOAD_AUDIT` |

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

### 5. [CI](docs/cicd.md)

What runs on every push/PR (lint + tests) and what stays manual (deploy, `terraform apply`) — and why.

## Quick Reference

```bash
# Setup
cp .env.example .env   # edit with your values (defaults work for local dev)
uv sync

# Local smoke test (no AWS/Snowflake needed)
make scrape-smoke

# Process all raw data (or: make process DATE=2026-03-12)
make process

# Run tests
make test

# Start Airflow
astro dev start

# Full infrastructure setup
cd terraform/aws && terraform init && terraform apply
cd ../snowflake && terraform init && terraform apply
```

## Directory Layout

```text
dags/                  DAG definitions (no business logic)
include/
  tasks/
    extract/           Scraper → landing/raw/<type>/
    transform/         Cleaning → landing/processed/<type>/
    common/            Shared paths + post-upload quality validation
    load/              S3 upload + Snowflake COPY INTO + load reconciliation
  sql/                 SQL templates (tables are Terraform-managed, not created at runtime)
terraform/
  aws/                 S3 bucket, IAM role/user
  snowflake/           Database, schema, 4 review tables, LOAD_AUDIT, stage, PII masking policy
tests/                 Unit tests
docs/                  Setup guides
landing/               Local data directory (CSVs gitignored)
```
