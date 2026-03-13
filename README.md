# Skytrax Reviews Extract-Load Pipeline

EL pipeline that scrapes airline reviews from [AirlineQuality.com](https://www.airlinequality.com/), stages data to S3, and loads into Snowflake. Orchestrated with Apache Airflow on Astronomer.

## Stack

- **Orchestrator**: Apache Airflow (Astronomer Runtime, Docker)
- **Storage**: AWS S3 (landing zone) + Snowflake (`SKYTRAX_REVIEWS_DB.RAW.AIRLINE_REVIEWS`)
- **IaC**: Terraform (S3 bucket, IAM roles, Snowflake resources)
- **Language**: Python 3.12, pandas, BeautifulSoup

## Data Flow

```text
Scrape (26 A-Z tasks)
  → split by review date
  → upload to S3 raw/YYYY/MM/raw_data_YYYYMMDD.csv
  → Airflow pulls from S3, cleans/transforms
  → upload to S3 processed/YYYY/MM/clean_data_YYYYMMDD.csv
  → COPY INTO Snowflake SKYTRAX_REVIEWS_DB.RAW.AIRLINE_REVIEWS
```

## DAGs

| DAG | Schedule | Notes |
| --- | -------- | ----- |
| `skytrax_crawl` | Daily 02:00 UTC | Param `full_scrape=True` for initial load |
| `skytrax_process` | Dataset | Triggered when crawl uploads raw files |
| `skytrax_snowflake` | Dataset | Triggered when process uploads cleaned files |

## Quickstart (local dev)

```bash
# Install dependencies
uv sync

# Smoke test (1 airline, 1 page)
make scrape-smoke

# Full scrape (all airlines)
make scrape

# Run tests
make test
```

Set `STORAGE_MODE=local` to skip all S3/Snowflake calls during local development.

## Airflow Setup

1. Start Airflow: `astro dev start`
1. Set Airflow Variables:

   | Variable | Description |
   | -------- | ----------- |
   | `STORAGE_MODE` | `local` or `s3` |
   | `S3_BUCKET` | S3 bucket name |
   | `SCRAPER_WORKERS` | Parallel scraper threads |
   | `AWS_CONN_ID` | Airflow AWS connection ID |

1. Set Airflow connections: `aws_s3_connection`, `snowflake_default`

## Terraform

```bash
cd terraform
cp terraform.tfvars.example terraform.tfvars  # fill in values
export SNOWFLAKE_PASSWORD=yourpassword
terraform init
terraform apply
```

Resources provisioned: S3 bucket (versioned, encrypted, lifecycle rules), Airflow IAM role (S3 read/write), analyst IAM role (S3 read-only), Snowflake database/schema/table/stage.
