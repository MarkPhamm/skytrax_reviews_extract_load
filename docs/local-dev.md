# Local Development

Run the scraper and processing pipeline on your machine without any cloud services.

## Prerequisites

- Python 3.12+
- [uv](https://docs.astral.sh/uv/) (Python package manager)

## Step 1: Set up environment

Copy the example env file and fill in your values (for local-only development, the defaults work as-is):

```bash
cp .env.example .env
```

## Step 2: Install dependencies

```bash
uv sync
```

## Step 3: Run a smoke test

Scrape 1 airline, 1 page (~100 rows). Output goes to `landing/raw/`:

```bash
make scrape-smoke
```

Verify the output:

```bash
ls landing/raw/
# You should see YYYY/MM/raw_data_YYYYMMDD.csv files
```

## Step 4: Process the raw data

Process a specific date's raw file into a cleaned CSV:

```bash
make process DATE=2026-03-12
```

Or process yesterday's data:

```bash
make process-yesterday
```

Output goes to `landing/processed/YYYY/MM/clean_data_YYYYMMDD.csv`.

## Step 5: Run a full scrape (optional)

Scrape all airlines across all pages. This takes a while:

```bash
make scrape
```

## Step 6: Run tests

```bash
make test
```

## Environment variables

| Variable | Default | Description |
| -------- | ------- | ----------- |
| `STORAGE_MODE` | `local` | Set to `local` to skip all S3/Snowflake calls |

When `STORAGE_MODE=local`, the pipeline reads and writes only to the `landing/` directory. No AWS or Snowflake credentials are needed.

## Linting

```bash
# Check for issues
make lint

# Auto-fix (isort + black)
make lint-fix

# Install pre-commit hook
make install-hooks
```

## Next step

When you're ready to deploy to S3 and Snowflake, follow the [Terraform setup guide](terraform.md).
