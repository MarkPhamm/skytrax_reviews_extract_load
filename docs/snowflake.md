# Snowflake

How data gets into Snowflake and how to manage the table.

## How it works

Terraform owns the infrastructure:

- Database: `SKYTRAX_REVIEWS_DB`
- Schema: `RAW`
- Table: `AIRLINE_REVIEWS` (25 columns)
- S3 external stage: `SKYTRAX_S3_STAGE` (points to your S3 bucket, uses IAM role)

Python (Airflow) only runs `COPY INTO` to load data. It does not create or modify any Snowflake objects.

## Incremental loads (automatic)

The `skytrax_snowflake` DAG runs automatically after `skytrax_process` finishes. For each review date, it executes:

```sql
COPY INTO SKYTRAX_REVIEWS_DB.RAW.AIRLINE_REVIEWS
FROM @SKYTRAX_REVIEWS_DB.RAW.SKYTRAX_S3_STAGE/processed/YYYY/MM/clean_data_YYYYMMDD.csv
ON_ERROR = 'CONTINUE'
PURGE    = FALSE;
```

Snowflake tracks which files have been loaded, so re-running is safe (no duplicates).

## Bulk backfill

To load all processed CSVs at once (e.g., after the initial full scrape), run this in the Snowflake UI or SnowSQL:

```sql
COPY INTO SKYTRAX_REVIEWS_DB.RAW.AIRLINE_REVIEWS
FROM @SKYTRAX_REVIEWS_DB.RAW.SKYTRAX_S3_STAGE/processed/
ON_ERROR = 'CONTINUE'
PURGE    = FALSE;
```

This SQL is also available at [`include/sql/copy_into_bulk.sql`](../include/sql/copy_into_bulk.sql).

## Verify the load

```sql
-- Total row count
SELECT COUNT(*) FROM SKYTRAX_REVIEWS_DB.RAW.AIRLINE_REVIEWS;

-- Sample rows
SELECT * FROM SKYTRAX_REVIEWS_DB.RAW.AIRLINE_REVIEWS LIMIT 10;

-- Check load history
SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.COPY_HISTORY
WHERE TABLE_NAME = 'AIRLINE_REVIEWS'
ORDER BY LAST_LOAD_TIME DESC
LIMIT 20;
```

## Resetting the table

If you need to reload all data (e.g., after a column order fix):

```sql
-- 1. Drop the table
DROP TABLE SKYTRAX_REVIEWS_DB.RAW.AIRLINE_REVIEWS;
```

Then recreate it with Terraform:

```bash
cd terraform
terraform apply
```

Then re-run the bulk COPY INTO (see above).

## Column order

The table columns are defined in `terraform/snowflake.tf` and must match the CSV output order from the processing pipeline. The column order is:

```text
verify, date_submitted, date_flown, customer_name, nationality,
airline_name, type_of_traveller, seat_type, aircraft,
origin_city, origin_airport, destination_city, destination_airport,
transit_city, transit_airport,
seat_comfort, cabin_staff_service, food_and_beverages,
inflight_entertainment, ground_service, wifi_and_connectivity,
value_for_money, recommended, review, updated_at
```

`COPY INTO` loads by position (not by column name), so if the CSV column order changes, you must update the Snowflake table to match — which requires a DROP + recreate.
