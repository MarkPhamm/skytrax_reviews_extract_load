-- Bulk load ALL processed CSVs from S3 into Snowflake.
-- Snowflake tracks loaded files, so re-running is safe (no duplicates).
--
-- Usage: run manually in Snowflake UI or SnowSQL for initial/backfill loads.

COPY INTO SKYTRAX_REVIEWS_DB.RAW.AIRLINE_REVIEWS
FROM @SKYTRAX_REVIEWS_DB.RAW.SKYTRAX_S3_STAGE/processed/
ON_ERROR = 'CONTINUE'
PURGE    = FALSE;
