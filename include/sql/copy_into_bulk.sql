-- Bulk load ALL processed CSVs from S3 into Snowflake, one COPY per review type.
-- Each type's processed files live under processed/<type>/ and load into its own table.
-- Snowflake tracks loaded files, so re-running is safe (no duplicates).
--
-- Usage: run manually in Snowflake UI or SnowSQL for initial/backfill loads.

COPY INTO SKYTRAX_REVIEWS_DB.RAW.AIRLINE_REVIEWS
FROM @SKYTRAX_REVIEWS_DB.RAW.SKYTRAX_S3_STAGE/processed/airlines/
ON_ERROR = 'CONTINUE'
PURGE    = FALSE;

COPY INTO SKYTRAX_REVIEWS_DB.RAW.SEAT_REVIEWS
FROM @SKYTRAX_REVIEWS_DB.RAW.SKYTRAX_S3_STAGE/processed/seats/
ON_ERROR = 'CONTINUE'
PURGE    = FALSE;

COPY INTO SKYTRAX_REVIEWS_DB.RAW.LOUNGE_REVIEWS
FROM @SKYTRAX_REVIEWS_DB.RAW.SKYTRAX_S3_STAGE/processed/lounges/
ON_ERROR = 'CONTINUE'
PURGE    = FALSE;

COPY INTO SKYTRAX_REVIEWS_DB.RAW.AIRPORT_REVIEWS
FROM @SKYTRAX_REVIEWS_DB.RAW.SKYTRAX_S3_STAGE/processed/airports/
ON_ERROR = 'CONTINUE'
PURGE    = FALSE;
