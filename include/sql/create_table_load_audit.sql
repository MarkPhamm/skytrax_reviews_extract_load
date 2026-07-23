CREATE TABLE IF NOT EXISTS SKYTRAX_REVIEWS_DB.RAW.LOAD_AUDIT (
    load_ts       TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    category      VARCHAR,
    review_date   DATE,
    s3_key        VARCHAR,
    target_table  VARCHAR,
    status        VARCHAR,
    rows_parsed   NUMBER,
    rows_loaded   NUMBER,
    errors_seen   NUMBER,
    first_error   VARCHAR
);
