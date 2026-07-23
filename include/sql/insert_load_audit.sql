INSERT INTO SKYTRAX_REVIEWS_DB.RAW.LOAD_AUDIT
    (category, review_date, s3_key, target_table, status,
     rows_parsed, rows_loaded, errors_seen, first_error)
VALUES
    (%(category)s, %(review_date)s, %(s3_key)s, %(target_table)s, %(status)s,
     %(rows_parsed)s, %(rows_loaded)s, %(errors_seen)s, %(first_error)s);
