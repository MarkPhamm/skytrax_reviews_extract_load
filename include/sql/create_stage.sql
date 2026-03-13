CREATE OR REPLACE STAGE SKYTRAX_REVIEWS_DB.RAW.SKYTRAX_S3_STAGE
    URL = 's3://{{ bucket }}/'
    CREDENTIALS = (
        AWS_ROLE = '{{ role_arn }}'
    )
    FILE_FORMAT = (
        TYPE                         = 'CSV'
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        SKIP_HEADER                  = 1
        FIELD_DELIMITER              = ','
        NULL_IF                      = ('', 'NULL', 'None')
        ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
    );
