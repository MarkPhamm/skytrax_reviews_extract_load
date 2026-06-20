CREATE DATABASE IF NOT EXISTS SKYTRAX_REVIEWS_DB;
CREATE SCHEMA IF NOT EXISTS SKYTRAX_REVIEWS_DB.RAW;

CREATE TABLE IF NOT EXISTS SKYTRAX_REVIEWS_DB.RAW.LOUNGE_REVIEWS (
    verify              BOOLEAN,
    date_submitted      DATE,
    date_visit          DATE,
    customer_name       STRING,
    nationality         STRING,
    airline_name        STRING,
    lounge_name         STRING,
    airport             STRING,
    type_of_lounge      STRING,
    type_of_traveller   STRING,
    comfort             INT,
    cleanliness         INT,
    bar_and_beverages   INT,
    catering            INT,
    washrooms           INT,
    wifi_connectivity   INT,
    staff_service       INT,
    recommended         BOOLEAN,
    review              STRING,
    updated_at          TIMESTAMP_NTZ
);
