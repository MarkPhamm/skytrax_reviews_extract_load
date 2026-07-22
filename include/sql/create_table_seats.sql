CREATE DATABASE IF NOT EXISTS SKYTRAX_REVIEWS_DB;
CREATE SCHEMA IF NOT EXISTS SKYTRAX_REVIEWS_DB.RAW;

CREATE TABLE IF NOT EXISTS SKYTRAX_REVIEWS_DB.RAW.SEAT_REVIEWS (
    verify              BOOLEAN,
    date_submitted      DATE,
    date_flown          DATE,
    customer_name       STRING,
    nationality         STRING,
    airline_name        STRING,
    type_of_traveller   STRING,
    seat_type           STRING,
    aircraft_type       STRING,
    seat_layout         STRING,
    seat_legroom        INT,
    seat_recline        INT,
    seat_width          INT,
    aisle_space         INT,
    seat_storage        INT,
    power_supply        INT,
    viewing_tv_screen   INT,
    sleep_comfort       INT,
    sitting_comfort     INT,
    seat_bed_width      INT,
    seat_bed_length     INT,
    seat_privacy        INT,
    recommended         BOOLEAN,
    review              STRING,
    updated_at          TIMESTAMP_NTZ
);
