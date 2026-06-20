CREATE DATABASE IF NOT EXISTS SKYTRAX_REVIEWS_DB;
CREATE SCHEMA IF NOT EXISTS SKYTRAX_REVIEWS_DB.RAW;

CREATE TABLE IF NOT EXISTS SKYTRAX_REVIEWS_DB.RAW.AIRLINE_REVIEWS (
    verify                  BOOLEAN,
    date_submitted          DATE,
    date_flown              DATE,
    customer_name           STRING,
    nationality             STRING,
    airline_name            STRING,
    type_of_traveller       STRING,
    seat_type               STRING,
    aircraft                STRING,
    origin_city             STRING,
    origin_airport          STRING,
    destination_city        STRING,
    destination_airport     STRING,
    transit_city            STRING,
    transit_airport         STRING,
    seat_comfort            INT,
    cabin_staff_service     INT,
    food_and_beverages      INT,
    inflight_entertainment  INT,
    ground_service          INT,
    wifi_and_connectivity   INT,
    value_for_money         INT,
    recommended             BOOLEAN,
    review                  STRING,
    updated_at              TIMESTAMP_NTZ
);
