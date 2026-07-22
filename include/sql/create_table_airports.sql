CREATE DATABASE IF NOT EXISTS SKYTRAX_REVIEWS_DB;
CREATE SCHEMA IF NOT EXISTS SKYTRAX_REVIEWS_DB.RAW;

CREATE TABLE IF NOT EXISTS SKYTRAX_REVIEWS_DB.RAW.AIRPORT_REVIEWS (
    verify                  BOOLEAN,
    date_submitted          DATE,
    date_visit              DATE,
    customer_name           STRING,
    nationality             STRING,
    airport_name            STRING,
    experience_at_airport   STRING,
    type_of_traveller       STRING,
    queuing_times           INT,
    terminal_cleanliness    INT,
    terminal_seating        INT,
    terminal_signs          INT,
    food_beverages          INT,
    airport_shopping        INT,
    airport_staff           INT,
    wifi_connectivity       INT,
    recommended             BOOLEAN,
    review                  STRING,
    updated_at              TIMESTAMP_NTZ
);
