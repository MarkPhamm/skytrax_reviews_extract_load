# ---------------------------------------------------------------------------
# Snowflake — Database, Schema, Table, and S3 Stage
#
# Terraform owns the infrastructure; Python (dag_snowflake) runs COPY INTO.
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Database + Schema
# ---------------------------------------------------------------------------

resource "snowflake_database" "skytrax" {
  name = "SKYTRAX_REVIEWS_DB"
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.skytrax.name
  name     = "RAW"
}

# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------

resource "snowflake_table" "airline_reviews" {
  database = snowflake_database.skytrax.name
  schema   = snowflake_schema.raw.name
  name     = "AIRLINE_REVIEWS"

  # Column order matches CSV output from processing pipeline
  column {
    name = "VERIFY"
    type = "BOOLEAN"
  }
  column {
    name = "DATE_SUBMITTED"
    type = "DATE"
  }
  column {
    name = "DATE_FLOWN"
    type = "DATE"
  }
  column {
    name = "CUSTOMER_NAME"
    type = "VARCHAR"
  }
  column {
    name = "NATIONALITY"
    type = "VARCHAR"
  }
  column {
    name = "AIRLINE_NAME"
    type = "VARCHAR"
  }
  column {
    name = "TYPE_OF_TRAVELLER"
    type = "VARCHAR"
  }
  column {
    name = "SEAT_TYPE"
    type = "VARCHAR"
  }
  column {
    name = "AIRCRAFT"
    type = "VARCHAR"
  }
  column {
    name = "ORIGIN_CITY"
    type = "VARCHAR"
  }
  column {
    name = "ORIGIN_AIRPORT"
    type = "VARCHAR"
  }
  column {
    name = "DESTINATION_CITY"
    type = "VARCHAR"
  }
  column {
    name = "DESTINATION_AIRPORT"
    type = "VARCHAR"
  }
  column {
    name = "TRANSIT_CITY"
    type = "VARCHAR"
  }
  column {
    name = "TRANSIT_AIRPORT"
    type = "VARCHAR"
  }
  column {
    name = "SEAT_COMFORT"
    type = "NUMBER(38,0)"
  }
  column {
    name = "CABIN_STAFF_SERVICE"
    type = "NUMBER(38,0)"
  }
  column {
    name = "FOOD_AND_BEVERAGES"
    type = "NUMBER(38,0)"
  }
  column {
    name = "INFLIGHT_ENTERTAINMENT"
    type = "NUMBER(38,0)"
  }
  column {
    name = "GROUND_SERVICE"
    type = "NUMBER(38,0)"
  }
  column {
    name = "WIFI_AND_CONNECTIVITY"
    type = "NUMBER(38,0)"
  }
  column {
    name = "VALUE_FOR_MONEY"
    type = "NUMBER(38,0)"
  }
  column {
    name = "RECOMMENDED"
    type = "BOOLEAN"
  }
  column {
    name = "REVIEW"
    type = "VARCHAR"
  }
  column {
    name = "UPDATED_AT"
    type = "TIMESTAMP_NTZ"
  }
}

# ---------------------------------------------------------------------------
# Seat reviews table  (matches processing.CLEANING_PROFILES["seat"])
# ---------------------------------------------------------------------------

resource "snowflake_table" "seat_reviews" {
  database = snowflake_database.skytrax.name
  schema   = snowflake_schema.raw.name
  name     = "SEAT_REVIEWS"

  column {
    name = "VERIFY"
    type = "BOOLEAN"
  }
  column {
    name = "DATE_SUBMITTED"
    type = "DATE"
  }
  column {
    name = "DATE_FLOWN"
    type = "DATE"
  }
  column {
    name = "CUSTOMER_NAME"
    type = "VARCHAR"
  }
  column {
    name = "NATIONALITY"
    type = "VARCHAR"
  }
  column {
    name = "AIRLINE_NAME"
    type = "VARCHAR"
  }
  column {
    name = "TYPE_OF_TRAVELLER"
    type = "VARCHAR"
  }
  column {
    name = "SEAT_TYPE"
    type = "VARCHAR"
  }
  column {
    name = "AIRCRAFT_TYPE"
    type = "VARCHAR"
  }
  column {
    name = "SEAT_LAYOUT"
    type = "VARCHAR"
  }
  column {
    name = "SEAT_LEGROOM"
    type = "NUMBER(38,0)"
  }
  column {
    name = "SEAT_RECLINE"
    type = "NUMBER(38,0)"
  }
  column {
    name = "SEAT_WIDTH"
    type = "NUMBER(38,0)"
  }
  column {
    name = "AISLE_SPACE"
    type = "NUMBER(38,0)"
  }
  column {
    name = "SEAT_STORAGE"
    type = "NUMBER(38,0)"
  }
  column {
    name = "POWER_SUPPLY"
    type = "NUMBER(38,0)"
  }
  column {
    name = "VIEWING_TV_SCREEN"
    type = "NUMBER(38,0)"
  }
  column {
    name = "SLEEP_COMFORT"
    type = "NUMBER(38,0)"
  }
  column {
    name = "SITTING_COMFORT"
    type = "NUMBER(38,0)"
  }
  column {
    name = "SEAT_BED_WIDTH"
    type = "NUMBER(38,0)"
  }
  column {
    name = "SEAT_BED_LENGTH"
    type = "NUMBER(38,0)"
  }
  column {
    name = "SEAT_PRIVACY"
    type = "NUMBER(38,0)"
  }
  column {
    name = "RECOMMENDED"
    type = "BOOLEAN"
  }
  column {
    name = "REVIEW"
    type = "VARCHAR"
  }
  column {
    name = "UPDATED_AT"
    type = "TIMESTAMP_NTZ"
  }
}

# ---------------------------------------------------------------------------
# Lounge reviews table  (matches processing.CLEANING_PROFILES["lounge"])
# ---------------------------------------------------------------------------

resource "snowflake_table" "lounge_reviews" {
  database = snowflake_database.skytrax.name
  schema   = snowflake_schema.raw.name
  name     = "LOUNGE_REVIEWS"

  column {
    name = "VERIFY"
    type = "BOOLEAN"
  }
  column {
    name = "DATE_SUBMITTED"
    type = "DATE"
  }
  column {
    name = "DATE_VISIT"
    type = "DATE"
  }
  column {
    name = "CUSTOMER_NAME"
    type = "VARCHAR"
  }
  column {
    name = "NATIONALITY"
    type = "VARCHAR"
  }
  column {
    name = "AIRLINE_NAME"
    type = "VARCHAR"
  }
  column {
    name = "LOUNGE_NAME"
    type = "VARCHAR"
  }
  column {
    name = "AIRPORT"
    type = "VARCHAR"
  }
  column {
    name = "TYPE_OF_LOUNGE"
    type = "VARCHAR"
  }
  column {
    name = "TYPE_OF_TRAVELLER"
    type = "VARCHAR"
  }
  column {
    name = "COMFORT"
    type = "NUMBER(38,0)"
  }
  column {
    name = "CLEANLINESS"
    type = "NUMBER(38,0)"
  }
  column {
    name = "BAR_AND_BEVERAGES"
    type = "NUMBER(38,0)"
  }
  column {
    name = "CATERING"
    type = "NUMBER(38,0)"
  }
  column {
    name = "WASHROOMS"
    type = "NUMBER(38,0)"
  }
  column {
    name = "WIFI_CONNECTIVITY"
    type = "NUMBER(38,0)"
  }
  column {
    name = "STAFF_SERVICE"
    type = "NUMBER(38,0)"
  }
  column {
    name = "RECOMMENDED"
    type = "BOOLEAN"
  }
  column {
    name = "REVIEW"
    type = "VARCHAR"
  }
  column {
    name = "UPDATED_AT"
    type = "TIMESTAMP_NTZ"
  }
}

# ---------------------------------------------------------------------------
# Airport reviews table  (matches processing.CLEANING_PROFILES["airport"])
# ---------------------------------------------------------------------------

resource "snowflake_table" "airport_reviews" {
  database = snowflake_database.skytrax.name
  schema   = snowflake_schema.raw.name
  name     = "AIRPORT_REVIEWS"

  column {
    name = "VERIFY"
    type = "BOOLEAN"
  }
  column {
    name = "DATE_SUBMITTED"
    type = "DATE"
  }
  column {
    name = "DATE_VISIT"
    type = "DATE"
  }
  column {
    name = "CUSTOMER_NAME"
    type = "VARCHAR"
  }
  column {
    name = "NATIONALITY"
    type = "VARCHAR"
  }
  column {
    name = "AIRPORT_NAME"
    type = "VARCHAR"
  }
  column {
    name = "EXPERIENCE_AT_AIRPORT"
    type = "VARCHAR"
  }
  column {
    name = "TYPE_OF_TRAVELLER"
    type = "VARCHAR"
  }
  column {
    name = "QUEUING_TIMES"
    type = "NUMBER(38,0)"
  }
  column {
    name = "TERMINAL_CLEANLINESS"
    type = "NUMBER(38,0)"
  }
  column {
    name = "TERMINAL_SEATING"
    type = "NUMBER(38,0)"
  }
  column {
    name = "TERMINAL_SIGNS"
    type = "NUMBER(38,0)"
  }
  column {
    name = "FOOD_BEVERAGES"
    type = "NUMBER(38,0)"
  }
  column {
    name = "AIRPORT_SHOPPING"
    type = "NUMBER(38,0)"
  }
  column {
    name = "AIRPORT_STAFF"
    type = "NUMBER(38,0)"
  }
  column {
    name = "WIFI_CONNECTIVITY"
    type = "NUMBER(38,0)"
  }
  column {
    name = "RECOMMENDED"
    type = "BOOLEAN"
  }
  column {
    name = "REVIEW"
    type = "VARCHAR"
  }
  column {
    name = "UPDATED_AT"
    type = "TIMESTAMP_NTZ"
  }
}

# ---------------------------------------------------------------------------
# Load audit table  (written by include/tasks/load/snowflake_load.copy_into
# after every COPY INTO — reconciliation record, not raw review data)
# ---------------------------------------------------------------------------

resource "snowflake_table" "load_audit" {
  database = snowflake_database.skytrax.name
  schema   = snowflake_schema.raw.name
  name     = "LOAD_AUDIT"

  column {
    name = "LOAD_TS"
    type = "TIMESTAMP_NTZ"
    default {
      expression = "CURRENT_TIMESTAMP()"
    }
  }
  column {
    name = "CATEGORY"
    type = "VARCHAR"
  }
  column {
    name = "REVIEW_DATE"
    type = "DATE"
  }
  column {
    name = "S3_KEY"
    type = "VARCHAR"
  }
  column {
    name = "TARGET_TABLE"
    type = "VARCHAR"
  }
  column {
    name = "STATUS"
    type = "VARCHAR"
  }
  column {
    name = "ROWS_PARSED"
    type = "NUMBER(38,0)"
  }
  column {
    name = "ROWS_LOADED"
    type = "NUMBER(38,0)"
  }
  column {
    name = "ERRORS_SEEN"
    type = "NUMBER(38,0)"
  }
  column {
    name = "FIRST_ERROR"
    type = "VARCHAR"
  }
}

# ---------------------------------------------------------------------------
# S3 External Stage
# ---------------------------------------------------------------------------

resource "snowflake_stage" "s3" {
  database = snowflake_database.skytrax.name
  schema   = snowflake_schema.raw.name
  name     = "SKYTRAX_S3_STAGE"
  url      = "s3://${var.bucket_name}/"

  credentials = "AWS_ROLE = '${var.snowflake_s3_role_arn}'"

  file_format = <<-EOF
    TYPE                         = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER                  = 1
    FIELD_DELIMITER              = ','
    NULL_IF                      = ('', 'NULL', 'None')
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  EOF
}
