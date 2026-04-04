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
