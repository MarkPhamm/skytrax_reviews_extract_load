provider "snowflake" {
  organization_name = var.snowflake_org
  account_name      = var.snowflake_account
  username          = var.snowflake_user
  # Password / key pulled from SNOWFLAKE_PASSWORD env var by the provider
  # Never put credentials in .tf files or tfvars — use env vars or a secrets manager
}

# ---------------------------------------------------------------------------
# Database + Schemas
# ---------------------------------------------------------------------------

resource "snowflake_database" "skytrax" {
  name    = "SKYTRAX_REVIEWS_DB"
  comment = "Skytrax airline reviews pipeline"
}

resource "snowflake_schema" "raw" {
  database = snowflake_database.skytrax.name
  name     = "RAW"
}

resource "snowflake_schema" "model" {
  database = snowflake_database.skytrax.name
  name     = "MODEL"
}

# ---------------------------------------------------------------------------
# Table
# ---------------------------------------------------------------------------

resource "snowflake_table" "airline_reviews" {
  database = snowflake_database.skytrax.name
  schema   = snowflake_schema.raw.name
  name     = "AIRLINE_REVIEWS"

  column {
    name = "airline_name"
    type = "STRING"
  }
  column {
    name = "date_submitted"
    type = "DATE"
  }
  column {
    name = "customer_name"
    type = "STRING"
  }
  column {
    name = "nationality"
    type = "STRING"
  }
  column {
    name = "verify"
    type = "BOOLEAN"
  }
  column {
    name = "review"
    type = "STRING"
  }
  column {
    name = "type_of_traveller"
    type = "STRING"
  }
  column {
    name = "seat_type"
    type = "STRING"
  }
  column {
    name = "date_flown"
    type = "DATE"
  }
  column {
    name = "seat_comfort"
    type = "INT"
  }
  column {
    name = "cabin_staff_service"
    type = "INT"
  }
  column {
    name = "food_and_beverages"
    type = "INT"
  }
  column {
    name = "inflight_entertainment"
    type = "INT"
  }
  column {
    name = "ground_service"
    type = "INT"
  }
  column {
    name = "wifi_and_connectivity"
    type = "INT"
  }
  column {
    name = "value_for_money"
    type = "INT"
  }
  column {
    name = "recommended"
    type = "BOOLEAN"
  }
  column {
    name = "aircraft"
    type = "STRING"
  }
  column {
    name = "origin_city"
    type = "STRING"
  }
  column {
    name = "origin_airport"
    type = "STRING"
  }
  column {
    name = "destination_city"
    type = "STRING"
  }
  column {
    name = "destination_airport"
    type = "STRING"
  }
  column {
    name = "transit_city"
    type = "STRING"
  }
  column {
    name = "transit_airport"
    type = "STRING"
  }
  column {
    name = "updated_at"
    type = "TIMESTAMP_NTZ"
  }
}

# ---------------------------------------------------------------------------
# S3 Storage Integration (lets Snowflake assume the Airflow IAM role)
# ---------------------------------------------------------------------------

resource "snowflake_storage_integration" "s3" {
  name    = "SKYTRAX_S3_INTEGRATION"
  type    = "EXTERNAL_STAGE"
  enabled = true

  storage_provider     = "S3"
  storage_aws_role_arn = aws_iam_role.airflow.arn
  storage_allowed_locations = ["s3://${aws_s3_bucket.landing.id}/"]
}

# ---------------------------------------------------------------------------
# External Stage
# ---------------------------------------------------------------------------

resource "snowflake_stage" "s3" {
  database = snowflake_database.skytrax.name
  schema   = snowflake_schema.raw.name
  name     = "SKYTRAX_S3_STAGE"
  url      = "s3://${aws_s3_bucket.landing.id}/"

  storage_integration = snowflake_storage_integration.s3.name

  file_format = <<-EOF
    TYPE                           = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY  = '"'
    SKIP_HEADER                   = 1
    FIELD_DELIMITER               = ','
    NULL_IF                       = ('', 'NULL', 'None')
    ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
  EOF
}

# ---------------------------------------------------------------------------
# Trust policy update — Snowflake needs permission to assume the Airflow role
#
# After `terraform apply`, Snowflake generates a unique AWS principal
# (snowflake_storage_integration.s3.storage_aws_iam_user_arn) that must be
# added to the Airflow role's trust policy.  This extra statement does that.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "airflow_assume_role_with_snowflake" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.airflow_trusted_arn]
    }
  }

  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [snowflake_storage_integration.s3.storage_aws_iam_user_arn]
    }
    condition {
      test     = "StringEquals"
      variable = "sts:ExternalId"
      values   = [snowflake_storage_integration.s3.storage_aws_external_id]
    }
  }
}

resource "aws_iam_role" "airflow_snowflake_trust" {
  name               = aws_iam_role.airflow.name
  assume_role_policy = data.aws_iam_policy_document.airflow_assume_role_with_snowflake.json

  lifecycle {
    create_before_destroy = true
  }
}
