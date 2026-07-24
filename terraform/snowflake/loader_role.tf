# ---------------------------------------------------------------------------
# SKYTRAX_LOADER — least-privilege runtime role for Airflow COPY INTO
#
# dag_snowflake used to run as SYSADMIN. That works, but SYSADMIN can alter
# warehouses, create users, and touch every database. The loader only needs:
#   - USAGE on a warehouse + the SKYTRAX_REVIEWS_DB.RAW schema
#   - USAGE on the external stage
#   - INSERT + SELECT on the four review tables + LOAD_AUDIT
#
# Point AIRFLOW_CONN_SNOWFLAKE_DEFAULT.extra.role at SKYTRAX_LOADER after apply,
# and grant the role to the Airflow Snowflake user (see grant below — set
# var.loader_user when you know the login name).
# ---------------------------------------------------------------------------

variable "loader_warehouse" {
  type        = string
  description = "Warehouse SKYTRAX_LOADER may use for COPY INTO"
  default     = "COMPUTE_WH"
}

variable "loader_user" {
  type        = string
  description = "Snowflake user that receives SKYTRAX_LOADER (Airflow login). Empty = skip user grant."
  default     = ""
}

resource "snowflake_account_role" "loader" {
  name    = "SKYTRAX_LOADER"
  comment = "Least-privilege role for Airflow COPY INTO into RAW (not SYSADMIN)."
}

resource "snowflake_grant_privileges_to_account_role" "loader_warehouse_usage" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "WAREHOUSE"
    object_name = var.loader_warehouse
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_database_usage" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_account_object {
    object_type = "DATABASE"
    object_name = snowflake_database.skytrax.name
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_schema_usage" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_schema {
    schema_name = "${snowflake_database.skytrax.name}.${snowflake_schema.raw.name}"
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_stage_usage" {
  account_role_name = snowflake_account_role.loader.name
  privileges        = ["USAGE"]
  on_schema_object {
    object_type = "STAGE"
    object_name = "${snowflake_database.skytrax.name}.${snowflake_schema.raw.name}.${snowflake_stage.s3.name}"
  }
}

locals {
  loader_tables = {
    airline = snowflake_table.airline_reviews
    seat    = snowflake_table.seat_reviews
    lounge  = snowflake_table.lounge_reviews
    airport = snowflake_table.airport_reviews
    audit   = snowflake_table.load_audit
  }
}

resource "snowflake_grant_privileges_to_account_role" "loader_table_dml" {
  for_each = local.loader_tables

  account_role_name = snowflake_account_role.loader.name
  privileges        = ["SELECT", "INSERT"]
  on_schema_object {
    object_type = "TABLE"
    object_name = each.value.fully_qualified_name
  }
}

resource "snowflake_grant_account_role" "loader_to_user" {
  count = var.loader_user == "" ? 0 : 1

  role_name = snowflake_account_role.loader.name
  user_name = var.loader_user
}
