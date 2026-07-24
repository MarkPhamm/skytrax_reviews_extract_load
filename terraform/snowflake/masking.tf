# ---------------------------------------------------------------------------
# Data governance — PII masking (tag-based)
#
# Three pieces, all needed before any masking actually happens:
#   1. the MASK_PII_STRING policy (what masking does),
#   2. the PII tag with the policy bound via `masking_policies` (the
#      snowflakedb provider v1.x replacement for the old
#      snowflake_tag_masking_policy_association resource),
#   3. snowflake_tag_association resources setting the tag on every PII
#      column — a tag + policy with no column associations masks nothing.
# Any column tagged SKYTRAX_REVIEWS_DB.RAW.PII is dynamically masked unless
# the session has the PII_READER role (or ACCOUNTADMIN). dbt re-applies the
# tag to mart columns via post-hooks after each rebuild, so masking survives
# `dbt run`.
# ---------------------------------------------------------------------------

# create the account role that allows sessions to see unmasked PII columns
resource "snowflake_account_role" "pii_reader" {
  name    = "PII_READER"
  comment = "Sessions with this role see unmasked PII columns."
}

# create the masking policy that masks PII columns
resource "snowflake_masking_policy" "pii_string" {
  name     = "MASK_PII_STRING"
  database = snowflake_database.skytrax.name
  schema   = snowflake_schema.raw.name

  argument {
    name = "VAL"
    type = "VARCHAR"
  }

  body = <<-EOF
    case
      when current_role() = 'ACCOUNTADMIN' then val
      when is_role_in_session('${snowflake_account_role.pii_reader.name}') then val
      else '***MASKED***'
    end
  EOF

  return_data_type = "VARCHAR"
  comment          = "Masks string PII (e.g. customer_name, nationality) for non-privileged roles."
}

resource "snowflake_tag" "pii" {
  name     = "PII"
  database = snowflake_database.skytrax.name
  schema   = snowflake_schema.raw.name
  comment  = "Marks columns containing personally identifiable information."

  masking_policies = [snowflake_masking_policy.pii_string.fully_qualified_name]
}

# ---------------------------------------------------------------------------
# Tag → column associations
#
# Setting the PII tag on a column is what triggers masking — every raw review
# table carries the same two reviewer-PII columns, so one association per
# column name covers all four tables.
# ---------------------------------------------------------------------------

locals {
  pii_review_tables = [
    snowflake_table.airline_reviews,
    snowflake_table.seat_reviews,
    snowflake_table.lounge_reviews,
    snowflake_table.airport_reviews,
  ]
  pii_columns = ["CUSTOMER_NAME", "NATIONALITY"]
}

resource "snowflake_tag_association" "pii_columns" {
  for_each = toset(local.pii_columns)

  object_identifiers = [
    for table in local.pii_review_tables : "${table.fully_qualified_name}.\"${each.value}\""
  ]
  object_type = "COLUMN"
  tag_id      = snowflake_tag.pii.fully_qualified_name
  tag_value   = lower(each.value)
}
