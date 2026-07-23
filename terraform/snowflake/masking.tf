# ---------------------------------------------------------------------------
# Data governance — PII masking (tag-based)
#
# A masking policy attached to the PII tag: any column tagged with
# SKYTRAX_REVIEWS_DB.RAW.PII is dynamically masked unless the session has the
# PII_READER role (or ACCOUNTADMIN). dbt re-applies the tag to mart columns
# via post-hooks after each rebuild, so masking survives `dbt run`.
# ---------------------------------------------------------------------------

resource "snowflake_account_role" "pii_reader" {
  name    = "PII_READER"
  comment = "Sessions with this role see unmasked PII columns."
}

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
