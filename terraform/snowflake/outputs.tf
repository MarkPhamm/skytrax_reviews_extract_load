output "snowflake_stage_name" {
  description = "Fully-qualified Snowflake stage name"
  value       = "${snowflake_database.skytrax.name}.${snowflake_schema.raw.name}.${snowflake_stage.s3.name}"
}

output "snowflake_table_name" {
  description = "Fully-qualified Snowflake table name"
  value       = "${snowflake_database.skytrax.name}.${snowflake_schema.raw.name}.${snowflake_table.airline_reviews.name}"
}
