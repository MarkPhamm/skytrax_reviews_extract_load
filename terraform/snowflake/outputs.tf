output "snowflake_stage_name" {
  description = "Fully-qualified Snowflake stage name"
  value       = "${snowflake_database.skytrax.name}.${snowflake_schema.raw.name}.${snowflake_stage.s3.name}"
}

output "snowflake_table_names" {
  description = "Fully-qualified Snowflake table names, keyed by review type"
  value = {
    airlines = "${snowflake_database.skytrax.name}.${snowflake_schema.raw.name}.${snowflake_table.airline_reviews.name}"
    seats    = "${snowflake_database.skytrax.name}.${snowflake_schema.raw.name}.${snowflake_table.seat_reviews.name}"
    lounges  = "${snowflake_database.skytrax.name}.${snowflake_schema.raw.name}.${snowflake_table.lounge_reviews.name}"
    airports = "${snowflake_database.skytrax.name}.${snowflake_schema.raw.name}.${snowflake_table.airport_reviews.name}"
  }
}
