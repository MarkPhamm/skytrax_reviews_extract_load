output "bucket_name" {
  description = "S3 bucket name"
  value       = aws_s3_bucket.landing.id
}

output "bucket_arn" {
  description = "S3 bucket ARN"
  value       = aws_s3_bucket.landing.arn
}

output "airflow_role_arn" {
  description = "ARN of the Airflow IAM role — use this in the Airflow AWS connection"
  value       = aws_iam_role.airflow.arn
}

output "analyst_role_arn" {
  description = "ARN of the analyst read-only IAM role"
  value       = aws_iam_role.analyst.arn
}

output "snowflake_stage_name" {
  description = "Fully-qualified Snowflake stage name"
  value       = "${snowflake_database.skytrax.name}.${snowflake_schema.raw.name}.${snowflake_stage.s3.name}"
}

output "snowflake_table_name" {
  description = "Fully-qualified Snowflake table name"
  value       = "${snowflake_database.skytrax.name}.${snowflake_schema.raw.name}.${snowflake_table.airline_reviews.name}"
}
