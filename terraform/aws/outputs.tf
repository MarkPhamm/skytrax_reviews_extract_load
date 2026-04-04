output "bucket_name" {
  description = "S3 bucket name"
  value       = aws_s3_bucket.landing.id
}

output "bucket_arn" {
  description = "S3 bucket ARN"
  value       = aws_s3_bucket.landing.arn
}

output "snowflake_s3_role_arn" {
  description = "ARN of the IAM role Snowflake assumes to read from S3"
  value       = aws_iam_role.snowflake_s3.arn
}

output "airflow_access_key_id" {
  description = "Access key ID for the Airflow IAM user"
  value       = aws_iam_access_key.airflow.id
}

output "airflow_secret_access_key" {
  description = "Secret access key for the Airflow IAM user"
  value       = aws_iam_access_key.airflow.secret
  sensitive   = true
}
