variable "snowflake_org" {
  description = "Snowflake organization name (from account identifier: ORG-ACCOUNT)"
  type        = string
}

variable "snowflake_account" {
  description = "Snowflake account name (from account identifier: ORG-ACCOUNT)"
  type        = string
}

variable "snowflake_admin_user" {
  description = "Snowflake user for Terraform (must have SYSADMIN + SECURITYADMIN)"
  type        = string
}

variable "snowflake_admin_password" {
  description = "Snowflake password for the Terraform user"
  type        = string
  sensitive   = true
}

variable "bucket_name" {
  description = "S3 bucket name for the Skytrax landing data (from AWS stack output)"
  type        = string
}

variable "airflow_role_arn" {
  description = "ARN of the Airflow IAM role used for S3 stage credentials (from AWS stack output)"
  type        = string
}
