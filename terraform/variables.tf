variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "bucket_name" {
  description = "S3 bucket name for Skytrax landing data"
  type        = string
}

variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
  default     = "dev"
}

# ---------------------------------------------------------------------------
# Airflow trust — the AWS account / entity that runs Airflow
# Set to the ARN of the IAM user or role that Airflow runs as.
# Example (EC2 instance profile): "arn:aws:iam::123456789012:root"
# Example (specific user):        "arn:aws:iam::123456789012:user/airflow-runner"
# ---------------------------------------------------------------------------
variable "airflow_trusted_arn" {
  description = "IAM principal ARN allowed to assume the Airflow role"
  type        = string
}

# ---------------------------------------------------------------------------
# Analyst trust — team members who need read-only access
# Can be a list of ARNs (users, SSO roles, etc.)
# ---------------------------------------------------------------------------
variable "analyst_trusted_arns" {
  description = "List of IAM principal ARNs allowed to assume the analyst role"
  type        = list(string)
  default     = []
}

variable "raw_prefix" {
  description = "S3 key prefix for raw files"
  type        = string
  default     = "raw/"
}

variable "processed_prefix" {
  description = "S3 key prefix for processed files"
  type        = string
  default     = "processed/"
}

# ---------------------------------------------------------------------------
# Snowflake
# ---------------------------------------------------------------------------

variable "snowflake_org" {
  description = "Snowflake organization name (from account identifier: ORG-ACCOUNT)"
  type        = string
}

variable "snowflake_account" {
  description = "Snowflake account name (from account identifier: ORG-ACCOUNT)"
  type        = string
}

variable "snowflake_user" {
  description = "Snowflake user for Terraform (must have SYSADMIN + SECURITYADMIN)"
  type        = string
}

variable "noncurrent_version_expiry_days" {
  description = "Days before old object versions are permanently deleted"
  type        = number
  default     = 90
}
