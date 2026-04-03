variable "aws_region" {
  description = "AWS region to deploy resources"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Deployment environment (dev / staging / prod)"
  type        = string
  default     = "dev"
}

variable "snowflake_iam_user_arn" {
  description = "Snowflake IAM user ARN from DESC STAGE — needed so Snowflake can assume the Airflow role"
  type        = string
  default     = ""
}

locals {
  bucket_name = "skytrax-reviews-landing-${data.aws_caller_identity.current.account_id}"
  trust_arns = compact([
    data.aws_caller_identity.current.account_id,
    var.snowflake_iam_user_arn,
  ])
}
