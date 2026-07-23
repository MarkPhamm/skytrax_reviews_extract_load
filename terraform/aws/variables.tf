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
  description = "Snowflake IAM user ARN from DESC STAGE — so Snowflake can assume the S3 access role"
  type        = string
  default     = ""
}

variable "analyst_trusted_arns" {
  description = "IAM principal ARNs allowed to assume the analyst read-only role (users, roles, or SSO roles). Empty = trust this account root."
  type        = list(string)
  default     = []
}

locals {
  bucket_name = "skytrax-reviews-landing-${data.aws_caller_identity.current.account_id}"
  trust_arns = compact([
    data.aws_caller_identity.current.account_id,
    var.snowflake_iam_user_arn,
  ])
  analyst_trust_arns = length(var.analyst_trusted_arns) > 0 ? var.analyst_trusted_arns : [
    "arn:aws:iam::${data.aws_caller_identity.current.account_id}:root",
  ]
}
