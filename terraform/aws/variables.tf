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

variable "noncurrent_version_expiry_days" {
  description = "Days before old object versions are permanently deleted"
  type        = number
  default     = 90
}

# ---------------------------------------------------------------------------
# Variables below are used by disabled modules (IAM analyst).
# Uncomment and fill in when re-enabling those .tf files.
# ---------------------------------------------------------------------------

# variable "analyst_trusted_arns" {
#   description = "List of IAM principal ARNs allowed to assume the analyst role"
#   type        = list(string)
#   default     = []
# }
