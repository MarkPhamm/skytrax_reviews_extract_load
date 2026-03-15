terraform {
  required_version = ">= 1.6"
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 1.0"
    }
  }
  # Uncomment for remote state (prod):
  # backend "s3" {
  #   bucket = "my-terraform-state-bucket"
  #   key    = "skytrax/snowflake/terraform.tfstate"
  #   region = "us-east-1"
  # }
}

provider "snowflake" {
  organization_name        = var.snowflake_org
  account_name             = var.snowflake_account
  user                     = var.snowflake_admin_user
  password                 = var.snowflake_admin_password
  authenticator            = "SNOWFLAKE"
  preview_features_enabled = ["snowflake_table_resource", "snowflake_stage_resource"]
}
