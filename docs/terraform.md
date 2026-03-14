# Terraform Setup

Provision the AWS and Snowflake infrastructure from scratch.

## Prerequisites

- [Terraform >= 1.6](https://developer.hashicorp.com/terraform/install)
- AWS CLI configured (`aws configure`)
- An AWS account with admin access
- A Snowflake account (free trial works)

## Step 1: Create a Terraform admin IAM user

Create a dedicated IAM user for Terraform to use:

```bash
# Create the user
aws iam create-user --user-name terraform-admin

# Attach admin policy (scope down for prod)
aws iam attach-user-policy \
  --user-name terraform-admin \
  --policy-arn arn:aws:iam::aws:policy/AdministratorAccess

# Create access keys
aws iam create-access-key --user-name terraform-admin
```

Save the `AccessKeyId` and `SecretAccessKey`.

## Step 2: Configure the AWS CLI profile

```bash
aws configure --profile terraform-admin
```

Enter the access key, secret key, region (`us-east-1`), and output format (`json`).

Terraform uses `profile = "terraform-admin"` in `main.tf`.

## Step 3: Set your variables

Create `terraform/terraform.tfvars` (gitignored):

```hcl
bucket_name       = "skytrax-reviews-landing-<YOUR_AWS_ACCOUNT_ID>"
snowflake_org     = "<YOUR_SNOWFLAKE_ORG>"
snowflake_account = "<YOUR_SNOWFLAKE_ACCOUNT>"
snowflake_user    = "<YOUR_SNOWFLAKE_USERNAME>"
```

Your Snowflake account identifier is in the format `ORG-ACCOUNT`. You can find it in the Snowflake UI under **Admin > Accounts**.

Optional overrides (defaults are fine for dev):

```hcl
aws_region   = "us-east-1"    # default
environment  = "dev"           # default
```

## Step 4: Set the Snowflake password

Terraform reads the Snowflake password from an environment variable (never stored in files):

```bash
export SNOWFLAKE_PASSWORD=<your-snowflake-password>
```

## Step 5: Initialize Terraform

```bash
cd terraform
terraform init
```

This downloads the AWS and Snowflake provider plugins.

## Step 6: Preview and apply

```bash
# See what will be created
terraform plan

# Create everything
terraform apply
```

Type `yes` when prompted.

## What gets created

### AWS resources

| Resource | Purpose |
| -------- | ------- |
| S3 bucket | Landing zone for raw + processed CSVs |
| Bucket versioning | Protects against accidental overwrites |
| Bucket encryption | AES256 server-side encryption |
| Public access block | Prevents accidental public exposure |
| Lifecycle rules | Transitions to Standard-IA after 30 days, expires old versions after 90 days |
| IAM role `skytrax-airflow-dev` | S3-scoped role (also used by Snowflake to read from S3) |
| IAM user `skytrax-airflow-dev` | Programmatic access for Airflow with direct S3 policy |
| Access key | Credentials for the Airflow IAM user |

### Snowflake resources

| Resource | Purpose |
| -------- | ------- |
| Database `SKYTRAX_REVIEWS_DB` | Top-level container |
| Schema `RAW` | Schema for raw/processed data |
| Table `AIRLINE_REVIEWS` | 25-column table matching the processed CSV layout |
| Stage `SKYTRAX_S3_STAGE` | S3 external stage pointing to the bucket, using the IAM role |

## Step 7: Retrieve Airflow credentials

After `terraform apply`, get the access key for your `.env`:

```bash
terraform output airflow_access_key_id
terraform output -raw airflow_secret_access_key
```

## Step 8: Update the IAM trust policy for Snowflake

After creating the Snowflake stage, Snowflake assigns an AWS IAM user that needs permission to assume your IAM role. To find it:

1. In Snowflake, run: `DESC STAGE SKYTRAX_REVIEWS_DB.RAW.SKYTRAX_S3_STAGE;`
1. Note the `AWS_IAM_USER_ARN` value (e.g., `arn:aws:iam::612990353424:user/651j1000-s`)
1. Add this ARN to the trust policy in `terraform/iam_airflow.tf`:

```hcl
principals {
  type        = "AWS"
  identifiers = [
    "arn:aws:iam::<YOUR_ACCOUNT_ID>:root",
    "<SNOWFLAKE_AWS_IAM_USER_ARN>",  # from DESC STAGE
  ]
}
```

1. Run `terraform apply` again

## File overview

```text
terraform/
  main.tf              Provider config + S3 bucket
  iam_airflow.tf       IAM role, user, access key, S3 policy
  snowflake.tf         Snowflake database, schema, table, S3 stage
  variables.tf         Input variables
  outputs.tf           Outputs (bucket name, role ARN, access keys, Snowflake names)
  terraform.tfvars     Your variable values (gitignored)
```

## Next step

Configure Airflow to use these credentials: [docs/airflow.md](airflow.md).
