data "aws_caller_identity" "current" {}

# ---------------------------------------------------------------------------
# IAM Role — Snowflake S3 Access
#
# Permissions: full read/write/delete on the landing bucket.
# Snowflake assumes this role to read from S3 during COPY INTO.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "snowflake_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = local.trust_arns
    }
  }
}

resource "aws_iam_role" "snowflake_s3" {
  name               = "skytrax-snowflake-s3-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.snowflake_assume_role.json
  description        = "Assumed by Snowflake to read from the Skytrax landing bucket during COPY INTO"
}

data "aws_iam_policy_document" "airflow_s3" {
  # List the bucket itself (needed for ListBucket)
  statement {
    sid       = "ListBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.landing.arn]
  }

  # Full object-level access on all prefixes
  statement {
    sid    = "ReadWriteObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:PutObject",
      "s3:DeleteObject",
      "s3:GetObjectVersion",
      "s3:DeleteObjectVersion",
    ]
    resources = ["${aws_s3_bucket.landing.arn}/*"]
  }
}

resource "aws_iam_policy" "airflow_s3" {
  name        = "skytrax-airflow-s3-${var.environment}"
  description = "Airflow read/write access to the Skytrax landing bucket"
  policy      = data.aws_iam_policy_document.airflow_s3.json
}

resource "aws_iam_role_policy_attachment" "snowflake_s3" {
  role       = aws_iam_role.snowflake_s3.name
  policy_arn = aws_iam_policy.airflow_s3.arn
}

# ---------------------------------------------------------------------------
# IAM User — Airflow (programmatic access)
#
# Direct S3 access via the same policy. Access keys are generated for use
# in the Airflow AWS connection.
# ---------------------------------------------------------------------------

resource "aws_iam_user" "airflow" {
  name = "skytrax-airflow-${var.environment}"
}

resource "aws_iam_user_policy_attachment" "airflow_s3_direct" {
  user       = aws_iam_user.airflow.name
  policy_arn = aws_iam_policy.airflow_s3.arn
}

resource "aws_iam_access_key" "airflow" {
  user = aws_iam_user.airflow.name
}
