# ---------------------------------------------------------------------------
# Shared S3 policy — landing bucket read/write
#
# Attached to:
#   - Airflow IAM user  (this file)  — upload / manage objects
#   - Snowflake IAM role (iam_snowflake.tf) — COPY INTO from the stage
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "airflow_s3" {
  statement {
    sid       = "ListBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.landing.arn]
  }

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

# ---------------------------------------------------------------------------
# IAM User — Airflow (programmatic access)
#
# Access keys feed the Airflow AWS connection so DAGs can write crawled
# CSVs to the landing bucket (and clean up objects as needed).
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
