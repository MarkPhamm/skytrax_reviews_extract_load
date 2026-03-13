# ---------------------------------------------------------------------------
# IAM Role — Airflow
#
# Permissions: full read/write/delete on the landing bucket.
# Used by Astronomer/Airflow to upload raw + processed CSVs and (optionally)
# clean up old files.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "airflow_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = [var.airflow_trusted_arn]
    }
  }
}

resource "aws_iam_role" "airflow" {
  name               = "skytrax-airflow-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.airflow_assume_role.json
  description        = "Assumed by Airflow to read/write the Skytrax landing bucket"
}

data "aws_iam_policy_document" "airflow_s3" {
  # List the bucket itself (needed for ListBucket)
  statement {
    sid     = "ListBucket"
    effect  = "Allow"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
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

resource "aws_iam_role_policy_attachment" "airflow_s3" {
  role       = aws_iam_role.airflow.name
  policy_arn = aws_iam_policy.airflow_s3.arn
}
