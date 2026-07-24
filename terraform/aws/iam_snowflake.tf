# ---------------------------------------------------------------------------
# IAM Role — Snowflake external stage / COPY INTO
#
# Snowflake's storage integration assumes this role. Trust principals come
# from local.trust_arns (account + var.snowflake_iam_user_arn from DESC STAGE).
#
# Permissions: READ-ONLY on the landing bucket. COPY INTO only needs List +
# GetObject; Put/Delete stay on the Airflow user (iam_airflow.tf) which owns
# the write path. Narrower than the shared RW policy on purpose.
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

data "aws_iam_policy_document" "snowflake_s3_readonly" {
  statement {
    sid       = "ListBucket"
    effect    = "Allow"
    actions   = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.landing.arn]
  }

  statement {
    sid    = "ReadObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]
    resources = ["${aws_s3_bucket.landing.arn}/*"]
  }
}

resource "aws_iam_policy" "snowflake_s3_readonly" {
  name        = "skytrax-snowflake-s3-readonly-${var.environment}"
  description = "Snowflake stage read-only access to the Skytrax landing bucket (COPY INTO)"
  policy      = data.aws_iam_policy_document.snowflake_s3_readonly.json
}

resource "aws_iam_role_policy_attachment" "snowflake_s3" {
  role       = aws_iam_role.snowflake_s3.name
  policy_arn = aws_iam_policy.snowflake_s3_readonly.arn
}
