# ---------------------------------------------------------------------------
# IAM Role — Snowflake external stage / COPY INTO
#
# Snowflake's storage integration assumes this role. Trust principals come
# from local.trust_arns (account + var.snowflake_iam_user_arn from DESC STAGE).
#
# Permissions: same landing-bucket RW policy as Airflow (aws_iam_policy.airflow_s3).
# COPY INTO only needs read; tighten to a read-only policy later if desired.
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

resource "aws_iam_role_policy_attachment" "snowflake_s3" {
  role       = aws_iam_role.snowflake_s3.name
  policy_arn = aws_iam_policy.airflow_s3.arn
}
