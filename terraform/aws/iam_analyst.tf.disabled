# ---------------------------------------------------------------------------
# IAM Role — Analyst (read-only)
#
# Analysts assume this role to browse and download files from the landing
# bucket via the AWS console or CLI.  Full read access on all prefixes.
# ---------------------------------------------------------------------------

data "aws_iam_policy_document" "analyst_assume_role" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "AWS"
      identifiers = length(var.analyst_trusted_arns) > 0 ? var.analyst_trusted_arns : ["arn:aws:iam::000000000000:root"]
    }
  }
}

resource "aws_iam_role" "analyst" {
  name               = "skytrax-analyst-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.analyst_assume_role.json
  description        = "Read-only access to the Skytrax landing bucket"
}

data "aws_iam_policy_document" "analyst_s3" {
  statement {
    sid     = "ListBucket"
    effect  = "Allow"
    actions = ["s3:ListBucket", "s3:GetBucketLocation"]
    resources = [aws_s3_bucket.landing.arn]
  }

  statement {
    sid     = "ReadObjects"
    effect  = "Allow"
    actions = ["s3:GetObject", "s3:GetObjectVersion"]
    resources = ["${aws_s3_bucket.landing.arn}/*"]
  }
}

resource "aws_iam_policy" "analyst_s3" {
  name        = "skytrax-analyst-s3-${var.environment}"
  description = "Read-only access to the Skytrax landing bucket"
  policy      = data.aws_iam_policy_document.analyst_s3.json
}

resource "aws_iam_role_policy_attachment" "analyst_s3" {
  role       = aws_iam_role.analyst.name
  policy_arn = aws_iam_policy.analyst_s3.arn
}
