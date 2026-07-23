# S3 Integrations

How the two consumers of the landing bucket — Airflow and Snowflake — each authenticate
to S3, and why they use different mechanisms.

There are two independent paths into the same bucket:

```text
Airflow (skytrax-airflow user)  --PutObject/GetObject-->  S3 landing bucket
Snowflake (assumes IAM role)    --GetObject (COPY INTO)-->  S3 landing bucket
```

## Why two different mechanisms

| | Airflow | Snowflake |
|---|---|---|
| AWS principal | IAM **user** (`skytrax-airflow-<env>`) | IAM **role** (`skytrax-snowflake-s3-<env>`) |
| Credential type | Static access key/secret | Temporary STS credentials |
| Who holds the secret | Stored in the Airflow `aws_s3_connection` | Never leaves AWS — Snowflake calls `sts:AssumeRole` on demand |
| Direction | Writes raw + processed CSVs | Reads processed CSVs for `COPY INTO` |

Airflow is *our* infrastructure, so a long-lived IAM user with an access key is
acceptable (it's rotated by re-running Terraform). Snowflake is a third-party
AWS account we don't control, so it gets no standing credentials at all — it's
only ever handed a short-lived session token, scoped to one role, and only for
as long as it keeps re-assuming it.

## Path 1: Airflow → S3 (IAM user)

Defined in `terraform/aws/iam_airflow.tf`:

- `aws_iam_user.airflow` (`skytrax-airflow-<env>`) with `aws_iam_access_key.airflow`
- `aws_iam_policy.airflow_s3` grants `ListBucket`/`GetBucketLocation` on the bucket
  and `GetObject`/`PutObject`/`DeleteObject`(+`Version` variants) on `bucket/*`
- Attached directly to the user via `aws_iam_user_policy_attachment.airflow_s3_direct`

The access key/secret (`terraform output airflow_access_key_id` /
`airflow_secret_access_key`) is stored in the Airflow **connection**
`aws_s3_connection` (see [docs/airflow.md](airflow.md)).

At runtime, `include/tasks/load/s3_upload.py` uploads the raw and processed CSVs:

```python
def get_s3_client(use_airflow_hook: bool = False):
    if use_airflow_hook:
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        hook = S3Hook(aws_conn_id=AWS_CONN_ID)  # aws_s3_connection
        return hook.get_conn()
    import boto3
    return boto3.client("s3")  # standalone: boto3 default credential chain
```

- Inside Airflow (`use_airflow_hook=True`), boto3 is built from the
  `aws_s3_connection` connection — i.e., the static access key above.
  This is the code path used by the DAGs. Callers uploading many files (e.g. a
  full backfill) build this client once and reuse it across uploads instead of
  creating a new one per file.
- Run standalone (e.g. `python s3_upload.py --date ...` from a shell), it falls
  back to boto3's default credential chain (env vars, `~/.aws/credentials`,
  or an instance/role profile) — no Airflow connection involved.

Upload paths mirror the local landing layout, partitioned type-first:

```text
s3://<bucket>/raw/<type>/YYYY/MM/raw_data_YYYYMMDD.csv
s3://<bucket>/processed/<type>/YYYY/MM/clean_data_YYYYMMDD.csv
```

## Path 2: Snowflake → S3 (IAM role assumption)

Defined in `terraform/aws/iam_snowflake.tf` (role) and `terraform/snowflake/snowflake.tf` (stage).

Snowflake never gets an access key. Instead, it assumes an IAM role scoped to
read-only access on the same bucket, via AWS STS:

```text
Snowflake's own IAM user  --sts:AssumeRole-->  skytrax-snowflake-s3-<env> role  --temp creds-->  GetObject on landing bucket
```

**The role and its policy:**

```hcl
resource "aws_iam_role" "snowflake_s3" {
  name               = "skytrax-snowflake-s3-${var.environment}"
  assume_role_policy = data.aws_iam_policy_document.snowflake_assume_role.json
}

resource "aws_iam_role_policy_attachment" "snowflake_s3" {
  role       = aws_iam_role.snowflake_s3.name
  policy_arn = aws_iam_policy.airflow_s3.arn   # same S3 permissions as the Airflow user
}
```

**The trust policy** (`data.aws_iam_policy_document.snowflake_assume_role`) controls
*who* is allowed to call `sts:AssumeRole` on this role:

```hcl
statement {
  effect  = "Allow"
  actions = ["sts:AssumeRole"]
  principals {
    type        = "AWS"
    identifiers = local.trust_arns   # [account_id, snowflake_iam_user_arn]
  }
}
```

The Snowflake stage points at this role by ARN:

```hcl
resource "snowflake_stage" "s3" {
  url         = "s3://${var.bucket_name}/"
  credentials = "AWS_ROLE = '${var.snowflake_s3_role_arn}'"
  ...
}
```

### Why this needs a two-pass apply

You can't wire up the trust policy correctly on the first `terraform apply` —
Snowflake hasn't generated its IAM user yet, and that user's ARN is what needs
to go into the trust policy. The bootstrap sequence:

1. **`terraform apply` (AWS module)** — creates `skytrax-snowflake-s3-<env>`
   with a placeholder trust policy (`local.trust_arns` only contains your own
   account ID at this point, since `snowflake_iam_user_arn` defaults to `""`
   and gets `compact()`-ed out).
2. **`terraform apply` (Snowflake module)** — creates the stage with
   `AWS_ROLE = '<role_arn>'`. Snowflake now *knows* which role to use, but
   can't assume it yet — the role doesn't trust Snowflake's AWS account.
3. **`DESC STAGE SKYTRAX_REVIEWS_DB.RAW.SKYTRAX_S3_STAGE;`** in Snowflake —
   returns `STORAGE_AWS_IAM_USER_ARN`, the ARN of the IAM user in Snowflake's
   own AWS account that will actually call `AssumeRole`.
4. **Set `snowflake_iam_user_arn`** in `terraform/aws/terraform.tfvars` to that
   ARN and **re-apply the AWS module**. `local.trust_arns` now includes it, so
   the trust policy allows Snowflake's user to assume the role.

After step 4, `COPY INTO` against `@SKYTRAX_S3_STAGE` works: Snowflake calls
`sts:AssumeRole`, gets temporary credentials scoped to the `airflow_s3` policy
(read/write on the bucket, though Snowflake only ever reads), and uses them for
the duration of the load.

### Known gap: no external ID

The trust policy currently authorizes by ARN only — there's no
`sts:ExternalId` condition. AWS recommends an external ID for third-party role
assumption to guard against the confused-deputy problem (a case where
Snowflake's IAM user ARN, if ever reused across customers, could let another
Snowflake customer's role-assumption call succeed against your role). Snowflake
returns a `STORAGE_AWS_EXTERNAL_ID` from the same `DESC STAGE` output that isn't
currently being applied as a `condition` block on
`data.aws_iam_policy_document.snowflake_assume_role`. If tightening this up,
add:

```hcl
condition {
  test     = "StringEquals"
  variable = "sts:ExternalId"
  values   = ["<STORAGE_AWS_EXTERNAL_ID from DESC STAGE>"]
}
```

## See also

- [docs/terraform.md](terraform.md) — full apply walkthrough, including Step 11
  (updating the trust policy)
- [docs/snowflake.md](snowflake.md) — how `COPY INTO` reads from the stage
- [docs/airflow.md](airflow.md) — configuring `aws_s3_connection`
