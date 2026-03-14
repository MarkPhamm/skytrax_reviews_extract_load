"""
Upload landing files to S3 (or stay local when STORAGE_MODE=local).

Upload paths mirror the local layout exactly:
  local:  landing/raw/YYYY/MM/raw_data_YYYYMMDD.csv
  S3:     s3://<bucket>/raw/YYYY/MM/raw_data_YYYYMMDD.csv

  local:  landing/processed/YYYY/MM/clean_data_YYYYMMDD.csv
  S3:     s3://<bucket>/processed/YYYY/MM/clean_data_YYYYMMDD.csv

Environment variables
---------------------
STORAGE_MODE   : "s3" (default) or "local" — when "local" uploads are skipped
S3_BUCKET      : bucket name (required when STORAGE_MODE=s3)
AWS_CONN_ID    : Airflow connection id (default: aws_s3_connection)
                 Only used when running inside Airflow; standalone runs use
                 boto3 default credential chain (env vars / ~/.aws / IAM role).
"""

import logging
import os
from datetime import date
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
LANDING_DIR = Path(os.getenv("LANDING_DIR", _PROJECT_ROOT / "landing"))

STORAGE_MODE = os.getenv("STORAGE_MODE", "s3")  # "s3" | "local"
S3_BUCKET = os.getenv("S3_BUCKET", "")
AWS_CONN_ID = os.getenv("AWS_CONN_ID", "aws_s3_connection")


# ---------------------------------------------------------------------------
# Path helpers (same scheme as scraper / processing)
# ---------------------------------------------------------------------------


def raw_local_path(run_date: date) -> Path:
    return (
        LANDING_DIR
        / "raw"
        / run_date.strftime("%Y")
        / run_date.strftime("%m")
        / f"raw_data_{run_date.strftime('%Y%m%d')}.csv"
    )


def processed_local_path(run_date: date) -> Path:
    return (
        LANDING_DIR
        / "processed"
        / run_date.strftime("%Y")
        / run_date.strftime("%m")
        / f"clean_data_{run_date.strftime('%Y%m%d')}.csv"
    )


def raw_s3_key(run_date: date) -> str:
    y, m = run_date.strftime("%Y"), run_date.strftime("%m")
    return f"raw/{y}/{m}/raw_data_{run_date.strftime('%Y%m%d')}.csv"


def processed_s3_key(run_date: date) -> str:
    y, m = run_date.strftime("%Y"), run_date.strftime("%m")
    return f"processed/{y}/{m}/clean_data_{run_date.strftime('%Y%m%d')}.csv"


# ---------------------------------------------------------------------------
# Upload helpers
# ---------------------------------------------------------------------------


def _get_s3_client(use_airflow_hook: bool = False):
    """
    Return a boto3 S3 client.

    When use_airflow_hook=True and running inside Airflow, credentials are
    pulled from the Airflow connection (AWS_CONN_ID).  Otherwise fall back to
    boto3's default credential chain.
    """
    if use_airflow_hook:
        try:
            from airflow.providers.amazon.aws.hooks.s3 import S3Hook

            hook = S3Hook(aws_conn_id=AWS_CONN_ID)
            return hook.get_client_type("s3")
        except ImportError:
            logger.debug("Airflow not available — falling back to boto3 default chain")

    import boto3

    return boto3.client("s3")


def upload_file(
    local_path: Path,
    s3_key: str,
    bucket: str,
    use_airflow_hook: bool = False,
) -> str:
    """
    Upload a single file to S3.

    Returns the s3:// URI of the uploaded object.
    Raises FileNotFoundError if local_path does not exist.
    """
    if not local_path.exists():
        raise FileNotFoundError(f"File not found: {local_path}")

    client = _get_s3_client(use_airflow_hook=use_airflow_hook)
    client.upload_file(str(local_path), bucket, s3_key)
    uri = f"s3://{bucket}/{s3_key}"
    logger.info("Uploaded %s → %s", local_path.name, uri)
    return uri


# ---------------------------------------------------------------------------
# Public API — called from Airflow tasks or CLI
# ---------------------------------------------------------------------------


def upload_raw(
    run_date: date,
    bucket: Optional[str] = None,
    use_airflow_hook: bool = False,
) -> Optional[str]:
    """
    Upload the raw CSV for run_date to S3.

    Returns the s3:// URI, or None when STORAGE_MODE=local.
    """
    if STORAGE_MODE == "local":
        logger.info("STORAGE_MODE=local — skipping raw upload for %s", run_date)
        return None

    bucket = bucket or S3_BUCKET
    if not bucket:
        raise ValueError("S3_BUCKET env var is not set and no bucket was passed")

    return upload_file(
        local_path=raw_local_path(run_date),
        s3_key=raw_s3_key(run_date),
        bucket=bucket,
        use_airflow_hook=use_airflow_hook,
    )


def upload_processed(
    run_date: date,
    bucket: Optional[str] = None,
    use_airflow_hook: bool = False,
) -> Optional[str]:
    """
    Upload the processed CSV for run_date to S3.

    Returns the s3:// URI, or None when STORAGE_MODE=local.
    """
    if STORAGE_MODE == "local":
        logger.info("STORAGE_MODE=local — skipping processed upload for %s", run_date)
        return None

    bucket = bucket or S3_BUCKET
    if not bucket:
        raise ValueError("S3_BUCKET env var is not set and no bucket was passed")

    return upload_file(
        local_path=processed_local_path(run_date),
        s3_key=processed_s3_key(run_date),
        bucket=bucket,
        use_airflow_hook=use_airflow_hook,
    )


# ---------------------------------------------------------------------------
# CLI — quick manual upload: python s3_upload.py --date 2026-03-12
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from datetime import timedelta

    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

    parser = argparse.ArgumentParser(description="Upload landing files to S3")
    parser.add_argument(
        "--date", type=date.fromisoformat, default=None, help="Run date YYYY-MM-DD (default: today)"
    )
    parser.add_argument("--yesterday", action="store_true", help="Upload yesterday's files")
    parser.add_argument("--bucket", default=None, help="Override S3_BUCKET env var")
    parser.add_argument("--raw-only", action="store_true", help="Upload only the raw file")
    parser.add_argument(
        "--processed-only", action="store_true", help="Upload only the processed file"
    )
    args = parser.parse_args()

    run_date = args.date or (date.today() - timedelta(days=1) if args.yesterday else date.today())

    if not args.processed_only:
        uri = upload_raw(run_date, bucket=args.bucket)
        if uri:
            print(f"Raw      → {uri}")

    if not args.raw_only:
        uri = upload_processed(run_date, bucket=args.bucket)
        if uri:
            print(f"Processed → {uri}")
