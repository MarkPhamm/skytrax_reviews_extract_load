"""
DAG: skytrax_process

Triggered automatically when dag_crawl emits RAW_DATASET.

Reads the list of review dates written by dag_crawl (via LAST_CRAWL_DATES
Airflow Variable), then for each date:
  1. download_raw   — pull raw CSV from S3 → landing/raw/ (no-op when local)
  2. clean_date     — run processing pipeline → landing/processed/YYYY/MM/
  3. upload_processed — push clean CSV to S3 (no-op when local)

Each review date is processed as an independent dynamically-mapped task,
so you get one task instance per date in the Airflow UI.
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path

import boto3
import pandas as pd
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable

from include.tasks.extract.scraper import LANDING_DIR
from include.tasks.load.s3_upload import raw_s3_key
from include.tasks.load.s3_upload import upload_processed as _upload
from include.tasks.transform.processing import clean as _clean
from include.tasks.transform.processing import get_output_path

RAW_DATASET = Dataset("skytrax://raw")
PROCESSED_DATASET = Dataset("skytrax://processed")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=0),
}


@dag(
    dag_id="skytrax_process",
    schedule=[RAW_DATASET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["skytrax", "transform", "load"],
)
def process_dag():

    @task()
    def get_dates_to_process() -> list[str]:
        """Read the date list written by dag_crawl."""
        raw = Variable.get("LAST_CRAWL_DATES", default_var="[]")
        return json.loads(raw)

    @task()
    def download_raw(date_str: str) -> str:
        """S3 mode: pull raw CSV from S3. Local mode: already on disk."""
        review_date = date.fromisoformat(date_str)
        local_path = (
            LANDING_DIR
            / "raw"
            / review_date.strftime("%Y")
            / review_date.strftime("%m")
            / f"raw_data_{review_date.strftime('%Y%m%d')}.csv"
        )

        if os.getenv("STORAGE_MODE", "local") == "s3":
            bucket = os.environ["S3_BUCKET"]
            s3_key = raw_s3_key(review_date)
            local_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                from airflow.providers.amazon.aws.hooks.s3 import S3Hook

                conn_id = os.getenv("AWS_CONN_ID", "aws_s3_connection")
                hook = S3Hook(aws_conn_id=conn_id)
                hook.get_conn().download_file(bucket, s3_key, str(local_path))
            except ImportError:
                boto3.client("s3").download_file(bucket, s3_key, str(local_path))

        if not local_path.exists():
            raise FileNotFoundError(f"Raw file not found: {local_path}")

        return str(local_path)

    @task()
    def clean_date(raw_path: str) -> str:
        """Run the full cleaning pipeline for one date's raw CSV."""
        # Derive review date from the raw filename: raw_data_YYYYMMDD.csv
        stem = Path(raw_path).stem  # raw_data_20260312
        date_part = stem.split("_")[-1]  # 20260312
        review_date = date(int(date_part[:4]), int(date_part[4:6]), int(date_part[6:]))

        # Filter scraper junk before cleaning
        df = pd.read_csv(raw_path, low_memory=False)
        df = df[df["airline_name"] != "Read more"]

        output_path = get_output_path(review_date)
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as tmp:
            df.to_csv(tmp, index=False)
            tmp_path = tmp.name
        try:
            _clean(Path(tmp_path), output_path)
        finally:
            Path(tmp_path).unlink(missing_ok=True)
        return str(output_path)

    @task(outlets=[PROCESSED_DATASET])
    def upload_processed(processed_path: str) -> str | None:
        """Upload one processed CSV to S3 (skipped when STORAGE_MODE=local)."""
        if os.getenv("STORAGE_MODE", "local") == "local":
            return None

        stem = Path(processed_path).stem  # clean_data_20260312
        date_part = stem.split("_")[-1]
        review_date = date(int(date_part[:4]), int(date_part[4:6]), int(date_part[6:]))

        bucket = os.environ["S3_BUCKET"]
        return _upload(review_date, bucket=bucket, use_airflow_hook=True)

    # ── Wire up ──────────────────────────────────────────────────────────────

    dates = get_dates_to_process()

    # Dynamic task mapping: one task instance per review date
    raw_paths = download_raw.expand(date_str=dates)
    processed_paths = clean_date.expand(raw_path=raw_paths)
    upload_processed.expand(processed_path=processed_paths)


process_dag()
