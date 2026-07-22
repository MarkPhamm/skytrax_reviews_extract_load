"""
DAG: skytrax_process

Triggered automatically when dag_crawl emits RAW_DATASET.

Reads the {category: [dates]} map written by dag_crawl (LAST_CRAWL_DATES Airflow
Variable), then for each (review type, review date):
  1. download_raw     — pull raw CSV from S3 → landing/raw/<type>/… (no-op when local)
  2. clean_one        — run the type-aware cleaning pipeline → landing/processed/<type>/…
  3. upload_processed — push clean CSV to S3 (no-op when local)

Each (type, date) is an independent dynamically-mapped task instance.
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

from include.tasks.common import paths
from include.tasks.load.s3_upload import upload_processed as _upload
from include.tasks.transform.processing import clean_file

RAW_DATASET = Dataset("skytrax://raw")
PROCESSED_DATASET = Dataset("skytrax://processed")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=0),
}


def _entity_col(category: str) -> str:
    """Column holding the scraped entity name (used to drop 'Read more' junk rows)."""
    return "airport_name" if category == "airport" else "airline_name"


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
    def get_work_items() -> list[dict]:
        """Expand the {category: [dates]} map into [{category, date_str}, ...]."""
        mapping = json.loads(Variable.get("LAST_CRAWL_DATES", default_var="{}"))
        return [
            {"category": category, "date_str": d}
            for category, dates in mapping.items()
            for d in dates
        ]

    @task()
    def download_raw(category: str, date_str: str) -> dict:
        """S3 mode: pull raw CSV from S3. Local mode: already on disk."""
        review_date = date.fromisoformat(date_str)
        local_path = paths.raw_local_path(category, review_date)

        if os.getenv("STORAGE_MODE", "local") == "s3":
            bucket = os.environ["S3_BUCKET"]
            s3_key = paths.raw_key(category, review_date)
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

        return {"category": category, "date_str": date_str, "raw_path": str(local_path)}

    @task()
    def clean_one(category: str, date_str: str, raw_path: str) -> dict:
        """Filter scraper junk then run the type-aware cleaning pipeline."""
        review_date = date.fromisoformat(date_str)

        df = pd.read_csv(raw_path, low_memory=False)
        entity_col = _entity_col(category)
        if entity_col in df.columns:
            df = df[df[entity_col] != "Read more"]

        output_path = paths.processed_local_path(category, review_date)
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as tmp:
            df.to_csv(tmp, index=False)
            tmp_path = tmp.name
        try:
            clean_file(category, Path(tmp_path), output_path)
        finally:
            Path(tmp_path).unlink(missing_ok=True)

        return {"category": category, "date_str": date_str, "processed_path": str(output_path)}

    @task(outlets=[PROCESSED_DATASET])
    def upload_processed(category: str, date_str: str, processed_path: str) -> str | None:
        """Upload one processed CSV to S3 (skipped when STORAGE_MODE=local)."""
        if os.getenv("STORAGE_MODE", "local") == "local":
            return None
        bucket = os.environ["S3_BUCKET"]
        return _upload(category, date.fromisoformat(date_str), bucket=bucket, use_airflow_hook=True)

    # ── Wire up ──────────────────────────────────────────────────────────────

    work_items = get_work_items()
    raw = download_raw.expand_kwargs(work_items)
    cleaned = clean_one.expand_kwargs(raw)
    upload_processed.expand_kwargs(cleaned)


process_dag()
