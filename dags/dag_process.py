"""
DAG: skytrax_process

Triggered automatically when dag_crawl emits RAW_DATASET.

Reads the {category: [dates]} map written by dag_crawl (LAST_CRAWL_DATES Airflow
Variable). For each (review type, review date):
  1. download raw CSV from S3 → landing/raw/<type>/… (no-op when local)
  2. run the type-aware cleaning pipeline → landing/processed/<type>/…
  3. pre-load quality gate
  4. upload clean CSV to S3 (no-op when local)

Small batches (a category with <= BULK_THRESHOLD queued dates — the normal daily
case) use one Airflow-mapped-task-instance per (type, date), for per-file retry
isolation. Larger batches (a full backfill) instead run in a single bulk task per
category, processing with a thread pool — Airflow's dynamic task mapping has a
hard max_map_length ceiling (default 1024) that a full backfill blows past.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable

from include.tasks.common import paths
from include.tasks.common.quality import validate_processed_csv
from include.tasks.load.s3_upload import get_s3_client
from include.tasks.load.s3_upload import upload_processed as _upload
from include.tasks.transform.processing import clean_file

logger = logging.getLogger(__name__)

RAW_DATASET = Dataset("skytrax://raw")
PROCESSED_DATASET = Dataset("skytrax://processed")

# Above this many queued dates for a category, process it with one bulk task
# (thread-pool loop) instead of one Airflow-mapped-task-instance per file.
BULK_THRESHOLD = 20

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}


def _entity_col(category: str) -> str:
    """Column holding the scraped entity name (used to drop 'Read more' junk rows)."""
    return "airport_name" if category == "airport" else "airline_name"


def _crawl_mapping() -> dict[str, list[str]]:
    return json.loads(Variable.get("LAST_CRAWL_DATES", default_var="{}"))


# ---------------------------------------------------------------------------
# Shared per-(type, date) steps — called directly by the small-batch tasks
# below, and looped over (with a thread pool) by process_bulk for large batches
# ---------------------------------------------------------------------------


def _download_one(category: str, date_str: str, client=None) -> Path:
    """S3 mode: pull raw CSV from S3. Local mode: already on disk."""
    review_date = date.fromisoformat(date_str)
    local_path = paths.raw_local_path(category, review_date)

    if os.getenv("STORAGE_MODE", "local") == "s3":
        bucket = os.environ["S3_BUCKET"]
        s3_key = paths.raw_key(category, review_date)
        local_path.parent.mkdir(parents=True, exist_ok=True)
        s3_client = client or get_s3_client(use_airflow_hook=True)
        s3_client.download_file(bucket, s3_key, str(local_path))

    if not local_path.exists():
        raise FileNotFoundError(f"Raw file not found: {local_path}")
    return local_path


def _clean_one(category: str, date_str: str, raw_path: Path) -> Path:
    """Filter scraper junk, run the type-aware cleaning pipeline, and quality-gate it."""
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

    # Pre-load quality gate: schema, non-empty, null rates, rating ranges.
    # Raises DataQualityError so a bad file never reaches S3/Snowflake.
    validate_processed_csv(category, output_path)

    return output_path


def _upload_one(category: str, date_str: str, processed_path: Path, client=None) -> str | None:
    if os.getenv("STORAGE_MODE", "local") == "local":
        return None
    bucket = os.environ["S3_BUCKET"]
    return _upload(
        category,
        date.fromisoformat(date_str),
        bucket=bucket,
        use_airflow_hook=True,
        client=client,
    )


def _process_one(category: str, date_str: str, client=None) -> dict:
    """Download → clean → quality gate → upload, for one (category, date)."""
    raw_path = _download_one(category, date_str, client=client)
    processed_path = _clean_one(category, date_str, raw_path)
    uri = _upload_one(category, date_str, processed_path, client=client)
    return {"category": category, "date_str": date_str, "uri": uri}


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
        """[{category, date_str}, ...] for categories under BULK_THRESHOLD dates."""
        return [
            {"category": category, "date_str": d}
            for category, dates in _crawl_mapping().items()
            if len(dates) <= BULK_THRESHOLD
            for d in dates
        ]

    @task()
    def get_bulk_categories() -> list[str]:
        """Categories with more than BULK_THRESHOLD queued dates (backfill-sized)."""
        return [
            category for category, dates in _crawl_mapping().items() if len(dates) > BULK_THRESHOLD
        ]

    @task()
    def download_raw(category: str, date_str: str) -> dict:
        raw_path = _download_one(category, date_str)
        return {"category": category, "date_str": date_str, "raw_path": str(raw_path)}

    @task()
    def clean_one(category: str, date_str: str, raw_path: str) -> dict:
        processed_path = _clean_one(category, date_str, Path(raw_path))
        return {"category": category, "date_str": date_str, "processed_path": str(processed_path)}

    @task(outlets=[PROCESSED_DATASET])
    def upload_processed(category: str, date_str: str, processed_path: str) -> str | None:
        """Upload one processed CSV to S3 (skipped when STORAGE_MODE=local)."""
        return _upload_one(category, date_str, Path(processed_path))

    @task(outlets=[PROCESSED_DATASET])
    def process_bulk(category: str) -> dict:
        """Download→clean→validate→upload every queued date for one category.

        Runs in a thread pool within a single Airflow task, so a backfill-sized
        batch never needs to create thousands of mapped task instances.
        """
        dates = _crawl_mapping().get(category, [])
        client = get_s3_client(use_airflow_hook=True)

        workers = int(Variable.get("PROCESS_WORKERS", default_var="8"))
        succeeded, failed = [], []
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = {pool.submit(_process_one, category, d, client): d for d in dates}
            for future in as_completed(futures):
                date_str = futures[future]
                try:
                    succeeded.append(future.result())
                except Exception as e:  # noqa: BLE001 — collected below, not swallowed
                    logger.error("Failed processing %s %s: %s", category, date_str, e)
                    failed.append({"date_str": date_str, "error": str(e)})

        logger.info(
            "Bulk process %s: %d succeeded, %d failed (of %d)",
            category,
            len(succeeded),
            len(failed),
            len(dates),
        )
        if failed:
            raise RuntimeError(
                f"Bulk processing failed for {len(failed)}/{len(dates)} date(s) in "
                f"{category}: {failed[:5]}{' ...' if len(failed) > 5 else ''}"
            )
        return {"category": category, "processed": len(succeeded)}

    # ── Wire up ──────────────────────────────────────────────────────────────

    work_items = get_work_items()
    raw = download_raw.expand_kwargs(work_items)
    cleaned = clean_one.expand_kwargs(raw)
    upload_processed.expand_kwargs(cleaned)

    process_bulk.expand(category=get_bulk_categories())


process_dag()
