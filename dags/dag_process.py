"""
DAG: skytrax_process

Triggered automatically when dag_crawl emits RAW_DATASET.

Reads the {category: [dates]} map written by dag_crawl (LAST_CRAWL_DATES Airflow
Variable) and processes one category per mapped task instance. Each category task
downloads, cleans, and uploads every one of its (type, date) files to S3 in a
thread pool — unconditionally, so processing never blocks or fails on a data
quality issue.

A post-upload quality check then validates each file; a date that fails is
recorded (QUALITY_REJECTED__<category> Airflow Variable) so skytrax_snowflake can
skip loading it, without ever refusing to process/upload it. Only a genuine
processing error (download/clean/upload) fails a category task.

One task per category (not per file) keeps the mapping small and constant — four
categories, never thousands — so it works identically for a single daily date or
a full multi-year backfill, without tripping Airflow's max_map_length ceiling.
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
from include.tasks.common.alerts import notify_failure
from include.tasks.common.quality import DataQualityError, validate_processed_csv
from include.tasks.load.s3_upload import get_s3_client
from include.tasks.load.s3_upload import upload_processed as _upload
from include.tasks.transform.processing import clean_file

logger = logging.getLogger(__name__)

RAW_DATASET = Dataset("skytrax://raw")
PROCESSED_DATASET = Dataset("skytrax://processed")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
    "on_failure_callback": notify_failure,
}


def _entity_col(category: str) -> str:
    """Column holding the scraped entity name (used to drop 'Read more' junk rows)."""
    return "airport_name" if category == "airport" else "airline_name"


def _crawl_mapping() -> dict[str, list[str]]:
    return json.loads(Variable.get("LAST_CRAWL_DATES", default_var="{}"))


def _set_quality_rejected(category: str, dates: list[str]) -> None:
    """Record which dates failed the post-upload quality check for a category.

    One Variable per category — each category is only ever written by its own
    mapped task instance, so concurrent categories never race on the same key.
    """
    Variable.set(f"QUALITY_REJECTED__{category}", json.dumps(sorted(set(dates))))


# ---------------------------------------------------------------------------
# Per-(type, date) steps — looped over (with a thread pool) by process_category
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
    """Filter scraper junk and run the type-aware cleaning pipeline."""
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
    """Download → clean → upload → post-upload quality check, for one (category, date).

    Download/clean/upload always run and are never skipped or blocked by a
    quality issue. Only the quality check's outcome is conditional: a
    DataQualityError is caught and recorded as quality_passed=False rather
    than raised — the file is still processed and uploaded either way. Any
    other exception (download/clean/upload failure) still propagates and
    fails the task, since that's a real processing error, not a quality result.
    """
    raw_path = _download_one(category, date_str, client=client)
    processed_path = _clean_one(category, date_str, raw_path)
    uri = _upload_one(category, date_str, processed_path, client=client)

    try:
        validate_processed_csv(category, processed_path)
        quality_passed, quality_error = True, None
    except DataQualityError as e:
        quality_passed, quality_error = False, str(e)
        logger.warning("Quality check failed for %s %s: %s", category, date_str, e)

    return {
        "category": category,
        "date_str": date_str,
        "uri": uri,
        "quality_passed": quality_passed,
        "quality_error": quality_error,
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
    def get_categories() -> list[str]:
        """Categories that have queued dates this run (one mapped task each)."""
        return [category for category, dates in _crawl_mapping().items() if dates]

    @task(outlets=[PROCESSED_DATASET])
    def process_category(category: str) -> dict:
        """Download→clean→upload→validate every queued date for one category.

        Runs in a thread pool within a single Airflow task, so the mapping stays
        at one instance per category regardless of how many dates each holds — a
        single daily date and a multi-year backfill use the exact same path. A
        quality-check failure is recorded (not raised); only a genuine processing
        error (download/clean/upload) fails this task.
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

        rejected_dates = [r["date_str"] for r in succeeded if not r["quality_passed"]]
        _set_quality_rejected(category, rejected_dates)

        logger.info(
            "Process %s: %d processed (%d quality-rejected), %d failed (of %d)",
            category,
            len(succeeded),
            len(rejected_dates),
            len(failed),
            len(dates),
        )
        if failed:
            raise RuntimeError(
                f"Processing failed for {len(failed)}/{len(dates)} date(s) in "
                f"{category}: {failed[:5]}{' ...' if len(failed) > 5 else ''}"
            )
        return {
            "category": category,
            "processed": len(succeeded),
            "quality_rejected": rejected_dates,
        }

    # ── Wire up ──────────────────────────────────────────────────────────────

    process_category.expand(category=get_categories())


process_dag()
