"""
DAG: skytrax_snowflake

Triggered automatically when dag_process emits PROCESSED_DATASET.

Infrastructure (database, schema, per-type tables, stage) is managed by Terraform.
This DAG runs one COPY INTO per category (one mapped task each) covering all of
that category's processed files in a single statement — a single daily date and a
multi-year backfill use the exact same path. Dates dag_process flagged as
quality-rejected (QUALITY_REJECTED__<category>) are excluded from the load.

STORAGE_MODE=local skips this DAG entirely — no Snowflake needed for local dev.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable

from include.tasks.common.alerts import notify_failure
from include.tasks.load.snowflake_load import copy_into_bulk

PROCESSED_DATASET = Dataset("skytrax://processed")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
    "on_failure_callback": notify_failure,
}


def _crawl_mapping() -> dict[str, list[str]]:
    if os.getenv("STORAGE_MODE", "local") == "local":
        return {}
    return json.loads(Variable.get("LAST_CRAWL_DATES", default_var="{}"))


def _quality_rejected(category: str) -> list[str]:
    """Dates dag_process flagged as quality-rejected for this category — never loaded."""
    return json.loads(Variable.get(f"QUALITY_REJECTED__{category}", default_var="[]"))


@dag(
    dag_id="skytrax_snowflake",
    schedule=[PROCESSED_DATASET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["skytrax", "load", "snowflake"],
)
def snowflake_dag():

    @task()
    def get_categories() -> list[str]:
        """Categories that have queued dates this run (one mapped load task each)."""
        return [category for category, dates in _crawl_mapping().items() if dates]

    @task()
    def load_category(category: str) -> dict:
        """One COPY INTO covering every processed file for a category, excluding
        any date dag_process flagged as quality-rejected.

        copy_into_bulk runs the post-load reconciliation (rows_parsed vs
        rows_loaded, rejected rows) and writes RAW.LOAD_AUDIT per file; the
        returned totals land in XCom as load evidence.
        """
        rejected = set(_quality_rejected(category))
        all_dates = _crawl_mapping().get(category, [])
        return copy_into_bulk(category, all_dates=all_dates, exclude_dates=rejected)

    # ── Wire up ──────────────────────────────────────────────────────────────

    load_category.expand(category=get_categories())


snowflake_dag()
