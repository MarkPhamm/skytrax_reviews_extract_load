"""
DAG: skytrax_snowflake

Triggered automatically when dag_process emits PROCESSED_DATASET.

Infrastructure (database, schema, per-type tables, stage) is managed by Terraform.
This DAG runs COPY INTO into each category's table — per (type, date) for the
normal small daily batch, or one bulk COPY INTO per category (covering the whole
processed/<type>/ prefix) once a category's batch crosses BULK_THRESHOLD, which
is what a full backfill produces. Bulk avoids one Snowflake round trip per file.

STORAGE_MODE=local skips this DAG entirely — no Snowflake needed for local dev.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable

from include.tasks.load.snowflake_load import copy_into, copy_into_bulk

PROCESSED_DATASET = Dataset("skytrax://processed")

# Above this many queued dates for a category, load it with one bulk COPY INTO
# over the whole prefix instead of one COPY INTO per date.
BULK_THRESHOLD = 20

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}


def _crawl_mapping() -> dict[str, list[str]]:
    if os.getenv("STORAGE_MODE", "local") == "local":
        return {}
    return json.loads(Variable.get("LAST_CRAWL_DATES", default_var="{}"))


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
    def load_one(category: str, date_str: str) -> dict:
        """COPY INTO the category's Snowflake table for one review date.

        copy_into runs the post-load reconciliation (rows_parsed vs
        rows_loaded, rejected rows) and writes RAW.LOAD_AUDIT; the returned
        summary lands in XCom as load evidence.
        """
        return copy_into(category, date.fromisoformat(date_str))

    @task()
    def load_bulk(category: str) -> dict:
        """One COPY INTO covering every processed file for a category."""
        return copy_into_bulk(category)

    # ── Wire up ──────────────────────────────────────────────────────────────

    load_one.expand_kwargs(get_work_items())
    load_bulk.expand(category=get_bulk_categories())


snowflake_dag()
