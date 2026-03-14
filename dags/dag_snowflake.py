"""
DAG: skytrax_snowflake

Triggered automatically when dag_process emits PROCESSED_DATASET.

Steps:
  1. setup        — create DB / schema / table / S3 stage (idempotent)
  2. load_date    — COPY INTO for each review date (dynamically mapped)

STORAGE_MODE=local skips this DAG entirely — no Snowflake needed for local dev.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable

from include.tasks.load.snowflake_load import copy_into, ensure_stage, ensure_table

PROCESSED_DATASET = Dataset("skytrax://processed")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


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
    def check_storage_mode() -> str:
        return Variable.get("STORAGE_MODE", default_var="local")

    @task()
    def setup(storage_mode: str) -> None:
        """Create table + S3 stage. Skipped when local."""
        if storage_mode == "local":
            return

        ensure_table()
        ensure_stage(
            bucket=Variable.get("S3_BUCKET"),
            role_arn=Variable.get("SNOWFLAKE_ROLE_ARN"),
        )

    @task()
    def get_dates(storage_mode: str) -> list[str]:
        """Read the date list written by dag_crawl. Returns [] when local."""
        if storage_mode == "local":
            return []

        raw = Variable.get("LAST_CRAWL_DATES", default_var="[]")
        return json.loads(raw)

    @task()
    def load_date(date_str: str) -> None:
        """COPY INTO Snowflake for one review date."""
        copy_into(date.fromisoformat(date_str))

    # ── Wire up ──────────────────────────────────────────────────────────────

    storage_mode = check_storage_mode()
    setup(storage_mode)
    dates = get_dates(storage_mode)
    load_date.expand(date_str=dates)


snowflake_dag()
