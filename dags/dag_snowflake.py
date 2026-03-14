"""
DAG: skytrax_snowflake

Triggered automatically when dag_process emits PROCESSED_DATASET.

Infrastructure (database, schema, table, stage) is managed by Terraform.
This DAG only runs COPY INTO for each review date.

STORAGE_MODE=local skips this DAG entirely — no Snowflake needed for local dev.
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Variable

from include.tasks.load.snowflake_load import copy_into

PROCESSED_DATASET = Dataset("skytrax://processed")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=0),
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
    def get_dates() -> list[str]:
        """Read the date list written by dag_crawl. Returns [] when local."""
        if os.getenv("STORAGE_MODE", "local") == "local":
            return []

        raw = Variable.get("LAST_CRAWL_DATES", default_var="[]")
        return json.loads(raw)

    @task()
    def load_date(date_str: str) -> None:
        """COPY INTO Snowflake for one review date."""
        copy_into(date.fromisoformat(date_str))

    # ── Wire up ──────────────────────────────────────────────────────────────

    dates = get_dates()
    load_date.expand(date_str=dates)


snowflake_dag()
