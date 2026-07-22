"""
DAG: skytrax_snowflake

Triggered automatically when dag_process emits PROCESSED_DATASET.

Infrastructure (database, schema, per-type tables, stage) is managed by Terraform.
This DAG only runs COPY INTO for each (review type, review date) into its table.

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
    def get_work_items() -> list[dict]:
        """Expand {category: [dates]} into [{category, date_str}, ...]. Empty when local."""
        if os.getenv("STORAGE_MODE", "local") == "local":
            return []

        mapping = json.loads(Variable.get("LAST_CRAWL_DATES", default_var="{}"))
        return [
            {"category": category, "date_str": d}
            for category, dates in mapping.items()
            for d in dates
        ]

    @task()
    def load_one(category: str, date_str: str) -> None:
        """COPY INTO the category's Snowflake table for one review date."""
        copy_into(category, date.fromisoformat(date_str))

    # ── Wire up ──────────────────────────────────────────────────────────────

    load_one.expand_kwargs(get_work_items())


snowflake_dag()
