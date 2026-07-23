"""
DAG: skytrax_crawl

Scrapes all four review types from airlinequality.com and writes date-partitioned
raw CSVs, one file per (type, review date):

  landing/raw/<type>/YYYY/MM/raw_data_YYYYMMDD.csv

A full scrape produces hundreds of files per type going back years.
Daily incremental runs produce only yesterday's files.

Params:
  full_scrape (bool, default False):
    False -> incremental: only fetch reviews posted yesterday
    True  -> full scrape: fetch all pages (use for initial load)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import date, datetime, timedelta

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Param, Variable
from airflow.operators.python import get_current_context

from include.tasks.extract.scraper import CATEGORIES, ReviewScraper
from include.tasks.load.s3_upload import upload_raw as _upload

logger = logging.getLogger(__name__)

RAW_DATASET = Dataset("skytrax://raw")

# Singular category keys: airline, seat, lounge, airport
REVIEW_TYPES = list(CATEGORIES)

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=0),
}


@dag(
    dag_id="skytrax_crawl",
    schedule="0 16 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["skytrax", "extract"],
    params={
        "full_scrape": Param(
            False,
            type="boolean",
            description=(
                "True = full re-scrape all pages (initial load)."
                " False = yesterday only (daily incremental)."
            ),
        ),
    },
)
def crawl_dag():

    @task(max_active_tis_per_dagrun=2)
    def scrape_type(review_type: str) -> dict:
        """Scrape one review type and write date-partitioned raw CSVs.

        Returns {"review_type": <category>, "dates": [ISO date strings]}.
        Entity-level parallelism is handled inside the scraper (max_workers).
        """
        context = get_current_context()
        full_scrape: bool = context["params"]["full_scrape"]
        # Derived from this run's logical_date (not wall-clock date.today()) so
        # clearing/rerunning a past run recomputes the correct target day instead
        # of silently re-scraping "yesterday relative to right now".
        run_day = context["logical_date"].date()
        since_date = None if full_scrape else run_day - timedelta(days=1)

        default_workers = "10" if full_scrape else "3"
        workers = int(Variable.get("SCRAPER_WORKERS", default_var=default_workers))

        scraper = ReviewScraper(
            category=review_type,
            since_date=since_date,
            max_workers=workers,
        )

        try:
            saved_paths = scraper.scrape_all_partitioned()
        except RuntimeError as e:
            # Incremental runs with no new reviews are normal — don't fail the DAG.
            logger.warning("No data for %s: %s", review_type, e)
            return {"review_type": review_type, "dates": []}

        dates = set()
        for p in saved_paths:
            ymd = p.stem.replace("raw_data_", "")  # YYYYMMDD
            dates.add(date(int(ymd[:4]), int(ymd[4:6]), int(ymd[6:])).isoformat())

        return {"review_type": review_type, "dates": sorted(dates)}

    @task()
    def record_dates(results: list[dict]) -> dict[str, list[str]]:
        """Collapse per-type results into {category: [ISO dates]} for downstream DAGs."""
        mapping = {r["review_type"]: r["dates"] for r in results if r and r["dates"]}
        if not mapping:
            raise RuntimeError("No reviews scraped across any type — aborting.")
        Variable.set("LAST_CRAWL_DATES", json.dumps(mapping))
        return mapping

    @task(outlets=[RAW_DATASET])
    def upload_raw(mapping: dict[str, list[str]]) -> list[str]:
        """Upload every (type, date) raw CSV to S3 (skipped when STORAGE_MODE=local)."""
        if os.getenv("STORAGE_MODE", "local") == "local":
            return []

        bucket = os.environ["S3_BUCKET"]
        uris = []
        for category, dates in mapping.items():
            for date_str in dates:
                uri = _upload(
                    category,
                    date.fromisoformat(date_str),
                    bucket=bucket,
                    use_airflow_hook=True,
                )
                if uri:
                    uris.append(uri)
        return uris

    # ── Wire up ──────────────────────────────────────────────────────────────

    results = scrape_type.expand(review_type=REVIEW_TYPES)
    mapping = record_dates(results)
    upload_raw(mapping)


crawl_dag()
