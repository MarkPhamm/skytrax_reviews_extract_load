"""
DAG: skytrax_crawl

Scrapes all airlines from airlinequality.com, grouped A-Z (26 parallel tasks).
Reviews are split by their actual review date and written as individual CSVs:

  landing/raw/YYYY/MM/raw_data_YYYYMMDD.csv  (one file per review date)

A full scrape will produce hundreds of files going back to 2010.
Daily incremental runs produce only yesterday's file.

Params:
  full_scrape (bool, default False):
    False -> incremental: only fetch reviews posted yesterday
    True  -> full scrape: fetch all pages (use for initial load)
"""

from __future__ import annotations

import json
import os
import string
from datetime import date, datetime, timedelta

import pandas as pd
from airflow.datasets import Dataset
from airflow.decorators import dag, task, task_group
from airflow.models import Param, Variable

from include.tasks.extract.scraper import LANDING_DIR, AllAirlineReviewScraper
from include.tasks.load.s3_upload import upload_raw as _upload

RAW_DATASET = Dataset("skytrax://raw")

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(seconds=0),
}


@dag(
    dag_id="skytrax_crawl",
    schedule="0 2 * * *",
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

    @task()
    def get_airline_urls() -> list[tuple[str, str]]:
        scraper = AllAirlineReviewScraper()
        return scraper.get_all_airline_urls()

    @task()
    def scrape_letter(
        letter: str,
        airline_urls: list[tuple[str, str]],
        **context,
    ) -> list[dict]:
        """Scrape all airlines whose name starts with `letter`."""
        subset = [(name, url) for name, url in airline_urls if name.upper().startswith(letter)]
        if not subset:
            return []

        full_scrape: bool = context["params"]["full_scrape"]
        yesterday = date.today() - timedelta(days=1)
        since_date = None if full_scrape else yesterday

        workers = int(Variable.get("SCRAPER_WORKERS", default_var="10"))

        scraper = AllAirlineReviewScraper(
            max_workers=workers,
            since_date=since_date,
        )

        reviews = []
        for name, url in subset:
            reviews.extend(scraper.scrape_airline_reviews(name, url))
        return reviews

    @task()
    def split_and_save(all_reviews: list[list[dict]]) -> list[str]:
        """
        Flatten reviews and write one CSV per review date.

        Returns the list of ISO date strings that were written, e.g.:
          ["2010-03-15", "2010-03-16", ..., "2026-03-12"]
        These are passed to dag_process via an Airflow Variable.
        """
        flat = [r for letter_reviews in all_reviews for r in letter_reviews]
        if not flat:
            raise RuntimeError("No reviews scraped across all letters — aborting.")

        df = pd.DataFrame(flat)

        # Normalise the date column to YYYY-MM-DD so we can group by it.
        # Raw dates come as "22nd March 2025" — strip ordinal suffixes first.
        cleaned = df["date"].str.replace(r"(\d+)(st|nd|rd|th)", r"\1", regex=True)
        df["date"] = pd.to_datetime(cleaned, format="%d %B %Y", errors="coerce").dt.strftime(
            "%Y-%m-%d"
        )
        df = df.dropna(subset=["date"])

        written_dates = []
        for review_date_str, group in df.groupby("date"):
            review_date = date.fromisoformat(review_date_str)
            output_path = (
                LANDING_DIR
                / "raw"
                / review_date.strftime("%Y")
                / review_date.strftime("%m")
                / f"raw_data_{review_date.strftime('%Y%m%d')}.csv"
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)
            group.to_csv(output_path, index=False)
            written_dates.append(review_date_str)

        written_dates.sort()
        # Store for dag_process to pick up
        Variable.set("LAST_CRAWL_DATES", json.dumps(written_dates))
        return written_dates

    @task(outlets=[RAW_DATASET])
    def upload_raw(written_dates: list[str]) -> list[str]:
        """Upload all date-partitioned raw CSVs to S3 (skipped when STORAGE_MODE=local)."""
        if os.getenv("STORAGE_MODE", "local") == "local":
            return []

        bucket = os.environ["S3_BUCKET"]
        uris = []
        for date_str in written_dates:
            uri = _upload(
                date.fromisoformat(date_str),
                bucket=bucket,
                use_airflow_hook=True,
            )
            if uri:
                uris.append(uri)
        return uris

    @task_group(group_id="scrape_airlines")
    def scrape_airlines(airline_urls):
        return [
            scrape_letter.override(task_id=f"scrape_{letter.lower()}")(
                letter=letter,
                airline_urls=airline_urls,
            )
            for letter in string.ascii_uppercase
        ]

    # ── Wire up ──────────────────────────────────────────────────────────────

    airline_urls = get_airline_urls()
    letter_results = scrape_airlines(airline_urls)
    written_dates = split_and_save(letter_results)
    upload_raw(written_dates)


crawl_dag()
