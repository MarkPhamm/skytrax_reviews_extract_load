"""
Skytrax airline review scraper.

Outputs raw CSV to:
  landing/YYYY/MM/raw_data_YYYYMMDD.csv

The landing root is resolved as (in priority order):
  1. LANDING_DIR env var
  2. {project_root}/landing   (project_root = 3 levels above this file)
"""

import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Project root is 3 dirs above this file:
# include/tasks/extract/scraper.py -> ... -> project_root
_PROJECT_ROOT = Path(__file__).resolve().parents[3]

LANDING_DIR = Path(os.getenv("LANDING_DIR", _PROJECT_ROOT / "landing"))


def get_output_path(run_date: date) -> Path:
    """Return the landing path for a given run date.

    Pattern: landing/YYYY/MM/raw_data_YYYYMMDD.csv
    """
    return (
        LANDING_DIR
        / "raw"
        / run_date.strftime("%Y")
        / run_date.strftime("%m")
        / f"raw_data_{run_date.strftime('%Y%m%d')}.csv"
    )


class AllAirlineReviewScraper:
    """
    Scrapes airline reviews from airlinequality.com for all airlines.

    Args:
        num_pages_per_airline: Max pages per airline (100 reviews/page). Ignored when
            since_date is set — scraping stops early once older reviews are reached.
        max_airlines: Cap on number of airlines (None = all).
        run_date: Date used for the output file path partition (default: today).
        since_date: Only collect reviews on or after this date. Because the site
            serves reviews newest-first, scraping stops as soon as an older review
            is found, making date-filtered runs very fast.
    """

    BASE_URL = "https://www.airlinequality.com"
    AIRLINES_LIST_URL = "https://www.airlinequality.com/review-pages/a-z-airline-reviews/"
    PAGE_SIZE = 100

    def __init__(
        self,
        num_pages_per_airline: int = 100,
        max_airlines: Optional[int] = None,
        run_date: Optional[date] = None,
        since_date: Optional[date] = None,
        max_workers: int = 20,
    ):
        self.num_pages_per_airline = num_pages_per_airline
        self.max_airlines = max_airlines
        self.run_date = run_date or date.today()
        self.since_date = since_date
        self.max_workers = max_workers
        # Expand the urllib3 connection pool to match worker count so threads
        # don't queue waiting for a free connection slot.
        self._session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_maxsize=max_workers)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def get_all_airline_urls(self) -> List[Tuple[str, str]]:
        """Return list of (airline_name, airline_url) from the A-Z page."""
        logger.info("Fetching airline list...")

        try:
            response = self._session.get(self.AIRLINES_LIST_URL, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error("Failed to fetch airline list: %s", e)
            return []

        soup = BeautifulSoup(response.content, "html.parser")
        seen = set()
        airlines = []

        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "/airline-reviews/" not in href or href == "/airline-reviews/":
                continue

            name = link.get_text(strip=True) or (
                href.split("/airline-reviews/")[-1].split("/")[0].replace("-", " ").title()
            )
            url = urljoin(self.BASE_URL, href) if href.startswith("/") else href

            if name and url not in seen:
                seen.add(url)
                airlines.append((name, url))

        if self.max_airlines:
            airlines = airlines[: self.max_airlines]

        logger.info("Found %d airlines", len(airlines))
        return airlines

    def scrape_airline_reviews(self, airline_name: str, airline_url: str) -> List[Dict]:
        """Scrape pages for one airline, stopping early if since_date is set."""
        reviews = []

        for page in range(1, self.num_pages_per_airline + 1):
            url = f"{airline_url}/page/{page}/?sortby=post_date%3ADesc&pagesize={self.PAGE_SIZE}"

            response = None
            for attempt in range(1, 4):  # up to 3 attempts
                try:
                    response = self._session.get(url, timeout=30)
                    response.raise_for_status()
                    break
                except requests.Timeout:
                    wait = attempt * 5  # 5s, 10s, 15s
                    if attempt < 3:
                        logger.warning(
                            "Timeout %s page %d (attempt %d) — retrying in %ds",
                            airline_name,
                            page,
                            attempt,
                            wait,
                        )
                        time.sleep(wait)
                    else:
                        logger.warning("Skipping %s page %d after 3 timeouts", airline_name, page)
                except requests.RequestException as e:
                    logger.warning("Skipping %s page %d: %s", airline_name, page, e)
                    break

            if response is None or not response.ok:
                continue

            soup = BeautifulSoup(response.content, "html.parser")
            articles = soup.select('article[class*="comp_media-review-rated"]')

            if not articles:
                break

            done = False
            for article in articles:
                if self.since_date:
                    review_date = self._parse_article_date(article)
                    if review_date is not None and review_date < self.since_date:
                        # Reviews are newest-first — everything from here is older.
                        done = True
                        break

                data = self.extract_review_data(article)
                if data:
                    data["airline_name"] = airline_name
                    reviews.append(data)

            if done:
                break

        logger.info("Scraped %d reviews for %s", len(reviews), airline_name)
        return reviews

    def extract_review_data(self, article: BeautifulSoup) -> Optional[Dict]:
        """Extract fields from a single review article element."""
        data = {
            "date": self._text(article, "time", itemprop="datePublished"),
            "customer_name": self._text(article, "span", itemprop="name"),
            "country": self._extract_country(article),
            "review_body": self._text(article, "div", itemprop="reviewBody"),
        }
        self._extract_ratings(article, data)
        return data

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _parse_article_date(self, article: BeautifulSoup) -> Optional[date]:
        """Return the published date of a review article, or None if unparseable."""
        tag = article.find("time", itemprop="datePublished")
        if not tag:
            return None
        # Prefer machine-readable datetime attribute (ISO), fall back to visible text.
        raw = tag.get("datetime") or tag.text.strip()
        try:
            return date.fromisoformat(raw[:10])
        except (ValueError, TypeError):
            return None

    def _text(self, element: BeautifulSoup, tag: str, **attrs) -> Optional[str]:
        found = element.find(tag, attrs)
        return found.text.strip() if found else None

    def _extract_country(self, article: BeautifulSoup) -> Optional[str]:
        match = article.find(string=lambda t: t and "(" in t and ")" in t)
        if not match:
            return None
        # pull text inside the outermost parens
        m = re.search(r"\(([^)]+)\)", match)
        return m.group(1).strip() if m else None

    def _extract_ratings(self, article: BeautifulSoup, data: Dict) -> None:
        table = article.find("table", class_="review-ratings")
        if not table:
            return

        for row in table.find_all("tr"):
            header = row.find("td", class_="review-rating-header")
            if not header:
                continue

            label = header.text.strip()
            stars_td = row.find("td", class_="review-rating-stars")
            if stars_td:
                data[label] = len(stars_td.find_all("span", class_="star fill"))
            else:
                value_td = row.find("td", class_="review-value")
                if value_td:
                    data[label] = value_td.text.strip()

    # ------------------------------------------------------------------
    # Public orchestration
    # ------------------------------------------------------------------

    def scrape_all_airlines(self) -> List[Path]:
        """
        Scrape all airlines and split results by review date.

        Each review date gets its own CSV file:
            landing/raw/YYYY/MM/raw_data_YYYYMMDD.csv

        Returns:
            List[Path]: Paths of the saved CSV files.
        """
        airline_urls = self.get_all_airline_urls()
        if not airline_urls:
            raise RuntimeError("No airlines found — aborting.")

        all_reviews = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {
                pool.submit(self.scrape_airline_reviews, name, url): name
                for name, url in airline_urls
            }
            for future in as_completed(futures):
                airline_name = futures[future]
                try:
                    all_reviews.extend(future.result())
                except Exception as e:
                    logger.error("Error scraping %s: %s", airline_name, e)

        if not all_reviews:
            raise RuntimeError("No reviews scraped — aborting.")

        df = pd.DataFrame(all_reviews)
        parsed = pd.to_datetime(df["date"], format="mixed", dayfirst=False)
        df["_parsed_date"] = parsed.dt.date.fillna(self.run_date)

        saved_paths = []
        for review_date, group in df.groupby("_parsed_date"):
            output_path = get_output_path(review_date)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            group.drop(columns=["_parsed_date"]).to_csv(output_path, index=False)
            saved_paths.append(output_path)
            logger.info("Saved %d reviews → %s", len(group), output_path)

        logger.info("Total: %d reviews across %d date files", len(df), len(saved_paths))
        return saved_paths


if __name__ == "__main__":
    import argparse
    from datetime import timedelta

    parser = argparse.ArgumentParser(description="Scrape Skytrax airline reviews")
    parser.add_argument(
        "--pages", type=int, default=100, help="Max pages per airline (default: 100)"
    )
    parser.add_argument(
        "--max-airlines", type=int, default=None, help="Cap on number of airlines (default: all)"
    )
    parser.add_argument(
        "--date", type=date.fromisoformat, default=None, help="Run date YYYY-MM-DD (default: today)"
    )
    parser.add_argument(
        "--since",
        type=date.fromisoformat,
        default=None,
        help="Only fetch reviews on or after YYYY-MM-DD",
    )
    parser.add_argument(
        "--yesterday", action="store_true", help="Shorthand: --date yesterday --since yesterday"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Parallel workers for airline scraping (default: 20)",
    )
    args = parser.parse_args()

    yesterday = date.today() - timedelta(days=1)
    run_date = args.date or (yesterday if args.yesterday else date.today())
    since_date = args.since or (yesterday if args.yesterday else None)

    scraper = AllAirlineReviewScraper(
        num_pages_per_airline=args.pages,
        max_airlines=args.max_airlines,
        run_date=run_date,
        since_date=since_date,
        max_workers=args.workers,
    )
    saved_paths = scraper.scrape_all_airlines()
    print(f"Done: {len(saved_paths)} files written")
    for p in saved_paths:
        print(f"  {p}")
