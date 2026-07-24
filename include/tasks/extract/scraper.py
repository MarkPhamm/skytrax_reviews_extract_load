"""
Skytrax review scraper for airlinequality.com.

Supports four review categories (see ``CATEGORIES``):

  - All categories can be scraped **date-partitioned** to
      landing/raw/<type>/YYYY/MM/raw_data_YYYYMMDD.csv
    via ``scrape_all_partitioned`` (this is what the Airflow DAGs consume).
  - ``seat`` / ``lounge`` / ``airport`` can also be dumped as a single flat
    combined CSV at landing/all_<category>_review_raw.csv via ``scrape_combined``
    (ad-hoc convenience, no date-partition sub-directories).

Paths/keys are built by ``include.tasks.common.paths`` (single source of truth).
"""

import logging
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from include.tasks.common import paths
from include.tasks.common.paths import LANDING_DIR  # noqa: F401  (re-exported for callers)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BASE_URL = "https://www.airlinequality.com"
PAGE_SIZE = 100

# Politeness defaults for airlinequality.com — identify the bot and pace
# requests so a full scrape does not look like a denial-of-service.
USER_AGENT = (
    "SkytraxReviewsBot/1.0 (+https://github.com/MarkPhamm/skytrax_reviews_extract_load; "
    "research/analytics; contact via GitHub issues)"
)
# Seconds to wait between successful page fetches (retries use their own backoff).
REQUEST_THROTTLE_SECONDS = 0.5

# Link text that isn't an entity name (e.g. a "Read more" link pointing at the
# same URL as the real name link) — fall back to the slug-derived title instead.
_GENERIC_LINK_TEXT = {"read more", "more", "details", "view"}


@dataclass(frozen=True)
class ReviewCategory:
    """Per-category configuration for the Skytrax scraper.

    All four categories share the same page/article HTML; only the index page,
    the entity-link URL substring, and the entity-name column differ.
    """

    key: str  # "airline" | "seat" | "lounge" | "airport"
    index_url: str  # the A-Z index page listing every entity
    href_substring: str  # identifies entity links on the index page, e.g. "/seat-reviews/"
    name_field: str  # output column holding the entity name
    noun: str  # used in log / error messages
    combined_filename: Optional[str]  # flat output file; None = no flat-combine (airline)


CATEGORIES: Dict[str, ReviewCategory] = {
    "airline": ReviewCategory(
        key="airline",
        index_url=f"{BASE_URL}/review-pages/a-z-airline-reviews/",
        href_substring="/airline-reviews/",
        name_field="airline_name",
        noun="airlines",
        combined_filename=None,
    ),
    "seat": ReviewCategory(
        key="seat",
        index_url=f"{BASE_URL}/review-pages/a-z-seat-reviews/",
        href_substring="/seat-reviews/",
        name_field="airline_name",
        noun="seat-review airlines",
        combined_filename="all_seats_review_raw.csv",
    ),
    "lounge": ReviewCategory(
        key="lounge",
        index_url=f"{BASE_URL}/review-pages/a-z-lounge-reviews/",
        href_substring="/lounge-reviews/",
        name_field="airline_name",
        noun="lounge-review airlines",
        combined_filename="all_lounge_review_raw.csv",
    ),
    "airport": ReviewCategory(
        key="airport",
        index_url=f"{BASE_URL}/review-pages/a-z-airport-reviews/",
        href_substring="/airport-reviews/",
        name_field="airport_name",
        noun="airports",
        combined_filename="all_airport_review_raw.csv",
    ),
}


class ReviewScraper:
    """
    Scrapes reviews from airlinequality.com for a given category.

    Args:
        category: One of "airline", "seat", "lounge", "airport" (or a ReviewCategory).
        num_pages_per_entity: Max pages per entity (100 reviews/page). Ignored when
            since_date is set — scraping stops early once older reviews are reached.
        max_entities: Cap on number of entities scraped (None = all).
        run_date: Date used for the airline output path partition (default: today).
        since_date: Only collect reviews on or after this date. Because the site
            serves reviews newest-first, scraping stops as soon as an older review
            is found, making date-filtered runs very fast.
        max_workers: Parallel workers for entity scraping.
    """

    def __init__(
        self,
        category: Union[str, ReviewCategory] = "airline",
        num_pages_per_entity: int = 100,
        max_entities: Optional[int] = None,
        run_date: Optional[date] = None,
        since_date: Optional[date] = None,
        max_workers: int = 20,
    ):
        self.category = category if isinstance(category, ReviewCategory) else CATEGORIES[category]
        self.num_pages_per_entity = num_pages_per_entity
        self.max_entities = max_entities
        self.run_date = run_date or date.today()
        self.since_date = since_date
        self.max_workers = max_workers
        # Expand the urllib3 connection pool to match worker count so threads
        # don't queue waiting for a free connection slot.
        self._session = requests.Session()
        self._session.headers.update({"User-Agent": USER_AGENT})
        adapter = requests.adapters.HTTPAdapter(pool_maxsize=max_workers)
        self._session.mount("https://", adapter)
        self._session.mount("http://", adapter)

    def _get(self, url: str, timeout: int = 30) -> requests.Response:
        """GET with a polite inter-request throttle after each attempt."""
        try:
            return self._session.get(url, timeout=timeout)
        finally:
            time.sleep(REQUEST_THROTTLE_SECONDS)

    def get_entity_urls(self) -> List[Tuple[str, str]]:
        """Return list of (entity_name, entity_url) from the category's A-Z page."""
        logger.info("Fetching %s list...", self.category.key)

        try:
            response = self._get(self.category.index_url, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            logger.error("Failed to fetch %s list: %s", self.category.key, e)
            return []

        soup = BeautifulSoup(response.content, "html.parser")
        sub = self.category.href_substring
        by_url: Dict[str, str] = {}
        order: List[str] = []

        for link in soup.find_all("a", href=True):
            href = link["href"]
            # Skip links for other categories and the bare index link itself.
            if sub not in href or href == sub:
                continue

            text = link.get_text(strip=True)
            fallback = href.split(sub)[-1].split("/")[0].replace("-", " ").title()
            # Some index-page entries have a second "Read more"-style link
            # pointing at the same URL — treat that as no name and fall back.
            name = text if text and text.lower() not in _GENERIC_LINK_TEXT else fallback
            url = urljoin(BASE_URL, href) if href.startswith("/") else href

            if url not in by_url:
                by_url[url] = name
                order.append(url)
            elif by_url[url] == fallback and name != fallback:
                # An earlier link for this URL only yielded the generic
                # fallback — prefer the real name found on a later link.
                by_url[url] = name

        entities = [(by_url[url], url) for url in order]

        if self.max_entities:
            entities = entities[: self.max_entities]

        logger.info("Found %d %s", len(entities), self.category.noun)
        return entities

    def scrape_entity_reviews(self, entity_name: str, entity_url: str) -> List[Dict]:
        """Scrape pages for one entity, stopping early if since_date is set."""
        reviews = []

        for page in range(1, self.num_pages_per_entity + 1):
            url = f"{entity_url}/page/{page}/?sortby=post_date%3ADesc&pagesize={PAGE_SIZE}"

            response = None
            for attempt in range(1, 4):  # up to 3 attempts
                try:
                    response = self._get(url, timeout=30)
                    response.raise_for_status()
                    break
                except requests.Timeout:
                    wait = attempt * 5  # 5s, 10s, 15s
                    if attempt < 3:
                        logger.warning(
                            "Timeout %s page %d (attempt %d) — retrying in %ds",
                            entity_name,
                            page,
                            attempt,
                            wait,
                        )
                        time.sleep(wait)
                    else:
                        logger.warning("Skipping %s page %d after 3 timeouts", entity_name, page)
                except requests.RequestException as e:
                    logger.warning("Skipping %s page %d: %s", entity_name, page, e)
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
                    data[self.category.name_field] = entity_name
                    reviews.append(data)

            if done:
                break

        logger.info("Scraped %d reviews for %s", len(reviews), entity_name)
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
        # The country sits as "(Country)" text right next to the reviewer's
        # name span, inside the review's header — NOT anywhere in the
        # article. Searching the whole article (as this used to) can match
        # parenthesised text inside the review body itself (flight numbers,
        # aircraft codes, ages) and leak garbage into this field.
        name_span = article.find("span", itemprop="name")
        if not name_span:
            return None
        header = name_span.find_parent("h3") or name_span.parent
        if not header:
            return None
        match = header.find(string=lambda t: t and "(" in t and ")" in t)
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

    def _scrape_all_entities(self) -> List[Dict]:
        """Fan-out scrape every entity in the category and return all review rows."""
        entity_urls = self.get_entity_urls()
        if not entity_urls:
            raise RuntimeError(f"No {self.category.noun} found — aborting.")

        all_reviews: List[Dict] = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futures = {
                pool.submit(self.scrape_entity_reviews, name, url): name
                for name, url in entity_urls
            }
            for future in as_completed(futures):
                entity_name = futures[future]
                try:
                    all_reviews.extend(future.result())
                except Exception as e:
                    logger.error("Error scraping %s: %s", entity_name, e)

        if not all_reviews:
            raise RuntimeError("No reviews scraped — aborting.")
        return all_reviews

    def scrape_all_partitioned(self) -> List[Path]:
        """
        Scrape every entity and split results by review date.

        Each review date gets its own CSV file under the category's partition:
            landing/raw/<type>/YYYY/MM/raw_data_YYYYMMDD.csv

        Returns:
            List[Path]: Paths of the saved CSV files.
        """
        all_reviews = self._scrape_all_entities()

        df = pd.DataFrame(all_reviews)
        parsed = pd.to_datetime(df["date"], format="mixed", dayfirst=False)
        df["_parsed_date"] = parsed.dt.date.fillna(self.run_date)

        saved_paths = []
        for review_date, group in df.groupby("_parsed_date"):
            output_path = paths.raw_local_path(self.category.key, review_date)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            group.drop(columns=["_parsed_date"]).to_csv(output_path, index=False)
            saved_paths.append(output_path)
            logger.info("Saved %d reviews → %s", len(group), output_path)

        logger.info("Total: %d reviews across %d date files", len(df), len(saved_paths))
        return saved_paths

    def scrape_combined(self) -> Path:
        """
        Scrape every entity and write ALL reviews to a single flat CSV:
            landing/all_<category>_review.csv

        Columns are the union of fields seen across entities (pandas fills any
        rating label missing from a given review with NaN). Returns the path.
        """
        if not self.category.combined_filename:
            raise ValueError(f"Category '{self.category.key}' has no combined output configured.")

        all_reviews = self._scrape_all_entities()

        df = pd.DataFrame(all_reviews)
        output_path = paths.LANDING_DIR / self.category.combined_filename
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False)
        logger.info("Saved %d %s reviews → %s", len(df), self.category.key, output_path)
        return output_path


class AllAirlineReviewScraper(ReviewScraper):
    """Backwards-compatible airline scraper used by the Airflow DAGs.

    Preserves the original constructor kwargs and method names so the existing
    DAGs (dag_crawl) and tests keep working unchanged.
    """

    def __init__(
        self,
        num_pages_per_airline: int = 100,
        max_airlines: Optional[int] = None,
        run_date: Optional[date] = None,
        since_date: Optional[date] = None,
        max_workers: int = 20,
    ):
        super().__init__(
            category="airline",
            num_pages_per_entity=num_pages_per_airline,
            max_entities=max_airlines,
            run_date=run_date,
            since_date=since_date,
            max_workers=max_workers,
        )

    def get_all_airline_urls(self) -> List[Tuple[str, str]]:
        return self.get_entity_urls()

    def scrape_airline_reviews(self, airline_name: str, airline_url: str) -> List[Dict]:
        return self.scrape_entity_reviews(airline_name, airline_url)

    def scrape_all_airlines(self) -> List[Path]:
        return self.scrape_all_partitioned()


if __name__ == "__main__":
    import argparse
    from datetime import timedelta

    parser = argparse.ArgumentParser(
        description="Scrape Skytrax reviews (airline / seat / lounge / airport)"
    )
    parser.add_argument(
        "--category",
        choices=list(CATEGORIES),
        default="airline",
        help="Review category to scrape (default: airline).",
    )
    parser.add_argument(
        "--combined",
        action="store_true",
        help=(
            "Ad-hoc: write one flat landing/all_<category>_review_raw.csv instead of"
            " date-partitioned raw/<type>/YYYY/MM/ files (seat/lounge/airport only)."
        ),
    )
    parser.add_argument(
        "--pages", type=int, default=100, help="Max pages per entity (default: 100)"
    )
    parser.add_argument(
        "--max-entities",
        "--max-airlines",
        dest="max_entities",
        type=int,
        default=None,
        help="Cap on number of entities/airlines/airports (default: all)",
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
        help="Parallel workers for entity scraping (default: 10)",
    )
    args = parser.parse_args()

    yesterday = date.today() - timedelta(days=1)
    run_date = args.date or (yesterday if args.yesterday else date.today())
    since_date = args.since or (yesterday if args.yesterday else None)

    scraper = ReviewScraper(
        category=args.category,
        num_pages_per_entity=args.pages,
        max_entities=args.max_entities,
        run_date=run_date,
        since_date=since_date,
        max_workers=args.workers,
    )

    if args.combined:
        output_path = scraper.scrape_combined()
        print(f"Done: {output_path}")
    else:
        saved_paths = scraper.scrape_all_partitioned()
        print(f"Done: {len(saved_paths)} files written")
        for p in saved_paths:
            print(f"  {p}")
