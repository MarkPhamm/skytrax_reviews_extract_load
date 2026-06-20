"""
Unit tests for include/tasks/extract/scraper.py

Run with:  pytest tests/extract/test_scraper.py -v
"""

import textwrap
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from bs4 import BeautifulSoup

import include.tasks.common.paths as paths_mod
from include.tasks.extract.scraper import (
    CATEGORIES,
    AllAirlineReviewScraper,
    ReviewScraper,
)

# ---------------------------------------------------------------------------
# AllAirlineReviewScraper.get_all_airline_urls
# ---------------------------------------------------------------------------

_AZ_HTML = textwrap.dedent("""\
    <html><body>
      <a href="/airline-reviews/british-airways">British Airways</a>
      <a href="/airline-reviews/lufthansa">Lufthansa</a>
      <a href="/airline-reviews/">All Reviews</a>
      <a href="/something-else">Ignore me</a>
    </body></html>
""")


def _mock_response(html: str, status: int = 200) -> MagicMock:
    r = MagicMock()
    r.status_code = status
    r.content = html.encode()
    r.raise_for_status = MagicMock()
    return r


@patch("include.tasks.extract.scraper.requests.Session.get")
def test_get_all_airline_urls_returns_airlines(mock_get):
    mock_get.return_value = _mock_response(_AZ_HTML)
    scraper = AllAirlineReviewScraper()
    airlines = scraper.get_all_airline_urls()

    names = [a[0] for a in airlines]
    urls = [a[1] for a in airlines]

    assert "British Airways" in names
    assert "Lufthansa" in names
    assert any("british-airways" in u for u in urls)
    # The generic "/airline-reviews/" link must be excluded
    assert not any(u.endswith("/airline-reviews/") for u in urls)


@patch("include.tasks.extract.scraper.requests.Session.get")
def test_get_all_airline_urls_deduplicates(mock_get):
    duplicate_html = textwrap.dedent("""\
        <html><body>
          <a href="/airline-reviews/british-airways">British Airways</a>
          <a href="/airline-reviews/british-airways">British Airways</a>
        </body></html>
    """)
    mock_get.return_value = _mock_response(duplicate_html)
    scraper = AllAirlineReviewScraper()
    airlines = scraper.get_all_airline_urls()
    assert len(airlines) == 1


@patch("include.tasks.extract.scraper.requests.Session.get")
def test_get_all_airline_urls_respects_max(mock_get):
    mock_get.return_value = _mock_response(_AZ_HTML)
    scraper = AllAirlineReviewScraper(max_airlines=1)
    airlines = scraper.get_all_airline_urls()
    assert len(airlines) == 1


@patch("include.tasks.extract.scraper.requests.Session.get")
def test_get_all_airline_urls_returns_empty_on_error(mock_get):
    import requests as req

    mock_get.side_effect = req.RequestException("timeout")
    scraper = AllAirlineReviewScraper()
    assert scraper.get_all_airline_urls() == []


# ---------------------------------------------------------------------------
# AllAirlineReviewScraper.extract_review_data
# ---------------------------------------------------------------------------

_REVIEW_HTML = textwrap.dedent("""\
    <article class="comp comp_media-review-rated">
      <time itemprop="datePublished">2024-11-01</time>
      <span itemprop="name">John Doe</span>
      <div>(United Kingdom)</div>
      <div itemprop="reviewBody">✅ Trip Verified | Great flight overall.</div>
      <table class="review-ratings">
        <tr>
          <td class="review-rating-header">Seat Comfort</td>
          <td class="review-rating-stars">
            <span class="star fill"></span>
            <span class="star fill"></span>
            <span class="star fill"></span>
            <span class="star"></span>
            <span class="star"></span>
          </td>
        </tr>
        <tr>
          <td class="review-rating-header">Type Of Traveller</td>
          <td class="review-value">Solo Leisure</td>
        </tr>
      </table>
    </article>
""")


def test_extract_review_data_fields():
    scraper = AllAirlineReviewScraper()
    soup = BeautifulSoup(_REVIEW_HTML, "html.parser")
    article = soup.find("article")
    data = scraper.extract_review_data(article)

    assert data["date"] == "2024-11-01"
    assert data["customer_name"] == "John Doe"
    assert data["country"] == "United Kingdom"
    assert "Great flight overall" in data["review_body"]


def test_extract_review_data_star_rating():
    scraper = AllAirlineReviewScraper()
    soup = BeautifulSoup(_REVIEW_HTML, "html.parser")
    article = soup.find("article")
    data = scraper.extract_review_data(article)
    assert data["Seat Comfort"] == 3


def test_extract_review_data_text_rating():
    scraper = AllAirlineReviewScraper()
    soup = BeautifulSoup(_REVIEW_HTML, "html.parser")
    article = soup.find("article")
    data = scraper.extract_review_data(article)
    assert data["Type Of Traveller"] == "Solo Leisure"


def test_extract_review_data_missing_table():
    html = "<article class='comp_media-review-rated'><div itemprop='reviewBody'>ok</div></article>"
    scraper = AllAirlineReviewScraper()
    soup = BeautifulSoup(html, "html.parser")
    article = soup.find("article")
    data = scraper.extract_review_data(article)
    # Should not raise; ratings simply absent
    assert "Seat Comfort" not in data


# ---------------------------------------------------------------------------
# AllAirlineReviewScraper.scrape_airline_reviews
# ---------------------------------------------------------------------------

_PAGE_HTML = textwrap.dedent("""\
    <html><body>
      <article class="comp comp_media-review-rated">
        <time itemprop="datePublished">2024-01-10</time>
        <span itemprop="name">Alice</span>
        <div itemprop="reviewBody">Good flight</div>
      </article>
    </body></html>
""")

_EMPTY_PAGE_HTML = "<html><body></body></html>"


@patch("include.tasks.extract.scraper.requests.Session.get")
def test_scrape_airline_reviews_returns_reviews(mock_get):
    mock_get.side_effect = [
        _mock_response(_PAGE_HTML),
        _mock_response(_EMPTY_PAGE_HTML),  # page 2 → no articles → stop
    ]
    scraper = AllAirlineReviewScraper(num_pages_per_airline=5)
    reviews = scraper.scrape_airline_reviews(
        "Test Air", "https://example.com/airline-reviews/test-air"
    )

    assert len(reviews) == 1
    assert reviews[0]["customer_name"] == "Alice"
    assert reviews[0]["airline_name"] == "Test Air"


@patch("include.tasks.extract.scraper.requests.Session.get")
def test_scrape_airline_reviews_skips_failed_pages(mock_get):
    import requests as req

    mock_get.side_effect = req.RequestException("connection error")
    scraper = AllAirlineReviewScraper(num_pages_per_airline=2)
    reviews = scraper.scrape_airline_reviews(
        "Test Air", "https://example.com/airline-reviews/test-air"
    )
    assert reviews == []


@patch("include.tasks.extract.scraper.time.sleep")
@patch("include.tasks.extract.scraper.requests.Session.get")
def test_scrape_airline_reviews_retries_on_timeout(mock_get, mock_sleep):
    import requests as req

    # First two calls timeout, third succeeds
    mock_get.side_effect = [
        req.Timeout("timed out"),
        req.Timeout("timed out"),
        _mock_response(_PAGE_HTML),
        _mock_response(_EMPTY_PAGE_HTML),
    ]
    scraper = AllAirlineReviewScraper(num_pages_per_airline=2)
    reviews = scraper.scrape_airline_reviews(
        "Test Air", "https://example.com/airline-reviews/test-air"
    )
    assert len(reviews) == 1
    assert mock_sleep.call_count == 2  # slept between retries


# ---------------------------------------------------------------------------
# AllAirlineReviewScraper.scrape_all_airlines (save to disk)
# ---------------------------------------------------------------------------


@patch("include.tasks.extract.scraper.requests.Session.get")
def test_scrape_all_airlines_saves_csv(mock_get, tmp_path, monkeypatch):
    monkeypatch.setattr(paths_mod, "LANDING_DIR", tmp_path)

    mock_get.side_effect = [
        _mock_response(_AZ_HTML),  # A-Z page
        _mock_response(_PAGE_HTML),  # BA page 1
        _mock_response(_EMPTY_PAGE_HTML),  # BA page 2 → stop
        _mock_response(_PAGE_HTML),  # Lufthansa page 1
        _mock_response(_EMPTY_PAGE_HTML),  # Lufthansa page 2 → stop
    ]

    scraper = AllAirlineReviewScraper(num_pages_per_airline=5, run_date=date(2025, 3, 27))
    outputs = scraper.scrape_all_airlines()

    # Both test reviews have date 2024-01-10, so they land in one file
    assert len(outputs) == 1
    assert outputs[0].exists()
    # Type-first partition: .../raw/airlines/2024/01/raw_data_20240110.csv
    assert outputs[0].parts[-5:] == ("raw", "airlines", "2024", "01", "raw_data_20240110.csv")
    df = pd.read_csv(outputs[0])
    assert len(df) == 2  # 1 review per airline
    assert set(df["airline_name"]) == {"British Airways", "Lufthansa"}


@patch("include.tasks.extract.scraper.requests.Session.get")
def test_scrape_all_airlines_raises_when_no_airlines(mock_get):
    mock_get.return_value = _mock_response("<html><body></body></html>")
    scraper = AllAirlineReviewScraper()
    with pytest.raises(RuntimeError, match="No airlines found"):
        scraper.scrape_all_airlines()


# ---------------------------------------------------------------------------
# ReviewScraper — seat / lounge / airport categories
# ---------------------------------------------------------------------------

# A-Z seat index page mixing the wanted category, another category, and the
# bare index link — only the real seat entity should survive filtering.
_SEAT_AZ_HTML = textwrap.dedent("""\
    <html><body>
      <a href="/seat-reviews/british-airways">British Airways</a>
      <a href="/airline-reviews/lufthansa">Lufthansa</a>
      <a href="/seat-reviews/">All Seat Reviews</a>
      <a href="/something-else">Ignore me</a>
    </body></html>
""")

_AIRPORT_AZ_HTML = textwrap.dedent("""\
    <html><body>
      <a href="/airport-reviews/aberdeen-airport">Aberdeen Airport</a>
    </body></html>
""")


def test_category_combined_filenames():
    assert CATEGORIES["seat"].combined_filename == "all_seats_review_raw.csv"
    assert CATEGORIES["lounge"].combined_filename == "all_lounge_review_raw.csv"
    assert CATEGORIES["airport"].combined_filename == "all_airport_review_raw.csv"
    # airline has no flat-combine output (it uses the date-partitioned pipeline)
    assert CATEGORIES["airline"].combined_filename is None


@patch("include.tasks.extract.scraper.requests.Session.get")
def test_get_entity_urls_filters_to_category(mock_get):
    mock_get.return_value = _mock_response(_SEAT_AZ_HTML)
    scraper = ReviewScraper(category="seat")
    entities = scraper.get_entity_urls()

    names = [n for n, _ in entities]
    urls = [u for _, u in entities]

    # Only the /seat-reviews/ entity is kept — the airline link and bare index
    # link are both excluded.
    assert names == ["British Airways"]
    assert all("/seat-reviews/" in u for u in urls)
    assert not any("/airline-reviews/" in u for u in urls)
    assert not any(u.endswith("/seat-reviews/") for u in urls)


@patch("include.tasks.extract.scraper.requests.Session.get")
def test_scrape_entity_reviews_sets_airline_name_for_seat(mock_get):
    mock_get.side_effect = [
        _mock_response(_PAGE_HTML),
        _mock_response(_EMPTY_PAGE_HTML),
    ]
    scraper = ReviewScraper(category="seat", num_pages_per_entity=5)
    reviews = scraper.scrape_entity_reviews(
        "British Airways", "https://example.com/seat-reviews/british-airways"
    )
    assert len(reviews) == 1
    assert reviews[0]["airline_name"] == "British Airways"


@patch("include.tasks.extract.scraper.requests.Session.get")
def test_scrape_entity_reviews_sets_airport_name(mock_get):
    mock_get.side_effect = [
        _mock_response(_PAGE_HTML),
        _mock_response(_EMPTY_PAGE_HTML),
    ]
    scraper = ReviewScraper(category="airport", num_pages_per_entity=5)
    reviews = scraper.scrape_entity_reviews(
        "Aberdeen Airport", "https://example.com/airport-reviews/aberdeen-airport"
    )
    assert len(reviews) == 1
    # airport reviews key the entity name as airport_name, not airline_name
    assert reviews[0]["airport_name"] == "Aberdeen Airport"
    assert "airline_name" not in reviews[0]


def test_scrape_combined_rejects_airline():
    scraper = ReviewScraper(category="airline")
    with pytest.raises(ValueError, match="no combined output"):
        scraper.scrape_combined()


@patch("include.tasks.extract.scraper.requests.Session.get")
def test_scrape_combined_writes_single_flat_csv(mock_get, tmp_path, monkeypatch):
    monkeypatch.setattr(paths_mod, "LANDING_DIR", tmp_path)

    mock_get.side_effect = [
        _mock_response(_SEAT_AZ_HTML),  # A-Z seat index → 1 entity (British Airways)
        _mock_response(_PAGE_HTML),  # BA seat page 1
        _mock_response(_EMPTY_PAGE_HTML),  # BA seat page 2 → stop
    ]

    scraper = ReviewScraper(category="seat", num_pages_per_entity=5)
    output_path = scraper.scrape_combined()

    # A single flat CSV in the landing root — no YYYY/MM partition dirs.
    assert output_path == tmp_path / "all_seats_review_raw.csv"
    assert output_path.exists()
    df = pd.read_csv(output_path)
    assert len(df) == 1
    assert df["airline_name"].tolist() == ["British Airways"]
    assert df["customer_name"].tolist() == ["Alice"]
