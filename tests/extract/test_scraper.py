"""
Unit tests for include/tasks/extract/scraper.py

Run with:  pytest tests/extract/test_scraper.py -v
"""

import textwrap
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from bs4 import BeautifulSoup

from include.tasks.extract.scraper import AllAirlineReviewScraper, get_output_path


# ---------------------------------------------------------------------------
# get_output_path
# ---------------------------------------------------------------------------


def test_get_output_path_format():
    path = get_output_path(date(2025, 3, 27))
    assert path.parts[-1] == "raw_data_20250327.csv"
    assert path.parts[-2] == "03"
    assert path.parts[-3] == "2025"
    assert path.parts[-4] == "raw"


def test_get_output_path_zero_padded_month():
    path = get_output_path(date(2025, 1, 5))
    assert path.parts[-2] == "01"
    assert path.parts[-1] == "raw_data_20250105.csv"
    assert path.parts[-4] == "raw"


def test_get_output_path_ends_in_landing(tmp_path, monkeypatch):
    monkeypatch.setenv("LANDING_DIR", str(tmp_path))
    # Re-import to pick up the new env var value
    import importlib
    import include.tasks.extract.scraper as mod
    importlib.reload(mod)
    path = mod.get_output_path(date(2025, 6, 15))
    assert path.is_relative_to(tmp_path)


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
    reviews = scraper.scrape_airline_reviews("Test Air", "https://example.com/airline-reviews/test-air")

    assert len(reviews) == 1
    assert reviews[0]["customer_name"] == "Alice"
    assert reviews[0]["airline_name"] == "Test Air"


@patch("include.tasks.extract.scraper.requests.Session.get")
def test_scrape_airline_reviews_skips_failed_pages(mock_get):
    import requests as req
    mock_get.side_effect = req.RequestException("timeout")
    scraper = AllAirlineReviewScraper(num_pages_per_airline=2)
    reviews = scraper.scrape_airline_reviews("Test Air", "https://example.com/airline-reviews/test-air")
    assert reviews == []


# ---------------------------------------------------------------------------
# AllAirlineReviewScraper.scrape_all_airlines (save to disk)
# ---------------------------------------------------------------------------


@patch("include.tasks.extract.scraper.requests.Session.get")
def test_scrape_all_airlines_saves_csv(mock_get, tmp_path, monkeypatch):
    monkeypatch.setattr("include.tasks.extract.scraper.LANDING_DIR", tmp_path)

    mock_get.side_effect = [
        _mock_response(_AZ_HTML),            # A-Z page
        _mock_response(_PAGE_HTML),           # BA page 1
        _mock_response(_EMPTY_PAGE_HTML),     # BA page 2 → stop
        _mock_response(_PAGE_HTML),           # Lufthansa page 1
        _mock_response(_EMPTY_PAGE_HTML),     # Lufthansa page 2 → stop
    ]

    scraper = AllAirlineReviewScraper(num_pages_per_airline=5, run_date=date(2025, 3, 27))
    output = scraper.scrape_all_airlines()

    assert output.exists()
    assert output.suffix == ".csv"
    df = pd.read_csv(output)
    assert len(df) == 2  # 1 review per airline
    assert set(df["airline_name"]) == {"British Airways", "Lufthansa"}


@patch("include.tasks.extract.scraper.requests.Session.get")
def test_scrape_all_airlines_raises_when_no_airlines(mock_get):
    mock_get.return_value = _mock_response("<html><body></body></html>")
    scraper = AllAirlineReviewScraper()
    with pytest.raises(RuntimeError, match="No airlines found"):
        scraper.scrape_all_airlines()
