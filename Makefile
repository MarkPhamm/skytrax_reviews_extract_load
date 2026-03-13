PYTHON := uv run python

# ── Scraper ──────────────────────────────────────────────────────────────────

## Quick smoke test: 1 airline, 1 page (~100 rows). Writes to landing/
scrape-smoke:
	$(PYTHON) include/tasks/extract/scraper.py --max-airlines 1 --pages 1

## Full scrape: all airlines, 100 pages each. Writes to landing/
scrape:
	$(PYTHON) include/tasks/extract/scraper.py --pages 100

# ── Tests ─────────────────────────────────────────────────────────────────────

## Run all unit tests
test:
	uv run pytest tests/ -v

## Run only scraper unit tests
test-scraper:
	uv run pytest tests/extract/test_scraper.py -v

.PHONY: scrape-smoke scrape test test-scraper
