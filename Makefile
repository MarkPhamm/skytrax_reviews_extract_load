PYTHON := uv run python

# ── Scraper ──────────────────────────────────────────────────────────────────

## Quick smoke test: 1 airline, 1 page (~100 rows). Writes to landing/
scrape-smoke:
	$(PYTHON) include/tasks/extract/scraper.py --max-airlines 1 --pages 1

## Full scrape: all airlines, 100 pages each. Writes to landing/
scrape:
	$(PYTHON) include/tasks/extract/scraper.py --pages 100

# ── Processing ───────────────────────────────────────────────────────────────

## Process yesterday's raw file → landing/processed/
process-yesterday:
	$(PYTHON) include/tasks/transform/processing.py --yesterday

## Process a specific date: make process DATE=2026-03-12
process:
	$(PYTHON) include/tasks/transform/processing.py --date $(DATE)

# ── Upload ────────────────────────────────────────────────────────────────────

## Upload yesterday's raw + processed files to S3
upload-yesterday:
	$(PYTHON) include/tasks/load/s3_upload.py --yesterday

## Upload a specific date: make upload DATE=2026-03-12
upload:
	$(PYTHON) include/tasks/load/s3_upload.py --date $(DATE)

## Dry-run: set STORAGE_MODE=local to skip actual upload
upload-local:
	STORAGE_MODE=local $(PYTHON) include/tasks/load/s3_upload.py --yesterday

# ── Tests ─────────────────────────────────────────────────────────────────────

## Run all unit tests
test:
	uv run pytest tests/ -v

## Run only scraper unit tests
test-scraper:
	uv run pytest tests/extract/test_scraper.py -v

.PHONY: scrape-smoke scrape process-yesterday process test test-scraper
