PYTHON := uv run python

# ── Airflow / Astronomer ──────────────────────────────────────────────────────

## First-time setup: build the Astronomer image and patch docker-compose.override.yml
## with the correct local image name. Pass PORT=NNNN if 8080 is taken on your machine.
##   make dev-setup           # uses default port 8080
##   make dev-setup PORT=8081 # uses port 8081
dev-setup:
	@echo "Building Astronomer image..."
	@astro dev start --no-browser 2>&1 | tail -3; astro dev stop 2>/dev/null || true
	@IMAGE=$$(docker images --format '{{.Repository}}:{{.Tag}}' | grep 'airflow:latest' | grep skytrax | head -1); \
	echo "Detected image: $$IMAGE"; \
	sed -i.bak "s|image: skytrax-reviews_.*/airflow:latest|image: $$IMAGE|g" docker-compose.override.yml && rm -f docker-compose.override.yml.bak; \
	echo "docker-compose.override.yml patched."
	@if [ -n "$(PORT)" ]; then \
		astro config set webserver.port $(PORT); \
		astro config set api-server.port $(PORT); \
		echo "Port set to $(PORT)."; \
	fi

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

# ── Lint ──────────────────────────────────────────────────────────────────────

## Run all linters (isort, black, flake8)
lint:
	$(PYTHON) -m isort --check --diff dags/ include/ tests/
	$(PYTHON) -m black --check dags/ include/ tests/
	$(PYTHON) -m flake8 --max-line-length 100 --extend-ignore E203 dags/ include/ tests/

## Auto-fix lint issues (isort + black)
lint-fix:
	$(PYTHON) -m isort dags/ include/ tests/
	$(PYTHON) -m black dags/ include/ tests/

## Install the pre-commit git hook
install-hooks:
	cp scripts/pre-commit .git/hooks/pre-commit
	chmod +x .git/hooks/pre-commit
	@echo "Pre-commit hook installed."

# ── Tests ─────────────────────────────────────────────────────────────────────

## Run all unit tests
test:
	uv run pytest tests/ -v

## Run only scraper unit tests
test-scraper:
	uv run pytest tests/extract/test_scraper.py -v

.PHONY: dev-setup scrape-smoke scrape process-yesterday process lint lint-fix install-hooks test test-scraper
