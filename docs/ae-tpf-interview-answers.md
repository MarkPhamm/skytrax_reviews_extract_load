# AE-TPF Interview — This Repo's Story (Extract-Load Scope)

Prep notes for the AE-TPF technical interview (see [`prompts/AE-TPF-interview-prep.md`](../prompts/AE-TPF-interview-prep.md)), scoped strictly to what this repo owns.

**Scope**: this repo is Extract-Load only — it scrapes AirlineQuality.com, stages to S3, and loads raw data into Snowflake. **Transformation (dbt) and BI live in separate repos.** Of the interview's 5 core areas, this repo is strong evidence for **Data Governance** and **DataOps**, partial evidence for **Data Modeling & Architecture** (raw-layer design only), and out of scope for **Data Transformation** and **Data Insight** — say so plainly rather than stretching this repo to cover them.

## 0. Stack

| Layer | Technology | Where |
| ----- | ---------- | ----- |
| Orchestrator | Apache Airflow (Astronomer Runtime, Docker) | `dags/` |
| Extraction | Python — `requests` + BeautifulSoup, custom scraper | `include/tasks/extract/` |
| Staging | AWS S3 (raw + processed zones) | `include/tasks/load/s3_upload.py` |
| Warehouse | Snowflake (RAW schema, external stage, `COPY INTO`) | `include/tasks/load/snowflake_load.py` |
| IaC | Terraform (two root modules: AWS, Snowflake) | `terraform/` |
| CI | GitHub Actions (lint + pytest) | `.github/workflows/cicd-pipeline.yml` |

Architecture (mirrors `README.md`):

```text
airlinequality.com → scrape (26 A-Z tasks × 4 types)
                   → S3 raw/<type>/YYYY/MM/raw_data_YYYYMMDD.csv
                   → clean + transform
                   → S3 processed/<type>/YYYY/MM/clean_data_YYYYMMDD.csv
                   → COPY INTO
                   → Snowflake SKYTRAX_REVIEWS_DB.RAW.<TYPE>_REVIEWS
```

---

## 1. Why Python for ingestion

**Show**: `include/tasks/extract/scraper.py` — `ReviewCategory` dataclass parametrizes all 4 review types (airline/seat/lounge/airport) over one shared scraper; `ReviewScraper.scrape_all_partitioned()` does entity discovery, per-entity pagination, and `ThreadPoolExecutor`-based entity-level parallelism; `dag_crawl.py` dynamically maps this over `REVIEW_TYPES` via `.expand()`.

**Why**: AirlineQuality.com has no API — this is pure HTML scraping, which rules out a managed ELT connector (Fivetran/Airbyte/Stitch don't scrape arbitrary sites). Writing it in Python gives full control over retry/backoff behavior, edge-case handling (country extraction, entity-name resolution when a page's "read more" link isn't the real name — see commit `e041a106`), and output shaped exactly to the pipeline's own partition scheme. It's also free — no per-row connector pricing for a personal-scale project.

**What-if**: "Add a 5th review type live" — add one `ReviewCategory` entry to the `CATEGORIES` dict (index URL, href substring, name field); partitioning, S3 keys, and the dynamic task mapping in all 3 DAGs already key off `category`, so nothing else changes.

---

## 2. Why S3 as a staging layer (not straight into Snowflake)

**Show**: `include/tasks/common/paths.py` is the single source of truth for the partition scheme — `raw/<type>/YYYY/MM/raw_data_YYYYMMDD.csv` and `processed/<type>/YYYY/MM/clean_data_YYYYMMDD.csv`, mirrored identically on local disk and in S3. `terraform/aws/main.tf` provisions the bucket with versioning, SSE-S3 encryption, public-access block, and lifecycle rules (raw/processed → Standard-IA after 30 days, noncurrent versions expire after 90).

**Why**: scraping is the expensive, rate-limited step — it hits a live website. Staging to S3 decouples "get the data" from "load the data," so a load-side bug (like the `LOAD_AUDIT` privilege gap found in this very session) or a schema fix can be replayed from S3 without re-scraping. S3 is also the natural checkpoint for pre-load quality gates to run *before* anything reaches the warehouse. Object storage is far cheaper than warehouse compute for holding raw history.

| Reason | Why it matters |
| ------ | --------------- |
| Reliability | If Snowflake is unavailable, data is safely stored in S3 instead of being lost. |
| Replayability | If a load fails or a transformation has a bug, you can reload the original files without asking the source system again. |
| Scalability | S3 can ingest huge amounts of data cheaply before Snowflake processes it. |
| Cost | S3 storage is much cheaper than storing raw historical files in Snowflake. |
| Decoupling | Producers only write to S3. Consumers (Snowflake, Spark, Athena, EMR, Databricks) read independently. |
| Auditability | You always have the original raw files for compliance or debugging. |
| Multi-consumer | The same raw data can feed Snowflake, ML pipelines, Spark jobs, etc. |

**What-if**: "What if S3 is unavailable, or you're doing local dev?" — `STORAGE_MODE=local` env var short-circuits `upload_raw`/`upload_processed` into no-ops; the rest of the pipeline runs unchanged against local disk.

---

## 3. Why Snowflake via external stage + `COPY INTO` (not row-by-row insert)

**Show**: `terraform/snowflake/snowflake.tf` owns the external stage (`SKYTRAX_S3_STAGE`, AWS_ROLE credentials) and all 5 tables (4 review types + `LOAD_AUDIT`) as code. `include/sql/copy_into.sql` + `snowflake_load.py::copy_into()` run the load and reconcile `rows_parsed` vs `rows_loaded`/`errors_seen` from the `COPY INTO` result, writing every outcome to `RAW.LOAD_AUDIT`.

**Why**: `COPY INTO` is Snowflake's native bulk-load path — it dedupes by file automatically (a re-run of an already-loaded file is a safe no-op, which is exactly what makes backfills idempotent), it's far faster than row-by-row `INSERT`, and `ON_ERROR = 'CONTINUE'` plus the post-load reconciliation in `copy_into()` turns a normally-silent bulk load into an auditable one — if any row is rejected or the row counts don't match, the task fails loudly instead of silently under-loading.

**What-if**: "What if you needed stricter correctness over throughput?" — swap `ON_ERROR = 'CONTINUE'` to `ABORT_STATEMENT` so a single bad row fails the whole file instead of partially loading; trade-off is losing partial-load visibility for guaranteed all-or-nothing loads.

---

## 4. Why Airflow orchestration (Dataset-chained DAGs + dynamic task mapping)

**Show**: three DAGs — `skytrax_crawl` → `skytrax_process` → `skytrax_snowflake` — chained via Airflow **Datasets** (`RAW_DATASET`, `PROCESSED_DATASET`) rather than fixed schedule offsets: `dag_process` is `schedule=[RAW_DATASET]`, `dag_snowflake` is `schedule=[PROCESSED_DATASET]`. Every DAG uses dynamic task mapping (`scrape_type.expand(review_type=REVIEW_TYPES)`, `load_one.expand_kwargs(...)`) instead of one task per category.

**Why**: dataset-driven scheduling means `dag_process` fires the moment `dag_crawl` actually finishes uploading — no guessing "process should run ~1 hour after crawl." Dynamic task mapping means adding a 5th review type or scaling to more (type, date) pairs doesn't require duplicating DAG code — the same task fans out over however many items it's given. TaskFlow API (`@task`) keeps the actual logic in plain, unit-testable Python functions under `include/tasks/`, which is why 62 tests can run in under a second with no live Airflow/Snowflake/S3 needed.

**What-if**: "Add a Slack notification after load" — add an `outlets=[...]`-driven or downstream `@task` after `upload_raw`/`load_one`; the dataset-chaining pattern already supports adding more consumers of the same event without touching upstream DAGs.

---

## 5. Data Governance (Quality & Security) — this repo's strongest area

**Show**:

- **Pre-load quality gate**: `include/tasks/common/quality.py::validate_processed_csv()` — checks schema drift (exact column match), non-empty files, null-rate thresholds on required columns, and star-rating range `[1, 5]`. Runs inside `dag_process.py` before a file is ever uploaded, so a bad file never reaches S3 or Snowflake.
- **Post-load reconciliation**: `copy_into()` compares `rows_parsed`/`rows_loaded`/`errors_seen` from Snowflake's own `COPY INTO` result and writes the outcome to a Terraform-managed `RAW.LOAD_AUDIT` table.
- **PII masking**: `terraform/snowflake/masking.tf` — a `PII` tag + `MASK_PII_STRING` masking policy, with a `PII_READER` role that's the only one exempt from masking (besides `ACCOUNTADMIN`). Applies to `CUSTOMER_NAME`/`NATIONALITY` across all 4 raw tables.
- **Secrets hygiene**: `.env`, `terraform.tfvars`, and `.tfstate` are all gitignored and were verified untracked; no credentials ever hit git history.

**Why**: a scraper is an unreliable, uncontrolled data source — the site's HTML can change shape at any time — so the quality gate exists to fail loudly on drift rather than silently loading garbage. The post-load reconciliation exists because `COPY INTO ... ON_ERROR = 'CONTINUE'` can *partially* succeed without raising by default; without reconciliation, a partial load would look identical to a full one. Masking is tag-based (not hardcoded per-column grants) so it's declarative, auditable in Terraform, and doesn't need updating every time a new table adds a PII column with the same tag.

**What-if**: "A new field turns out to contain PII" — add the `PII` tag to that column via Terraform; the existing masking policy applies immediately without redefining the policy itself. "What if the quality gate should catch duplicate rows" — extend `validate_processed_csv()` with a dedup-key check before the null-rate checks.

---

## 6. DataOps

**Show**:

- **Branching**: feature branches per change (`feat/data-quality-and-pii-masking`, `feat/scrape-seat-lounge-airport-reviews`, etc.), merged to `main` via reviewed GitHub PRs (#4, #5).
- **CI**: `.github/workflows/cicd-pipeline.yml` — on every push/PR to `main`: `uv sync`, `flake8` (fatal-error subset: `E9,F63,F7,F82`), then `pytest tests/ -v`.
- **Local shift-left**: `scripts/pre-commit` runs `isort` + `black` + `flake8` on staged `.py` files before a commit is even made (`make install-hooks`).
- **IaC**: all infrastructure (S3, IAM, Snowflake DB/schema/tables/stage/masking) defined in Terraform, applied deliberately (not via CI) with plan review first.
- **Live incident this session**: migrating to a new Snowflake account surfaced two real bugs — a missing IAM trust-policy update (Snowflake couldn't assume the S3 role) and a privilege gap on a runtime `CREATE TABLE IF NOT EXISTS` that only worked on the old account by an undocumented manual grant. Fixed by moving `LOAD_AUDIT` into Terraform (least-privilege: the runtime role only needs `INSERT` now) and re-planning/applying both Terraform stacks. Good concrete "What-if" material — diagnosing an auth/IAM chain and Terraform state drift under real constraints.

**Why**: CI is intentionally lint+test only — deployment is manual (`astro deploy`, `terraform apply`) because this is a small, single-operator project where infra changes benefit from a human reviewing the plan before it touches real cloud spend, rather than an automated apply on every merge.

---

## 7. Simple CI/CD for Airflow

What's actually running today, and what a next increment would look like:

**Today (CI only)**:

1. `uv sync --extra dev` — reproducible dependency install
2. `flake8` restricted to fatal error codes (`E9,F63,F7,F82`) — catches syntax errors and undefined names, not style nits
3. `pytest tests/ -v` — 62 unit tests, all task logic mocked (no live Airflow/S3/Snowflake needed), so CI stays fast and free of cloud credentials in the common case
4. (Local, not yet in CI) `scripts/pre-commit` — same lint stack, run before every commit

**Not yet automated — deliberate, and worth being able to explain why**:

- **DAG integrity/parse check**: `.astro/test_dag_integrity_default.py` already exists (used by `astro dev parse` locally) and would be a natural addition to CI — it'd catch DAG import errors, like the `scrape_type.log` `AttributeError` found in this session, *before* merge instead of at 4am in production.
- **CD**: `astro deploy` (Airflow) and `terraform apply` (infra) are both manual today. A "simple CD" next step would be: on merge to `main`, run `astro deploy` automatically to a dev Astronomer deployment; keep `terraform apply` manual behind a plan-review step (`terraform plan` could run in CI and post the diff as a PR comment, but `apply` stays a deliberate human action — infra changes are harder to reverse than a DAG deploy).

This is the honest answer if asked "why no CD": for a project at this scale, the highest-leverage automation is fast feedback on code correctness (lint + tests + DAG parse) before merge; automating the deploy step itself has a much smaller payoff than automating the safety net that runs before it.
