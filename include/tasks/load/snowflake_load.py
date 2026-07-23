"""
Load processed CSVs from S3 into Snowflake via COPY INTO.

For each (review type, review date):
  S3: processed/<type>/YYYY/MM/clean_data_YYYYMMDD.csv
  → Snowflake: SKYTRAX_REVIEWS_DB.RAW.<TYPE>_REVIEWS

SQL templates are read from include/sql/ — never inlined here.

STORAGE_MODE=local skips the Snowflake load entirely (no Snowflake needed for local dev).
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

from include.tasks.common import paths

logger = logging.getLogger(__name__)

_SQL_DIR = Path(__file__).resolve().parents[2] / "sql"

# Category key -> Snowflake table name.
TABLE = {
    "airline": "AIRLINE_REVIEWS",
    "seat": "SEAT_REVIEWS",
    "lounge": "LOUNGE_REVIEWS",
    "airport": "AIRPORT_REVIEWS",
}


def table_name(category: str) -> str:
    """Fully-qualified Snowflake table for a category key."""
    try:
        return f"SKYTRAX_REVIEWS_DB.RAW.{TABLE[category]}"
    except KeyError:
        raise ValueError(f"Unknown review category '{category}'. Expected one of {sorted(TABLE)}.")


def _read_sql(filename: str) -> str:
    return (_SQL_DIR / filename).read_text()


def _get_hook(conn_id: str = "snowflake_default"):
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    return SnowflakeHook(snowflake_conn_id=conn_id)


# ---------------------------------------------------------------------------
# One-time setup — idempotent, safe to call on every run
# ---------------------------------------------------------------------------


def ensure_table(category: str, conn_id: str = "snowflake_default") -> None:
    """Create DB, schema, and the category's table if they don't exist."""
    hook = _get_hook(conn_id)
    ddl = _read_sql(f"create_table_{paths.partition(category)}.sql")
    for statement in ddl.split(";"):
        statement = statement.strip()
        if statement:
            hook.run(statement)
    logger.info("Snowflake table ready: %s", table_name(category))


def ensure_stage(bucket: str, role_arn: str, conn_id: str = "snowflake_default") -> None:
    """Create or replace the S3 external stage."""
    sql = (
        _read_sql("create_stage.sql")
        .replace("{{ bucket }}", bucket)
        .replace("{{ role_arn }}", role_arn)
    )
    _get_hook(conn_id).run(sql)
    logger.info("Snowflake stage ready: SKYTRAX_REVIEWS_DB.RAW.SKYTRAX_S3_STAGE")


# ---------------------------------------------------------------------------
# Per-(type, date) load
# ---------------------------------------------------------------------------


def copy_into(category: str, review_date: date, conn_id: str = "snowflake_default") -> dict:
    """COPY INTO the category's table for one review date's processed CSV.

    Post-load quality gate: reconciles the COPY INTO result metadata
    (rows_parsed vs rows_loaded, errors_seen), records the outcome in
    RAW.LOAD_AUDIT, and raises if any row was rejected or nothing loaded.
    """
    s3_key = paths.processed_key(category, review_date)
    table = table_name(category)
    sql = _read_sql("copy_into.sql").replace("{{ table }}", table).replace("{{ s3_key }}", s3_key)

    hook = _get_hook(conn_id)
    results = hook.get_records(sql)
    summary = _summarize_copy_result(results)
    logger.info("COPY INTO %s ← %s: %s", table, s3_key, summary)

    _record_load_audit(hook, category, review_date, s3_key, table, summary)

    if summary["status"] == "SKIPPED":
        # File already loaded in a previous run (COPY dedupes by file) — a
        # no-op re-run, not a failure.
        logger.info("File already loaded, nothing to do: %s", s3_key)
        return summary
    if summary["errors_seen"]:
        raise RuntimeError(
            f"Post-load check failed for {s3_key} → {table}: "
            f"{summary['errors_seen']} rejected row(s), "
            f"first error: {summary['first_error']}"
        )
    if summary["rows_loaded"] != summary["rows_parsed"] or summary["rows_loaded"] == 0:
        raise RuntimeError(
            f"Post-load reconciliation failed for {s3_key} → {table}: "
            f"parsed {summary['rows_parsed']} row(s) but loaded {summary['rows_loaded']}."
        )
    return summary


def _summarize_copy_result(results: list) -> dict:
    """Normalize COPY INTO result rows into an audit-friendly summary.

    A load returns one row per file:
      (file, status, rows_parsed, rows_loaded, error_limit, errors_seen,
       first_error, ...).
    A re-run of an already-loaded file returns a single informational row
    ('Copy executed with 0 files processed.').
    """
    row = results[0] if results else ()
    if len(row) < 6:
        return {
            "status": "SKIPPED",
            "rows_parsed": 0,
            "rows_loaded": 0,
            "errors_seen": 0,
            "first_error": None,
        }
    return {
        "status": str(row[1]),
        "rows_parsed": int(row[2] or 0),
        "rows_loaded": int(row[3] or 0),
        "errors_seen": int(row[5] or 0),
        "first_error": row[6] if len(row) > 6 else None,
    }


def _record_load_audit(
    hook, category: str, review_date: date, s3_key: str, table: str, summary: dict
) -> None:
    """Persist one load outcome to RAW.LOAD_AUDIT (table is Terraform-managed)."""
    hook.run(
        _read_sql("insert_load_audit.sql"),
        parameters={
            "category": category,
            "review_date": review_date.isoformat(),
            "s3_key": s3_key,
            "target_table": table,
            "status": summary["status"],
            "rows_parsed": summary["rows_parsed"],
            "rows_loaded": summary["rows_loaded"],
            "errors_seen": summary["errors_seen"],
            "first_error": summary["first_error"],
        },
    )
