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


def copy_into(category: str, review_date: date, conn_id: str = "snowflake_default") -> None:
    """COPY INTO the category's table for one review date's processed CSV."""
    s3_key = paths.processed_key(category, review_date)
    table = table_name(category)
    sql = _read_sql("copy_into.sql").replace("{{ table }}", table).replace("{{ s3_key }}", s3_key)
    _get_hook(conn_id).run(sql)
    logger.info("Loaded %s → %s", s3_key, table)
