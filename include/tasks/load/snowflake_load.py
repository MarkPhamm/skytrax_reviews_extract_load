"""
Load processed CSVs from S3 into Snowflake via COPY INTO.

For each review date:
  S3: processed/YYYY/MM/clean_data_YYYYMMDD.csv
  → Snowflake: SKYTRAX_REVIEWS_DB.RAW.AIRLINE_REVIEWS

SQL templates are read from include/sql/ — never inlined here.

STORAGE_MODE=local skips the Snowflake load entirely (no Snowflake needed for local dev).
"""

from __future__ import annotations

import logging
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)

_SQL_DIR = Path(__file__).resolve().parents[2] / "sql"


def _read_sql(filename: str) -> str:
    return (_SQL_DIR / filename).read_text()


def _get_hook(conn_id: str = "snowflake_default"):
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    return SnowflakeHook(snowflake_conn_id=conn_id)


# ---------------------------------------------------------------------------
# One-time setup — idempotent, safe to call on every run
# ---------------------------------------------------------------------------


def ensure_table(conn_id: str = "snowflake_default") -> None:
    """Create DB, schema, and table if they don't exist."""
    hook = _get_hook(conn_id)
    for statement in _read_sql("create_table.sql").split(";"):
        statement = statement.strip()
        if statement:
            hook.run(statement)
    logger.info("Snowflake table ready: SKYTRAX_REVIEWS_DB.RAW.AIRLINE_REVIEWS")


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
# Per-date load
# ---------------------------------------------------------------------------


def copy_into(review_date: date, conn_id: str = "snowflake_default") -> None:
    """COPY INTO Snowflake for one review date's processed CSV."""
    from include.tasks.load.s3_upload import processed_s3_key

    s3_key = processed_s3_key(review_date)
    sql = _read_sql("copy_into.sql").replace("{{ s3_key }}", s3_key)
    _get_hook(conn_id).run(sql)
    logger.info("Loaded %s → AIRLINE_REVIEWS", s3_key)
