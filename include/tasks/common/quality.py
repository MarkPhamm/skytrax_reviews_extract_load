"""
Data quality gates for the EL pipeline.

Pre-load  — ``validate_processed_csv``: run against a processed CSV *before*
            it is uploaded to S3 / loaded into Snowflake. Fails the Airflow
            task on schema drift, empty files, null-rate breaches, or
            out-of-range star ratings.
Post-load — consumed by ``include.tasks.load.snowflake_load.copy_into``:
            reconciles Snowflake COPY INTO results (rows_parsed vs rows_loaded,
            errors_seen) and records every load in RAW.LOAD_AUDIT.

Expected schemas are derived from the cleaning configs
(``transform.config.COLUMN_ORDER`` and ``transform.processing.CLEANING_PROFILES``)
so the checks can never drift from the pipeline that produces the files.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Dict, List, Union

import pandas as pd

logger = logging.getLogger(__name__)

# Star ratings are 1-5 after cleaning (0 is coerced to NA).
RATING_MIN = 1
RATING_MAX = 5

# Columns that must never be null in a processed file (all categories).
REQUIRED_NOT_NULL = ["date_submitted", "updated_at"]

# Tolerated null rate for soft-required columns, per category.
MAX_NULL_RATE = {"review": 0.05}


class DataQualityError(ValueError):
    """A processed file failed a pre-load quality gate."""


def expected_columns(category: str) -> List[str]:
    """Processed-file column schema for a category (from the cleaning configs)."""
    from include.tasks.transform import config as cfg
    from include.tasks.transform.processing import CLEANING_PROFILES

    if category == "airline":
        # The airline pipeline reorders to COLUMN_ORDER then appends updated_at.
        return [*cfg.COLUMN_ORDER, "updated_at"]
    try:
        return list(CLEANING_PROFILES[category].column_order)
    except KeyError:
        raise ValueError(
            f"Unknown review category '{category}'. "
            f"Expected 'airline' or one of {sorted(CLEANING_PROFILES)}."
        )


def rating_columns(category: str) -> List[str]:
    """Star-rated columns for a category (from the cleaning configs)."""
    from include.tasks.transform.processing import _AIRLINE_RATING_COLS, CLEANING_PROFILES

    if category == "airline":
        return list(_AIRLINE_RATING_COLS)
    return list(CLEANING_PROFILES[category].rating_cols)


def validate_processed_csv(category: str, csv_path: Union[str, Path]) -> Dict:
    """Pre-load gate: validate one processed CSV before upload/load.

    Checks, in order:
      1. file exists and has at least one data row
      2. columns exactly match the expected schema for the category
      3. required columns have no nulls; soft-required stay under MAX_NULL_RATE
      4. star ratings are within [RATING_MIN, RATING_MAX] (nulls allowed)

    Returns a summary dict {category, path, rows, columns} on success.
    Raises DataQualityError on the first violation so Airflow fails loudly.
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        raise DataQualityError(f"[{category}] processed file not found: {csv_path}")

    df = pd.read_csv(csv_path, low_memory=False)

    # 1. Non-empty
    if df.empty:
        raise DataQualityError(f"[{category}] processed file has no rows: {csv_path}")

    # 2. Schema — exact match, order-insensitive, so drift in either direction fails
    expected = expected_columns(category)
    missing = [c for c in expected if c not in df.columns]
    unexpected = [c for c in df.columns if c not in expected]
    if missing or unexpected:
        raise DataQualityError(
            f"[{category}] schema mismatch in {csv_path.name}: "
            f"missing={missing} unexpected={unexpected}"
        )

    # 3. Null-rate gates
    for col in REQUIRED_NOT_NULL:
        nulls = int(df[col].isna().sum())
        if nulls:
            raise DataQualityError(
                f"[{category}] column '{col}' has {nulls} null(s) in {csv_path.name}"
            )
    for col, max_rate in MAX_NULL_RATE.items():
        if col not in df.columns:
            continue
        rate = float(df[col].isna().mean())
        if rate > max_rate:
            raise DataQualityError(
                f"[{category}] column '{col}' null rate {rate:.1%} exceeds "
                f"{max_rate:.1%} in {csv_path.name}"
            )

    # 4. Rating ranges (nulls allowed — reviewers can skip a rating)
    for col in rating_columns(category):
        if col not in df.columns:
            continue
        values = pd.to_numeric(df[col], errors="coerce").dropna()
        bad = values[(values < RATING_MIN) | (values > RATING_MAX)]
        if not bad.empty:
            raise DataQualityError(
                f"[{category}] column '{col}' has {len(bad)} value(s) outside "
                f"[{RATING_MIN}, {RATING_MAX}] in {csv_path.name}"
            )

    summary = {
        "category": category,
        "path": str(csv_path),
        "rows": int(len(df)),
        "columns": int(len(df.columns)),
    }
    logger.info("Pre-load quality checks passed: %s", summary)
    return summary
