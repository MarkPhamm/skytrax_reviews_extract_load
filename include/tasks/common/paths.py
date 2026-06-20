"""
Central path / S3-key builder for the Skytrax pipeline.

Single source of truth for the on-disk landing layout and the matching S3 keys.
Everything is partitioned **type-first**, then by review date:

    raw/<type>/YYYY/MM/raw_data_YYYYMMDD.csv
    processed/<type>/YYYY/MM/clean_data_YYYYMMDD.csv

where <type> is the plural partition name (airlines / seats / lounges / airports).
Local paths live under LANDING_DIR; S3 keys are the same relative strings, so the
S3 layout mirrors local exactly.

The landing root is resolved as (in priority order):
  1. LANDING_DIR env var
  2. {project_root}/landing   (project_root = 4 levels above this file)
"""

import os
from datetime import date
from pathlib import Path
from typing import Dict

# include/tasks/common/paths.py -> include/tasks/common -> include/tasks -> include -> project_root
_PROJECT_ROOT = Path(__file__).resolve().parents[3]

LANDING_DIR = Path(os.getenv("LANDING_DIR", _PROJECT_ROOT / "landing"))

# Singular scraper category key -> plural partition directory name.
PARTITION: Dict[str, str] = {
    "airline": "airlines",
    "seat": "seats",
    "lounge": "lounges",
    "airport": "airports",
}


def partition(category: str) -> str:
    """Return the plural partition dir name for a category key."""
    try:
        return PARTITION[category]
    except KeyError:
        raise ValueError(
            f"Unknown review category '{category}'. Expected one of {sorted(PARTITION)}."
        )


def raw_key(category: str, run_date: date) -> str:
    """S3 key / relative path for a raw CSV: raw/<type>/YYYY/MM/raw_data_YYYYMMDD.csv"""
    return (
        f"raw/{partition(category)}/{run_date.strftime('%Y')}/{run_date.strftime('%m')}"
        f"/raw_data_{run_date.strftime('%Y%m%d')}.csv"
    )


def processed_key(category: str, run_date: date) -> str:
    """S3 key for a processed CSV: processed/<type>/YYYY/MM/clean_data_YYYYMMDD.csv"""
    return (
        f"processed/{partition(category)}/{run_date.strftime('%Y')}/{run_date.strftime('%m')}"
        f"/clean_data_{run_date.strftime('%Y%m%d')}.csv"
    )


def raw_local_path(category: str, run_date: date) -> Path:
    """Absolute local path for a raw CSV under LANDING_DIR."""
    return LANDING_DIR / raw_key(category, run_date)


def processed_local_path(category: str, run_date: date) -> Path:
    """Absolute local path for a processed CSV under LANDING_DIR."""
    return LANDING_DIR / processed_key(category, run_date)
