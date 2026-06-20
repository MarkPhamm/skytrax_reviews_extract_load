"""
One-off migration to the type-first partition layout.

Two actions (both idempotent-ish, raw flat files are left in place):

1. Move existing AIRLINE partitions:
     landing/raw/<YYYY>/...        -> landing/raw/airlines/<YYYY>/...
     landing/processed/<YYYY>/...  -> landing/processed/airlines/<YYYY>/...

2. Split the flat combined CSVs (in landing/all/) into per-type date partitions:
     all_<type>_review_raw.csv        -> raw/<type>/YYYY/MM/raw_data_YYYYMMDD.csv
     all_<type>_review_processed.csv  -> processed/<type>/YYYY/MM/clean_data_YYYYMMDD.csv
   (split by the review date: `date` for raw, `date_submitted` for processed)

Usage:
  python scripts/migrate_to_typed_layout.py            # dry run — report only
  python scripts/migrate_to_typed_layout.py --apply    # perform the migration
"""

import argparse
import re
import shutil
import sys

import pandas as pd

from include.tasks.common import paths
from include.tasks.extract.scraper import CATEGORIES

YEAR_RE = re.compile(r"^\d{4}$")
FLAT_TYPES = ["seat", "lounge", "airport"]  # airline is already date-partitioned
# Flat combined CSVs live under landing/all/ (e.g. landing/all/all_seats_review_raw.csv).
FLAT_DIR = "all"


def move_airline_partitions(stage: str, apply: bool) -> list[str]:
    """Move landing/<stage>/<YYYY>/ dirs under landing/<stage>/airlines/."""
    base = paths.LANDING_DIR / stage
    if not base.exists():
        return []
    dest = base / "airlines"
    moved = []
    for child in sorted(base.iterdir()):
        if child.is_dir() and YEAR_RE.match(child.name):
            moved.append(child.name)
            if apply:
                dest.mkdir(parents=True, exist_ok=True)
                shutil.move(str(child), str(dest / child.name))
    return moved


def _parse_raw_dates(s: pd.Series) -> pd.Series:
    """Parse raw 'date' values like '26th August 2023' to datetime (NaT on failure)."""
    cleaned = s.astype(str).str.replace(r"(\d+)(st|nd|rd|th)", r"\1", regex=True)
    return pd.to_datetime(cleaned, format="%d %B %Y", errors="coerce")


def split_flat(category: str, apply: bool) -> dict:
    """Split a category's flat raw + processed CSVs into date-partitioned files."""
    raw_name = CATEGORIES[category].combined_filename
    processed_name = raw_name.replace("_raw", "_processed")
    report = {
        "category": category,
        "raw_rows": 0,
        "raw_files": 0,
        "processed_rows": 0,
        "processed_files": 0,
        "dropped": 0,
    }

    flat_dir = paths.LANDING_DIR / FLAT_DIR

    # ---- raw flat -> raw partitions (partition by `date`) ----
    raw_flat = flat_dir / raw_name
    if raw_flat.exists():
        df = pd.read_csv(raw_flat, low_memory=False)
        report["raw_rows"] = len(df)
        dates = _parse_raw_dates(df["date"])
        report["dropped"] += int(dates.isna().sum())
        df = df[dates.notna()].copy()
        df["_d"] = dates[dates.notna()].dt.date
        for d, group in df.groupby("_d"):
            out = paths.raw_local_path(category, d)
            report["raw_files"] += 1
            if apply:
                out.parent.mkdir(parents=True, exist_ok=True)
                group.drop(columns=["_d"]).to_csv(out, index=False)

    # ---- processed flat -> processed partitions (partition by `date_submitted`) ----
    proc_flat = flat_dir / processed_name
    if proc_flat.exists():
        df = pd.read_csv(proc_flat, low_memory=False)
        report["processed_rows"] = len(df)
        dates = pd.to_datetime(df["date_submitted"], errors="coerce")
        df = df[dates.notna()].copy()
        df["_d"] = dates[dates.notna()].dt.date
        for d, group in df.groupby("_d"):
            out = paths.processed_local_path(category, d)
            report["processed_files"] += 1
            if apply:
                out.parent.mkdir(parents=True, exist_ok=True)
                group.drop(columns=["_d"]).to_csv(out, index=False)

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description="Migrate landing/ to the type-first layout")
    parser.add_argument(
        "--apply", action="store_true", help="Perform the migration (default: dry run)"
    )
    args = parser.parse_args()
    mode = "APPLY" if args.apply else "DRY RUN"
    print(f"=== Migration ({mode}) — LANDING_DIR={paths.LANDING_DIR} ===")

    for stage in ("raw", "processed"):
        years = move_airline_partitions(stage, args.apply)
        verb = "moved" if args.apply else "would move"
        preview = ", ".join(years[:3]) + ("..." if len(years) > 3 else "")
        print(f"airline {stage}: {verb} {len(years)} year dirs -> {stage}/airlines/  [{preview}]")

    for category in FLAT_TYPES:
        r = split_flat(category, args.apply)
        print(
            f"{category}: raw {r['raw_rows']} rows -> {r['raw_files']} files | "
            f"processed {r['processed_rows']} rows -> {r['processed_files']} files | "
            f"dropped(no date) {r['dropped']}"
        )

    if not args.apply:
        print("\nDry run only. Re-run with --apply to perform the migration.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
