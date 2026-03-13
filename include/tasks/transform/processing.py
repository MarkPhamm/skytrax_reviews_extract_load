"""
Transforms raw scraped reviews into a clean, analysis-ready CSV.

Reads from:  landing/raw/YYYY/MM/raw_data_YYYYMMDD.csv
Writes to:   landing/processed/YYYY/MM/clean_data_YYYYMMDD.csv

Both paths are under LANDING_DIR (env var or {project_root}/landing).
This means local runs and production runs use identical code — the only
difference is whether landing/ is a local directory or a mounted volume.
"""

import logging
import os
import re
from datetime import date
from pathlib import Path
from typing import Dict, Optional, Tuple, Union

import pandas as pd

from include.tasks.transform import config as cfg

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[3]
LANDING_DIR = Path(os.getenv("LANDING_DIR", _PROJECT_ROOT / "landing"))


def get_input_path(run_date: date) -> Path:
    return (
        LANDING_DIR
        / "raw"
        / run_date.strftime("%Y")
        / run_date.strftime("%m")
        / f"raw_data_{run_date.strftime('%Y%m%d')}.csv"
    )


def get_output_path(run_date: date) -> Path:
    return (
        LANDING_DIR
        / "processed"
        / run_date.strftime("%Y")
        / run_date.strftime("%m")
        / f"clean_data_{run_date.strftime('%Y%m%d')}.csv"
    )


def clean(input_path: Path, output_path: Path) -> Path:
    """
    Run the full cleaning pipeline on a raw CSV and write cleaned output.

    Args:
        input_path:  Path to raw_data CSV produced by the scraper.
        output_path: Where to write the cleaned CSV.

    Returns:
        output_path after writing.
    """
    df = pd.read_csv(input_path)
    logger.info("Loaded %d rows from %s", len(df), input_path)

    df = _rename_columns(df)
    df = _clean_date_submitted(df)
    df = _clean_nationality(df)
    df = _clean_review_body(df)
    df = _clean_date_flown(df)
    df = _clean_recommended(df)
    df = _clean_ratings(df)
    df = _clean_route(df)
    df = _clean_aircraft(df)
    df = _reorder_columns(df)
    df = _add_updated_at(df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info("Saved %d rows → %s", len(df), output_path)
    return output_path


# ---------------------------------------------------------------------------
# Transformation steps (pure functions — each takes and returns a DataFrame)
# ---------------------------------------------------------------------------

def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.rename(columns=lambda x: x.strip().lower().replace(" ", "_"), inplace=True)
    df.columns = df.columns.str.replace("&", "and").str.replace("-", "_")
    df.rename(columns={"date": "date_submitted", "country": "nationality"}, inplace=True)
    return df


def _clean_date_submitted(df: pd.DataFrame) -> pd.DataFrame:
    df["date_submitted"] = df["date_submitted"].str.replace(
        r"(\d+)(st|nd|rd|th)", r"\1", regex=True
    )
    df["date_submitted"] = (
        pd.to_datetime(df["date_submitted"], format="%d %B %Y").dt.strftime("%Y-%m-%d")
    )
    return df


def _clean_nationality(df: pd.DataFrame) -> pd.DataFrame:
    df["nationality"] = (
        df["nationality"].astype(str)
        .str.replace(r"[()]", "", regex=True)
        .str.strip()
        .replace({"nan": pd.NA, "": pd.NA})
    )
    return df


def _clean_review_body(df: pd.DataFrame) -> pd.DataFrame:
    if "review_body" not in df.columns:
        return df

    split_df = df["review_body"].str.split("|", expand=True)

    if len(split_df.columns) == 1:
        df["review"] = split_df[0]
        df["verify"] = pd.NA
    else:
        df["verify"], df["review"] = split_df[0], split_df[1]

    # Swap when review is null but verify has content
    mask = df["review"].isnull() & df["verify"].notnull()
    df.loc[mask, ["review", "verify"]] = df.loc[mask, ["verify", "review"]].values

    df["verify"] = df["verify"].str.contains("Trip Verified", case=False, na=False)
    df["review"] = df["review"].str.strip()
    df.drop(columns=["review_body"], inplace=True)
    return df


def _clean_date_flown(df: pd.DataFrame) -> pd.DataFrame:
    df["date_flown"] = (
        pd.to_datetime(df["date_flown"], format="%B %Y", errors="coerce")
        .dt.strftime("%Y-%m-%d")
    )
    return df


def _clean_recommended(df: pd.DataFrame) -> pd.DataFrame:
    df["recommended"] = df["recommended"].str.contains("yes", case=False, na=False)
    return df


def _clean_ratings(df: pd.DataFrame) -> pd.DataFrame:
    rating_cols = [
        "seat_comfort",
        "cabin_staff_service",
        "food_and_beverages",
        "wifi_and_connectivity",
        "value_for_money",
    ]
    for col in rating_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df


def _parse_route(route: str) -> Dict[str, Union[str, None]]:
    """Parse a route string into city/airport components."""

    def extract(location: str) -> Tuple[Optional[str], Optional[str]]:
        if not location or pd.isna(location) or not location.strip():
            return None, None
        loc = location.strip().lower()

        iata = re.search(r"\b([a-z]{3})\b", loc)
        if iata:
            code = iata.group(1).upper()
            if code in cfg.AIRPORT_TO_CITY:
                return cfg.AIRPORT_TO_CITY[code], code
            if code in cfg.AIRPORT_CODES.values():
                rest = re.sub(r"\b" + iata.group(1) + r"\b", "", loc).strip()
                return (rest.title() if rest else "Unknown"), code

        for name, code in cfg.AIRPORT_CODES.items():
            if name in loc:
                city = cfg.AIRPORT_TO_CITY.get(code) or loc.replace(name, "").strip().title()
                return city or "Unknown", code

        for city_name, code in cfg.CITY_TO_AIRPORT.items():
            if city_name in loc and len(city_name) > 3:
                return city_name.title(), code

        return location.strip().title(), None

    empty = dict(
        origin=None, destination=None, transit=None,
        origin_city=None, origin_airport=None,
        destination_city=None, destination_airport=None,
        transit_city=None, transit_airport=None,
    )

    if not route or pd.isna(route) or not route.strip():
        return empty

    result = dict(empty)
    r = route.strip()

    if " via " in r.lower():
        main, transit_str = r.lower().split(" via ", 1)
        origin_str, _, dest_str = main.partition(" to ")
    elif " to " in r.lower():
        parts = r.lower().split(" to ", 1)
        origin_str, dest_str, transit_str = parts[0], parts[1], None
    else:
        origin_str, dest_str, transit_str = r.lower(), None, None

    result["origin"] = origin_str.strip().title() if origin_str else None
    result["destination"] = dest_str.strip().title() if dest_str else None
    result["transit"] = transit_str.strip().title() if transit_str else None

    result["origin_city"], result["origin_airport"] = extract(origin_str)
    if dest_str:
        result["destination_city"], result["destination_airport"] = extract(dest_str)
    if transit_str:
        result["transit_city"], result["transit_airport"] = extract(transit_str)

    return result


def _clean_route(df: pd.DataFrame) -> pd.DataFrame:
    if "route" not in df.columns:
        return df

    parsed = df["route"].apply(_parse_route)
    for key in ["origin_city", "origin_airport", "destination_city",
                "destination_airport", "transit_city", "transit_airport"]:
        df[key] = parsed.apply(lambda x: x[key])

    for col in ["origin_city", "destination_city", "transit_city"]:
        df[col] = df[col].replace(cfg.city_replacements)

    df.drop(columns=["route", "origin", "destination", "transit"], errors="ignore", inplace=True)
    return df


def _clean_aircraft(df: pd.DataFrame) -> pd.DataFrame:
    def clean_entry(entry):
        if pd.isna(entry):
            return None
        entry = str(entry).replace("\xa0", " ")
        entry = re.sub(r"\bE-?(\d{3})\b", r"Embraer \1", entry, flags=re.IGNORECASE)
        entry = re.sub(r"\bEmbraer[- ]?(\d{3})\b", r"Embraer \1", entry, flags=re.IGNORECASE)
        entry = re.sub(r"\bEmbraerE(\d{3})\b", r"Embraer \1", entry, flags=re.IGNORECASE)
        entry = re.sub(r"\bEmbraer(\d{3})\b", r"Embraer \1", entry, flags=re.IGNORECASE)

        if re.match(r".*(Embraer\s(170|190|195)).*", entry, re.IGNORECASE):
            m = re.match(r".*(Embraer\s(170|190|195)).*", entry, re.IGNORECASE)
            return f"Embraer {m.group(2)}"
        if re.match(r".*(Boeing\s7\d{2}).*", entry, re.IGNORECASE):
            return re.match(r".*(Boeing\s7\d{2}).*", entry, re.IGNORECASE).group(1)
        short = re.match(r".*\b(B744|B747|B757|B767|B777|B787|B789|B737)\b.*", entry, re.IGNORECASE)
        if short:
            return {"B737": "Boeing 737", "B744": "Boeing 744", "B747": "Boeing 747",
                    "B757": "Boeing 757", "B767": "Boeing 767", "B777": "Boeing 777",
                    "B787": "Boeing 787", "B789": "Boeing 789"}.get(short.group(1).upper())
        airbus = re.match(
            r".*\b(A318|A319|A320|A320NEO|A321|A321NEO|A322|A329|A330|A340|A350|A366|A380)\b.*",
            entry, re.IGNORECASE,
        )
        if airbus:
            return airbus.group(1).upper().replace("NEO", "")
        if re.match(r".*(Saab\s2000).*", entry, re.IGNORECASE):
            return "Saab 2000"
        return None

    df["aircraft"] = df["aircraft"].apply(clean_entry)
    return df


def _reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    existing = [c for c in cfg.COLUMN_ORDER if c in df.columns]
    return df[existing]


def _add_updated_at(df: pd.DataFrame) -> pd.DataFrame:
    from datetime import datetime
    df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from datetime import timedelta

    parser = argparse.ArgumentParser(description="Clean raw Skytrax reviews")
    parser.add_argument("--date", type=date.fromisoformat, default=None, help="Run date YYYY-MM-DD (default: today)")
    parser.add_argument("--yesterday", action="store_true", help="Process yesterday's file")
    args = parser.parse_args()

    run_date = args.date or (date.today() - timedelta(days=1) if args.yesterday else date.today())

    input_path = get_input_path(run_date)
    output_path = get_output_path(run_date)

    if not input_path.exists():
        raise FileNotFoundError(f"Raw file not found: {input_path}")

    clean(input_path, output_path)
    print(f"Done: {output_path}")
