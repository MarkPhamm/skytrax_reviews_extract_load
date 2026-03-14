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
    df["date_submitted"] = pd.to_datetime(df["date_submitted"], format="%d %B %Y").dt.strftime(
        "%Y-%m-%d"
    )
    return df


def _clean_nationality(df: pd.DataFrame) -> pd.DataFrame:
    df["nationality"] = (
        df["nationality"]
        .astype(str)
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
    df["date_flown"] = pd.to_datetime(
        df["date_flown"], format="%B %Y", errors="coerce"
    ).dt.strftime("%Y-%m-%d")
    return df


def _clean_recommended(df: pd.DataFrame) -> pd.DataFrame:
    df["recommended"] = df["recommended"].str.contains("yes", case=False, na=False)
    return df


def _clean_ratings(df: pd.DataFrame) -> pd.DataFrame:
    rating_cols = [
        "seat_comfort",
        "cabin_staff_service",
        "food_and_beverages",
        "inflight_entertainment",
        "ground_service",
        "wifi_and_connectivity",
        "value_for_money",
    ]
    for col in rating_cols:
        if col in df.columns:
            s = pd.to_numeric(df[col], errors="coerce")
            s = s.where(s >= 1, other=pd.NA)  # treat 0 as null
            df[col] = s.astype("Int64")
    return df


def _parse_route(route: str) -> Dict[str, Union[str, None]]:
    """Parse a route string into city/airport components."""

    def _strip_qualifier(loc: str) -> str:
        """Remove trailing state/country qualifiers added by reviewers.

        'Miami, FL'              → 'Miami'
        'San Juan, Puerto Rico'  → 'San Juan'
        'Denver, Colorado'       → 'Denver'
        'Raleigh Durham'         → 'Raleigh Durham'  (no comma → unchanged)
        """
        return loc.split(",")[0].strip()

    def extract(location: str) -> Tuple[Optional[str], Optional[str]]:
        if not location or pd.isna(location) or not location.strip():
            return None, None

        loc_clean = _strip_qualifier(location.strip())
        loc = loc_clean.lower()

        # 1. Explicit IATA code written in uppercase by the reviewer
        #    ("New York JFK", "London LHR") — must use original case
        m = re.search(r"\b([A-Z]{3})\b", location.strip())
        if m:
            code = m.group(1)
            if code in cfg.AIRPORT_TO_CITY:
                return cfg.AIRPORT_TO_CITY[code], code

        # 1b. Location is exactly a 3-letter code in lowercase ("lhr", "jfk")
        if re.fullmatch(r"[a-z]{3}", loc):
            code_upper = loc.upper()
            if code_upper in cfg.AIRPORT_TO_CITY:
                return cfg.AIRPORT_TO_CITY[code_upper], code_upper

        # 2. Known multi-word airport/city names — word-boundary match, longest
        #    first to prefer specific entries over short ambiguous ones.
        #    3-char keys (bare IATA codes like "san", "del") are excluded here
        #    to avoid matching them as prefixes of unrelated city names
        #    ("san" in "San Juan" → San Diego would be wrong).
        for name, code in sorted(cfg.AIRPORT_CODES.items(), key=lambda x: -len(x[0])):
            if len(name) <= 3:
                break  # sorted descending, so all remaining are ≤3 chars
            if re.search(r"\b" + re.escape(name) + r"\b", loc):
                city = cfg.AIRPORT_TO_CITY.get(code) or loc_clean.title()
                return city or "Unknown", code

        # 3. City-to-airport fallback — also word-boundary guarded
        for city_name, code in cfg.CITY_TO_AIRPORT.items():
            if len(city_name) > 3 and re.search(r"\b" + re.escape(city_name) + r"\b", loc):
                return city_name.title(), code

        return loc_clean.title(), None

    empty = dict(
        origin=None,
        destination=None,
        transit=None,
        origin_city=None,
        origin_airport=None,
        destination_city=None,
        destination_airport=None,
        transit_city=None,
        transit_airport=None,
    )

    if not route or pd.isna(route) or not route.strip():
        return empty

    result = dict(empty)
    # Normalise whitespace but preserve original case so extract() can detect
    # uppercase IATA codes written inline by reviewers ("New York JFK").
    r = re.sub(r"\s+", " ", route.strip())
    r_lower = r.lower()

    if " via " in r_lower:
        via_pos = r_lower.index(" via ")
        main = r[:via_pos]
        # Multiple transit stops ("via Frankfurt, Munich, Porto") — take only
        # the first one; the rest are not stored in the single transit slot.
        transit_raw = r[via_pos + 5 :].split(",")[0].strip()
        to_pos = main.lower().find(" to ")
        origin_str = main[:to_pos].strip() if to_pos != -1 else main.strip()
        dest_str = main[to_pos + 4 :].strip() if to_pos != -1 else None
        transit_str = transit_raw or None
    elif " to " in r_lower:
        to_pos = r_lower.index(" to ")
        origin_str = r[:to_pos].strip()
        dest_str = r[to_pos + 4 :].strip()
        transit_str = None
    else:
        origin_str, dest_str, transit_str = r, None, None

    result["origin"] = origin_str.title() if origin_str else None
    result["destination"] = dest_str.title() if dest_str else None
    result["transit"] = transit_str.title() if transit_str else None

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
    for key in [
        "origin_city",
        "origin_airport",
        "destination_city",
        "destination_airport",
        "transit_city",
        "transit_airport",
    ]:
        df[key] = parsed.apply(lambda x: x[key])

    for col in ["origin_city", "destination_city", "transit_city"]:
        df[col] = df[col].replace(cfg.city_replacements)

    df.drop(columns=["route", "origin", "destination", "transit"], errors="ignore", inplace=True)
    return df


def _clean_aircraft(df: pd.DataFrame) -> pd.DataFrame:
    # ICAO/IATA shortcodes → canonical "Manufacturer Model" form
    _SHORTCODES: dict[str, str] = {
        "A318": "Airbus A318",
        "A319": "Airbus A319",
        "A320": "Airbus A320",
        "A20N": "Airbus A320neo",
        "A321": "Airbus A321",
        "A21N": "Airbus A321neo",
        "A330": "Airbus A330",
        "A332": "Airbus A330-200",
        "A333": "Airbus A330-300",
        "A339": "Airbus A330neo",
        "A340": "Airbus A340",
        "A350": "Airbus A350",
        "A359": "Airbus A350-900",
        "A35K": "Airbus A350-1000",
        "A380": "Airbus A380",
        "B733": "Boeing 737-300",
        "B734": "Boeing 737-400",
        "B735": "Boeing 737-500",
        "B737": "Boeing 737",
        "B738": "Boeing 737-800",
        "B739": "Boeing 737-900",
        "B744": "Boeing 747-400",
        "B747": "Boeing 747",
        "B752": "Boeing 757-200",
        "B757": "Boeing 757",
        "B762": "Boeing 767-200",
        "B763": "Boeing 767-300",
        "B767": "Boeing 767",
        "B772": "Boeing 777-200",
        "B773": "Boeing 777-300",
        "B77W": "Boeing 777-300ER",
        "B777": "Boeing 777",
        "B787": "Boeing 787",
        "B788": "Boeing 787-8",
        "B789": "Boeing 787-9",
        "B78X": "Boeing 787-10",
        "E145": "Embraer 145",
        "E170": "Embraer 170",
        "E175": "Embraer 175",
        "E190": "Embraer 190",
        "E195": "Embraer 195",
    }

    _VALID_EMBRAER = {135, 140, 145, 170, 175, 190, 195}

    def clean_entry(entry):
        if pd.isna(entry):
            return None
        entry = str(entry).replace("\xa0", " ").strip()
        if not entry or entry.upper() in ("UNKNOWN", "N/A", "-"):
            return None

        # Multi-aircraft entries ("Boeing 787 / 777", "A330 / Boeing 787", etc.):
        # take only the first aircraft mentioned so the result is unambiguous.
        for sep in ("/", "&"):
            if sep in entry:
                entry = entry.split(sep)[0].strip()
        if "," in entry:
            entry = entry.split(",")[0].strip()

        # Shortcode lookup: normalise to uppercase, strip hyphens/spaces
        key = entry.upper().replace("-", "").replace(" ", "")
        if key in _SHORTCODES:
            return _SHORTCODES[key]

        # Boeing long-form: preserve subvariant when present
        # Matches "Boeing 737", "Boeing 737-800", "Boeing 777-300ER", "Boeing 777-300 ER",
        # "Boeing 787-9", "Boeing 737 Max 8"
        m = re.search(
            r"\bBoeing\s+(7\d{2})(?:[- ](MAX\s*\d*|\d{1,3}(?:\s*[A-Z]{1,3})*))?(?=\b|$)",
            entry,
            re.IGNORECASE,
        )
        if m:
            base = m.group(1)
            suffix = re.sub(r"\s+", "", m.group(2) or "").upper()
            return f"Boeing {base}-{suffix}" if suffix else f"Boeing {base}"

        # Airbus NEO/neo variants with various spellings:
        # "A320neo", "A320 neo", "A320-neo", "A320N", "A321 NEO", etc.
        m = re.search(r"\bA(3[012]\d)[-\s]*(?:NEO|N)\b", entry, re.IGNORECASE)
        if m:
            num = m.group(1)
            return f"Airbus A{num}neo"

        # Airbus plain (strip subvariants like "-200" for consistency)
        # Excludes A322/A329/A366 which are not real Airbus types
        m = re.search(
            r"\b(?:Airbus\s+)?A(31[89]|32[01]|33[02-9]|34[0-9]|35[0-9]|380)\b",
            entry,
            re.IGNORECASE,
        )
        if m:
            return f"Airbus A{m.group(1).upper()}"

        # Embraer: covers 145/170/175/190/195 (previously only 170/190/195)
        # Handles "Embraer 190", "Embraer E190", "E-190", "E190"
        m = re.search(
            r"\b(?:Embraer\s*(?:E[-\s]?)?|E-?)(\d{3})\b",
            entry,
            re.IGNORECASE,
        )
        if m and int(m.group(1)) in _VALID_EMBRAER:
            return f"Embraer {m.group(1)}"

        # Other commercial types
        m = re.search(r"\bATR\s*(\d{2})\b", entry, re.IGNORECASE)
        if m:
            return f"ATR {m.group(1)}"
        if re.search(r"\bSaab\s+2000\b", entry, re.IGNORECASE):
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
    parser.add_argument(
        "--date", type=date.fromisoformat, default=None, help="Run date YYYY-MM-DD (default: today)"
    )
    parser.add_argument("--yesterday", action="store_true", help="Process yesterday's file")
    args = parser.parse_args()

    run_date = args.date or (date.today() - timedelta(days=1) if args.yesterday else date.today())

    input_path = get_input_path(run_date)
    output_path = get_output_path(run_date)

    if not input_path.exists():
        raise FileNotFoundError(f"Raw file not found: {input_path}")

    clean(input_path, output_path)
    print(f"Done: {output_path}")
