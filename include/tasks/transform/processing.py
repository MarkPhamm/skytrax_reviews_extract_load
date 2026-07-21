"""
Transforms raw scraped reviews into clean, analysis-ready CSVs.

Two entry points:
  - ``clean(input, output)`` — the airline cleaning pipeline (route + aircraft
    normalisation + airline column order).
  - ``clean_combined(category)`` / ``clean_file(category, input, output)`` —
    category-aware cleaning for seat/lounge/airport (and airline).

Paths/keys are built by ``include.tasks.common.paths`` (single source of truth).
"""

import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd

from include.tasks.common import paths
from include.tasks.extract.scraper import CATEGORIES as _SCRAPER_CATEGORIES
from include.tasks.transform import config as cfg

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


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
    df = _drop_duplicate_rows(df, input_path)

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


def _drop_duplicate_rows(df: pd.DataFrame, source: Union[str, Path]) -> pd.DataFrame:
    """Drop exact-duplicate rows (repeat scrapes merged into the same raw CSV)."""
    before = len(df)
    df = df.drop_duplicates().reset_index(drop=True)
    dropped = before - len(df)
    if dropped:
        logger.info("Dropped %d duplicate row(s) from %s", dropped, source)
    return df


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.rename(columns=lambda x: x.strip().lower().replace(" ", "_"), inplace=True)
    df.columns = df.columns.str.replace("&", "and").str.replace("-", "_").str.replace("/", "_")
    df.rename(columns={"date": "date_submitted", "country": "nationality"}, inplace=True)
    return df


def _clean_date_submitted(df: pd.DataFrame, coerce: bool = False) -> pd.DataFrame:
    # Dates may already be ISO-formatted (from split_and_save) or raw ("22nd March 2025").
    # coerce=True (combined raw CSVs) turns an unparseable date into NaT instead of raising.
    errors = "coerce" if coerce else "raise"
    try:
        df["date_submitted"] = pd.to_datetime(df["date_submitted"], format="%Y-%m-%d").dt.strftime(
            "%Y-%m-%d"
        )
    except (ValueError, TypeError):
        df["date_submitted"] = df["date_submitted"].str.replace(
            r"(\d+)(st|nd|rd|th)", r"\1", regex=True
        )
        df["date_submitted"] = pd.to_datetime(
            df["date_submitted"], format="%d %B %Y", errors=errors
        ).dt.strftime("%Y-%m-%d")
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
    return _clean_month_year(df, "date_flown")


def _clean_month_year(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Parse a 'Month YYYY' column (e.g. 'March 2025') to ISO date string."""
    df[col] = pd.to_datetime(df[col], format="%B %Y", errors="coerce").dt.strftime("%Y-%m-%d")
    return df


def _clean_recommended(df: pd.DataFrame) -> pd.DataFrame:
    df["recommended"] = df["recommended"].str.contains("yes", case=False, na=False)
    return df


# Star-rated columns for the airline pipeline (used when no explicit list is given).
_AIRLINE_RATING_COLS = [
    "seat_comfort",
    "cabin_staff_service",
    "food_and_beverages",
    "inflight_entertainment",
    "ground_service",
    "wifi_and_connectivity",
    "value_for_money",
]


def _clean_ratings(df: pd.DataFrame, rating_cols: Optional[List[str]] = None) -> pd.DataFrame:
    if rating_cols is None:
        rating_cols = _AIRLINE_RATING_COLS
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


def _normalize_aircraft_value(entry):
    """Normalize a single raw aircraft string to canonical 'Manufacturer Model' or None."""
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


def _clean_aircraft(df: pd.DataFrame) -> pd.DataFrame:
    df["aircraft"] = df["aircraft"].apply(_normalize_aircraft_value)
    return df


def _reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    return _reorder(df, cfg.COLUMN_ORDER)


def _reorder(df: pd.DataFrame, order: List[str]) -> pd.DataFrame:
    """Keep only the columns in `order` that exist, in that order."""
    existing = [c for c in order if c in df.columns]
    return df[existing]


def _add_updated_at(df: pd.DataFrame) -> pd.DataFrame:
    df["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df


# ---------------------------------------------------------------------------
# Combined (flat) cleaning for seat / lounge / airport categories
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CleaningProfile:
    """Per-category cleaning configuration for the flat combined CSVs."""

    event_date_col: str  # "date_flown" (seat) or "date_visit" (lounge/airport)
    rating_cols: List[str]  # snake_case star-rated columns (0-5) → Int64, 0 = NA
    normalize_aircraft_col: Optional[str]  # column to canonicalise (seat only) or None
    column_order: List[str]  # final processed column order


CLEANING_PROFILES: Dict[str, CleaningProfile] = {
    "seat": CleaningProfile(
        event_date_col="date_flown",
        rating_cols=[
            "seat_legroom",
            "seat_recline",
            "seat_width",
            "aisle_space",
            "seat_storage",
            "power_supply",
            "viewing_tv_screen",
            "sleep_comfort",
            "sitting_comfort",
            "seat_bed_width",
            "seat_bed_length",
            "seat_privacy",
        ],
        normalize_aircraft_col="aircraft_type",
        column_order=[
            "verify",
            "date_submitted",
            "date_flown",
            "customer_name",
            "nationality",
            "airline_name",
            "type_of_traveller",
            "seat_type",
            "aircraft_type",
            "seat_layout",
            "seat_legroom",
            "seat_recline",
            "seat_width",
            "aisle_space",
            "seat_storage",
            "power_supply",
            "viewing_tv_screen",
            "sleep_comfort",
            "sitting_comfort",
            "seat_bed_width",
            "seat_bed_length",
            "seat_privacy",
            "recommended",
            "review",
            "updated_at",
        ],
    ),
    "lounge": CleaningProfile(
        event_date_col="date_visit",
        rating_cols=[
            "comfort",
            "cleanliness",
            "bar_and_beverages",
            "catering",
            "washrooms",
            "wifi_connectivity",
            "staff_service",
        ],
        normalize_aircraft_col=None,
        column_order=[
            "verify",
            "date_submitted",
            "date_visit",
            "customer_name",
            "nationality",
            "airline_name",
            "lounge_name",
            "airport",
            "type_of_lounge",
            "type_of_traveller",
            "comfort",
            "cleanliness",
            "bar_and_beverages",
            "catering",
            "washrooms",
            "wifi_connectivity",
            "staff_service",
            "recommended",
            "review",
            "updated_at",
        ],
    ),
    "airport": CleaningProfile(
        event_date_col="date_visit",
        rating_cols=[
            "queuing_times",
            "terminal_cleanliness",
            "terminal_seating",
            "terminal_signs",
            "food_beverages",
            "airport_shopping",
            "airport_staff",
            "wifi_connectivity",
        ],
        normalize_aircraft_col=None,
        column_order=[
            "verify",
            "date_submitted",
            "date_visit",
            "customer_name",
            "nationality",
            "airport_name",
            "experience_at_airport",
            "type_of_traveller",
            "queuing_times",
            "terminal_cleanliness",
            "terminal_seating",
            "terminal_signs",
            "food_beverages",
            "airport_shopping",
            "airport_staff",
            "wifi_connectivity",
            "recommended",
            "review",
            "updated_at",
        ],
    ),
}


def combined_paths(category: str) -> Tuple[Path, Path]:
    """Return (raw_input_path, processed_output_path) for a combined-CSV category.

    Input filename comes from the scraper registry (single source of truth);
    output mirrors it with ``_raw`` → ``_processed``. Flat combined CSVs live
    under landing/all/ (see scripts/migrate_to_typed_layout.py's FLAT_DIR).
    """
    raw_name = _SCRAPER_CATEGORIES[category].combined_filename
    if not raw_name:
        raise ValueError(f"Category '{category}' has no combined CSV to process.")
    flat_dir = paths.LANDING_DIR / "all"
    return (
        flat_dir / raw_name,
        flat_dir / raw_name.replace("_raw", "_processed"),
    )


def _clean_combined_df(df: pd.DataFrame, category: str) -> pd.DataFrame:
    """Apply the profile-driven cleaning pipeline for seat/lounge/airport."""
    if category not in CLEANING_PROFILES:
        raise ValueError(
            f"Unknown combined category '{category}'. "
            f"Expected one of {sorted(CLEANING_PROFILES)}."
        )
    profile = CLEANING_PROFILES[category]

    df = _rename_columns(df)
    df = _clean_date_submitted(df, coerce=True)
    df = _clean_nationality(df)
    df = _clean_review_body(df)
    df = _clean_month_year(df, profile.event_date_col)
    df = _clean_recommended(df)
    df = _clean_ratings(df, profile.rating_cols)
    if profile.normalize_aircraft_col and profile.normalize_aircraft_col in df.columns:
        df[profile.normalize_aircraft_col] = df[profile.normalize_aircraft_col].apply(
            _normalize_aircraft_value
        )
    df = _add_updated_at(df)
    df = _reorder(df, profile.column_order)
    return df


def clean_combined(category: str) -> Path:
    """Clean a flat combined raw CSV (seat/lounge/airport) into a processed CSV.

    Reads  landing/all_<category>_review_raw.csv
    Writes landing/all_<category>_review_processed.csv
    """
    input_path, output_path = combined_paths(category)

    df = pd.read_csv(input_path)
    logger.info("Loaded %d rows from %s", len(df), input_path)
    df = _drop_duplicate_rows(df, input_path)

    df = _clean_combined_df(df, category)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info("Saved %d rows → %s", len(df), output_path)
    return output_path


def clean_file(category: str, input_path: Path, output_path: Path) -> Path:
    """Clean one date-partitioned raw CSV → processed CSV, dispatching by category.

    airline → the full airline pipeline (route + aircraft + airline column order);
    seat/lounge/airport → the profile-driven pipeline.
    """
    input_path, output_path = Path(input_path), Path(output_path)
    if category == "airline":
        return clean(input_path, output_path)

    df = pd.read_csv(input_path)
    logger.info("Loaded %d rows from %s", len(df), input_path)
    df = _drop_duplicate_rows(df, input_path)
    df = _clean_combined_df(df, category)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info("Saved %d rows → %s", len(df), output_path)
    return output_path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse
    from datetime import timedelta

    parser = argparse.ArgumentParser(description="Clean raw Skytrax reviews")
    parser.add_argument(
        "--category",
        choices=sorted(_SCRAPER_CATEGORIES),
        default="airline",
        help="Review category to clean (default: airline).",
    )
    parser.add_argument(
        "--combined",
        action="store_true",
        help="Ad-hoc: clean the flat landing/all_<cat>_review_raw.csv (seat/lounge/airport).",
    )
    parser.add_argument(
        "--date", type=date.fromisoformat, default=None, help="Run date YYYY-MM-DD (default: today)"
    )
    parser.add_argument("--yesterday", action="store_true", help="Process yesterday's file")
    parser.add_argument(
        "--all", action="store_true", help="Process all raw files for the category."
    )
    args = parser.parse_args()

    if args.combined:
        output_path = clean_combined(args.category)
        print(f"Done: {output_path}")
    elif args.all:
        raw_dir = paths.LANDING_DIR / "raw" / paths.partition(args.category)
        raw_files = sorted(raw_dir.glob("**/raw_data_*.csv"))
        if not raw_files:
            raise FileNotFoundError(f"No raw files found in {raw_dir}")
        for raw_file in raw_files:
            date_str = raw_file.stem.replace("raw_data_", "")
            run_date = datetime.strptime(date_str, "%Y%m%d").date()
            output_path = paths.processed_local_path(args.category, run_date)
            clean_file(args.category, raw_file, output_path)
            print(f"Done: {output_path}")
    else:
        run_date = args.date or (
            date.today() - timedelta(days=1) if args.yesterday else date.today()
        )

        input_path = paths.raw_local_path(args.category, run_date)
        output_path = paths.processed_local_path(args.category, run_date)

        if not input_path.exists():
            raise FileNotFoundError(f"Raw file not found: {input_path}")

        clean_file(args.category, input_path, output_path)
        print(f"Done: {output_path}")
