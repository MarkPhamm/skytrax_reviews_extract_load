"""
Unit tests for include/tasks/transform/processing.py

Covers the flat combined-CSV cleaning (clean_combined) for the seat / lounge /
airport categories, plus a guard test that the airline clean() pipeline still
produces the canonical airline column order.

Run with:  pytest tests/transform/test_processing.py -v
"""

import pandas as pd
import pytest

import include.tasks.common.paths as paths_mod
import include.tasks.transform.config as cfg
import include.tasks.transform.processing as proc


@pytest.fixture
def landing(tmp_path, monkeypatch):
    """Point the shared LANDING_DIR at a temp dir for the duration of a test."""
    monkeypatch.setattr(paths_mod, "LANDING_DIR", tmp_path)
    return tmp_path


def _write_raw(landing, category, rows):
    """Write a synthetic combined raw CSV under the category's raw filename."""
    raw_name = proc._SCRAPER_CATEGORIES[category].combined_filename
    path = landing / raw_name
    pd.DataFrame(rows).to_csv(path, index=False)
    return path


# ---------------------------------------------------------------------------
# seat
# ---------------------------------------------------------------------------

_SEAT_ROWS = [
    {
        "date": "26th August 2023",
        "customer_name": "T Carsen",
        "country": "Canada",
        "review_body": "✅ Trip Verified | Great seat",
        "Seat Type": "Economy Class",
        "Aircraft Type": "boeing 737-800",
        "Seat Layout": "3x3",
        "Date Flown": "August 2023",
        "Type Of Traveller": "Couple Leisure",
        "Seat Legroom": 5,
        "Seat Recline": 0,
        "Seat Width": 3,
        "Aisle Space": 2,
        "Power Supply": 2,
        "Seat Storage": 1,
        "Recommended": "yes",
        "airline_name": "Aeromexico",
        "Sleep Comfort": 4,
        "Sitting Comfort": 4,
        "Seat/bed Width": 3,
        "Seat/bed Length": 3,
        "Seat Privacy": 2,
        "Viewing Tv Screen": 2,
    },
    {
        "date": "1st March 2024",
        "customer_name": "J Doe",
        "country": "United Kingdom",
        "review_body": "Not Verified | Cramped",
        "Seat Type": "Business Class",
        "Aircraft Type": "A320",
        "Seat Layout": "2x2",
        "Date Flown": "March 2024",
        "Type Of Traveller": "Business",
        "Seat Legroom": 1,
        "Seat Recline": 1,
        "Seat Width": 1,
        "Aisle Space": 1,
        "Power Supply": 0,
        "Seat Storage": 1,
        "Recommended": "no",
        "airline_name": "Aeromexico",
        "Sleep Comfort": 1,
        "Sitting Comfort": 1,
        "Seat/bed Width": 1,
        "Seat/bed Length": 1,
        "Seat Privacy": 1,
        "Viewing Tv Screen": 1,
    },
]


def test_clean_combined_seat(landing):
    _write_raw(landing, "seat", _SEAT_ROWS)
    out = proc.clean_combined("seat")

    assert out == landing / "all_seats_review_processed.csv"
    df = pd.read_csv(out)

    # Column order matches the seat profile exactly.
    assert list(df.columns) == proc.CLEANING_PROFILES["seat"].column_order
    # slash column was normalised to underscore.
    assert "seat_bed_width" in df.columns

    # verify split: bool, derived from review_body.
    assert df["verify"].tolist() == [True, False]
    assert df["review"].tolist() == ["Great seat", "Cramped"]

    # dates → ISO.
    assert df["date_submitted"].tolist() == ["2023-08-26", "2024-03-01"]
    assert df["date_flown"].tolist() == ["2023-08-01", "2024-03-01"]

    # recommended → bool.
    assert df["recommended"].tolist() == [True, False]

    # aircraft_type canonicalised.
    assert df["aircraft_type"].tolist() == ["Boeing 737-800", "Airbus A320"]


def test_clean_combined_seat_ratings_int64_zero_is_na(landing):
    _write_raw(landing, "seat", _SEAT_ROWS)
    out = proc.clean_combined("seat")
    df = pd.read_csv(out)
    # Re-read with proper dtype to check the 0 → NA conversion.
    df = pd.read_csv(out, dtype={"seat_recline": "Int64"})
    # Row 0 has Seat Recline = 0 → NA; Seat Legroom = 5 → 5.
    assert pd.isna(df["seat_recline"].iloc[0])
    assert df["seat_legroom"].iloc[0] == 5


# ---------------------------------------------------------------------------
# lounge
# ---------------------------------------------------------------------------

_LOUNGE_ROWS = [
    {
        "date": "21st May 2024",
        "customer_name": "Kevin T",
        "country": "United Kingdom",
        "review_body": "✅ Trip Verified | Nice lounge",
        "Lounge Name": "JFK Terminal 7",
        "Airport": "New York JFK Airport",
        "Type Of Lounge": "Business Class",
        "Date Visit": "May 2024",
        "Type Of Traveller": "Business",
        "Comfort": 5,
        "Cleanliness": 5,
        "Bar & Beverages": 4,
        "Catering": 4,
        "Washrooms": 4,
        "Wifi Connectivity": 5,
        "Staff Service": 4,
        "Recommended": "yes",
        "airline_name": "Aer Lingus",
        "Type Of Lounge Other": 0,
    },
]


def test_clean_combined_lounge(landing):
    _write_raw(landing, "lounge", _LOUNGE_ROWS)
    out = proc.clean_combined("lounge")

    assert out == landing / "all_lounge_review_processed.csv"
    df = pd.read_csv(out)

    assert list(df.columns) == proc.CLEANING_PROFILES["lounge"].column_order
    # The stray scraper artifact column is dropped.
    assert "type_of_lounge_other" not in df.columns
    assert df["date_visit"].tolist() == ["2024-05-01"]
    assert df["verify"].tolist() == [True]
    assert df["airline_name"].tolist() == ["Aer Lingus"]


# ---------------------------------------------------------------------------
# airport
# ---------------------------------------------------------------------------

_AIRPORT_ROWS = [
    {
        "date": "6th June 2022",
        "customer_name": "Pieter Boone",
        "country": "Netherlands",
        "review_body": "Not Verified | Newish airport",
        "Experience At Airport": "Arrival and Departure",
        "Date Visit": "March 2022",
        "Type Of Traveller": "Couple Leisure",
        "Queuing Times": 5,
        "Terminal Cleanliness": 5,
        "Terminal Seating": 5,
        "Terminal Signs": 5,
        "Food Beverages": 5,
        "Airport Shopping": 5,
        "Airport Staff": 5,
        "Wifi Connectivity": 1,
        "Recommended": "yes",
        "airport_name": "Acapulco",
    },
]


def test_clean_combined_airport(landing):
    _write_raw(landing, "airport", _AIRPORT_ROWS)
    out = proc.clean_combined("airport")

    assert out == landing / "all_airport_review_processed.csv"
    df = pd.read_csv(out)

    assert list(df.columns) == proc.CLEANING_PROFILES["airport"].column_order
    # airport keys the entity name as airport_name.
    assert "airport_name" in df.columns
    assert "airline_name" not in df.columns
    assert df["date_visit"].tolist() == ["2022-03-01"]
    assert df["verify"].tolist() == [False]
    assert df["experience_at_airport"].tolist() == ["Arrival and Departure"]


def test_clean_combined_rejects_airline(landing):
    # airline has no flat combined CSV — it uses the date-partitioned pipeline.
    with pytest.raises(ValueError, match="no combined CSV"):
        proc.clean_combined("airline")


def test_clean_combined_df_rejects_unknown_category():
    with pytest.raises(ValueError, match="Unknown combined category"):
        proc._clean_combined_df(pd.DataFrame({"date": []}), "trains")


# ---------------------------------------------------------------------------
# guard: airline clean() pipeline still works (protects reused helpers)
# ---------------------------------------------------------------------------

_AIRLINE_ROWS = [
    {
        "date": "2025-03-27",
        "customer_name": "Alice",
        "country": "(United Kingdom)",
        "review_body": "✅ Trip Verified | Good flight",
        "Type Of Traveller": "Solo Leisure",
        "Seat Type": "Economy Class",
        "Route": "London to Paris",
        "Date Flown": "March 2025",
        "Aircraft": "A320",
        "Seat Comfort": 4,
        "Cabin Staff Service": 5,
        "Food & Beverages": 0,
        "Inflight Entertainment": 3,
        "Ground Service": 4,
        "Wifi & Connectivity": 2,
        "Value For Money": 4,
        "Recommended": "yes",
        "airline_name": "British Airways",
    },
]


def test_airline_clean_still_produces_column_order(tmp_path):
    raw = tmp_path / "raw_data_20250327.csv"
    pd.DataFrame(_AIRLINE_ROWS).to_csv(raw, index=False)
    out = tmp_path / "clean_data_20250327.csv"

    proc.clean(raw, out)
    df = pd.read_csv(out)

    # clean() reorders to COLUMN_ORDER then appends updated_at.
    assert list(df.columns) == [c for c in cfg.COLUMN_ORDER if c in df.columns] + ["updated_at"]
    assert df["verify"].tolist() == [True]
    assert df["recommended"].tolist() == [True]
    assert df["date_submitted"].tolist() == ["2025-03-27"]
    # 0-star rating became NA.
    df_int = pd.read_csv(out, dtype={"food_and_beverages": "Int64"})
    assert pd.isna(df_int["food_and_beverages"].iloc[0])


# ---------------------------------------------------------------------------
# clean_file dispatcher (used by dag_process per (type, date))
# ---------------------------------------------------------------------------


def test_clean_file_airline_uses_airline_pipeline(tmp_path):
    raw = tmp_path / "raw_data_20250327.csv"
    pd.DataFrame(_AIRLINE_ROWS).to_csv(raw, index=False)
    out = tmp_path / "out.csv"

    result = proc.clean_file("airline", raw, out)
    assert result == out
    df = pd.read_csv(out)
    # airline pipeline yields the airline column order (+ route/aircraft cols).
    assert list(df.columns) == [c for c in cfg.COLUMN_ORDER if c in df.columns] + ["updated_at"]
    assert "origin_city" in df.columns  # route parsing only happens for airline
    assert df["aircraft"].tolist() == ["Airbus A320"]


def test_clean_file_seat_uses_profile_pipeline(tmp_path):
    raw = tmp_path / "raw_data_20230826.csv"
    pd.DataFrame(_SEAT_ROWS).to_csv(raw, index=False)
    out = tmp_path / "out.csv"

    result = proc.clean_file("seat", raw, out)
    assert result == out
    df = pd.read_csv(out)
    assert list(df.columns) == proc.CLEANING_PROFILES["seat"].column_order
    assert "origin_city" not in df.columns  # no route parsing for seats
    assert df["aircraft_type"].tolist() == ["Boeing 737-800", "Airbus A320"]
