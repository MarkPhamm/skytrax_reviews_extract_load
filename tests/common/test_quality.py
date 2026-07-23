"""
Unit tests for include/tasks/common/quality.py (pre-load quality gate).

Run with:  pytest tests/common/test_quality.py -v
"""

import pandas as pd
import pytest

from include.tasks.common.quality import (
    DataQualityError,
    expected_columns,
    validate_processed_csv,
)


def _valid_airline_df(rows: int = 3) -> pd.DataFrame:
    """Minimal valid processed airline frame matching the expected schema."""
    cols = expected_columns("airline")
    df = pd.DataFrame({c: [None] * rows for c in cols})
    df["date_submitted"] = "2026-07-01"
    df["updated_at"] = "2026-07-02 00:00:00"
    df["review"] = "Great flight"
    df["seat_comfort"] = 4
    df["value_for_money"] = 1
    return df


def _write(tmp_path, df):
    path = tmp_path / "clean_data_20260701.csv"
    df.to_csv(path, index=False)
    return path


def test_valid_file_passes(tmp_path):
    path = _write(tmp_path, _valid_airline_df())
    summary = validate_processed_csv("airline", path)
    assert summary["rows"] == 3
    assert summary["category"] == "airline"


def test_missing_file_fails(tmp_path):
    with pytest.raises(DataQualityError, match="not found"):
        validate_processed_csv("airline", tmp_path / "nope.csv")


def test_empty_file_fails(tmp_path):
    path = _write(tmp_path, _valid_airline_df(rows=0))
    with pytest.raises(DataQualityError, match="no rows"):
        validate_processed_csv("airline", path)


def test_missing_column_fails(tmp_path):
    df = _valid_airline_df().drop(columns=["recommended"])
    path = _write(tmp_path, df)
    with pytest.raises(DataQualityError, match="missing=\\['recommended'\\]"):
        validate_processed_csv("airline", path)


def test_unexpected_column_fails(tmp_path):
    df = _valid_airline_df()
    df["surprise"] = 1
    path = _write(tmp_path, df)
    with pytest.raises(DataQualityError, match="unexpected=\\['surprise'\\]"):
        validate_processed_csv("airline", path)


def test_null_date_submitted_fails(tmp_path):
    df = _valid_airline_df()
    df.loc[0, "date_submitted"] = None
    path = _write(tmp_path, df)
    with pytest.raises(DataQualityError, match="date_submitted"):
        validate_processed_csv("airline", path)


def test_review_null_rate_over_threshold_fails(tmp_path):
    df = _valid_airline_df(rows=10)
    df.loc[0, "review"] = None  # 10% > 5% threshold
    path = _write(tmp_path, df)
    with pytest.raises(DataQualityError, match="null rate"):
        validate_processed_csv("airline", path)


def test_out_of_range_rating_fails(tmp_path):
    df = _valid_airline_df()
    df.loc[0, "seat_comfort"] = 9
    path = _write(tmp_path, df)
    with pytest.raises(DataQualityError, match="seat_comfort"):
        validate_processed_csv("airline", path)


def test_unknown_category_raises():
    with pytest.raises(ValueError, match="Unknown review category"):
        expected_columns("trains")
