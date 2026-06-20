"""
Unit tests for include/tasks/common/paths.py

Run with:  pytest tests/common/test_paths.py -v
"""

from datetime import date

import pytest

import include.tasks.common.paths as paths


def test_partition_mapping():
    assert paths.partition("airline") == "airlines"
    assert paths.partition("seat") == "seats"
    assert paths.partition("lounge") == "lounges"
    assert paths.partition("airport") == "airports"


def test_partition_unknown_raises():
    with pytest.raises(ValueError, match="Unknown review category"):
        paths.partition("trains")


def test_raw_key_type_first_and_zero_padded():
    assert (
        paths.raw_key("airline", date(2026, 3, 12)) == "raw/airlines/2026/03/raw_data_20260312.csv"
    )
    # single-digit month is zero-padded
    assert paths.raw_key("seat", date(2025, 1, 5)) == "raw/seats/2025/01/raw_data_20250105.csv"


def test_processed_key_per_type():
    assert (
        paths.processed_key("lounge", date(2026, 3, 12))
        == "processed/lounges/2026/03/clean_data_20260312.csv"
    )
    assert (
        paths.processed_key("airport", date(2026, 3, 12))
        == "processed/airports/2026/03/clean_data_20260312.csv"
    )


def test_local_paths_under_landing_dir(monkeypatch, tmp_path):
    monkeypatch.setattr(paths, "LANDING_DIR", tmp_path)
    raw = paths.raw_local_path("airport", date(2026, 3, 12))
    proc = paths.processed_local_path("airport", date(2026, 3, 12))
    assert raw == tmp_path / "raw" / "airports" / "2026" / "03" / "raw_data_20260312.csv"
    assert proc == tmp_path / "processed" / "airports" / "2026" / "03" / "clean_data_20260312.csv"
    assert raw.is_relative_to(tmp_path)
