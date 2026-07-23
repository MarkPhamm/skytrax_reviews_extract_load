"""
Unit tests for include/tasks/load/snowflake_load.py

Run with:  pytest tests/load/test_snowflake_load.py -v
"""

from unittest.mock import MagicMock, patch

import pytest

import include.tasks.load.snowflake_load as mod


def test_table_name_mapping():
    assert mod.table_name("airline") == "SKYTRAX_REVIEWS_DB.RAW.AIRLINE_REVIEWS"
    assert mod.table_name("seat") == "SKYTRAX_REVIEWS_DB.RAW.SEAT_REVIEWS"
    assert mod.table_name("lounge") == "SKYTRAX_REVIEWS_DB.RAW.LOUNGE_REVIEWS"
    assert mod.table_name("airport") == "SKYTRAX_REVIEWS_DB.RAW.AIRPORT_REVIEWS"


def test_table_name_unknown_raises():
    with pytest.raises(ValueError, match="Unknown review category"):
        mod.table_name("trains")


def test_ensure_table_reads_per_type_ddl():
    hook = MagicMock()
    with patch.object(mod, "_get_hook", return_value=hook):
        mod.ensure_table("lounge")

    # DDL split on ';' → at least the CREATE TABLE statement runs.
    ran = " ".join(call.args[0] for call in hook.run.call_args_list)
    assert "LOUNGE_REVIEWS" in ran


# ---------------------------------------------------------------------------
# copy_into_bulk — one COPY INTO over the whole processed/<type>/ prefix
# ---------------------------------------------------------------------------


def test_copy_into_bulk_targets_whole_prefix():
    hook = MagicMock()
    hook.get_records.return_value = [
        ("processed/seats/2020/01/clean_data_20200101.csv", "LOADED", 10, 10, 1, 0, None),
        ("processed/seats/2020/01/clean_data_20200102.csv", "LOADED", 5, 5, 1, 0, None),
    ]
    with patch.object(mod, "_get_hook", return_value=hook):
        totals = mod.copy_into_bulk("seat")

    sql = hook.get_records.call_args.args[0]
    assert "SKYTRAX_REVIEWS_DB.RAW.SEAT_REVIEWS" in sql
    assert "processed/seats/" in sql
    assert totals["rows_loaded"] == 15
    assert totals["files_loaded"] == 2
    assert totals["files_skipped"] == 0


def test_copy_into_bulk_records_audit_row_per_file_with_parsed_date():
    hook = MagicMock()
    hook.get_records.return_value = [
        ("processed/seats/2020/01/clean_data_20200101.csv", "LOADED", 10, 10, 1, 0, None),
    ]
    with patch.object(mod, "_get_hook", return_value=hook):
        mod.copy_into_bulk("seat")

    insert_call = hook.run.call_args_list[-1]
    assert insert_call.kwargs["parameters"]["review_date"] == "2020-01-01"
    assert insert_call.kwargs["parameters"]["category"] == "seat"


def test_copy_into_bulk_all_already_loaded_is_noop():
    hook = MagicMock()
    hook.get_records.return_value = [("Copy executed with 0 files processed.",)]
    with patch.object(mod, "_get_hook", return_value=hook):
        totals = mod.copy_into_bulk("seat")

    assert totals["files_skipped"] == 1
    assert totals["files_loaded"] == 0


def test_copy_into_bulk_rejected_rows_raise():
    hook = MagicMock()
    hook.get_records.return_value = [
        (
            "processed/seats/2020/01/clean_data_20200101.csv",
            "PARTIALLY_LOADED",
            10,
            8,
            1,
            2,
            "bad row",
        ),
    ]
    with patch.object(mod, "_get_hook", return_value=hook):
        with pytest.raises(RuntimeError, match="2 rejected row"):
            mod.copy_into_bulk("seat")


# ---------------------------------------------------------------------------
# copy_into_bulk — exclude_dates: quality-rejected dates are never loaded
# ---------------------------------------------------------------------------


def test_copy_into_bulk_with_exclusions_lists_only_good_files():
    hook = MagicMock()
    hook.get_records.return_value = [
        ("processed/seats/2026/03/clean_data_20260310.csv", "LOADED", 5, 5, 1, 0, None),
    ]
    with patch.object(mod, "_get_hook", return_value=hook):
        totals = mod.copy_into_bulk(
            "seat",
            all_dates=["2026-03-10", "2026-03-11", "2026-03-12"],
            exclude_dates={"2026-03-11", "2026-03-12"},
        )

    sql = hook.get_records.call_args.args[0]
    assert "FILES = (" in sql
    assert "processed/seats/2026/03/clean_data_20260310.csv" in sql
    assert "clean_data_20260311.csv" not in sql
    assert "clean_data_20260312.csv" not in sql
    assert totals["rows_loaded"] == 5


def test_copy_into_bulk_all_dates_excluded_skips_copy():
    hook = MagicMock()
    with patch.object(mod, "_get_hook", return_value=hook):
        totals = mod.copy_into_bulk(
            "seat",
            all_dates=["2026-03-10"],
            exclude_dates={"2026-03-10"},
        )

    hook.get_records.assert_not_called()
    assert totals["files_loaded"] == 0
    assert totals["rows_loaded"] == 0
