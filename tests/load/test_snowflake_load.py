"""
Unit tests for include/tasks/load/snowflake_load.py

Run with:  pytest tests/load/test_snowflake_load.py -v
"""

from datetime import date
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


def test_copy_into_templates_table_and_key():
    hook = MagicMock()
    with patch.object(mod, "_get_hook", return_value=hook):
        mod.copy_into("seat", date(2026, 3, 12))

    hook.run.assert_called_once()
    sql = hook.run.call_args.args[0]
    assert "SKYTRAX_REVIEWS_DB.RAW.SEAT_REVIEWS" in sql
    assert "processed/seats/2026/03/clean_data_20260312.csv" in sql
    # placeholders fully substituted
    assert "{{" not in sql


def test_copy_into_airport_targets_airport_table():
    hook = MagicMock()
    with patch.object(mod, "_get_hook", return_value=hook):
        mod.copy_into("airport", date(2026, 3, 12))

    sql = hook.run.call_args.args[0]
    assert "AIRPORT_REVIEWS" in sql
    assert "processed/airports/2026/03/clean_data_20260312.csv" in sql


def test_ensure_table_reads_per_type_ddl():
    hook = MagicMock()
    with patch.object(mod, "_get_hook", return_value=hook):
        mod.ensure_table("lounge")

    # DDL split on ';' → at least the CREATE TABLE statement runs.
    ran = " ".join(call.args[0] for call in hook.run.call_args_list)
    assert "LOUNGE_REVIEWS" in ran
