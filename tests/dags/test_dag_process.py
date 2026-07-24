"""
Unit tests for dag_process's per-(category, date) pipeline: the post-upload
quality gate must quarantine rejected files, and only rejected files.

Imports dags/dag_process.py directly (apache-airflow comes from the dev
extra); every S3/disk step is mocked — no cloud access.

Run with:  pytest tests/dags/test_dag_process.py -v
"""

from datetime import date
from unittest.mock import MagicMock, patch

from dags import dag_process as dp


def _run_process_one(monkeypatch, *, storage_mode="s3", validate_side_effect=None):
    monkeypatch.setenv("STORAGE_MODE", storage_mode)
    monkeypatch.setenv("S3_BUCKET", "test-bucket")
    client = MagicMock()

    quarantine_uri = "s3://test-bucket/quarantine/seats/2026/03/clean_data_20260311.csv"
    with (
        patch.object(dp, "_download_one", return_value="raw.csv"),
        patch.object(dp, "_clean_one", return_value="clean.csv"),
        patch.object(dp, "_upload_one", return_value="s3://test-bucket/processed/x.csv"),
        patch.object(dp, "validate_processed_csv", side_effect=validate_side_effect),
        patch.object(dp, "_move_to_quarantine", return_value=quarantine_uri) as mock_move,
    ):
        result = dp._process_one("seat", "2026-03-11", client=client)
    return result, mock_move, client


def test_rejected_file_is_moved_to_quarantine(monkeypatch):
    result, mock_move, client = _run_process_one(
        monkeypatch, validate_side_effect=dp.DataQualityError("schema mismatch")
    )

    assert result["quality_passed"] is False
    assert "schema mismatch" in result["quality_error"]
    mock_move.assert_called_once_with(
        "seat",
        date(2026, 3, 11),
        bucket="test-bucket",
        use_airflow_hook=True,
        client=client,
    )
    # The returned URI points at quarantine, not the (now deleted) processed key.
    assert result["uri"].startswith("s3://test-bucket/quarantine/")


def test_passing_file_is_not_quarantined(monkeypatch):
    result, mock_move, _ = _run_process_one(monkeypatch, validate_side_effect=None)

    assert result["quality_passed"] is True
    assert result["quality_error"] is None
    mock_move.assert_not_called()
    assert result["uri"] == "s3://test-bucket/processed/x.csv"


def test_local_mode_rejection_skips_quarantine_move(monkeypatch):
    # STORAGE_MODE=local uploads nothing, so there is nothing in S3 to move —
    # the rejection is still recorded via quality_passed=False.
    result, mock_move, _ = _run_process_one(
        monkeypatch,
        storage_mode="local",
        validate_side_effect=dp.DataQualityError("empty file"),
    )

    assert result["quality_passed"] is False
    mock_move.assert_not_called()
