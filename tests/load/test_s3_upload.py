"""
Unit tests for include/tasks/load/s3_upload.py

Run with:  pytest tests/load/test_s3_upload.py -v
"""

from datetime import date
from unittest.mock import MagicMock, patch

import pytest

import include.tasks.common.paths as paths_mod
import include.tasks.load.s3_upload as mod

# ---------------------------------------------------------------------------
# Path helpers (type-first: raw/<type>/YYYY/MM/...)
# ---------------------------------------------------------------------------


def test_raw_local_path_format():
    p = mod.raw_local_path("airline", date(2026, 3, 12))
    assert p.parts[-1] == "raw_data_20260312.csv"
    assert p.parts[-2] == "03"
    assert p.parts[-3] == "2026"
    assert p.parts[-4] == "airlines"
    assert p.parts[-5] == "raw"


def test_processed_local_path_format():
    p = mod.processed_local_path("seat", date(2026, 3, 12))
    assert p.parts[-1] == "clean_data_20260312.csv"
    assert p.parts[-2] == "03"
    assert p.parts[-3] == "2026"
    assert p.parts[-4] == "seats"
    assert p.parts[-5] == "processed"


def test_raw_s3_key():
    assert (
        mod.raw_s3_key("airline", date(2026, 3, 12)) == "raw/airlines/2026/03/raw_data_20260312.csv"
    )
    assert (
        mod.raw_s3_key("airport", date(2026, 3, 12)) == "raw/airports/2026/03/raw_data_20260312.csv"
    )


def test_processed_s3_key():
    assert (
        mod.processed_s3_key("lounge", date(2026, 3, 12))
        == "processed/lounges/2026/03/clean_data_20260312.csv"
    )


# ---------------------------------------------------------------------------
# upload_file
# ---------------------------------------------------------------------------


def test_upload_file_raises_when_missing(tmp_path):
    with pytest.raises(FileNotFoundError):
        mod.upload_file(
            local_path=tmp_path / "nonexistent.csv",
            s3_key="raw/airlines/2026/03/raw_data_20260312.csv",
            bucket="my-bucket",
        )


def test_upload_file_calls_boto3(tmp_path):
    csv = tmp_path / "raw_data_20260312.csv"
    csv.write_text("col\nval")

    mock_client = MagicMock()
    with patch("boto3.client", return_value=mock_client):
        uri = mod.upload_file(
            local_path=csv,
            s3_key="raw/airlines/2026/03/raw_data_20260312.csv",
            bucket="my-bucket",
        )

    mock_client.upload_file.assert_called_once_with(
        str(csv), "my-bucket", "raw/airlines/2026/03/raw_data_20260312.csv"
    )
    assert uri == "s3://my-bucket/raw/airlines/2026/03/raw_data_20260312.csv"


# ---------------------------------------------------------------------------
# upload_raw / upload_processed — STORAGE_MODE=local skips upload
# ---------------------------------------------------------------------------


def test_upload_raw_skips_when_local(monkeypatch):
    monkeypatch.setattr(mod, "STORAGE_MODE", "local")
    assert mod.upload_raw("airline", date(2026, 3, 12), bucket="my-bucket") is None


def test_upload_processed_skips_when_local(monkeypatch):
    monkeypatch.setattr(mod, "STORAGE_MODE", "local")
    assert mod.upload_processed("seat", date(2026, 3, 12), bucket="my-bucket") is None


# ---------------------------------------------------------------------------
# upload_raw / upload_processed — STORAGE_MODE=s3 uploads
# ---------------------------------------------------------------------------


def test_upload_raw_uploads_correct_file(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "STORAGE_MODE", "s3")
    monkeypatch.setattr(paths_mod, "LANDING_DIR", tmp_path)

    run_date = date(2026, 3, 12)
    local = mod.raw_local_path("airport", run_date)
    local.parent.mkdir(parents=True)
    local.write_text("col\nval")

    mock_client = MagicMock()
    with patch("boto3.client", return_value=mock_client):
        uri = mod.upload_raw("airport", run_date, bucket="my-bucket")

    assert uri == "s3://my-bucket/raw/airports/2026/03/raw_data_20260312.csv"
    mock_client.upload_file.assert_called_once()


def test_upload_processed_uploads_correct_file(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "STORAGE_MODE", "s3")
    monkeypatch.setattr(paths_mod, "LANDING_DIR", tmp_path)

    run_date = date(2026, 3, 12)
    local = mod.processed_local_path("lounge", run_date)
    local.parent.mkdir(parents=True)
    local.write_text("col\nval")

    mock_client = MagicMock()
    with patch("boto3.client", return_value=mock_client):
        uri = mod.upload_processed("lounge", run_date, bucket="my-bucket")

    assert uri == "s3://my-bucket/processed/lounges/2026/03/clean_data_20260312.csv"
    mock_client.upload_file.assert_called_once()


def test_upload_raw_raises_without_bucket(monkeypatch):
    monkeypatch.setattr(mod, "STORAGE_MODE", "s3")
    monkeypatch.setattr(mod, "S3_BUCKET", "")
    with pytest.raises(ValueError, match="S3_BUCKET"):
        mod.upload_raw("airline", date(2026, 3, 12))


# ---------------------------------------------------------------------------
# client reuse — a caller-supplied client is used instead of building a new one
# ---------------------------------------------------------------------------


def test_upload_file_reuses_supplied_client(tmp_path):
    csv = tmp_path / "raw_data_20260312.csv"
    csv.write_text("col\nval")
    mock_client = MagicMock()

    with patch("boto3.client") as mock_boto3_client:
        uri = mod.upload_file(
            local_path=csv,
            s3_key="raw/airlines/2026/03/raw_data_20260312.csv",
            bucket="my-bucket",
            client=mock_client,
        )

    mock_boto3_client.assert_not_called()
    mock_client.upload_file.assert_called_once()
    assert uri == "s3://my-bucket/raw/airlines/2026/03/raw_data_20260312.csv"


def test_upload_raw_passes_through_supplied_client(tmp_path, monkeypatch):
    monkeypatch.setattr(mod, "STORAGE_MODE", "s3")
    monkeypatch.setattr(paths_mod, "LANDING_DIR", tmp_path)

    run_date = date(2026, 3, 12)
    local = mod.raw_local_path("airline", run_date)
    local.parent.mkdir(parents=True)
    local.write_text("col\nval")

    mock_client = MagicMock()
    with patch("boto3.client") as mock_boto3_client:
        mod.upload_raw("airline", run_date, bucket="my-bucket", client=mock_client)

    mock_boto3_client.assert_not_called()
    mock_client.upload_file.assert_called_once()
