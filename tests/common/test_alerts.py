"""
Unit tests for include/tasks/common/alerts.py (DAG failure callback).

Run with:  pytest tests/common/test_alerts.py -v
"""

import logging
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import include.tasks.common.alerts as mod


def _context(**overrides):
    """Minimal Airflow-shaped failure context (no Airflow import needed)."""
    ti = SimpleNamespace(
        dag_id="skytrax_crawl",
        task_id="scrape_type",
        try_number=2,
        log_url="http://localhost:8080/log",
    )
    context = {
        "task_instance": ti,
        "run_id": "manual__2026-07-24",
        "exception": RuntimeError("boom"),
    }
    context.update(overrides)
    return context


def test_message_includes_dag_task_run_and_exception():
    message = mod._format_failure_message(_context())
    assert "skytrax_crawl.scrape_type" in message
    assert "manual__2026-07-24" in message
    assert "try=2" in message
    assert "boom" in message
    assert "http://localhost:8080/log" in message


def test_message_survives_empty_context():
    # The callback must never itself raise, even on a malformed context.
    message = mod._format_failure_message({})
    assert "unknown_dag.unknown_task" in message


def test_no_webhook_logs_error_and_skips_slack(monkeypatch, caplog):
    monkeypatch.delenv("SLACK_WEBHOOK_URL", raising=False)
    with patch.object(mod.urllib.request, "urlopen") as mock_urlopen:
        with caplog.at_level(logging.ERROR, logger=mod.__name__):
            mod.notify_failure(_context())

    mock_urlopen.assert_not_called()
    assert any("skytrax_crawl.scrape_type" in r.message for r in caplog.records)


def test_webhook_posts_json_payload(monkeypatch):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.example/services/T/B/X")
    with patch.object(mod.urllib.request, "urlopen") as mock_urlopen:
        mod.notify_failure(_context())

    mock_urlopen.assert_called_once()
    request = mock_urlopen.call_args.args[0]
    assert request.full_url == "https://hooks.slack.example/services/T/B/X"
    assert b"skytrax_crawl.scrape_type" in request.data
    assert request.get_header("Content-type") == "application/json"


def test_webhook_failure_never_raises(monkeypatch, caplog):
    monkeypatch.setenv("SLACK_WEBHOOK_URL", "https://hooks.slack.example/services/T/B/X")
    with patch.object(mod.urllib.request, "urlopen", side_effect=OSError("network down")):
        with caplog.at_level(logging.ERROR, logger=mod.__name__):
            mod.notify_failure(_context())  # must not raise

    assert any("Failed to send Slack failure alert" in r.message for r in caplog.records)


def test_dags_wire_notify_failure_into_default_args():
    # All three DAG modules must register the callback in default_args.
    # Skipped without Airflow, since the DAG files import airflow at module
    # level (CI installs airflow via the dev extra, so this runs there).
    pytest.importorskip("airflow", reason="DAG files need apache-airflow importable")
    from dags import dag_crawl, dag_process, dag_snowflake

    for dag_module in (dag_crawl, dag_process, dag_snowflake):
        assert dag_module.default_args["on_failure_callback"] is mod.notify_failure
