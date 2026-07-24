"""
Failure alerting for all Skytrax DAGs.

``notify_failure`` is wired into every DAG's default_args as
``on_failure_callback``. It always logs an ERROR with the failed task's
context; if the SLACK_WEBHOOK_URL env var is set it also POSTs a short
message to that webhook. No webhook configured = log-only, so local dev and
CI never need Slack. The callback never raises — an alerting failure must
not mask the task failure it is reporting.

Uses only the stdlib (urllib) so it stays importable and unit-testable
without Airflow or any HTTP client dependency.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.request

logger = logging.getLogger(__name__)

SLACK_TIMEOUT_SECONDS = 10


def _format_failure_message(context: dict) -> str:
    """Build a one-alert summary from an Airflow task-failure context.

    Reads defensively with .get/getattr: Airflow's context shape varies by
    version, and the callback must never itself fail on a missing key.
    """
    ti = context.get("task_instance") or context.get("ti")
    dag_id = getattr(ti, "dag_id", "unknown_dag")
    task_id = getattr(ti, "task_id", "unknown_task")
    try_number = getattr(ti, "try_number", "?")
    log_url = getattr(ti, "log_url", None)
    run_id = context.get("run_id", "unknown_run")
    exception = context.get("exception")

    lines = [
        f"Airflow task failed: {dag_id}.{task_id}",
        f"run_id={run_id} try={try_number}",
    ]
    if exception is not None:
        lines.append(f"exception: {exception!r}")
    if log_url:
        lines.append(f"log: {log_url}")
    return "\n".join(lines)


def _post_to_slack(webhook_url: str, message: str) -> None:
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps({"text": message}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
    )
    urllib.request.urlopen(request, timeout=SLACK_TIMEOUT_SECONDS)


def notify_failure(context: dict) -> None:
    """on_failure_callback: log ERROR always, POST to Slack when configured."""
    message = _format_failure_message(context)
    logger.error("%s", message)

    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        logger.debug("SLACK_WEBHOOK_URL not set — failure logged, no Slack alert sent")
        return

    try:
        _post_to_slack(webhook_url, message)
    except Exception:  # noqa: BLE001 — alerting must never re-fail the task
        logger.exception("Failed to send Slack failure alert")
