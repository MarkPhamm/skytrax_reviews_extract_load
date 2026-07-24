"""
DAG integrity: every file in dags/ imports cleanly and defines the expected DAGs.

Same guarantee as `astro dev parse` (.astro/test_dag_integrity_default.py), but
in plain pytest so it runs locally and in CI on every push — a DAG-level import
error (bad import, syntax error, missing dependency) fails the build instead of
surfacing in the Airflow UI after deploy. Airflow comes from the dev extra
(pinned to the Astro Runtime 3.0-x line), so no running Airflow is needed.

Run with:  pytest tests/dags/test_dag_integrity.py -v
"""

from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]

EXPECTED_DAG_IDS = {"skytrax_crawl", "skytrax_process", "skytrax_snowflake"}


@pytest.fixture(scope="module")
def dag_bag():
    from airflow.models.dagbag import DagBag

    return DagBag(dag_folder=str(PROJECT_ROOT / "dags"), include_examples=False)


def test_dags_import_without_errors(dag_bag):
    assert dag_bag.import_errors == {}, f"DAG import errors: {dag_bag.import_errors}"


def test_expected_dag_ids_present(dag_bag):
    missing = EXPECTED_DAG_IDS - set(dag_bag.dag_ids)
    assert not missing, f"Missing DAGs: {missing} (found: {sorted(dag_bag.dag_ids)})"


def test_every_dag_has_a_failure_callback(dag_bag):
    # C1 guarantee: no DAG ships without failure alerting wired in.
    for dag_id in EXPECTED_DAG_IDS:
        dag = dag_bag.dags[dag_id]
        assert dag.default_args.get(
            "on_failure_callback"
        ), f"{dag_id} has no on_failure_callback in default_args"
