"""
Shared test configuration.

Importing airflow (the DAG-integrity tests, and any test that imports a
dags/ module) creates $AIRFLOW_HOME — config file, logs — as a side effect.
Point it at a throwaway directory before any test module is imported, so
test runs never scribble on a developer's real ~/airflow.
"""

import os
import tempfile

os.environ.setdefault("AIRFLOW_HOME", tempfile.mkdtemp(prefix="skytrax-test-airflow-home-"))
