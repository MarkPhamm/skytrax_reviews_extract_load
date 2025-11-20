from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

logger = logging.getLogger(__name__)


def test_snowflake_create_privileges():
    """
    Check that the current Snowflake role (from snowflake_default)
    can:
      - CREATE DATABASE SKYTRAX_REVIEWS_DB (if missing)
      - CREATE SCHEMA SKYTRAX_REVIEWS_DB.RAW (if missing)
      - CREATE and DROP a table in SKYTRAX_REVIEWS_DB.RAW
    """
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

    logger.info("🔍 Testing Snowflake create privileges on SKYTRAX_REVIEWS_DB.RAW ...")

    statements = [
        # Can this role create the DB if it doesn't exist?
        "CREATE DATABASE IF NOT EXISTS SKYTRAX_REVIEWS_DB",
        # Can it create the RAW schema?
        "CREATE SCHEMA IF NOT EXISTS SKYTRAX_REVIEWS_DB.RAW",
        # Can it create a table in that schema?
        "CREATE OR REPLACE TABLE SKYTRAX_REVIEWS_DB.RAW._AIRFLOW_PRIV_TEST (id INTEGER)",
        # Can it drop that table?
        "DROP TABLE IF EXISTS SKYTRAX_REVIEWS_DB.RAW._AIRFLOW_PRIV_TEST",
    ]

    for stmt in statements:
        logger.info(f"Executing SQL: {stmt}")
        hook.run(stmt)

    logger.info("✅ Role CAN create DB/schema/table in SKYTRAX_REVIEWS_DB.RAW")


with DAG(
    dag_id="test_snowflake_privileges",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["snowflake", "test", "permissions"],
) as dag:

    check_privileges = PythonOperator(
        task_id="check_snowflake_create_privileges",
        python_callable=test_snowflake_create_privileges,
    )
