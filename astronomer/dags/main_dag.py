from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import os
import sys
sys.path.append('/usr/local/airflow/include/tasks')
import snowflake_load

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 0
}


def upload_to_s3():
    # Path to the cleaned data file
    file_path = "/usr/local/airflow/include/data/clean_data.csv"
    s3_key = "uploads/cleaned_data.csv"
    bucket_name = "new-british-airline"

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"{file_path} not found")

    hook = S3Hook(aws_conn_id="aws_s3_connection")
    hook.load_file(
        filename=file_path,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True,
    )


def snowflake_copy_task():
    """Wrapper function to call the snowflake load script"""
    snowflake_load.main()


with DAG(
    dag_id="british_airways_ingestion_pipeline",
    schedule="0 5 * * 1",
    default_args=default_args,
    start_date=datetime(2025, 3, 27),
    catchup=False,
    max_active_runs=1,
) as dag:

    scrape_british_data = BashOperator(
        task_id="scrape_british_data",
        bash_command="chmod -R 777 /usr/local/airflow/include/data && python /usr/local/airflow/include/tasks/scraper_extract/scraper.py",
    )

    note = BashOperator(
        task_id="note", bash_command="echo 'Successfully extracted data to raw_data.csv'"
    )

    clean_data = BashOperator(
        task_id="clean_data",
        bash_command="python /usr/local/airflow/include/tasks/transform/transform.py",
    )

    note_clean_data = BashOperator(
        task_id="clean_data_to_upload_s3", bash_command="echo 'Cleaned Data'"
    )

    upload_cleaned_data_to_s3 = PythonOperator(
        task_id="upload_cleaned_data_to_s3",
        python_callable=upload_to_s3,
    )

    snowflake_copy_operator = PythonOperator(
        task_id="snowflake_copy_from_s3",
        python_callable=snowflake_copy_task,
    )
(
    scrape_british_data
    >> note
    >> clean_data
    >> note_clean_data
    >> upload_cleaned_data_to_s3
    >> snowflake_copy_operator
)
