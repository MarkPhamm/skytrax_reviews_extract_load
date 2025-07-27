import logging
import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
AWS_BUCKET = "new-british-airline"  # Replace with your actual bucket name
file_path = "/usr/local/airflow/include/data/clean_data.csv"
s3_filename = "clean_data.csv"


def upload_csv_s3(local_file_path=file_path, s3_file_name=s3_filename):
    """
    Upload a CSV file to S3 using Airflow S3Hook
    """
    try:
        # Check if file exists
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"File not found: {local_file_path}")

        logger.info(f"Starting upload of {local_file_path} to S3...")

        # Use Airflow S3Hook with connection
        s3_hook = S3Hook(aws_conn_id="aws_s3_connection")

        # Upload file
        s3_hook.load_file(
            filename=local_file_path,
            key=s3_file_name,
            bucket_name=AWS_BUCKET,
            replace=True,
        )

        logger.info(
            f"✅ Successfully uploaded {local_file_path} to s3://{AWS_BUCKET}/{s3_file_name}"
        )
        print(
            f"✅ Successfully uploaded {local_file_path} to s3://{AWS_BUCKET}/{s3_file_name}"
        )

    except FileNotFoundError as e:
        logger.error(f"❌ File not found: {e}")
        print(f"❌ File not found: {e}")
        raise
    except Exception as e:
        logger.error(f"❌ Upload error: {e}")
        print(f"❌ Upload error: {e}")
        raise


if __name__ == "__main__":
    upload_csv_s3()
