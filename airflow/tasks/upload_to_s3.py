import os
import pathlib
import sys

import boto3
from boto3.s3.transfer import S3Transfer
from botocore.exceptions import ClientError, NoCredentialsError
from dotenv import dotenv_values

# Load config
script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")

# Set config variables
AWS_BUCKET = config.get("bucket_name")
access_key = config.get("aws_access_key_id")
secret_key = config.get("aws_secret_access_key")

file_path = "/opt/airflow/data/clean_data.csv"
s3_filename = "clean_data.csv"


def connect_s3():
    """
    Create a boto3 session and connect to the S3 Resource

    Returns:
        connection to the S3 bucket
    """
    if not access_key or not secret_key:
        raise ValueError("AWS credentials are missing in the configuration file.")

    try:
        client = boto3.client(
            "s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key
        )
        return client
    except NoCredentialsError as e:
        print("AWS Credentials not found.", file=sys.stderr)
        raise e
    except ClientError as e:
        print("Failed to connect to S3:", e, file=sys.stderr)
        raise e


def upload_csv_s3(local_file_path=file_path, s3_file_name=s3_filename):
    """
    Upload a CSV file to the S3 bucket
    """
    try:
        client = connect_s3()
        transfer = S3Transfer(client)
        transfer.upload_file(local_file_path, AWS_BUCKET, s3_file_name)
        print(f"Successfully uploaded {local_file_path} to {AWS_BUCKET}/{s3_file_name}")
    except FileNotFoundError:
        print(f"Error: The file {local_file_path} was not found.", file=sys.stderr)
    except ClientError as e:
        print("AWS upload error:", e, file=sys.stderr)
        raise e


if __name__ == "__main__":
    upload_csv_s3()
