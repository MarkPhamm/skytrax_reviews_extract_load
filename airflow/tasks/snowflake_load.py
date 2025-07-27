from snowflake import connector
import pathlib
from dotenv import dotenv_values
import pandas as pd

# Load config
script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")

conn = connector.connect(
    user=config.get("snowflake_user"),
    password=config.get("snowflake_password"),
    account=config.get("snowflake_account"),
)

access_key = config.get("aws_access_key_id")
secret_key = config.get("aws_secret_access_key")

cur = conn.cursor()

# Tạo database/schema
cur.execute("CREATE DATABASE IF NOT EXISTS BRITISH_AIRWAYS_DB;")
cur.execute("CREATE SCHEMA IF NOT EXISTS BRITISH_AIRWAYS_DB.RAW;")
cur.execute("CREATE SCHEMA IF NOT EXISTS BRITISH_AIRWAYS_DB.MODEL;")

# Tạo bảng (bạn có thể thêm phần CREATE TABLE nếu chưa tạo)
csv_path = "/opt/airflow/data/clean_data.csv"
df = pd.read_csv(csv_path)


def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INTEGER"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "TIMESTAMP"
    else:
        return "STRING"


# Tạo câu CREATE TABLE từ dataframe
table_name = "BRITISH_AIRWAYS_DB.RAW.REVIEW"
columns = ",\n    ".join(
    [f"{col} {map_dtype(dtype)}" for col, dtype in df.dtypes.items()]
)
create_table_sql = f"CREATE OR REPLACE TABLE {table_name} (\n    {columns}\n);"
print(create_table_sql)
cur.execute(create_table_sql)

# Load dữ liệu từ S3
cur.execute(
    f"""
COPY INTO BRITISH_AIRWAYS_DB.RAW.REVIEW
FROM 's3://datalakechat/clean_data.csv'
CREDENTIALS = (
    AWS_KEY_ID='{access_key}',
    AWS_SECRET_KEY='{secret_key}'
)
FILE_FORMAT = (
    TYPE=CSV,
    FIELD_DELIMITER=',',
    SKIP_HEADER=1,
    FIELD_OPTIONALLY_ENCLOSED_BY='"',
    ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE
)
ON_ERROR = 'CONTINUE';
"""
)
