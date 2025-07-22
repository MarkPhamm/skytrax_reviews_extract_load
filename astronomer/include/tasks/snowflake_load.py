from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
csv_path = "/usr/local/airflow/include/data/clean_data.csv"
table_name = "BRITISH_AIRWAYS_DB.RAW.REVIEW"
s3_bucket = "new-british-airline"
s3_key = "clean_data.csv"
stage_name = "MY_S3_STAGE"

def map_dtype(dtype):
    """Map pandas dtypes to Snowflake data types"""
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

def create_snowflake_infrastructure():
    """Create database, schemas, and stage in Snowflake"""
    try:
        # Connect to Snowflake using Airflow connection
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # Step 1: Create database and schemas
        create_commands = [
            "CREATE DATABASE IF NOT EXISTS BRITISH_AIRWAYS_DB;",
            "CREATE SCHEMA IF NOT EXISTS BRITISH_AIRWAYS_DB.RAW;",
            "CREATE SCHEMA IF NOT EXISTS BRITISH_AIRWAYS_DB.MODEL;"
        ]
        
        for command in create_commands:
            logger.info(f"Executing: {command}")
            snowflake_hook.run(command)
        
        # Step 2: Get AWS credentials from Airflow connection
        s3_hook = S3Hook(aws_conn_id='aws_s3_connection')
        credentials = s3_hook.get_credentials()
        
        # Step 3: Create or replace S3 stage with Airflow credentials
        create_stage_sql = f"""
        CREATE OR REPLACE STAGE BRITISH_AIRWAYS_DB.RAW.{stage_name}
        URL = 's3://new-british-airline/'
        CREDENTIALS = (
            AWS_KEY_ID = '{credentials.access_key}'
            AWS_SECRET_KEY = '{credentials.secret_key}'
        )
        FILE_FORMAT = (
            TYPE = 'CSV' 
            FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
            SKIP_HEADER = 1
            FIELD_DELIMITER = ','
            ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
        );
        """
        
        logger.info(f"Creating stage: {stage_name}")
        snowflake_hook.run(create_stage_sql)
        
        # Step 4: Read CSV to determine table structure
        df = pd.read_csv(csv_path)
        
        # Step 5: Generate CREATE TABLE statement
        columns = ",\n    ".join([
            f"{col} {map_dtype(dtype)}" for col, dtype in df.dtypes.items()
        ])
        create_table_sql = f"CREATE OR REPLACE TABLE {table_name} (\n    {columns}\n);"
        
        logger.info(f"Creating table: {table_name}")
        snowflake_hook.run(create_table_sql)
        
        return True
        
    except Exception as e:
        logger.error(f"Error creating Snowflake infrastructure: {e}")
        raise

def load_data_from_stage():
    """Load data from S3 stage to Snowflake table"""
    try:
        # Connect to Snowflake
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # COPY command from stage to table
        copy_sql = f"""
        COPY INTO {table_name}
        FROM @BRITISH_AIRWAYS_DB.RAW.{stage_name}/{s3_key}
        ON_ERROR = 'CONTINUE';
        """
        
        logger.info("Loading data from S3 stage to Snowflake table...")
        result = snowflake_hook.run(copy_sql)
        logger.info(f"Data load completed: {result}")
        
        # Simple verification - just count records instead of using COPY_HISTORY
        logger.info("Verifying data load...")
        count_sql = f"SELECT COUNT(*) as record_count FROM {table_name};"
        count_result = snowflake_hook.run(count_sql)
        logger.info(f"✅ Data verification successful! Records in table: {count_result}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error loading data from stage: {e}")
        raise

def verify_data_load():
    """Verify the data was loaded successfully"""
    try:
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default')
        
        # Count records in the table
        count_sql = f"SELECT COUNT(*) as record_count FROM {table_name};"
        result = snowflake_hook.run(count_sql)
        
        logger.info(f"Data verification - Total records loaded: {result}")
        print(f"✅ Data verification - Total records loaded: {result}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error verifying data load: {e}")
        raise

def main():
    """Main execution function"""
    logger.info("Starting Snowflake data load process...")
    
    # Step 1: Create infrastructure (database, schemas, stage, table)
    create_snowflake_infrastructure()
    
    # Step 2: Load data from stage
    load_data_from_stage()
    
    # Step 3: Verify data load
    verify_data_load()
    
    logger.info("Snowflake data load process completed successfully!")

if __name__ == "__main__":
    main()
