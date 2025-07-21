from snowflake import connector
import pathlib
from dotenv import dotenv_values
import pandas as pd
from sqlalchemy import create_engine

# Load config
script_path = pathlib.Path(__file__).parent.resolve()
config = dotenv_values(f"{script_path}/configuration.env")


engine = create_engine(
    f'snowflake://{config.get("snowflake_user")}:{config.get("snowflake_password")}@{config.get("snowflake_account")}/british_db/RAW'
)

# Đọc data với pandas
query = "SELECT * FROM BRITISH_DATA;"
df = pd.read_sql(query, engine)

print(df.head())