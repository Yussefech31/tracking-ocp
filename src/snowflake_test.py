import os

from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

print("ACCOUNT:", os.getenv("SNOWFLAKE_ACCOUNT"))
print("USER:", os.getenv("SNOWFLAKE_USER"))
print("WAREHOUSE:", os.getenv("SNOWFLAKE_WAREHOUSE"))
print("DATABASE:", os.getenv("SNOWFLAKE_DATABASE"))
print("SCHEMA:", os.getenv("SNOWFLAKE_SCHEMA"))

connection = snowflake.connector.connect(
    account=os.getenv("SNOWFLAKE_ACCOUNT"),
    user=os.getenv("SNOWFLAKE_USER"),
    password=os.getenv("SNOWFLAKE_PASSWORD"),
)

cursor = connection.cursor()

cursor.execute("USE WAREHOUSE OCP_TRANSPORTS_WH")
cursor.execute("USE DATABASE OCP_TRANSPORTS")
cursor.execute("USE SCHEMA RAW")

cursor.execute("""
SELECT
    CURRENT_USER(),
    CURRENT_WAREHOUSE(),
    CURRENT_DATABASE(),
    CURRENT_SCHEMA()
""")

print(cursor.fetchone())

cursor.close()
connection.close()