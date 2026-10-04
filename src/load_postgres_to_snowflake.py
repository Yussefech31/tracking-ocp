import os

import pandas as pd
import psycopg2
import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

TABLES = {
    "vehicle": """
        VEHICLE_ID VARCHAR(10),
        VEHICLE_TYPE VARCHAR(30),
        MANUFACTURER VARCHAR(50),
        MODEL VARCHAR(50),
        MATRICULATION VARCHAR(50),
        CAPACITY_TONS NUMBER(6,2),
        FUEL_TYPE VARCHAR(20),
        YEAR NUMBER(4,0),
        STATUS VARCHAR(20)
    """,
    "driver": """
        DRIVER_ID VARCHAR(10),
        FIRST_NAME VARCHAR(50),
        LAST_NAME VARCHAR(50),
        LICENSE_TYPE VARCHAR(30),
        EXPERIENCE_YEARS NUMBER,
        DEPARTMENT VARCHAR(50),
        STATUS VARCHAR(20)
    """,
    "route": """
        ROUTE_ID VARCHAR(10),
        ORIGIN VARCHAR(100),
        DESTINATION VARCHAR(100),
        DISTANCE_KM NUMBER(8,2),
        ROUTE_TYPE VARCHAR(30)
    """,
    "trip": """
        TRIP_ID VARCHAR(15),
        VEHICLE_ID VARCHAR(10),
        DRIVER_ID VARCHAR(10),
        ROUTE_ID VARCHAR(10),
        DEPARTURE_TIME TIMESTAMP,
        ARRIVAL_TIME TIMESTAMP,
        DISTANCE_KM NUMBER(8,2),
        CARGO_WEIGHT_TONS NUMBER(8,2),
        TRIP_STATUS VARCHAR(20)
    """,
    "gps_event": """
        EVENT_ID VARCHAR(30),
        VEHICLE_ID VARCHAR(10),
        TIMESTAMP TIMESTAMP,
        LATITUDE NUMBER(9,6),
        LONGITUDE NUMBER(9,6),
        SPEED_KMH NUMBER(6,2),
        FUEL_LEVEL NUMBER(5,2),
        ENGINE_TEMPERATURE NUMBER(6,2)
    """,
    "fuel_transaction": """
        FUEL_TRANSACTION_ID VARCHAR(20),
        VEHICLE_ID VARCHAR(10),
        TIMESTAMP TIMESTAMP,
        LITERS NUMBER(8,2),
        PRICE_PER_LITER NUMBER(6,3),
        STATION VARCHAR(100)
    """,
    "maintenance": """
        MAINTENANCE_ID VARCHAR(20),
        VEHICLE_ID VARCHAR(10),
        MAINTENANCE_DATE TIMESTAMP,
        MAINTENANCE_TYPE VARCHAR(50),
        DESCRIPTION VARCHAR,
        COST NUMBER(10,2),
        DOWNTIME_HOURS NUMBER(8,2)
    """,
    "incident": """
        INCIDENT_ID VARCHAR(20),
        VEHICLE_ID VARCHAR(10),
        TIMESTAMP TIMESTAMP,
        INCIDENT_TYPE VARCHAR(50),
        SEVERITY VARCHAR(20),
        DESCRIPTION VARCHAR
    """
}

postgres_connection = None
snowflake_connection = None
postgres_cursor = None
snowflake_cursor = None

try:
    postgres_connection = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST"),
        port=os.getenv("POSTGRES_PORT"),
        dbname=os.getenv("POSTGRES_DB"),
        user=os.getenv("POSTGRES_USER"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )

    snowflake_connection = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
    )

    postgres_cursor = postgres_connection.cursor()
    snowflake_cursor = snowflake_connection.cursor()

    snowflake_cursor.execute(
        f"USE WAREHOUSE {os.getenv('SNOWFLAKE_WAREHOUSE')}"
    )

    snowflake_cursor.execute(
        f"USE DATABASE {os.getenv('SNOWFLAKE_DATABASE')}"
    )

    snowflake_cursor.execute(
        f"USE SCHEMA {os.getenv('SNOWFLAKE_SCHEMA')}"
    )

    for table_name, table_definition in TABLES.items():

        print(f"\nProcessing {table_name}...")

        query = f"""
            SELECT *
            FROM transports.{table_name}
        """

        df = pd.read_sql_query(
            query,
            postgres_connection
        )

        df.columns = [column.upper() for column in df.columns]

        print(f"PostgreSQL rows: {len(df)}")

        snowflake_cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name.upper()} (
                {table_definition}
            )
            """
        )

        snowflake_cursor.execute(
            f"TRUNCATE TABLE {table_name.upper()}"
        )

        success, nchunks, nrows, output = write_pandas(
            conn=snowflake_connection,
            df=df,
            table_name=table_name.upper(),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            auto_create_table=False,
            overwrite=False,
            quote_identifiers=False,
        )

        if not success:
            raise RuntimeError(
                f"Failed to load table {table_name}"
            )

        snowflake_cursor.execute(
            f"SELECT COUNT(*) FROM {table_name.upper()}"
        )

        snowflake_count = snowflake_cursor.fetchone()[0]

        print(f"Snowflake rows: {snowflake_count}")
        print(f"Loaded rows: {nrows}")
        print(f"Success: {success}")

    snowflake_connection.commit()

    print("\nAll tables loaded successfully.")

finally:
    if postgres_cursor:
        postgres_cursor.close()

    if postgres_connection:
        postgres_connection.close()

    if snowflake_cursor:
        snowflake_cursor.close()

    if snowflake_connection:
        snowflake_connection.close()