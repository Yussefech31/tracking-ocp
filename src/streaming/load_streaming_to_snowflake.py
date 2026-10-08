import argparse
import os
import sys
import uuid

import pandas as pd
import pyarrow.dataset as ds
import snowflake.connector
from pyarrow import fs
from snowflake.connector.pandas_tools import write_pandas

DATASETS = [
    {
        "table": "STREAM_TELEMETRY",
        "prefix": "streaming/telemetry",
        "keys": ["VEHICLE_ID", "EVENT_TIME"],
        "order_by": None,
        "rename": {"EVENT_TIME": "EVENT_TIME"},
        "columns": [
            ("VEHICLE_ID", "VARCHAR(10)"),
            ("EVENT_TIME", "TIMESTAMP_NTZ"),
            ("LATITUDE", "FLOAT"),
            ("LONGITUDE", "FLOAT"),
            ("SPEED_KMH", "FLOAT"),
            ("FUEL_LEVEL", "FLOAT"),
            ("ENGINE_TEMPERATURE", "FLOAT"),
            ("IS_OVERSPEED", "BOOLEAN"),
            ("IS_OVERHEATING", "BOOLEAN"),
            ("IS_LOW_FUEL", "BOOLEAN"),
            ("IS_STOPPED", "BOOLEAN"),
            ("OPERATIONAL_STATUS", "VARCHAR(20)"),
        ],
    },
    {
        "table": "STREAM_ALERTS",
        "prefix": "streaming/alerts",
        "keys": ["ALERT_ID"],
        "order_by": None,
        "rename": {"TIMESTAMP": "ALERT_TIME"},
        "columns": [
            ("ALERT_ID", "VARCHAR(100)"),
            ("VEHICLE_ID", "VARCHAR(10)"),
            ("ALERT_TIME", "TIMESTAMP_NTZ"),
            ("ALERT_TYPE", "VARCHAR(40)"),
            ("SEVERITY", "VARCHAR(20)"),
            ("METRIC_VALUE", "FLOAT"),
            ("THRESHOLD", "FLOAT"),
            ("LATITUDE", "FLOAT"),
            ("LONGITUDE", "FLOAT"),
            ("DESCRIPTION", "VARCHAR"),
        ],
    },
    {
        "table": "STREAM_FLEET_KPIS",
        "prefix": "streaming/kpis/fleet",
        "keys": ["WINDOW_START"],
        "order_by": "TOTAL_EVENTS",
        "rename": {},
        "columns": [
            ("WINDOW_START", "TIMESTAMP_NTZ"),
            ("WINDOW_END", "TIMESTAMP_NTZ"),
            ("ACTIVE_VEHICLES", "NUMBER"),
            ("AVG_SPEED_KMH", "FLOAT"),
            ("MAX_SPEED_KMH", "FLOAT"),
            ("AVG_ENGINE_TEMPERATURE", "FLOAT"),
            ("MAX_ENGINE_TEMPERATURE", "FLOAT"),
            ("AVG_FUEL_LEVEL", "FLOAT"),
            ("OVERSPEED_EVENTS", "NUMBER"),
            ("TEMPERATURE_ANOMALIES", "NUMBER"),
            ("LOW_FUEL_EVENTS", "NUMBER"),
            ("STOPPED_VEHICLES", "NUMBER"),
            ("TOTAL_EVENTS", "NUMBER"),
        ],
    },
    {
        "table": "STREAM_VEHICLE_KPIS",
        "prefix": "streaming/kpis/vehicle",
        "keys": ["WINDOW_START", "VEHICLE_ID"],
        "order_by": "TOTAL_EVENTS",
        "rename": {},
        "columns": [
            ("WINDOW_START", "TIMESTAMP_NTZ"),
            ("WINDOW_END", "TIMESTAMP_NTZ"),
            ("VEHICLE_ID", "VARCHAR(10)"),
            ("AVG_SPEED_KMH", "FLOAT"),
            ("MAX_SPEED_KMH", "FLOAT"),
            ("AVG_ENGINE_TEMPERATURE", "FLOAT"),
            ("MAX_ENGINE_TEMPERATURE", "FLOAT"),
            ("AVG_FUEL_LEVEL", "FLOAT"),
            ("OVERSPEED_EVENTS", "NUMBER"),
            ("TEMPERATURE_ANOMALIES", "NUMBER"),
            ("LOW_FUEL_EVENTS", "NUMBER"),
            ("STOPPED_EVENTS", "NUMBER"),
            ("TOTAL_EVENTS", "NUMBER"),
        ],
    },
]


def minio_filesystem():
    endpoint = os.getenv("MINIO_ENDPOINT", "http://ocp-minio:9000")
    scheme = "https" if endpoint.startswith("https") else "http"
    return fs.S3FileSystem(
        access_key=os.getenv("MINIO_ACCESS_KEY", "minio_admin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minio_password123"),
        endpoint_override=endpoint.split("://", 1)[-1],
        scheme=scheme,
        region="us-east-1",
    )


def snowflake_connection(schema):
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "OCP_TRANSPORTS_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "OCP_TRANSPORTS"),
        schema=schema,
    )


def read_minio_dataset(filesystem, bucket, prefix):
    path = f"{bucket}/{prefix}"
    info = filesystem.get_file_info(path)
    if info.type != fs.FileType.Directory:
        return pd.DataFrame()
    dataset = ds.dataset(
        path,
        filesystem=filesystem,
        format="parquet",
        ignore_prefixes=[".", "_"],
    )
    return dataset.to_table().to_pandas()


def normalize_frame(df, spec):
    df = df.copy()
    df.columns = [c.upper() for c in df.columns]
    if "EVENT_TIME" in df.columns and spec["table"] == "STREAM_TELEMETRY":
        df = df.drop(columns=["TIMESTAMP"], errors="ignore")
    df = df.rename(columns=spec["rename"])
    for name, sf_type in spec["columns"]:
        if name not in df.columns:
            df[name] = None
        if sf_type == "TIMESTAMP_NTZ":
            series = pd.to_datetime(df[name], utc=True, errors="coerce")
            df[name] = series.dt.tz_localize(None).dt.strftime("%Y-%m-%d %H:%M:%S.%f")
    df = df[[name for name, _ in spec["columns"]]]
    df = df.dropna(subset=spec["keys"])
    if spec["order_by"]:
        df = df.sort_values(spec["order_by"])
    return df.drop_duplicates(subset=spec["keys"], keep="last").reset_index(drop=True)


def ensure_target_table(cursor, schema, spec):
    column_ddl = ",\n    ".join(f"{n} {t}" for n, t in spec["columns"])
    cursor.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}.{spec['table']} (\n"
        f"    {column_ddl},\n    LOADED_AT TIMESTAMP_NTZ\n)"
    )


def build_merge_sql(schema, landing, spec):
    names = [n for n, _ in spec["columns"]]
    casts = ",\n        ".join(
        f"TRY_TO_TIMESTAMP_NTZ({n}::VARCHAR) AS {n}" if t == "TIMESTAMP_NTZ" else f"{n}::{t} AS {n}"
        for n, t in spec["columns"]
    )
    on_clause = " AND ".join(f"t.{k} = s.{k}" for k in spec["keys"])
    updates = ",\n        ".join(f"t.{n} = s.{n}" for n in names if n not in spec["keys"])
    insert_cols = ", ".join(names + ["LOADED_AT"])
    insert_vals = ", ".join([f"s.{n}" for n in names] + ["CURRENT_TIMESTAMP()"])
    return (
        f"MERGE INTO {schema}.{spec['table']} t\n"
        f"USING (\n    SELECT\n        {casts}\n    FROM {schema}.{landing}\n) s\n"
        f"ON {on_clause}\n"
        f"WHEN MATCHED THEN UPDATE SET\n        {updates},\n        t.LOADED_AT = CURRENT_TIMESTAMP()\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols})\n    VALUES ({insert_vals})"
    )


def load_dataset(conn, filesystem, bucket, schema, spec):
    raw_df = read_minio_dataset(filesystem, bucket, spec["prefix"])
    cursor = conn.cursor()
    try:
        ensure_target_table(cursor, schema, spec)
        if raw_df.empty:
            print(f"{spec['table']}: no files under s3://{bucket}/{spec['prefix']}, skipped")
            return 0, 0
        df = normalize_frame(raw_df, spec)
        landing = f"{spec['table']}_LANDING_{uuid.uuid4().hex[:8].upper()}"
        success, _, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=landing,
            schema=schema,
            auto_create_table=True,
            table_type="temporary",
            quote_identifiers=False,
        )
        if not success:
            raise RuntimeError(f"write_pandas failed for {spec['table']}")
        cursor.execute(build_merge_sql(schema, landing, spec))
        inserted, updated = cursor.fetchone()[:2]
        cursor.execute(f"DROP TABLE IF EXISTS {schema}.{landing}")
        cursor.execute(f"SELECT COUNT(*) FROM {schema}.{spec['table']}")
        total = cursor.fetchone()[0]
        print(
            f"{spec['table']}: minio_rows={len(raw_df)} deduped={nrows} "
            f"inserted={inserted} updated={updated} snowflake_total={total}"
        )
        return inserted, total
    finally:
        cursor.close()


def run(bucket, schema, only):
    filesystem = minio_filesystem()
    conn = snowflake_connection(schema)
    try:
        totals = {}
        for spec in DATASETS:
            if only and spec["table"] not in only:
                continue
            _, total = load_dataset(conn, filesystem, bucket, schema, spec)
            totals[spec["table"]] = total
        conn.commit()
        return totals
    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default=os.getenv("MINIO_BUCKET", "ocp-data"))
    parser.add_argument("--schema", default=os.getenv("SNOWFLAKE_STREAMING_SCHEMA", "RAW"))
    parser.add_argument("--only", nargs="*", default=None)
    args = parser.parse_args()
    results = run(args.bucket, args.schema, set(args.only) if args.only else None)
    if not any(results.values()):
        print("No streaming data persisted to Snowflake.")
        sys.exit(1)
    print("Streaming persistence completed.")
