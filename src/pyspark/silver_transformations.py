import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    trim,
    upper,
    when,
    round,
    unix_timestamp,
    lit,
)

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]
JDBC_DRIVER = BASE_DIR / "drivers" / "postgresql-42.7.12.jar"

spark = (
    SparkSession.builder
    .appName("OCPTransportSilverTransformation")
    .master("local[*]")
    .config("spark.jars", JDBC_DRIVER.as_uri())
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minio_admin")
    .config("spark.hadoop.fs.s3a.secret.key", "minio_password123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
    .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "1")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

BRONZE_PATH = "s3a://ocp-data/bronze"
SILVER_PATH = "s3a://ocp-data/silver"
QUARANTINE_PATH = "s3a://ocp-data/quarantine"


def read_bronze(table):
    return spark.read.parquet(f"{BRONZE_PATH}/{table}")


def write_silver(df, table):
    df.write.mode("overwrite").parquet(f"{SILVER_PATH}/{table}")


def write_quarantine(df, table):
    df.write.mode("overwrite").parquet(f"{QUARANTINE_PATH}/{table}")


def transform_vehicle():
    df = read_bronze("vehicle")

    df = (
        df
        .withColumn("vehicle_id", trim(col("vehicle_id")))
        .withColumn("vehicle_type", trim(col("vehicle_type")))
        .withColumn("manufacturer", trim(col("manufacturer")))
        .withColumn("model", trim(col("model")))
        .withColumn("matriculation", upper(trim(col("matriculation"))))
        .withColumn("fuel_type", trim(col("fuel_type")))
        .withColumn("status", trim(col("status")))
        .dropDuplicates(["vehicle_id"])
    )

    valid_condition = (
        col("vehicle_id").isNotNull()
        & (col("vehicle_id") != "")
        & col("matriculation").isNotNull()
        & (col("matriculation") != "")
        & col("capacity_tons").isNotNull()
        & (col("capacity_tons") > 0)
        & col("year").isNotNull()
        & (col("year") >= 2000)
        & col("fuel_type").isin("Diesel", "Electric", "Hybrid")
        & col("status").isin("Active", "Maintenance", "Inactive")
    )

    valid = df.filter(valid_condition)
    invalid = df.filter(~valid_condition)

    write_silver(valid, "vehicle")
    write_quarantine(invalid, "vehicle")

    print(f"vehicle: {valid.count()} valid, {invalid.count()} quarantine")


def transform_driver():
    df = read_bronze("driver")

    df = (
        df
        .withColumn("driver_id", trim(col("driver_id")))
        .withColumn("first_name", trim(col("first_name")))
        .withColumn("last_name", trim(col("last_name")))
        .withColumn("license_type", trim(col("license_type")))
        .withColumn("department", trim(col("department")))
        .withColumn("status", trim(col("status")))
        .dropDuplicates(["driver_id"])
    )

    valid_condition = (
        col("driver_id").isNotNull()
        & (col("driver_id") != "")
        & col("first_name").isNotNull()
        & col("last_name").isNotNull()
        & col("experience_years").isNotNull()
        & (col("experience_years") >= 0)
        & col("status").isin("Active", "Inactive", "Suspended")
    )

    valid = df.filter(valid_condition)
    invalid = df.filter(~valid_condition)

    write_silver(valid, "driver")
    write_quarantine(invalid, "driver")

    print(f"driver: {valid.count()} valid, {invalid.count()} quarantine")


def transform_route():
    df = read_bronze("route")

    df = (
        df
        .withColumn("route_id", trim(col("route_id")))
        .withColumn("origin", trim(col("origin")))
        .withColumn("destination", trim(col("destination")))
        .withColumn("route_type", trim(col("route_type")))
        .dropDuplicates(["route_id"])
    )

    valid_condition = (
        col("route_id").isNotNull()
        & (col("route_id") != "")
        & col("origin").isNotNull()
        & col("destination").isNotNull()
        & (trim(col("origin")) != trim(col("destination")))
        & col("distance_km").isNotNull()
        & (col("distance_km") > 0)
    )

    valid = df.filter(valid_condition)
    invalid = df.filter(~valid_condition)

    write_silver(valid, "route")
    write_quarantine(invalid, "route")

    print(f"route: {valid.count()} valid, {invalid.count()} quarantine")


def transform_trip():
    df = read_bronze("trip")

    df = (
        df
        .withColumn("trip_id", trim(col("trip_id")))
        .withColumn("vehicle_id", trim(col("vehicle_id")))
        .withColumn("driver_id", trim(col("driver_id")))
        .withColumn("route_id", trim(col("route_id")))
        .withColumn("trip_status", trim(col("trip_status")))
        .dropDuplicates(["trip_id"])
    )

    df = df.withColumn(
        "trip_duration_minutes",
        when(
            col("arrival_time").isNotNull(),
            round(
                (
                    unix_timestamp(col("arrival_time"))
                    - unix_timestamp(col("departure_time"))
                ) / 60,
                2,
            ),
        )
    )

    valid_condition = (
        col("trip_id").isNotNull()
        & (col("trip_id") != "")
        & col("vehicle_id").isNotNull()
        & col("driver_id").isNotNull()
        & col("route_id").isNotNull()
        & col("departure_time").isNotNull()
        & col("distance_km").isNotNull()
        & (col("distance_km") >= 0)
        & col("cargo_weight_tons").isNotNull()
        & (col("cargo_weight_tons") >= 0)
        & col("trip_status").isin(
            "Planned",
            "In Progress",
            "Completed",
            "Cancelled",
        )
        & (
            col("arrival_time").isNull()
            | (col("arrival_time") >= col("departure_time"))
        )
    )

    valid = df.filter(valid_condition)
    invalid = df.filter(~valid_condition)

    write_silver(valid, "trip")
    write_quarantine(invalid, "trip")

    print(f"trip: {valid.count()} valid, {invalid.count()} quarantine")


def transform_gps_event():
    df = read_bronze("gps_event")

    df = (
        df
        .withColumn("event_id", trim(col("event_id")))
        .withColumn("vehicle_id", trim(col("vehicle_id")))
        .dropDuplicates(["event_id"])
    )

    valid_condition = (
        col("event_id").isNotNull()
        & (col("event_id") != "")
        & col("vehicle_id").isNotNull()
        & col("timestamp").isNotNull()
        & col("latitude").isNotNull()
        & col("longitude").isNotNull()
        & col("latitude").between(-90, 90)
        & col("longitude").between(-180, 180)
        & col("speed_kmh").isNotNull()
        & (col("speed_kmh") >= 0)
        & (
            col("fuel_level").isNull()
            | col("fuel_level").between(0, 100)
        )
    )

    valid = df.filter(valid_condition)
    invalid = df.filter(~valid_condition)

    write_silver(valid, "gps_event")
    write_quarantine(invalid, "gps_event")

    print(f"gps_event: {valid.count()} valid, {invalid.count()} quarantine")


def transform_fuel_transaction():
    df = read_bronze("fuel_transaction")

    df = (
        df
        .withColumn(
            "fuel_transaction_id",
            trim(col("fuel_transaction_id")),
        )
        .withColumn("vehicle_id", trim(col("vehicle_id")))
        .withColumn("station", trim(col("station")))
        .dropDuplicates(["fuel_transaction_id"])
    )

    df = df.withColumn(
        "total_cost",
        round(
            col("liters") * col("price_per_liter"),
            2,
        ),
    )

    valid_condition = (
        col("fuel_transaction_id").isNotNull()
        & (col("fuel_transaction_id") != "")
        & col("vehicle_id").isNotNull()
        & col("timestamp").isNotNull()
        & col("liters").isNotNull()
        & (col("liters") > 0)
        & col("price_per_liter").isNotNull()
        & (col("price_per_liter") > 0)
        & col("station").isNotNull()
        & (col("station") != "")
    )

    valid = df.filter(valid_condition)
    invalid = df.filter(~valid_condition)

    write_silver(valid, "fuel_transaction")
    write_quarantine(invalid, "fuel_transaction")

    print(
        f"fuel_transaction: "
        f"{valid.count()} valid, "
        f"{invalid.count()} quarantine"
    )


def transform_maintenance():
    df = read_bronze("maintenance")

    df = (
        df
        .withColumn("maintenance_id", trim(col("maintenance_id")))
        .withColumn("vehicle_id", trim(col("vehicle_id")))
        .withColumn("maintenance_type", trim(col("maintenance_type")))
        .dropDuplicates(["maintenance_id"])
    )

    valid_condition = (
        col("maintenance_id").isNotNull()
        & (col("maintenance_id") != "")
        & col("vehicle_id").isNotNull()
        & col("maintenance_date").isNotNull()
        & col("maintenance_type").isNotNull()
        & (col("maintenance_type") != "")
        & col("cost").isNotNull()
        & (col("cost") >= 0)
        & col("downtime_hours").isNotNull()
        & (col("downtime_hours") >= 0)
    )

    valid = df.filter(valid_condition)
    invalid = df.filter(~valid_condition)

    write_silver(valid, "maintenance")
    write_quarantine(invalid, "maintenance")

    print(
        f"maintenance: "
        f"{valid.count()} valid, "
        f"{invalid.count()} quarantine"
    )


def transform_incident():
    df = read_bronze("incident")

    df = (
        df
        .withColumn("incident_id", trim(col("incident_id")))
        .withColumn("vehicle_id", trim(col("vehicle_id")))
        .withColumn("incident_type", trim(col("incident_type")))
        .withColumn("severity", trim(col("severity")))
        .dropDuplicates(["incident_id"])
    )

    valid_condition = (
        col("incident_id").isNotNull()
        & (col("incident_id") != "")
        & col("vehicle_id").isNotNull()
        & col("timestamp").isNotNull()
        & col("incident_type").isNotNull()
        & (col("incident_type") != "")
        & col("severity").isin(
            "Low",
            "Medium",
            "High",
            "Critical",
        )
    )

    valid = df.filter(valid_condition)
    invalid = df.filter(~valid_condition)

    write_silver(valid, "incident")
    write_quarantine(invalid, "incident")

    print(f"incident: {valid.count()} valid, {invalid.count()} quarantine")


transform_vehicle()
transform_driver()
transform_route()
transform_trip()
transform_gps_event()
transform_fuel_transaction()
transform_maintenance()
transform_incident()

spark.stop()