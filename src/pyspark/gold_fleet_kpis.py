from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    round,
    sum,
    when,
)

BASE_DIR = Path(__file__).resolve().parents[2]
JDBC_DRIVER = BASE_DIR / "drivers" / "postgresql-42.7.12.jar"

spark = (
    SparkSession.builder
    .appName("OCPTransportGoldFleetKPIs")
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
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .config("spark.hadoop.fs.s3a.fast.upload", "true")
    .config(
        "spark.hadoop.fs.s3a.fast.upload.buffer",
        "bytebuffer"
    )
    .config(
        "spark.hadoop.fs.s3a.fast.upload.active.blocks",
        "1"
    )
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

SILVER_PATH = "s3a://ocp-data/silver"
GOLD_PATH = "s3a://ocp-data/gold"

vehicle = spark.read.parquet(
    f"{SILVER_PATH}/vehicle"
)

trip = spark.read.parquet(
    f"{SILVER_PATH}/trip"
)

fuel = spark.read.parquet(
    f"{SILVER_PATH}/fuel_transaction"
)

maintenance = spark.read.parquet(
    f"{SILVER_PATH}/maintenance"
)

incident = spark.read.parquet(
    f"{SILVER_PATH}/incident"
)

total_vehicles = vehicle.agg(
    count("vehicle_id").alias("total_vehicles")
)

active_vehicles = (
    vehicle
    .filter(col("status") == "Active")
    .agg(
        count("vehicle_id").alias("active_vehicles")
    )
)

trip_kpis = trip.agg(
    count("trip_id").alias("total_trips"),
    sum(
        when(
            col("trip_status") == "Completed",
            1
        ).otherwise(0)
    ).alias("completed_trips"),
    sum(
        when(
            col("trip_status") == "Completed",
            col("distance_km")
        ).otherwise(0)
    ).alias("total_distance_km"),
    sum(
        when(
            col("trip_status") == "Completed",
            col("cargo_weight_tons")
        ).otherwise(0)
    ).alias("total_cargo_tons")
)

fuel_kpis = fuel.agg(
    sum("liters").alias("total_fuel_liters"),
    sum("total_cost").alias("total_fuel_cost")
)

maintenance_kpis = maintenance.agg(
    sum("cost").alias("total_maintenance_cost"),
    sum("downtime_hours").alias("total_downtime_hours")
)

incident_kpis = incident.agg(
    count("incident_id").alias("total_incidents")
)

fleet_kpis = (
    total_vehicles
    .crossJoin(active_vehicles)
    .crossJoin(trip_kpis)
    .crossJoin(fuel_kpis)
    .crossJoin(maintenance_kpis)
    .crossJoin(incident_kpis)
)

fleet_kpis = (
    fleet_kpis
    .withColumn(
        "fuel_liters_per_100km",
        when(
            col("total_distance_km") > 0,
            round(
                (
                    col("total_fuel_liters")
                    / col("total_distance_km")
                ) * 100,
                2
            )
        ).otherwise(0)
    )
    .withColumn(
        "fuel_cost_per_km",
        when(
            col("total_distance_km") > 0,
            round(
                col("total_fuel_cost")
                / col("total_distance_km"),
                2
            )
        ).otherwise(0)
    )
    .withColumn(
        "maintenance_cost_per_km",
        when(
            col("total_distance_km") > 0,
            round(
                col("total_maintenance_cost")
                / col("total_distance_km"),
                2
            )
        ).otherwise(0)
    )
    .withColumn(
        "incidents_per_trip",
        when(
            col("completed_trips") > 0,
            round(
                col("total_incidents")
                / col("completed_trips"),
                3
            )
        ).otherwise(0)
    )
)

fleet_kpis.write.mode("overwrite").parquet(
    f"{GOLD_PATH}/fleet_kpis"
)

fleet_kpis.show(
    truncate=False
)

spark.stop()