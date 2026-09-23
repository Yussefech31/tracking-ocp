from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    coalesce,
    col,
    count,
    lit,
    round,
    sum,
    when,
)

BASE_DIR = Path(__file__).resolve().parents[2]
JDBC_DRIVER = BASE_DIR / "drivers" / "postgresql-42.7.12.jar"

spark = (
    SparkSession.builder
    .appName("OCPTransportGoldVehicleKPIs")
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

SILVER_PATH = "s3a://ocp-data/silver"
GOLD_PATH = "s3a://ocp-data/gold"

vehicle = spark.read.parquet(f"{SILVER_PATH}/vehicle")
trip = spark.read.parquet(f"{SILVER_PATH}/trip")
fuel = spark.read.parquet(f"{SILVER_PATH}/fuel_transaction")
maintenance = spark.read.parquet(f"{SILVER_PATH}/maintenance")
incident = spark.read.parquet(f"{SILVER_PATH}/incident")

completed_trips = trip.filter(
    col("trip_status") == "Completed"
)

trip_kpis = (
    completed_trips
    .groupBy("vehicle_id")
    .agg(
        count("trip_id").alias("completed_trips"),
        sum("distance_km").alias("total_distance_km"),
        sum("cargo_weight_tons").alias("total_cargo_tons"),
        avg("distance_km").alias("avg_distance_km")
    )
)

fuel_kpis = (
    fuel
    .groupBy("vehicle_id")
    .agg(
        sum("liters").alias("total_fuel_liters"),
        sum("total_cost").alias("total_fuel_cost")
    )
)

maintenance_kpis = (
    maintenance
    .groupBy("vehicle_id")
    .agg(
        count("maintenance_id").alias("maintenance_operations"),
        sum("cost").alias("total_maintenance_cost"),
        sum("downtime_hours").alias("total_downtime_hours")
    )
)

incident_kpis = (
    incident
    .groupBy("vehicle_id")
    .agg(
        count("incident_id").alias("incident_count")
    )
)

vehicle_kpis = (
    vehicle
    .select(
        "vehicle_id",
        "vehicle_type",
        "manufacturer",
        "model",
        "capacity_tons",
        "fuel_type",
        "year",
        "status"
    )
    .join(trip_kpis, "vehicle_id", "left")
    .join(fuel_kpis, "vehicle_id", "left")
    .join(maintenance_kpis, "vehicle_id", "left")
    .join(incident_kpis, "vehicle_id", "left")
)

numeric_columns = [
    "completed_trips",
    "total_distance_km",
    "total_cargo_tons",
    "avg_distance_km",
    "total_fuel_liters",
    "total_fuel_cost",
    "maintenance_operations",
    "total_maintenance_cost",
    "total_downtime_hours",
    "incident_count"
]

for column_name in numeric_columns:
    vehicle_kpis = vehicle_kpis.withColumn(
        column_name,
        coalesce(col(column_name), lit(0))
    )

vehicle_kpis = (
    vehicle_kpis
    .withColumn(
        "fuel_liters_per_100km",
        when(
            col("total_distance_km") > 0,
            round(
                col("total_fuel_liters") / col("total_distance_km") * 100,
                2
            )
        ).otherwise(0)
    )
    .withColumn(
        "fuel_cost_per_km",
        when(
            col("total_distance_km") > 0,
            round(
                col("total_fuel_cost") / col("total_distance_km"),
                2
            )
        ).otherwise(0)
    )
    .withColumn(
        "maintenance_cost_per_km",
        when(
            col("total_distance_km") > 0,
            round(
                col("total_maintenance_cost") / col("total_distance_km"),
                2
            )
        ).otherwise(0)
    )
    .withColumn(
        "incidents_per_trip",
        when(
            col("completed_trips") > 0,
            round(
                col("incident_count") / col("completed_trips"),
                3
            )
        ).otherwise(0)
    )
)

vehicle_kpis = vehicle_kpis.select(
    "vehicle_id",
    "vehicle_type",
    "manufacturer",
    "model",
    "capacity_tons",
    "fuel_type",
    "year",
    "status",
    "completed_trips",
    "total_distance_km",
    "total_cargo_tons",
    "avg_distance_km",
    "total_fuel_liters",
    "total_fuel_cost",
    "maintenance_operations",
    "total_maintenance_cost",
    "total_downtime_hours",
    "incident_count",
    "fuel_liters_per_100km",
    "fuel_cost_per_km",
    "maintenance_cost_per_km",
    "incidents_per_trip"
)

vehicle_kpis.write.mode("overwrite").parquet(
    f"{GOLD_PATH}/vehicle_kpis"
)

vehicle_kpis.orderBy(
    col("total_distance_km").desc()
).show(10, truncate=False)

spark.stop()