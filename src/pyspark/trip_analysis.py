import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, coalesce, count, lit, round, sum

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]
JDBC_DRIVER = BASE_DIR / "drivers" / "postgresql-42.7.12.jar"

if not JDBC_DRIVER.exists():
    raise FileNotFoundError(f"JDBC driver not found: {JDBC_DRIVER}")

spark = (
    SparkSession.builder
    .appName("OCPTransportVehicleAnalytics")
    .master("local[*]")
    .config("spark.jars", JDBC_DRIVER.as_uri())
    .config("spark.ui.showConsoleProgress", "false")
    .config(
        "spark.hadoop.fs.file.impl",
        "org.apache.hadoop.fs.LocalFileSystem"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

jdbc_url = (
    f"jdbc:postgresql://"
    f"{os.getenv('POSTGRES_HOST')}:"
    f"{os.getenv('POSTGRES_PORT')}/"
    f"{os.getenv('POSTGRES_DB')}"
)

connection_properties = {
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "driver": "org.postgresql.Driver",
}

vehicles_df = spark.read.jdbc(
    url=jdbc_url,
    table="transports.vehicle",
    properties=connection_properties,
)

trips_df = spark.read.jdbc(
    url=jdbc_url,
    table="transports.trip",
    properties=connection_properties,
)

fuel_df = spark.read.jdbc(
    url=jdbc_url,
    table="transports.fuel_transaction",
    properties=connection_properties,
)

maintenance_df = spark.read.jdbc(
    url=jdbc_url,
    table="transports.maintenance",
    properties=connection_properties,
)

incidents_df = spark.read.jdbc(
    url=jdbc_url,
    table="transports.incident",
    properties=connection_properties,
)

completed_trips_df = trips_df.filter(
    trips_df.trip_status == "Completed"
)

trip_metrics_df = (
    completed_trips_df
    .groupBy("vehicle_id")
    .agg(
        count("trip_id").alias("completed_trips"),
        round(sum("distance_km"), 2).alias("total_distance_km"),
        round(sum("cargo_weight_tons"), 2).alias("total_cargo_tons"),
        round(avg("distance_km"), 2).alias("avg_distance_km"),
    )
)

fuel_metrics_df = (
    fuel_df
    .groupBy("vehicle_id")
    .agg(
        round(sum("liters"), 2).alias("total_fuel_liters"),
        round(
            sum(
                fuel_df.liters * fuel_df.price_per_liter
            ),
            2,
        ).alias("total_fuel_cost"),
    )
)

maintenance_metrics_df = (
    maintenance_df
    .groupBy("vehicle_id")
    .agg(
        count("maintenance_id").alias("maintenance_operations"),
        round(sum("cost"), 2).alias("total_maintenance_cost"),
        round(
            sum("downtime_hours"),
            2,
        ).alias("total_downtime_hours"),
    )
)

incident_metrics_df = (
    incidents_df
    .groupBy("vehicle_id")
    .agg(
        count("incident_id").alias("incident_count"),
    )
)

vehicle_kpi_df = (
    vehicles_df
    .select(
        "vehicle_id",
        "vehicle_type",
        "manufacturer",
        "model",
        "fuel_type",
        "capacity_tons",
        "status",
    )
    .join(
        trip_metrics_df,
        on="vehicle_id",
        how="left",
    )
    .join(
        fuel_metrics_df,
        on="vehicle_id",
        how="left",
    )
    .join(
        maintenance_metrics_df,
        on="vehicle_id",
        how="left",
    )
    .join(
        incident_metrics_df,
        on="vehicle_id",
        how="left",
    )
    .fillna(
        {
            "completed_trips": 0,
            "total_distance_km": 0,
            "total_cargo_tons": 0,
            "avg_distance_km": 0,
            "total_fuel_liters": 0,
            "total_fuel_cost": 0,
            "maintenance_operations": 0,
            "total_maintenance_cost": 0,
            "total_downtime_hours": 0,
            "incident_count": 0,
        }
    )
)

vehicle_kpi_df = (
    vehicle_kpi_df
    .withColumn(
        "fuel_liters_per_100km",
        round(
            (
                vehicle_kpi_df.total_fuel_liters
                / vehicle_kpi_df.total_distance_km
            ) * 100,
            2,
        ),
    )
    .withColumn(
        "fuel_cost_per_km",
        round(
            vehicle_kpi_df.total_fuel_cost
            / vehicle_kpi_df.total_distance_km,
            2,
        ),
    )
    .withColumn(
        "maintenance_cost_per_km",
        round(
            vehicle_kpi_df.total_maintenance_cost
            / vehicle_kpi_df.total_distance_km,
            2,
        ),
    )
    .withColumn(
        "incidents_per_trip",
        round(
            vehicle_kpi_df.incident_count
            / vehicle_kpi_df.completed_trips,
            2,
        ),
    )
)

vehicle_kpi_df = vehicle_kpi_df.na.fill(
    {
        "fuel_liters_per_100km": 0,
        "fuel_cost_per_km": 0,
        "maintenance_cost_per_km": 0,
        "incidents_per_trip": 0,
    }
)

print()
print("========================================")
print("   OCP TRANSPORT VEHICLE KPIs")
print("========================================")
print()

vehicle_kpi_df.show(
    50,
    truncate=False,
)

print()
print("========================================")
print("   TOP VEHICLES BY DISTANCE")
print("========================================")
print()

vehicle_kpi_df.select(
    "vehicle_id",
    "vehicle_type",
    "completed_trips",
    "total_distance_km",
    "total_cargo_tons",
).orderBy(
    "total_distance_km",
    ascending=False,
).show(
    10,
    truncate=False,
)

print()
print("========================================")
print("   FUEL PERFORMANCE")
print("========================================")
print()

vehicle_kpi_df.select(
    "vehicle_id",
    "total_distance_km",
    "total_fuel_liters",
    "total_fuel_cost",
    "fuel_liters_per_100km",
    "fuel_cost_per_km",
).orderBy(
    "fuel_liters_per_100km",
    ascending=True,
).show(
    10,
    truncate=False,
)

print()
print("========================================")
print("   MAINTENANCE PERFORMANCE")
print("========================================")
print()

vehicle_kpi_df.select(
    "vehicle_id",
    "maintenance_operations",
    "total_maintenance_cost",
    "total_downtime_hours",
    "maintenance_cost_per_km",
).orderBy(
    "total_maintenance_cost",
    ascending=False,
).show(
    10,
    truncate=False,
)

print()
print("========================================")
print("   INCIDENT PERFORMANCE")
print("========================================")
print()

vehicle_kpi_df.select(
    "vehicle_id",
    "completed_trips",
    "incident_count",
    "incidents_per_trip",
).orderBy(
    "incident_count",
    ascending=False,
).show(
    10,
    truncate=False,
)
from pathlib import Path

LAKE_PATH = BASE_DIR / "data" / "lake" / "vehicle_kpis"

vehicle_kpi_df.write.mode("overwrite").parquet(
    str(LAKE_PATH)
)

print()
print("========================================")
print("   PARQUET DATA LAKE")
print("========================================")
print()

print(f"Path      : {LAKE_PATH}")

parquet_df = spark.read.parquet(
    str(LAKE_PATH)
)

print(f"Row count : {parquet_df.count()}")

print()
print("Schema:")
parquet_df.printSchema()

print()
print("Sample:")
parquet_df.show(10, truncate=False)

spark.stop()