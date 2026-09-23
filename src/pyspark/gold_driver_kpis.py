from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, sum, when

BASE_DIR = Path(__file__).resolve().parents[2]
JDBC_DRIVER = BASE_DIR / "drivers" / "postgresql-42.7.12.jar"

spark = (
    SparkSession.builder
    .appName("OCPTransportGoldDriverKPIs")
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

driver = spark.read.parquet(f"{SILVER_PATH}/driver")
trip = spark.read.parquet(f"{SILVER_PATH}/trip")

trip_kpis = (
    trip
    .groupBy("driver_id")
    .agg(
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
        ).alias("total_cargo_tons"),
        avg(
            when(
                col("trip_status") == "Completed",
                col("distance_km")
            )
        ).alias("avg_trip_distance_km"),
        avg(
            when(
                col("trip_status") == "Completed",
                col("trip_duration_minutes")
            )
        ).alias("avg_trip_duration_minutes")
    )
)

driver_kpis = (
    driver
    .select(
        "driver_id",
        "first_name",
        "last_name",
        "license_type",
        "experience_years",
        "department",
        "status"
    )
    .join(
        trip_kpis,
        "driver_id",
        "left"
    )
)

driver_kpis = (
    driver_kpis
    .withColumn(
        "total_trips",
        when(
            col("total_trips").isNull(),
            0
        ).otherwise(col("total_trips"))
    )
    .withColumn(
        "completed_trips",
        when(
            col("completed_trips").isNull(),
            0
        ).otherwise(col("completed_trips"))
    )
    .withColumn(
        "total_distance_km",
        when(
            col("total_distance_km").isNull(),
            0
        ).otherwise(col("total_distance_km"))
    )
    .withColumn(
        "total_cargo_tons",
        when(
            col("total_cargo_tons").isNull(),
            0
        ).otherwise(col("total_cargo_tons"))
    )
    .withColumn(
        "avg_trip_distance_km",
        when(
            col("avg_trip_distance_km").isNull(),
            0
        ).otherwise(col("avg_trip_distance_km"))
    )
    .withColumn(
        "avg_trip_duration_minutes",
        when(
            col("avg_trip_duration_minutes").isNull(),
            0
        ).otherwise(col("avg_trip_duration_minutes"))
    )
)

driver_kpis.write.mode("overwrite").parquet(
    f"{GOLD_PATH}/driver_kpis"
)

driver_kpis.orderBy(
    col("completed_trips").desc()
).show(10, truncate=False)

spark.stop()