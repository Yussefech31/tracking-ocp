import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, round, sum, when

spark = (
    SparkSession.builder.appName("OCPTransportGoldRouteKPIs")
    .config(
        "spark.jars",
        "/opt/airflow/jars/hadoop-aws-3.3.4.jar,/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/airflow/jars/wildfly-openssl-1.0.7.Final.jar",
    )
    .config("spark.hadoop.fs.s3a.endpoint", "http://ocp-minio:9000")
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

silver_route_path = "s3a://ocp-data/silver/route"
silver_trip_path = "s3a://ocp-data/silver/trip"
gold_path = "s3a://ocp-data/gold/route_kpis"

route_df = spark.read.parquet(silver_route_path)
trip_df = spark.read.parquet(silver_trip_path)

trip_kpis = trip_df.groupBy("route_id").agg(
    count("*").alias("total_trips"),
    sum(when(col("trip_status") == "Completed", 1).otherwise(0)).alias(
        "completed_trips"
    ),
    sum("distance_km").alias("total_actual_distance_km"),
    sum("cargo_weight_tons").alias("total_cargo_tons"),
    avg("distance_km").alias("avg_trip_distance_km"),
    avg("trip_duration_minutes").alias("avg_trip_duration_minutes"),
)

route_kpis = (
    route_df.join(trip_kpis, "route_id", "left")
    .withColumn(
        "total_trips",
        when(col("total_trips").isNull(), 0).otherwise(col("total_trips")),
    )
    .withColumn(
        "completed_trips",
        when(col("completed_trips").isNull(), 0).otherwise(col("completed_trips")),
    )
    .withColumn(
        "total_actual_distance_km",
        when(col("total_actual_distance_km").isNull(), 0).otherwise(
            col("total_actual_distance_km")
        ),
    )
    .withColumn(
        "total_cargo_tons",
        when(col("total_cargo_tons").isNull(), 0).otherwise(col("total_cargo_tons")),
    )
    .withColumn("avg_trip_distance_km", round(col("avg_trip_distance_km"), 2))
    .withColumn("avg_trip_duration_minutes", round(col("avg_trip_duration_minutes"), 2))
    .withColumn(
        "total_planned_distance_km",
        round(col("distance_km") * col("completed_trips"), 2),
    )
    .withColumn(
        "route_deviation_km",
        round(col("total_actual_distance_km") - col("total_planned_distance_km"), 2),
    )
    .withColumn(
        "route_deviation_percent",
        round(
            when(
                col("total_planned_distance_km") > 0,
                (col("route_deviation_km") / col("total_planned_distance_km")) * 100,
            ),
            2,
        ),
    )
    .select(
        "route_id",
        "origin",
        "destination",
        "distance_km",
        "route_type",
        "total_trips",
        "completed_trips",
        "total_planned_distance_km",
        "total_actual_distance_km",
        "route_deviation_km",
        "route_deviation_percent",
        "total_cargo_tons",
        "avg_trip_distance_km",
        "avg_trip_duration_minutes",
    )
)

route_kpis.write.mode("overwrite").parquet(gold_path)

spark.stop()
