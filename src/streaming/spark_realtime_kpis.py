import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    approx_count_distinct,
    avg,
    col,
    count,
    from_json,
    lit,
    max as spark_max,
    round as spark_round,
    sum as spark_sum,
    to_timestamp,
    when,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    StringType,
    StructField,
    StructType,
)

TELEMETRY_SCHEMA = StructType(
    [
        StructField("vehicle_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField("latitude", DoubleType(), False),
        StructField("longitude", DoubleType(), False),
        StructField("speed_kmh", DoubleType(), False),
        StructField("fuel_level", DoubleType(), True),
        StructField("engine_temperature", DoubleType(), True),
    ]
)


def create_spark_session(app_name="OCPTransportRealTimeKPIs"):
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config(
            "spark.jars",
            ",".join(
                [
                    "/opt/airflow/jars/hadoop-aws-3.3.4.jar",
                    "/opt/airflow/jars/aws-java-sdk-bundle-1.12.262.jar",
                    "/opt/airflow/jars/wildfly-openssl-1.0.7.Final.jar",
                ]
            ),
        )
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3",
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            os.getenv("MINIO_ENDPOINT", "http://ocp-minio:9000"),
        )
        .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("MINIO_ACCESS_KEY", "minio_admin"),
        )
        .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("MINIO_SECRET_KEY", "minio_password123"),
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_streaming_source(
    spark, bootstrap_servers, topic_name, starting_offsets="latest"
):
    raw_kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic_name)
        .option("startingOffsets", starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        raw_kafka_df.selectExpr(
            "CAST(key AS STRING) AS message_key",
            "CAST(value AS STRING) AS json_payload",
        )
        .select(from_json(col("json_payload"), TELEMETRY_SCHEMA).alias("data"))
        .select("data.*")
    )

    valid_condition = (
        col("vehicle_id").isNotNull()
        & (col("vehicle_id") != "")
        & col("timestamp").isNotNull()
        & col("latitude").isNotNull()
        & col("longitude").isNotNull()
        & col("latitude").between(-90.0, 90.0)
        & col("longitude").between(-180.0, 180.0)
        & col("speed_kmh").isNotNull()
        & (col("speed_kmh") >= 0.0)
    )

    cleaned_df = (
        parsed_df.filter(valid_condition)
        .withColumn("event_time", to_timestamp(col("timestamp")))
        .filter(col("event_time").isNotNull())
        .withWatermark("event_time", "1 minute")
    )

    enriched_df = (
        cleaned_df.withColumn(
            "is_overspeed",
            when(col("speed_kmh") > 100.0, 1).otherwise(0),
        )
        .withColumn(
            "is_overheating",
            when(col("engine_temperature") > 100.0, 1).otherwise(0),
        )
        .withColumn(
            "is_low_fuel",
            when(col("fuel_level") < 15.0, 1).otherwise(0),
        )
        .withColumn(
            "is_stopped",
            when(col("speed_kmh") == 0.0, 1).otherwise(0),
        )
    )

    return enriched_df


def process_kpis_batch(
    df,
    batch_id,
    fleet_kpi_path,
    vehicle_kpi_path,
):
    if df.rdd.isEmpty():
        return

    fleet_kpis = (
        df.groupBy(window(col("event_time"), "1 minute"))
        .agg(
            approx_count_distinct("vehicle_id").alias("active_vehicles"),
            spark_round(avg("speed_kmh"), 2).alias("avg_speed_kmh"),
            spark_round(spark_max("speed_kmh"), 2).alias("max_speed_kmh"),
            spark_round(avg("engine_temperature"), 2).alias("avg_engine_temperature"),
            spark_round(spark_max("engine_temperature"), 2).alias(
                "max_engine_temperature"
            ),
            spark_round(avg("fuel_level"), 2).alias("avg_fuel_level"),
            spark_sum("is_overspeed").alias("overspeed_events"),
            spark_sum("is_overheating").alias("temperature_anomalies"),
            spark_sum("is_low_fuel").alias("low_fuel_events"),
            spark_sum("is_stopped").alias("stopped_vehicles"),
            count(lit(1)).alias("total_events"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "active_vehicles",
            "avg_speed_kmh",
            "max_speed_kmh",
            "avg_engine_temperature",
            "max_engine_temperature",
            "avg_fuel_level",
            "overspeed_events",
            "temperature_anomalies",
            "low_fuel_events",
            "stopped_vehicles",
            "total_events",
        )
    )

    vehicle_kpis = (
        df.groupBy(window(col("event_time"), "1 minute"), col("vehicle_id"))
        .agg(
            spark_round(avg("speed_kmh"), 2).alias("avg_speed_kmh"),
            spark_round(spark_max("speed_kmh"), 2).alias("max_speed_kmh"),
            spark_round(avg("engine_temperature"), 2).alias("avg_engine_temperature"),
            spark_round(spark_max("engine_temperature"), 2).alias(
                "max_engine_temperature"
            ),
            spark_round(avg("fuel_level"), 2).alias("avg_fuel_level"),
            spark_sum("is_overspeed").alias("overspeed_events"),
            spark_sum("is_overheating").alias("temperature_anomalies"),
            spark_sum("is_low_fuel").alias("low_fuel_events"),
            spark_sum("is_stopped").alias("stopped_events"),
            count(lit(1)).alias("total_events"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "vehicle_id",
            "avg_speed_kmh",
            "max_speed_kmh",
            "avg_engine_temperature",
            "max_engine_temperature",
            "avg_fuel_level",
            "overspeed_events",
            "temperature_anomalies",
            "low_fuel_events",
            "stopped_events",
            "total_events",
        )
    )

    fleet_kpis.write.mode("append").parquet(fleet_kpi_path)
    vehicle_kpis.write.mode("append").parquet(vehicle_kpi_path)


def run_realtime_kpis(
    bootstrap_servers,
    topic_name,
    fleet_kpi_path="s3a://ocp-data/streaming/kpis/fleet",
    vehicle_kpi_path="s3a://ocp-data/streaming/kpis/vehicle",
    checkpoint_path="s3a://ocp-data/streaming/checkpoints/kpis",
    trigger_available_now=False,
):
    spark = create_spark_session()
    enriched_df = build_streaming_source(
        spark=spark,
        bootstrap_servers=bootstrap_servers,
        topic_name=topic_name,
        starting_offsets="earliest" if trigger_available_now else "latest",
    )

    writer = enriched_df.writeStream.foreachBatch(
        lambda df, b_id: process_kpis_batch(
            df,
            b_id,
            fleet_kpi_path,
            vehicle_kpi_path,
        )
    ).option("checkpointLocation", checkpoint_path)

    if trigger_available_now:
        query = writer.trigger(availableNow=True).start()
    else:
        query = writer.trigger(processingTime="5 seconds").start()

    query.awaitTermination()
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "ocp-kafka:9092"),
    )
    parser.add_argument(
        "--topic",
        default=os.getenv("KAFKA_TOPIC", "vehicle_telemetry"),
    )
    parser.add_argument(
        "--fleet-kpi-path",
        default="s3a://ocp-data/streaming/kpis/fleet",
    )
    parser.add_argument(
        "--vehicle-kpi-path",
        default="s3a://ocp-data/streaming/kpis/vehicle",
    )
    parser.add_argument(
        "--checkpoint-path",
        default="s3a://ocp-data/streaming/checkpoints/kpis",
    )
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args()

    run_realtime_kpis(
        bootstrap_servers=args.bootstrap_servers,
        topic_name=args.topic,
        fleet_kpi_path=args.fleet_kpi_path,
        vehicle_kpi_path=args.vehicle_kpi_path,
        checkpoint_path=args.checkpoint_path,
        trigger_available_now=args.available_now,
    )
