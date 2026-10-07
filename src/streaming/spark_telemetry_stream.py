import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    when,
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


def create_spark_session(app_name="OCPTransportStreamingTelemetry"):
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


def build_streaming_pipeline(
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
        .withWatermark("event_time", "30 seconds")
    )

    enriched_df = (
        cleaned_df.withColumn(
            "is_overspeed",
            when(col("speed_kmh") > 100.0, True).otherwise(False),
        )
        .withColumn(
            "is_overheating",
            when(col("engine_temperature") > 100.0, True).otherwise(False),
        )
        .withColumn(
            "is_low_fuel",
            when(col("fuel_level") < 15.0, True).otherwise(False),
        )
        .withColumn(
            "is_stopped",
            when(col("speed_kmh") == 0.0, True).otherwise(False),
        )
        .withColumn(
            "operational_status",
            when(col("speed_kmh") > 100.0, "OVERSPEED")
            .when(col("engine_temperature") > 100.0, "OVERHEATING")
            .when(col("speed_kmh") == 0.0, "STOPPED")
            .otherwise("NORMAL"),
        )
    )

    return enriched_df


def run_stream(
    bootstrap_servers,
    topic_name,
    output_path="s3a://ocp-data/streaming/telemetry",
    checkpoint_path="s3a://ocp-data/streaming/checkpoints/telemetry",
    output_mode="append",
    trigger_available_now=False,
    console_output=False,
):
    spark = create_spark_session()
    enriched_df = build_streaming_pipeline(
        spark=spark,
        bootstrap_servers=bootstrap_servers,
        topic_name=topic_name,
        starting_offsets="earliest" if trigger_available_now else "latest",
    )

    if console_output:
        query_builder = (
            enriched_df.writeStream.format("console")
            .outputMode("append")
            .option("truncate", "false")
        )
    else:
        query_builder = (
            enriched_df.writeStream.format("parquet")
            .outputMode(output_mode)
            .option("path", output_path)
            .option("checkpointLocation", checkpoint_path)
        )

    if trigger_available_now:
        query = query_builder.trigger(availableNow=True).start()
    else:
        query = query_builder.trigger(processingTime="5 seconds").start()

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
        "--output-path",
        default="s3a://ocp-data/streaming/telemetry",
    )
    parser.add_argument(
        "--checkpoint-path",
        default="s3a://ocp-data/streaming/checkpoints/telemetry",
    )
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--console", action="store_true")
    args = parser.parse_args()

    run_stream(
        bootstrap_servers=args.bootstrap_servers,
        topic_name=args.topic,
        output_path=args.output_path,
        checkpoint_path=args.checkpoint_path,
        trigger_available_now=args.available_now,
        console_output=args.console,
    )
