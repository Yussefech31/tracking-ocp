import argparse
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat,
    from_json,
    lit,
    round as spark_round,
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


def create_spark_session(app_name="OCPTransportAlertEngine"):
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


def build_raw_telemetry_source(
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
    )

    return cleaned_df


def extract_operational_alerts(cleaned_df):
    overspeed_df = (
        cleaned_df.filter(col("speed_kmh") > 100.0)
        .withColumn("alert_type", lit("OVERSPEED"))
        .withColumn(
            "severity",
            when(col("speed_kmh") > 115.0, "CRITICAL").otherwise("WARNING"),
        )
        .withColumn("metric_value", spark_round(col("speed_kmh"), 2))
        .withColumn("threshold", lit(100.0))
        .withColumn(
            "description",
            concat(
                lit("Vehicle exceeded speed limit: "),
                col("speed_kmh"),
                lit(" km/h (threshold: 100.0 km/h)"),
            ),
        )
    )

    overheat_df = (
        cleaned_df.filter(col("engine_temperature") > 100.0)
        .withColumn("alert_type", lit("HIGH_ENGINE_TEMPERATURE"))
        .withColumn(
            "severity",
            when(col("engine_temperature") > 108.0, "CRITICAL").otherwise("WARNING"),
        )
        .withColumn("metric_value", spark_round(col("engine_temperature"), 2))
        .withColumn("threshold", lit(100.0))
        .withColumn(
            "description",
            concat(
                lit("Engine temperature excessive: "),
                col("engine_temperature"),
                lit(" C (threshold: 100.0 C)"),
            ),
        )
    )

    low_fuel_df = (
        cleaned_df.filter(col("fuel_level") < 15.0)
        .withColumn("alert_type", lit("LOW_FUEL"))
        .withColumn(
            "severity",
            when(col("fuel_level") < 8.0, "CRITICAL").otherwise("WARNING"),
        )
        .withColumn("metric_value", spark_round(col("fuel_level"), 2))
        .withColumn("threshold", lit(15.0))
        .withColumn(
            "description",
            concat(
                lit("Critical fuel level reserve: "),
                col("fuel_level"),
                lit(" % (threshold: 15.0 %)"),
            ),
        )
    )

    stopped_df = (
        cleaned_df.filter(col("speed_kmh") == 0.0)
        .withColumn("alert_type", lit("UNEXPECTED_STOP"))
        .withColumn("severity", lit("WARNING"))
        .withColumn("metric_value", lit(0.0))
        .withColumn("threshold", lit(0.0))
        .withColumn(
            "description",
            concat(
                lit("Vehicle stationary on route at lat="),
                col("latitude"),
                lit(", lon="),
                col("longitude"),
            ),
        )
    )

    union_alerts = (
        overspeed_df.unionByName(overheat_df)
        .unionByName(low_fuel_df)
        .unionByName(stopped_df)
    )

    alerts_df = (
        union_alerts.withColumn(
            "alert_id",
            concat(
                lit("ALT-"),
                col("vehicle_id"),
                lit("-"),
                col("alert_type"),
                lit("-"),
                col("event_time").cast("long"),
            ),
        )
        .select(
            "alert_id",
            "vehicle_id",
            col("timestamp"),
            "alert_type",
            "severity",
            "metric_value",
            "threshold",
            "latitude",
            "longitude",
            "description",
        )
    )

    return alerts_df


def process_alerts_batch(
    df,
    batch_id,
    alerts_path,
    bootstrap_servers,
    alerts_topic,
):
    if df.rdd.isEmpty():
        return

    df.write.mode("append").parquet(alerts_path)

    kafka_payload_df = df.selectExpr(
        "CAST(vehicle_id AS STRING) AS key",
        "to_json(struct(*)) AS value",
    )

    (
        kafka_payload_df.write.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("topic", alerts_topic)
        .save()
    )


def run_alert_engine(
    bootstrap_servers,
    telemetry_topic="vehicle_telemetry",
    alerts_topic="vehicle_alerts",
    alerts_path="s3a://ocp-data/streaming/alerts",
    checkpoint_path="s3a://ocp-data/streaming/checkpoints/alerts",
    trigger_available_now=False,
):
    spark = create_spark_session()
    cleaned_df = build_raw_telemetry_source(
        spark=spark,
        bootstrap_servers=bootstrap_servers,
        topic_name=telemetry_topic,
        starting_offsets="earliest" if trigger_available_now else "latest",
    )

    alerts_df = extract_operational_alerts(cleaned_df)

    writer = alerts_df.writeStream.foreachBatch(
        lambda df, b_id: process_alerts_batch(
            df,
            b_id,
            alerts_path,
            bootstrap_servers,
            alerts_topic,
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
        "--telemetry-topic",
        default=os.getenv("KAFKA_TELEMETRY_TOPIC", "vehicle_telemetry"),
    )
    parser.add_argument(
        "--alerts-topic",
        default=os.getenv("KAFKA_ALERTS_TOPIC", "vehicle_alerts"),
    )
    parser.add_argument(
        "--alerts-path",
        default="s3a://ocp-data/streaming/alerts",
    )
    parser.add_argument(
        "--checkpoint-path",
        default="s3a://ocp-data/streaming/checkpoints/alerts",
    )
    parser.add_argument("--available-now", action="store_true")
    args = parser.parse_args()

    run_alert_engine(
        bootstrap_servers=args.bootstrap_servers,
        telemetry_topic=args.telemetry_topic,
        alerts_topic=args.alerts_topic,
        alerts_path=args.alerts_path,
        checkpoint_path=args.checkpoint_path,
        trigger_available_now=args.available_now,
    )
