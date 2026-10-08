import argparse
import os
import sys
from kafka_producer import run_producer
from pyspark.sql import SparkSession


def verify_spark_streaming(
    bootstrap_servers,
    topic_name,
    minio_endpoint="http://ocp-minio:9000",
    minio_access_key="minio_admin",
    minio_secret_key="minio_password123",
    output_path="s3a://ocp-data/streaming/telemetry",
):
    produced = run_producer(
        bootstrap_servers=bootstrap_servers,
        topic_name=topic_name,
        num_vehicles=5,
        interval=0.05,
        max_events=20,
    )
    print(
        f"Produced {produced} events to '{topic_name}' for Spark Streaming validation."
    )

    spark = (
        SparkSession.builder.appName("OCPVerifyStreamingOutput")
        .master("local[1]")
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
        .config("spark.hadoop.fs.s3a.endpoint", minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key", minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key", minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet(output_path)
    count = df.count()
    print(f"Read {count} records from MinIO streaming sink at {output_path}")

    required_columns = [
        "vehicle_id",
        "timestamp",
        "latitude",
        "longitude",
        "speed_kmh",
        "fuel_level",
        "engine_temperature",
        "event_time",
        "is_overspeed",
        "is_overheating",
        "is_low_fuel",
        "is_stopped",
        "operational_status",
    ]

    for col_name in required_columns:
        if col_name not in df.columns:
            spark.stop()
            raise ValueError(f"Missing expected streaming column '{col_name}'")

    sample_rows = df.select(
        "vehicle_id", "speed_kmh", "engine_temperature", "operational_status"
    ).take(5)
    for r in sample_rows:
        print(
            f"  {r['vehicle_id']}: speed={r['speed_kmh']}km/h, "
            f"temp={r['engine_temperature']}C, status={r['operational_status']}"
        )

    spark.stop()
    print("Spark Structured Streaming validation PASSED successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "ocp-kafka:9092"),
    )
    parser.add_argument(
        "--topic", default=os.getenv("KAFKA_TOPIC", "vehicle_telemetry")
    )
    parser.add_argument("--output-path", default="s3a://ocp-data/streaming/telemetry")
    args = parser.parse_args()

    try:
        verify_spark_streaming(
            bootstrap_servers=args.bootstrap_servers,
            topic_name=args.topic,
            output_path=args.output_path,
        )
    except Exception as exc:
        print(f"Spark Streaming validation FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
