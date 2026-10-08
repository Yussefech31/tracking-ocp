import argparse
import json
import sys
from kafka import KafkaConsumer
from pyspark.sql import SparkSession


def verify_alerts(
    bootstrap_servers,
    alerts_topic="vehicle_alerts",
    alerts_path="s3a://ocp-data/streaming/alerts",
    minio_endpoint="http://ocp-minio:9000",
    minio_access_key="minio_admin",
    minio_secret_key="minio_password123",
):
    spark = (
        SparkSession.builder.appName("OCPVerifyAlerts")
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

    alerts_df = spark.read.parquet(alerts_path)
    count = alerts_df.count()
    if count == 0:
        spark.stop()
        raise ValueError("Alerts dataset in MinIO is empty.")

    required_columns = [
        "alert_id",
        "vehicle_id",
        "timestamp",
        "alert_type",
        "severity",
        "metric_value",
        "threshold",
        "description",
    ]

    for col_name in required_columns:
        if col_name not in alerts_df.columns:
            spark.stop()
            raise ValueError(f"Missing expected alert column '{col_name}'")

    print(f"Verified {count} persisted operational alerts in MinIO at {alerts_path}.")

    print("Sample Detected Real-Time Alerts:")
    for row in alerts_df.orderBy("timestamp", ascending=False).take(5):
        print(
            f"  [{row['severity']}] {row['alert_type']} | Vehicle: {row['vehicle_id']} | "
            f"Value: {row['metric_value']} (Threshold: {row['threshold']}) | "
            f"{row['description']}"
        )

    spark.stop()

    consumer = KafkaConsumer(
        alerts_topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        consumer_timeout_ms=5000,
    )

    kafka_alerts_read = 0
    for msg in consumer:
        payload = msg.value
        for col_name in required_columns:
            if col_name not in payload:
                consumer.close()
                raise ValueError(
                    f"Missing required field '{col_name}' in Kafka alert payload"
                )
        kafka_alerts_read += 1
        if kafka_alerts_read >= 5:
            break

    consumer.close()
    print(
        f"Verified {kafka_alerts_read} real-time alerts consumed from Kafka topic '{alerts_topic}'."
    )
    print("Alert Engine validation PASSED successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="ocp-kafka:9092")
    parser.add_argument("--alerts-topic", default="vehicle_alerts")
    parser.add_argument("--alerts-path", default="s3a://ocp-data/streaming/alerts")
    args = parser.parse_args()

    try:
        verify_alerts(
            bootstrap_servers=args.bootstrap_servers,
            alerts_topic=args.alerts_topic,
            alerts_path=args.alerts_path,
        )
    except Exception as exc:
        print(f"Alert validation FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
