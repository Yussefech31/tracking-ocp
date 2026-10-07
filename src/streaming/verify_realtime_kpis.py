import argparse
import sys
from pyspark.sql import SparkSession


def verify_kpis(
    minio_endpoint="http://ocp-minio:9000",
    minio_access_key="minio_admin",
    minio_secret_key="minio_password123",
    fleet_kpi_path="s3a://ocp-data/streaming/kpis/fleet",
    vehicle_kpi_path="s3a://ocp-data/streaming/kpis/vehicle",
):
    spark = (
        SparkSession.builder.appName("OCPVerifyRealTimeKPIs")
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

    fleet_df = spark.read.parquet(fleet_kpi_path)
    fleet_count = fleet_df.count()
    if fleet_count == 0:
        spark.stop()
        raise ValueError("Fleet KPIs dataset is empty.")

    vehicle_df = spark.read.parquet(vehicle_kpi_path)
    vehicle_count = vehicle_df.count()
    if vehicle_count == 0:
        spark.stop()
        raise ValueError("Vehicle KPIs dataset is empty.")

    print(
        f"Verified {fleet_count} Fleet KPI window records and {vehicle_count} Vehicle KPI window records."
    )

    print("Sample Fleet Real-Time KPIs:")
    for row in fleet_df.orderBy("window_start", ascending=False).take(3):
        print(
            f"  Window: {row['window_start']} to {row['window_end']} | Active: {row['active_vehicles']} | "
            f"Avg Speed: {row['avg_speed_kmh']} km/h | Overspeed: {row['overspeed_events']} | "
            f"Avg Temp: {row['avg_engine_temperature']} C | Stopped: {row['stopped_vehicles']}"
        )

    print("Sample Vehicle Real-Time KPIs:")
    for row in vehicle_df.orderBy("window_start", ascending=False).take(3):
        print(
            f"  Vehicle: {row['vehicle_id']} | Window: {row['window_start']} to {row['window_end']} | "
            f"Avg Speed: {row['avg_speed_kmh']} km/h | Max Speed: {row['max_speed_kmh']} km/h | "
            f"Avg Temp: {row['avg_engine_temperature']} C | Fuel: {row['avg_fuel_level']}%"
        )

    spark.stop()
    print("Real-Time KPI verification PASSED successfully.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--fleet-kpi-path", default="s3a://ocp-data/streaming/kpis/fleet"
    )
    parser.add_argument(
        "--vehicle-kpi-path", default="s3a://ocp-data/streaming/kpis/vehicle"
    )
    args = parser.parse_args()

    try:
        verify_kpis(
            fleet_kpi_path=args.fleet_kpi_path,
            vehicle_kpi_path=args.vehicle_kpi_path,
        )
    except Exception as exc:
        print(f"Real-Time KPI verification FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
