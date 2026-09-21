import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]
JDBC_DRIVER = BASE_DIR / "drivers" / "postgresql-42.7.12.jar"

spark = (
    SparkSession.builder
    .appName("OCPTransportBronzeIngestion")
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

tables = [
    "vehicle",
    "driver",
    "route",
    "trip",
    "gps_event",
    "fuel_transaction",
    "maintenance",
    "incident",
]

for table in tables:
    print(f"Processing {table}...")

    try:
        df = spark.read.jdbc(
            url=jdbc_url,
            table=f"transports.{table}",
            properties=connection_properties,
        )

        print(f"{table}: {df.count()} rows loaded from PostgreSQL")

        output_path = f"s3a://ocp-data/bronze/{table}"

        df.write.mode("overwrite").parquet(output_path)

        print(f"{table}: successfully written to MinIO")

    except Exception as e:
        print(f"{table}: FAILED")
        print(str(e))
        raise

spark.stop()
