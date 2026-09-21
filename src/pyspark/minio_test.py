from pathlib import Path
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[2]
JDBC_DRIVER = BASE_DIR / "drivers" / "postgresql-42.7.12.jar"

spark = (
    SparkSession.builder
    .appName("MinIOConnectionTest")
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
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

test_df = (
    spark.range(1, 4)
    .withColumnRenamed("id", "vehicle_number")
    .withColumn("vehicle_id", concat(lit("V"), col("vehicle_number")))
    .withColumn("distance_km", col("vehicle_number") * lit(40.0) + lit(80.5))
    .select("vehicle_id", "distance_km")
)

output_path = "s3a://ocp-data/test/vehicles"

test_df.write.mode("overwrite").parquet(output_path)

print("Parquet written successfully to MinIO")

result_df = spark.read.parquet(output_path)

result_df.show()

print("Parquet read successfully from MinIO")

spark.stop()