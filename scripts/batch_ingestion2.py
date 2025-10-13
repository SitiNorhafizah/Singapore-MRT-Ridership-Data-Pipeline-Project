# batch_ingestion.py

from pyspark.sql import SparkSession

# MinIO credentials
MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio1234"
BUCKET = "bronze"

# Local CSV path
LOCAL_FILE_PATH = "/home/ctnorhafizah/mrt_project/data/ridership.csv"

# Create Spark session with MinIO configs
spark = SparkSession.builder \
    .appName("BatchIngestion") \
    .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT) \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

print("Spark session created successfully!")

# Read CSV from local filesystem
df = spark.read.option("header", "true").csv(LOCAL_FILE_PATH)
print(f"Data loaded from {LOCAL_FILE_PATH} successfully!")

# Show first 10 rows
df.show(10)

# Write to MinIO as Delta (or parquet if preferred)
minio_path = f"s3a://{BUCKET}/ridership"
df.write.format("delta").mode("overwrite").save(minio_path)
print(f"Data written to MinIO at {minio_path} successfully!")

# Stop Spark session
spark.stop()
print("Spark session stopped.")

