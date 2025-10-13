import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pyspark.sql import SparkSession

# -----------------------
# 1. Generate synthetic ridership data with Pandas
# -----------------------

stations = ["NS1", "NS2", "EW1", "EW2", "CC1", "CC2"]

# Date range (last 6 months)
end_date = datetime.today()
start_date = end_date - timedelta(days=180)
dates = pd.date_range(start=start_date, end=end_date, freq="D")

# Generate ridership data
data = []
for station in stations:
    base = np.random.randint(8000, 20000)  # average ridership per station
    for d in dates:
        if d.weekday() < 5:  # Mon–Fri
            count = int(np.random.normal(base, 1500))
        else:  # Sat–Sun
            count = int(np.random.normal(base * 0.7, 1200))

        count = max(count, 0)  # prevent negatives
        data.append([station, d.strftime("%Y-%m-%d"), count])

df = pd.DataFrame(data, columns=["station_code", "date", "ridership"])
local_path = "/home/ctnorhafizah/mrt_project/data/ridership.csv"
df.to_csv(local_path, index=False)

print(f"✅ ridership.csv generated at {local_path}")
print(df.head())

# -----------------------
# 2. Load into Spark + Write to MinIO as Delta
# -----------------------

spark = SparkSession.builder \
    .appName("load_ridership") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio1234") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Load the CSV with Spark
df_spark = spark.read.csv(f"file://{local_path}", header=True, inferSchema=True)

# Ensure ridership column is integer
from pyspark.sql.types import IntegerType
df_spark = df_spark.withColumn("ridership", df_spark["ridership"].cast(IntegerType()))

# Write to MinIO as Delta (bronze layer)
df_spark.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("s3a://bronze/ridership")

# Create database if not exists
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# Register table in Spark
spark.sql("DROP TABLE IF EXISTS bronze.ridership")
spark.sql("CREATE TABLE bronze.ridership USING DELTA LOCATION 's3a://bronze/ridership'")

print("✅ bronze.ridership registered successfully in MinIO + Spark")

