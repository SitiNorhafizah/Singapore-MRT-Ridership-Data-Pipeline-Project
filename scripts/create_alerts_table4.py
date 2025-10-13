from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

# Initialize Spark session with Delta support (Hive disabled for now)
spark = SparkSession.builder \
    .appName("InitAlertsDelta") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Configure MinIO access
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "minio")
hadoop_conf.set("fs.s3a.secret.key", "minio1234")
hadoop_conf.set("fs.s3a.endpoint", "http://localhost:9000")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Create bronze database in Spark catalog (Hive disabled, Spark internal catalog)
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

# Define schema for alerts
schema = StructType([
    StructField("alert_id", StringType(), True),
    StructField("message", StringType(), True),
    StructField("message_created", StringType(), True),
    StructField("_ingestion_time", StringType(), True)
])

# Read JSON alerts from MinIO
alerts_df = spark.read.schema(schema).json("s3a://bronze/train_alerts/")

# Save as Delta table (physical path in MinIO + Spark catalog)
alerts_df.write.format("delta") \
    .mode("overwrite") \
    .option("path", "s3a://bronze/alerts/") \
    .saveAsTable("bronze.alerts")

print("✅ Delta table created in Spark catalog and ready for dbt")

