from pyspark.sql import SparkSession

# Create Spark session with Delta + MinIO configs
spark = SparkSession.builder \
    .appName("Check Bronze Alerts") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio1234") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

# Read the Bronze Delta table
bronze_alerts = spark.read.format("delta").load("s3a://bronze/alerts")

# Show sample data
bronze_alerts.show(10, truncate=False)

# Show schema
bronze_alerts.printSchema()

# Show row count
print("Row count:", bronze_alerts.count())

spark.stop()
