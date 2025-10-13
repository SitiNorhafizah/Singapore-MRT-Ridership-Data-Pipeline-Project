import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, LongType
from delta import configure_spark_with_delta_pip

# -----------------------------
# Ensure PySpark knows where Kafka, S3A, Delta JARs are
# -----------------------------
os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--jars "
    "/home/ctnorhafizah/spark_jars/spark-sql-kafka-0-10_2.12-3.2.1.jar,"
    "/home/ctnorhafizah/spark_jars/kafka-clients-2.8.0.jar,"
    "/home/ctnorhafizah/spark_jars/spark-token-provider-kafka-0-10_2.12-3.2.1.jar,"
    "/home/ctnorhafizah/spark_jars/commons-pool2-2.11.1.jar,"
    "/home/ctnorhafizah/spark_jars/hadoop-aws-3.3.4.jar,"
    "/home/ctnorhafizah/spark_jars/aws-java-sdk-bundle-1.12.379.jar "
    "pyspark-shell"
)

# -----------------------------
# Spark session with Delta + S3A configs
# -----------------------------
builder = (
    SparkSession.builder
    .appName("BronzeStreaming")
    .config("spark.hadoop.fs.s3a.access.key", "minio")
    .config("spark.hadoop.fs.s3a.secret.key", "minio1234")
    .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")  # host machine
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# -----------------------------
# Kafka configuration
# -----------------------------
KAFKA_BOOTSTRAP = "localhost:9092"  # host machine

TOPICS = {
    "train_alerts": StructType([
        StructField("alert_id", StringType(), True),
        StructField("message", StringType(), True),
        StructField("_ingestion_time", LongType(), True)
    ]),
    "station_crowd": StructType([
        StructField("Station", StringType(), True),
        StructField("Crowd", StringType(), True),
        StructField("_ingestion_time", LongType(), True)
    ]),
    "station_exits": StructType([
        StructField("StationCode", StringType(), True),
        StructField("ExitName", StringType(), True),
        StructField("_ingestion_time", LongType(), True)
    ])
}

# -----------------------------
# Function to create streaming query
# -----------------------------
def create_stream(topic, schema):
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", topic) \
        .option("startingOffsets", "earliest") \
        .load()

    df_parsed = df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), schema).alias("data")) \
        .select("data.*")

    delta_path = f"s3a://bronze/{topic}"
    checkpoint_path = f"s3a://bronze/_checkpoints/{topic}"

    query = df_parsed.writeStream \
        .format("delta") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_path) \
        .start(delta_path)

    print(f"✅ Streaming started for topic '{topic}' -> Delta path '{delta_path}'")
    return query

# -----------------------------
# Start streams for all topics
# -----------------------------
queries = []
for topic_name, schema in TOPICS.items():
    queries.append(create_stream(topic_name, schema))

# Wait for all streams
for q in queries:
    q.awaitTermination()

