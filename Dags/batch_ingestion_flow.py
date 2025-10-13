from prefect import flow, task
from pyspark.sql import SparkSession

@task
def load_geojson():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.option("multiline", "true").json("data/station_exits.geojson")
    df.write.format("delta").mode("overwrite").save("s3a://bronze/station_exits")

@task
def load_ridership():
    spark = SparkSession.builder.getOrCreate()
    df = spark.read.csv("data/ridership.csv", header=True, inferSchema=True)
    df.write.format("delta").mode("overwrite").save("s3a://bronze/ridership")

@flow
def batch_ingestion():
    load_geojson()
    load_ridership()

if __name__ == "__main__":
    batch_ingestion()
