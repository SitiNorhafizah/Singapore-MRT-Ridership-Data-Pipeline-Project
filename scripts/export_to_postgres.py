from pyspark.sql import SparkSession

# ------------------------------
# Spark session with Hive support
# ------------------------------
spark = SparkSession.builder \
    .appName("ExportGoldToPostgres") \
    .enableHiveSupport() \
    .config("spark.sql.catalogImplementation", "hive") \
    .getOrCreate()

print("[✔] Spark session connected to Hive Metastore")

# ------------------------------
# Define database and tables
# ------------------------------
database = "gold_gold"
tables = ["gold_station_map", "gold_station_crowd", "gold_crowd_timeline", "gold_alerts_summary"]

# ------------------------------
# Check which tables exist
# ------------------------------
existing_tables = [row.tableName for row in spark.sql(f"SHOW TABLES IN {database}").collect()]
print(f"Existing tables in {database}: {existing_tables}")

# ------------------------------
# Postgres connection
# ------------------------------
postgres_url = "jdbc:postgresql://localhost:5432/mrt_db"
postgres_properties = {
    "user": "mrt_user",
    "password": "mrt_pass",
    "driver": "org.postgresql.Driver"
}

# ------------------------------
# Export tables
# ------------------------------
for table in tables:
    if table in existing_tables:
        print(f"[⏳] Exporting {database}.{table} to Postgres...")
        df = spark.sql(f"SELECT * FROM {database}.{table}")
        df.write.jdbc(url=postgres_url, table=table, mode="overwrite", properties=postgres_properties)
        print(f"[✔] Exported {table} successfully")
    else:
        print(f"[❌] Table {table} not found in Hive")

spark.stop()
print("[✔] All exports finished, Spark session stopped")
