from pyspark.sql import SparkSession
import os

# ----------------------------
# Spark session with Hive support
# ----------------------------
spark = SparkSession.builder \
    .appName("ExportGoldToParquet") \
    .enableHiveSupport() \
    .getOrCreate()

# ----------------------------
# Output directory for Parquet
# ----------------------------
output_dir = os.path.expanduser("~/mrt_project/parquet_exports")
os.makedirs(output_dir, exist_ok=True)

# ----------------------------
# Tables to export (Hive DB → file name)
# ----------------------------
tables = [
    ("gold_gold.gold_station_crowd", "station_crowd"),
    ("gold_gold.gold_crowd_timeline", "crowd_timeline"),
    ("gold_gold.gold_alerts_summary", "alerts_summary"),
    ("gold_gold.gold_station_map", "station_map"),
]

# ----------------------------
# Export loop
# ----------------------------
for hive_table, filename in tables:
    try:
        print(f"[⏳] Exporting {hive_table} ...")
        df = spark.sql(f"SELECT * FROM {hive_table}")
        df.write.mode("overwrite").parquet(os.path.join(output_dir, filename))
        print(f"[✅] Exported {hive_table} → {filename}.parquet")
    except Exception as e:
        print(f"[❌] Failed to export {hive_table}: {e}")

spark.stop()
