from pyhive import hive
import pandas as pd

# ----------------------------
# Connect to your local Spark Thrift server
# ----------------------------
conn = hive.Connection(
    host='localhost',          # your local Spark/Hive host
    port=10000,                # default Thrift server port
    username='ctnorhafizah',   # your OS username (or set as needed)
    database='none_none_gold', # your gold schema
    auth='NONE'                # use 'KERBEROS' if required
)

# ----------------------------
# Function to query a table and return a pandas DataFrame
# ----------------------------
def fetch_table(table_name, limit=100):
    query = f"SELECT * FROM {table_name} LIMIT {limit}"
    df = pd.read_sql(query, conn)
    print(f"\n--- {table_name} ---")
    print(df.head())
    return df

# ----------------------------
# Query gold tables
# ----------------------------
df_crowd = fetch_table("gold_station_crowd")
df_timeline = fetch_table("gold_crowd_timeline")
df_alerts = fetch_table("gold_alerts_summary")
