from prefect import flow, task
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import os

# -------------------
# 1️⃣ Extract
# -------------------
@task
def extract_json():
    # Example: read from downloaded JSON
    df = pd.read_json("/opt/data/mrt_data.json")  
    print(f"Extracted {len(df)} rows")
    return df

# -------------------
# 2️⃣ Transform
# -------------------
@task
def transform_data(df):
    df = df.drop_duplicates()
    df = df.rename(columns={
        "Station": "station_name",
        "Latitude": "latitude",
        "Longitude": "longitude",
        "Crowd": "crowd_level"
    })
    print("Transformation done")
    return df

# -------------------
# 3️⃣ Load
# -------------------
@task
def load_to_postgres(df):
    engine = create_engine("postgresql+psycopg2://mrt_user:mrt_pass@postgres:5432/mrt_dw")
    df.to_sql("gold_gold", engine, if_exists="append", index=False)
    print(f"Inserted {len(df)} rows into gold_gold")

# -------------------
# FLOW
# -------------------
@flow(name="MRT ETL Flow")
def mrt_etl_flow():
    data = extract_json()
    transformed = transform_data(data)
    load_to_postgres(transformed)

if __name__ == "__main__":
    mrt_etl_flow()
