# prefect_mrt_demo_improved.py
import json
import pandas as pd
import sqlite3
from prefect import flow, task, get_run_logger

# ----- Tasks -----
@task
def load_data(file_path: str) -> pd.DataFrame:
    """Load JSON file and convert to DataFrame"""
    logger = get_run_logger()
    logger.info(f"Loading data from {file_path}")
    with open(file_path, "r") as f:
        data = json.load(f)
    df = pd.DataFrame(data["value"])  # 'value' contains the list of records
    return df

@task
def validate_data(df: pd.DataFrame) -> pd.DataFrame:
    """Validate data for required columns and non-empty DataFrame"""
    logger = get_run_logger()
    required_cols = ["Station", "StartTime", "EndTime", "CrowdLevel"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    if df.empty:
        raise ValueError("DataFrame is empty")
    logger.info("Data validation passed")
    return df

@task
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
    """Convert times to datetime and map crowd levels to numeric"""
    logger = get_run_logger()
    df["StartTime"] = pd.to_datetime(df["StartTime"])
    df["EndTime"] = pd.to_datetime(df["EndTime"])
    crowd_map = {"l": 1, "m": 2, "h": 3}
    df["CrowdLevelNumeric"] = df["CrowdLevel"].map(crowd_map)
    logger.info("Data transformation completed")
    return df

@task
def summarize_data(df: pd.DataFrame) -> pd.DataFrame:
    """Summarize average crowd per station"""
    logger = get_run_logger()
    summary = df.groupby("Station")["CrowdLevelNumeric"].mean().reset_index()
    summary = summary.rename(columns={"CrowdLevelNumeric": "AvgCrowdLevel"})
    logger.info("Data summarization completed")
    return summary

@task
def store_data(df: pd.DataFrame, db_path: str = "/data/prefect_demo.db"):
    """Store summary into SQLite database (simulates data warehouse)"""
    logger = get_run_logger()
    logger.info(f"Storing summary data into {db_path}")
    conn = sqlite3.connect(db_path)
    df.to_sql("station_summary", conn, if_exists="replace", index=False)
    conn.close()
    logger.info("Data stored successfully")

# ----- Flow -----
@flow(name="MRT Crowd ETL Demo")
def mrt_etl_demo(file_path: str = "/data/PCDRealTime.json"):
    raw_data = load_data(file_path)
    validated_data = validate_data(raw_data)
    transformed = transform_data(validated_data)
    summary = summarize_data(transformed)
    store_data(summary)

# ----- Run -----
if __name__ == "__main__":
    mrt_etl_demo()
