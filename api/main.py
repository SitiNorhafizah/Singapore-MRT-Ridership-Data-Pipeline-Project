from fastapi import FastAPI, HTTPException, Path
from typing import List
from fastapi.middleware.cors import CORSMiddleware
from datetime import datetime, date
from db import fetch_query
from schemas import StationCrowd, Alert

app = FastAPI(title="MRT Ridership API", version="1.0")

# Enable CORS (optional)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # adjust to your frontend URL if needed
    allow_methods=["*"],
    allow_headers=["*"],
)

# Root route
@app.get("/")
def root():
    return {"message": "MRT Ridership API is running!"}

# Get last 30 days of ridership for a station
@app.get("/stations/{code}/crowd", response_model=List[StationCrowd])
def get_station_crowd(
    code: str = Path(..., min_length=2, max_length=5, description="Station code")
):
    sql = f"""
        SELECT *
        FROM gold_gold.gold_station_crowd
        WHERE station_id = '{code}'
        ORDER BY ride_date DESC
        LIMIT 30
    """
    try:
        data = fetch_query(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    if not data:
        raise HTTPException(status_code=404, detail=f"Station '{code}' not found")

    # Convert ride_date string to date object
    for row in data:
        row['ride_date'] = date.fromisoformat(str(row['ride_date']))

    return data

# Get last 30 anomaly rides for a station
@app.get("/stations/{code}/anomalies", response_model=List[StationCrowd])
def get_station_anomalies(
    code: str = Path(..., min_length=2, max_length=5, description="Station code")
):
    sql = f"""
        SELECT *
        FROM gold_gold.gold_station_crowd
        WHERE station_id = '{code}' AND is_anomaly = 1
        ORDER BY ride_date DESC
        LIMIT 30
    """
    try:
        data = fetch_query(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    if not data:
        raise HTTPException(status_code=404, detail=f"No anomalies found for station '{code}'")

    # Convert ride_date string to date object
    for row in data:
        row['ride_date'] = date.fromisoformat(str(row['ride_date']))

    return data

# Get latest alerts from past 7 days
@app.get("/alerts/latest", response_model=List[Alert])
def get_latest_alerts():
    sql = """
        SELECT *
        FROM gold_gold.gold_alerts_summary
        WHERE alert_day >= DATE_SUB(CURRENT_DATE(), 7)
        ORDER BY alert_ts DESC
        LIMIT 10
    """
    try:
        data = fetch_query(sql)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")

    # Convert alert_ts and alert_day strings to datetime/date objects
    for row in data:
        row['alert_ts'] = datetime.fromisoformat(str(row['alert_ts']))
        row['alert_day'] = date.fromisoformat(str(row['alert_day']))

    return data

