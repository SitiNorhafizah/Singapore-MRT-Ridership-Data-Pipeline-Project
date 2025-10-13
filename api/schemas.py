from pydantic import BaseModel
from datetime import date, datetime

class StationCrowd(BaseModel):
    station_id: str
    station_name: str
    ride_date: date            
    total_riders: int
    avg_riders: float
    std_riders: float
    deviation: float
    is_anomaly: int

class Alert(BaseModel):
    alert_id: str
    alert_message: str
    alert_ts: datetime         
    alert_day: date
    station_id: str
    station_name: str

