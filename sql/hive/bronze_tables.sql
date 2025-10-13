-- 1️⃣ Station Exits
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.station_exits (
    station_id STRING,
    station_name STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    exits INT
)
STORED AS PARQUET
LOCATION 's3a://bronze/station_exits/';

-- 2️⃣ Alerts
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.alerts (
    alert_id STRING,
    message STRING,
    ingestion_time TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3a://bronze/alerts/';

-- 3️⃣ Ridership
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.ridership (
    station_code STRING,
    ride_date DATE,
    ridership INT
)
STORED AS PARQUET
LOCATION 's3a://bronze/ridership/';

-- 4️⃣ Station Crowd
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.station_crowd (
    station_id STRING,
    ride_date DATE,
    total_riders INT,
    avg_riders DOUBLE,
    std_riders DOUBLE,
    deviation DOUBLE,
    is_anomaly INT
)
STORED AS PARQUET
LOCATION 's3a://bronze/station_crowd/';

-- 5️⃣ Train Alerts
CREATE EXTERNAL TABLE IF NOT EXISTS bronze.train_alerts (
    train_id STRING,
    alert_message STRING,
    alert_time TIMESTAMP
)
STORED AS PARQUET
LOCATION 's3a://bronze/train_alerts/';
