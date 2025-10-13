-- =====================================
-- Restore Bronze, Silver, Gold Layers
-- =====================================

-- ===== Bronze Layer =====
CREATE DATABASE IF NOT EXISTS bronze
LOCATION '/home/ctnorhafizah/mrt_project/mrt_project_dbt/spark-warehouse/gold_bronze.db';

USE bronze;

CREATE EXTERNAL TABLE IF NOT EXISTS bronze.bronze_table_example (
    col1 STRING,
    col2 INT
)
STORED AS PARQUET
LOCATION '/home/ctnorhafizah/mrt_project/mrt_project_dbt/spark-warehouse/gold_bronze.db/bronze_table_example';

-- ===== Silver Layer =====
CREATE DATABASE IF NOT EXISTS silver
LOCATION '/home/ctnorhafizah/mrt_project/mrt_project_dbt/spark-warehouse/gold_silver.db';

USE silver;

CREATE EXTERNAL TABLE IF NOT EXISTS silver.dim_station (
    station_id STRING,
    station_name STRING
)
STORED AS PARQUET
LOCATION '/home/ctnorhafizah/mrt_project/mrt_project_dbt/spark-warehouse/gold_silver.db/dim_station';

CREATE EXTERNAL TABLE IF NOT EXISTS silver.fact_ridership (
    station_id STRING,
    ride_date DATE,
    total_riders INT
)
STORED AS PARQUET
LOCATION '/home/ctnorhafizah/mrt_project/mrt_project_dbt/spark-warehouse/gold_silver.db/fact_ridership';

CREATE EXTERNAL TABLE IF NOT EXISTS silver.fact_alerts (
    alert_id STRING,
    alert_time BIGINT,
    station_name_guess STRING
)
STORED AS PARQUET
LOCATION '/home/ctnorhafizah/mrt_project/mrt_project_dbt/spark-warehouse/gold_silver.db/fact_alerts';

-- ===== Gold Layer =====
CREATE DATABASE IF NOT EXISTS gold_gold
LOCATION '/home/ctnorhafizah/mrt_project/mrt_project_dbt/spark-warehouse/gold_gold.db';

USE gold_gold;

CREATE EXTERNAL TABLE IF NOT EXISTS gold_gold.gold_station_crowd (
    station_id STRING,
    station_name STRING,
    ride_date DATE,
    total_riders INT,
    avg_riders DOUBLE,
    std_riders DOUBLE,
    deviation DOUBLE,
    is_anomaly INT
)
STORED AS PARQUET
LOCATION '/home/ctnorhafizah/mrt_project/mrt_project_dbt/spark-warehouse/gold_gold.db/gold_station_crowd';

CREATE EXTERNAL TABLE IF NOT EXISTS gold_gold.gold_alerts_summary (
    alert_id STRING,
    alert_message STRING,
    alert_ts STRING,
    alert_day DATE,
    station_id STRING,
    station_name STRING
)
STORED AS PARQUET
LOCATION '/home/ctnorhafizah/mrt_project/mrt_project_dbt/spark-warehouse/gold_gold.db/gold_alerts_summary';

CREATE EXTERNAL TABLE IF NOT EXISTS gold_gold.gold_crowd_timeline (
    station_id STRING,
    station_name STRING,
    period_date DATE,
    period_type STRING,
    total_riders BIGINT
)
STORED AS PARQUET
LOCATION '/home/ctnorhafizah/mrt_project/mrt_project_dbt/spark-warehouse/gold_gold.db/gold_crowd_timeline';

CREATE EXTERNAL TABLE IF NOT EXISTS gold_gold.gold_station_map (
    station_id INT,
    station_name STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    ride_date DATE,
    total_riders INT,
    crowd_level STRING
)
STORED AS PARQUET
LOCATION '/home/ctnorhafizah/mrt_project/mrt_project_dbt/spark-warehouse/gold_gold.db/gold_station_map';
