{{ config(materialized='table') }}

WITH base AS (
    SELECT
        r.station_id,
        COALESCE(s.station_name, r.station_id) AS station_name,
        CAST(r.ride_date AS DATE) AS ride_date,
        r.total_riders,
        AVG(r.total_riders) OVER (PARTITION BY r.station_id) AS avg_riders,
        STDDEV(r.total_riders) OVER (PARTITION BY r.station_id) AS std_riders,
        r.total_riders - AVG(r.total_riders) OVER (PARTITION BY r.station_id) AS deviation
    FROM {{ ref('fact_ridership') }} r
    LEFT JOIN {{ ref('dim_station') }} s
        ON r.station_id = s.station_id
)

SELECT
    station_id,
    station_name,
    ride_date,
    total_riders,
    avg_riders,
    std_riders,
    deviation,
    CASE WHEN ABS(deviation) > std_riders THEN 1 ELSE 0 END AS is_anomaly
FROM base

