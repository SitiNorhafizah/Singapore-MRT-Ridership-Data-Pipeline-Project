WITH crowd AS (
    SELECT
        station_id,
        ride_date,
        total_riders,
        avg_riders,
        std_riders,
        deviation,
        is_anomaly
    FROM {{ ref('gold_station_crowd') }}
),

stations AS (
    SELECT
        station_id,
        station_name,
        latitude,
        longitude
    FROM {{ ref('dim_station') }}
)

SELECT
    c.station_id,
    c.ride_date,
    c.total_riders,
    c.avg_riders,
    c.std_riders,
    c.deviation,
    c.is_anomaly,
    s.station_name,
    s.latitude,
    s.longitude
FROM crowd c
LEFT JOIN stations s
    ON c.station_id = s.station_id

