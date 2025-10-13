{{ config(materialized='table') }}

-- Station-level ridership
WITH station_totals AS (
    SELECT
        station_id,
        station_name,
        CAST(ride_date AS DATE) AS period_date,
        total_riders
    FROM {{ ref('gold_station_crowd') }}
),

-- Overall ridership (all stations)
all_stations_totals AS (
    SELECT
        'ALL_STATIONS' AS station_id,
        'ALL_STATIONS' AS station_name,
        CAST(ride_date AS DATE) AS period_date,
        SUM(total_riders) AS total_riders
    FROM {{ ref('gold_station_crowd') }}
    GROUP BY CAST(ride_date AS DATE)
)

-- Combine both
SELECT
    station_id,
    station_name,
    period_date,
    'daily' AS period_type,
    total_riders
FROM station_totals

UNION ALL

SELECT
    station_id,
    station_name,
    period_date,
    'daily' AS period_type,
    total_riders
FROM all_stations_totals

ORDER BY period_date, station_id

