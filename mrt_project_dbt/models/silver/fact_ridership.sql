{{ config(materialized='view') }}  -- start as view to bypass MR issues

WITH cleaned AS (
    SELECT
        station_code AS station_id,
        ride_date,
        ridership AS total_riders
    FROM {{ source('bronze', 'ridership') }}
    WHERE station_code IS NOT NULL
      AND ride_date IS NOT NULL
      AND ridership IS NOT NULL
)

SELECT *
FROM cleaned

