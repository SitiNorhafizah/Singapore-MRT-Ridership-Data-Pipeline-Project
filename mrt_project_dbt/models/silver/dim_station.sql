{{ config(materialized='table') }}

SELECT
    station_id,
    UPPER(TRIM(station_name)) AS station_name,
    latitude,
    longitude,
    exits
FROM {{ source('bronze', 'station_exits') }}
WHERE station_id IS NOT NULL
  AND station_name IS NOT NULL

