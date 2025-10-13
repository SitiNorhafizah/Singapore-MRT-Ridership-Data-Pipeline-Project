{{ config(
    materialized='table',
    schema='gold'
) }}

WITH alerts_clean AS (
    SELECT
        alert_id,
        alert_message,
        alert_time AS alert_ts,
        date(alert_time) AS alert_day,
        -- Extract the station name part before the colon (:)
        trim(split(alert_message, ':')[0]) AS station_name_guess
    FROM {{ ref('fact_alerts') }}
    WHERE alert_id IS NOT NULL
),

stations AS (
    SELECT
        station_id,
        station_name
    FROM {{ ref('dim_station') }}
    WHERE station_id IS NOT NULL
      AND station_name IS NOT NULL
)

SELECT
    a.alert_id,
    a.alert_message,
    a.alert_ts,
    a.alert_day,
    s.station_id,
    s.station_name
FROM alerts_clean a
LEFT JOIN stations s
    ON lower(trim(a.station_name_guess)) = lower(trim(s.station_name))
WHERE s.station_id IS NOT NULL;
