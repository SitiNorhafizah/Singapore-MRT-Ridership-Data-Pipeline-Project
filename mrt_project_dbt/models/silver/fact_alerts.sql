{{ config(materialized='view') }}

WITH base AS (
    SELECT
        alert_id,
        message,
        ingestion_time AS alert_time
    FROM {{ source('bronze', 'alerts') }}
),

-- Extract station name if present
cleaned AS (
    SELECT
        alert_id,
        message AS alert_message,
        alert_time,
        -- Extract text ending with "Station", case-insensitive
        regexp_extract(lower(message), '(\\w+ station)', 1) AS station_name_guess
    FROM base
),

-- Remove nulls and normalize formatting
final AS (
    SELECT
        alert_id,
        alert_message,
        alert_time,
        CASE
            WHEN station_name_guess IS NOT NULL THEN initcap(trim(station_name_guess))
            ELSE NULL
        END AS station_name_guess
    FROM cleaned
)

SELECT *
FROM final
WHERE alert_id IS NOT NULL

