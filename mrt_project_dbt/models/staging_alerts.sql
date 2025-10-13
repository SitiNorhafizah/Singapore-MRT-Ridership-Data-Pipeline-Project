SELECT *
FROM {{ source('bronze', 'alerts') }}

