{{ config(
    materialized='view'
) }}

with baseline as (
    select
        station_id,
        avg(total_riders) as avg_riders,
        stddev(total_riders) as std_riders
    from {{ ref('fact_ridership') }}
    group by station_id
)

select
    s.station_id,
    s.station_name,
    s.latitude,
    s.longitude,
    r.ride_date,
    r.total_riders,
    case 
        when r.total_riders > b.avg_riders + 2*b.std_riders then 'High'
        when r.total_riders < b.avg_riders - 2*b.std_riders then 'Low'
        else 'Normal'
    end as crowd_level
from {{ ref('dim_station') }} s
left join {{ ref('fact_ridership') }} r
    on s.station_id = r.station_id
left join baseline b
    on s.station_id = b.station_id
