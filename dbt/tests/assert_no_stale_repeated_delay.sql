{{ config(severity = 'warn') }}

-- Flags vehicles reporting the exact same delay value many times in a
-- row, which likely means a trip update stopped arriving and the last
-- known delay is stale rather than reflecting the vehicle's real status.
select
    vehicle_id,
    route_short_name,
    delay_seconds,
    count(*) as repeated_reading_count
from {{ ref('stg_vehicle_events') }}
group by vehicle_id, route_short_name, delay_seconds
having count(*) > 5
order by repeated_reading_count desc