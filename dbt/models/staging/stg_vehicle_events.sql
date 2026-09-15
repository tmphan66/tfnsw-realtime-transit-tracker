SELECT
    vehicle_id,
    route_id,
    agency_id,
    route_short_name,
    trip_id,
    latitude,
    longitude,
    bearing,
    to_timestamp(timestamp) at time zone 'Australia/Sydney' as event_time,
    delay_seconds,
    is_bunching
from read_parquet('s3://{{ var("s3_bucket_name") }}/silver/vehicle-events/**/*.parquet')
qualify row_number() over (
    partition by vehicle_id, to_timestamp(timestamp)
    order by delay_seconds, is_bunching
) = 1