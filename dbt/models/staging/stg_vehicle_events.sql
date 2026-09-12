SELECT
    vehicle_id,
    route_id,
    agency_id,
    route_short_name,
    trip_id,
    latitude,
    longitude,
    bearing,
    to_timestamp(timestamp) AS event_time,
    delay_seconds,
    is_bunching
FROM read_parquet('s3://{{ var("s3_bucket_name") }}/**/*.parquet')