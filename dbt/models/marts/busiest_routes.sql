SELECT
    route_short_name,
    COUNT(DISTINCT vehicle_id) AS active_vehicle_count,
    AVG(delay_seconds) AS average_delay_seconds,
    SUM(CASE WHEN is_bunching THEN 1 ELSE 0 END) AS bunching_events
FROM {{ ref('stg_vehicle_events') }}
GROUP BY route_short_name
ORDER BY active_vehicle_count DESC