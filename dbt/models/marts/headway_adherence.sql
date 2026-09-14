WITH trip_first_seen AS (
    -- find the first time we saw each trip
    SELECT
        route_short_name,
        trip_id,
        MIN(event_time) AS first_seen_at
    FROM {{ ref('stg_vehicle_events') }}
    GROUP BY route_short_name, trip_id
),

with_gaps AS (
    -- find the gaps between trips for each route
    SELECT
        route_short_name,
        trip_id,
        first_seen_at,
        first_seen_at - LAG(first_seen_at) OVER (
            PARTITION BY route_short_name 
            ORDER BY first_seen_at
        ) AS gap_to_previous_trip
    FROM trip_first_seen
)

SELECT
    route_short_name,
    COUNT(*) AS trip_count,
    AVG(EPOCH(gap_to_previous_trip)) AS average_gap_seconds,
    STDDEV(EPOCH(gap_to_previous_trip)) AS headway_stddev_seconds
FROM with_gaps
WHERE gap_to_previous_trip IS NOT NULL
    AND EPOCH(gap_to_previous_trip) < 1800 -- filter out gaps longer than 30 minutes (likely due to service breaks)
GROUP BY route_short_name
HAVING COUNT(*) > 5
ORDER BY headway_stddev_seconds DESC