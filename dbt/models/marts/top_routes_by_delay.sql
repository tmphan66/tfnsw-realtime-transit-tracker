WITH route_stats AS (
    SELECT
        route_short_name,
        COUNT(DISTINCT vehicle_id) AS active_vehicle_count,
        AVG(delay_seconds) AS average_delay_seconds,
        SUM(CASE WHEN is_bunching THEN 1 ELSE 0 END) AS bunching_events
    FROM {{ ref('stg_vehicle_events') }}
    GROUP BY route_short_name
    HAVING COUNT(DISTINCT vehicle_id) >= 3 AND COUNT(*) >= 20
),

worst_5 AS (
    SELECT *
    FROM route_stats
    ORDER BY average_delay_seconds DESC
    LIMIT 5
),

best_5 AS (
    SELECT *
    FROM route_stats
    ORDER BY average_delay_seconds ASC
    LIMIT 5
)

SELECT *
FROM worst_5

UNION ALL

SELECT *
FROM best_5