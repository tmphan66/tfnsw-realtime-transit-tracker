SELECT
    date_trunc('hour', event_time) AS hour,
    AVG(delay_seconds) AS avg_delay_seconds,
    COUNT(*) AS event_count
FROM {{ ref('stg_vehicle_events') }}
GROUP BY 1
ORDER BY 1