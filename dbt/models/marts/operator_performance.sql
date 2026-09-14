SELECT
    COALESCE(a.agency_name, 'Unknown operator (' || e.agency_id || ')') AS operator_name,
    e.agency_id,
    COUNT(DISTINCT e.vehicle_id) AS active_vehicle_count,
    COUNT(DISTINCT e.route_id) AS route_count,
    COUNT(*) AS reading_count,
    AVG(e.delay_seconds) AS average_delay_seconds,
    SUM(CASE WHEN e.is_bunching THEN 1 ELSE 0 END) AS bunching_events
FROM {{ ref('stg_vehicle_events') }} e
LEFT JOIN {{ ref('agency_lookup') }} a
    ON e.agency_id = a.agency_id
GROUP BY operator_name, e.agency_id
HAVING COUNT(DISTINCT e.vehicle_id) >= 3
ORDER BY average_delay_seconds DESC