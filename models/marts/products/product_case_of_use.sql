WITH session_details AS (
    SELECT 
        e.session_id,
        e.user_id,
        MIN(e.created_at_utc) AS session_start, 
        MAX(e.created_at_utc) AS session_end, 
        COUNT(DISTINCT e.page_url) AS pages_viewed, 
        COUNT(CASE WHEN et.event_type = 'add_to_cart' THEN 1 END) AS add_to_cart_events, 
        COUNT(CASE WHEN et.event_type = 'checkout' THEN 1 END) AS checkout_events, 
        COUNT(CASE WHEN et.event_type = 'package_shipped' THEN 1 END) AS package_shipped_events
    FROM {{ ref('stg_sql_server_dbo__events') }} AS e
    JOIN {{ ref('stg_sql_server_dbo__event_type') }} AS et ON e.event_type_id = et.event_type_id
    GROUP BY e.session_id, e.user_id
)

SELECT 
    sd.session_id,
    sd.user_id,
    u.first_name,
    u.last_name,
    u.email,
    sd.session_start,
    sd.session_end,
    {{ dbt.datediff(
        'sd.session_start',
        'sd.session_end',
        'second'
        ) }} / 3600 AS session_duration_hours,
    sd.pages_viewed,
    sd.add_to_cart_events,
    sd.checkout_events,
    sd.package_shipped_events
FROM session_details sd
JOIN {{ ref('stg_sql_server_dbo__users') }} AS u ON sd.user_id = u.user_id
WHERE sd.session_end IS NOT NULL
ORDER BY sd.session_start

