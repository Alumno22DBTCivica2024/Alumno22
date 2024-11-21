WITH hourly_sessions AS (
    SELECT 
        DATE_TRUNC('hour', created_at_utc) AS session_hour,
        COUNT(DISTINCT session_id) AS unique_sessions
    FROM {{ ref('stg_sql_server_dbo__events') }}
    WHERE session_id IS NOT NULL
    GROUP BY 1
)
SELECT 
    AVG(unique_sessions) AS avg_sessions_per_hour
FROM hourly_sessions