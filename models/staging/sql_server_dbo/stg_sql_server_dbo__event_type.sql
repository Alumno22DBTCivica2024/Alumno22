WITH base_events AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__events') }}
    )

SELECT distinct event_type_id
        ,event_type
FROM base_events