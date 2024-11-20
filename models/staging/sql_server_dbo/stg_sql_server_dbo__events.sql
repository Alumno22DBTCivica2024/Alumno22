WITH base_events AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__events') }}
    )
    
SELECT event_id
        , page_url
        , event_type_id
        , user_id
        , product_id
        , session_id
        , order_id
        , created_at_utc
        , is_deleted
        , date_load_utc
FROM base_events