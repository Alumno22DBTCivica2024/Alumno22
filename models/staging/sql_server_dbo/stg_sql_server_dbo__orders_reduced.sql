WITH base_orders AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__orders') }}
    )

SELECT   order_id
        , user_id
        , shipping_service_id
        , created_at_utc
        , tracking_id
        , status_id
        , estimated_delivery_at_utc
        , delivered_at_utc 
        , is_deleted
        , date_load_utc
FROM base_orders
