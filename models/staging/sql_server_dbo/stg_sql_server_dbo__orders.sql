WITH base_orders AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__orders') }}
    )

SELECT   order_id
        , user_id
        , shipping_cost
        , order_cost
        , order_total
        , promo_id
        , shipping_service_id
        , date(created_at_utc) as created_at
        , tracking_id
        , status_id
        , date(estimated_delivery_at_utc) as estimated_delivery_at
        , date(delivered_at_utc) as delivered_at 
        , is_deleted
        , date_load_utc
FROM base_orders
