WITH base_orders AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__orders') }}
    )

SELECT   order_id
        , shipping_cost
        , order_cost
        , order_total
        , promo_id
FROM base_orders


