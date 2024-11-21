WITH base_orders AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__orders') }}
    )

SELECT distinct shipping_service_id
        ,shipping_service
FROM base_orders