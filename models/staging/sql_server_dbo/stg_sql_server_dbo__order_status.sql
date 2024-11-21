WITH base_orders AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__orders') }}
    )

SELECT distinct status_id
        ,status_desc
FROM base_orders