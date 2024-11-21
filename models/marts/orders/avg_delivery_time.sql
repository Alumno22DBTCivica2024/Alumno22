SELECT 
    AVG({{ dbt.datediff(
            'created_at_utc',
            'delivered_at_utc',
            'second'
        ) }} / 3600) AS avg_delivery_time_hours
FROM {{ ref('stg_sql_server_dbo__orders_reduced') }}
WHERE delivered_at_utc IS NOT NULL


