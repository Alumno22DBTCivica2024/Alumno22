WITH user_order_counts AS (
    SELECT 
        user_id,
        COUNT(order_id) AS order_count
    FROM {{ ref('stg_sql_server_dbo__orders_reduced') }}
    GROUP BY user_id
)

SELECT 
    COUNT(CASE WHEN order_count = 1 THEN 1 END) AS users_with_one_purchase,
    COUNT(CASE WHEN order_count = 2 THEN 1 END) AS users_with_two_purchases,
    COUNT(CASE WHEN order_count >= 3 THEN 1 END) AS users_with_three_or_more_purchases
FROM user_order_counts
