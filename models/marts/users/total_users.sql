SELECT 
    COUNT(DISTINCT user_id) AS total_users
FROM {{ ref('stg_sql_server_dbo__users') }}