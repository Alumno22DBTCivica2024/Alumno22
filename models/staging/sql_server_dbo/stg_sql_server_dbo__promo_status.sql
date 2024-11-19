WITH src_promos AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__promos') }}
    )

SELECT distinct status_id
        ,status_desc
FROM src_promos