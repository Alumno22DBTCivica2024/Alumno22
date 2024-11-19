WITH src_promos AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__promos') }}
    ),

renamed_casted AS (
    SELECT DISTINCT status_id
        ,status_desc
    FROM src_promos
    )

SELECT status_id
        ,status_desc
FROM renamed_casted