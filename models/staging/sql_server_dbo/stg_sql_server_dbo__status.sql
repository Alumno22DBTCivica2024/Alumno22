-- cambiar la base para un unknown status

WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

renamed_casted AS (
    SELECT DISTINCT status
    FROM src_promos
    )

SELECT {{ dbt_utils.generate_surrogate_key(['status']) }} as status_id
        ,status as status_desc
FROM renamed_casted