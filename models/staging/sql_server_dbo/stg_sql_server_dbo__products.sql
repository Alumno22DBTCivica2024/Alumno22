WITH src_products AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'products') }}
    ),

renamed_casted AS (
    SELECT
          product_id
        , price
        , name
        , inventory_number
        , _fivetran_deleted AS is_deleted
        , convert_timezone('UTC',_fivetran_synced) as date_load_utc
    FROM src_budget
    )

SELECT * FROM renamed_casted