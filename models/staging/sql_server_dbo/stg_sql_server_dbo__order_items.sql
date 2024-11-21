WITH src_order_items AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'order_items') }}
    ),

renamed_casted AS (
    SELECT
        order_id
        , product_id
        , quantity
        , _fivetran_deleted AS is_deleted
        , convert_timezone('UTC',_fivetran_synced) as date_load_utc
    FROM src_order_items
    )

SELECT * FROM renamed_casted