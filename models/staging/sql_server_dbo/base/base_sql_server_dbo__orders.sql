WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

renamed_casted AS (
    SELECT
          order_id
        , shipping_service
        , CASE WHEN discount = null THEN 0
            ELSE ABS(discount)  
            END as discount          
        , status
        , _fivetran_deleted AS is_deleted
        , convert_timezone('UTC',_fivetran_synced) as date_load_utc
    FROM src_promos
    union all
    select  'sin_promo'
            ,'sin_promo'
            , 0
            , 'undefined'
            , null
            , null
)
