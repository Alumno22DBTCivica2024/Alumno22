WITH src_promos AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'promos') }}
    ),

renamed_casted AS (
    SELECT
          promo_id as promo_id
        , promo_id as promo_desc
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
            , 'unknown'
            , null
            , null
)

SELECT {{ dbt_utils.generate_surrogate_key(['promo_id']) }} as promo_id
        , promo_id as promo_description
        , discount
        ,{{ dbt_utils.generate_surrogate_key(['status']) }} as status_id
        ,status as status_desc
        ,is_deleted
        ,date_load_utc
FROM renamed_casted