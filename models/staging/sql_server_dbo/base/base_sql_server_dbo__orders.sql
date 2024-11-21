WITH base_orders AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'orders') }}
    ),

renamed_casted AS (
    SELECT
          order_id
        , user_id
        , shipping_cost
        , order_cost
        , order_total
        , CASE WHEN
            promo_id = '' then 'sin_promo'
            ELSE promo_id
            END as promo_id 
        , CASE WHEN
            shipping_service = '' then 'unassigned'
            ELSE shipping_service
            END as shipping_service 
        , tracking_id
        , status
        , convert_timezone('UTC',created_at) as created_at_utc
        , convert_timezone('UTC',estimated_delivery_at) as estimated_delivery_at_utc
        , convert_timezone('UTC',delivered_at) as delivered_at_utc 
        , _fivetran_deleted AS is_deleted
        , convert_timezone('UTC',_fivetran_synced) as date_load_utc
       FROM base_orders
       )


SELECT   order_id
        , user_id
        , shipping_cost
        , order_cost
        , order_total
        , {{ dbt_utils.generate_surrogate_key(['promo_id']) }} as promo_id
        , {{ dbt_utils.generate_surrogate_key(['shipping_service']) }} as shipping_service_id
        , shipping_service 
        , tracking_id
        , {{ dbt_utils.generate_surrogate_key(['status']) }} as status_id
        , status as status_desc
        , created_at_utc
        , estimated_delivery_at_utc
        , delivered_at_utc 
        , is_deleted
        , date_load_utc
FROM renamed_casted
