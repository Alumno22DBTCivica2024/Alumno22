WITH src_events AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'events') }}
    ),

renamed_casted AS (
    SELECT
          event_id
        , page_url
        , event_type
        , user_id
        , product_id
        , session_id
        , convert_timezone('UTC',created_at) as created_at_utc
        , order_id
        , _fivetran_deleted AS is_deleted
        , convert_timezone('UTC',_fivetran_synced) as date_load_utc
    FROM src_events
)

SELECT event_id
        , page_url
        , {{ dbt_utils.generate_surrogate_key(['event_type']) }} as event_type_id
        , event_type
        , user_id
        , product_id
        , session_id
        , order_id
        , created_at_utc
        , is_deleted
        , date_load_utc
FROM renamed_casted