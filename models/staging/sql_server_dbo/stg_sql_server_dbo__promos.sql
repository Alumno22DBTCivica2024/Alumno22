WITH base_promos AS (
    SELECT * 
    FROM {{ ref('base_sql_server_dbo__promos') }}
    )
    
SELECT promo_id
        , promo_description
        , discount
        , status_id
        , is_deleted
        , date_load_utc
FROM base_promos