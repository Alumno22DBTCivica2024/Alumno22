WITH src_addresses AS (
    SELECT * 
    FROM {{ source('sql_server_dbo', 'users') }}
    ),

renamed_casted AS (
    SELECT
            user_id
            ,first_name
            ,last_name
            ,email
            ,phone_number
            ,address_id
            ,convert_timezone('UTC',created_at) as created_at_utc
            ,convert_timezone('UTC',updated_at) as updated_at_utc
            , _fivetran_deleted AS is_deleted
            , convert_timezone('UTC',_fivetran_synced) as date_load_utc
    FROM src_addresses
    )

SELECT * FROM renamed_casted