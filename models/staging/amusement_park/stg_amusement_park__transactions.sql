{{ config(
    materialized='incremental',
    unique_key= 'transaction_id'
    ) 
    }}


with source as (

    select * from {{ source('amusement_park', 'transactions') }}

),

renamed as (

    select
        transaction_id,
        visit_id,
        attraction_id,
        price,
        convert_timezone('UTC',transaction_time) as transaction_time_utc,
        item_type,
        payment_method,
        _dlt_load_id,
        _dlt_id,
        convert_timezone('UTC',load_time) as load_time_utc

    from source

)

select * from renamed

{% if is_incremental() %}

  where load_time_utc > (select max(load_time_utc) from {{ this }})

{% endif %}