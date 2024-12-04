{{ config(
    materialized='incremental',
    unique_key= 'visitor_id'
    ) 
    }}



with source as (
    select * from {{ source('amusement_park', 'visitors') }}
),

renamed as (

    select
        visitor_id,
        username,
        first_name,
        last_name,
        convert_timezone('UTC',signup_date) as signup_date_utc,
        CASE 
            WHEN membership_type IS NULL OR membership_type = 'NaN' THEN 'undefined'
            ELSE membership_type
        END AS membership_type,
        CASE 
            WHEN benefits IS NULL OR benefits = 'NaN' THEN 0
            ELSE benefits
        END AS benefits,
        address_id,
        state,
        zipcode,
        address,
        country,
        _dlt_load_id,
        _dlt_id,
        convert_timezone('UTC',load_time) as load_time_utc

    from source

)

select 
    visitor_id,
    username,
    first_name,
    last_name,
    signup_date_utc,
    {{ dbt_utils.generate_surrogate_key(['membership_type']) }} as membership_id,
    membership_type,
    benefits,
    address_id,
    state,
    zipcode,
    address,
    country,
    _dlt_load_id,
    _dlt_id,
    load_time_utc
from renamed

{% if is_incremental() %}

  where load_time_utc > (select max(load_time_utc) from {{ this }})

{% endif %}