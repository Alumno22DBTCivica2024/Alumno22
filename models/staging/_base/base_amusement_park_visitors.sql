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
        COALESCE(membership_type, 'undefined') AS membership_type,
        COALESCE(benefits, 0) AS benefits, --Snowflake interpreta los NaN como null
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
