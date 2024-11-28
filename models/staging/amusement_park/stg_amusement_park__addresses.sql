with base_visitors as (
    select * 
    from {{ ref('base_amusement_park_visitors') }}
)

select
    address_id,
    state,
    zipcode,
    address,
    country,
    _dlt_load_id,
    _dlt_id,
    load_time_utc

from base_visitors