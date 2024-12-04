{{ config(
    materialized='incremental',
    unique_key='reservation_id',
    incremental_strategy='merge'
) }}

with source as (
    select * from {{ source('amusement_park', 'bookings') }}
),

renamed as (

    select
        reservation_id,
        visitor_id,
        attraction_id,
        convert_timezone('UTC',reservation_date) as reservation_date_utc,
        visit_date,
        status,
        num_tickets,
        total_price,
        _dlt_load_id,
        _dlt_id,
        load_time

    from source

)

select * from renamed

{% if is_incremental() %}
WHERE load_time > (SELECT MAX(load_time) FROM {{ this }})
{% endif %}