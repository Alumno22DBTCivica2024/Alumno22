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
        load_time,
        _dlt_load_id,
        convert_timezone('UTC',load_time) as load_time_utc

    from source

)

select * from renamed
