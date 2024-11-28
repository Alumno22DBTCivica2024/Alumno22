with source as (
    select * from {{ source('amusement_park', 'attractions') }}
),

renamed as (

    select
        attraction_id,
        name,
        category,
        max_capacity,
        popularity_score,
        average_duration_minutes,
        maintenance_status,
        location_area,
        _dlt_load_id,
        _dlt_id,
        convert_timezone('UTC',load_time) as load_time_utc

    from source

)

select * from renamed
