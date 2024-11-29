with 

source as (

    select * from {{ source('amusement_park', 'visits') }}

),

renamed as (

    select
        visit_id,
        visitor_id,
        convert_timezone('UTC',visit_start_time) as visit_start_time_utc,
        convert_timezone('UTC',visit_end_time) as visit_end_time_utc,
        duration_minutes,
        device_used,
        entry_method,
        _dlt_load_id,
        _dlt_id,
        convert_timezone('UTC',load_time) as load_time_utc

    from source

)

select * from renamed
