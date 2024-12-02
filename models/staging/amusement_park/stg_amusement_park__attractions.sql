with source as (
    select * from {{ ref('attractions_snapshot') }}
),

renamed as (

    select
        attraction_id,
        name,
        category,
        COALESCE(max_capacity, '0') AS max_capacity,
        popularity_score,
        average_duration_minutes,
        maintenance_status,
        location_area,
        _dlt_load_id,
        _dlt_id,
        convert_timezone('UTC',load_time) as load_time_utc,
        convert_timezone('UTC',dbt_updated_at) as dbt_updated_at,
        convert_timezone('UTC',dbt_valid_from) as dbt_valid_from,
        convert_timezone('UTC',dbt_valid_to) as dbt_valid_to
        
    from source

)

select * from renamed
