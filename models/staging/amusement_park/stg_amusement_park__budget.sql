with source as (
    select * from {{ source('amusement_park', 'budget') }}
),

renamed as (

    select
        budget_id,
        attraction_id,
        month,
        ROUND(target_revenue, 2) as rounded_target_revenue,
        target_visits,
        _dlt_load_id,
        _dlt_id,
        convert_timezone('UTC',load_time) as load_time_utc

    from source

)

select * from renamed
