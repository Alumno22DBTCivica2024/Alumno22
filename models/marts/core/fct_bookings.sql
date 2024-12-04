with stg_bookings as (
    select
        reservation_id,
        visitor_id,
        attraction_id,
        reservation_date_utc,
        visit_date,
        status,
        num_tickets,
        total_price,
        convert_timezone('UTC',load_time) as load_time
    FROM {{ ref('stg_amusement_park__bookings') }}
),

visitors as (
    select
        visitor_id,
        first_name,
        last_name,
        membership_id,
        address_id
    from {{ ref('stg_amusement_park__visitors') }}
),

attractions as (
    select
        attraction_id,
        name as attraction_name,
        category as attraction_category,
        popularity_score,
        max_capacity,
        location_area
    from {{ ref('stg_amusement_park__attractions') }}
),

enriched as (
    select
        b.reservation_id,
        b.visitor_id,
        v.first_name || ' ' || v.last_name as visitor_name,
        m.membership_type,
        ad.country,
        b.attraction_id,
        a.attraction_name,
        a.attraction_category,
        a.popularity_score,
        a.max_capacity,
        a.location_area,
        b.reservation_date_utc,
        b.visit_date,
        b.status,
        b.num_tickets,
        b.total_price,
        -- Precio promedio por ticket
        case when b.num_tickets > 0 then b.total_price / b.num_tickets else null end as avg_price_per_ticket,
        -- Días entre reserva y visita
        datediff(day, b.reservation_date_utc, b.visit_date) as days_until_visit,
        -- Clasificación de ingresos
        case 
            when b.total_price < 50 then 'low'
            when b.total_price between 50 and 150 then 'medium'
            else 'high'
        end as revenue_category,
        -- Indicador de reserva anticipada
        case 
            when datediff(day, b.reservation_date_utc, b.visit_date) > 30 then 'early'
            else 'late'
        end as reservation_timing,
        b.load_time
    from stg_bookings b
    left join visitors v on b.visitor_id = v.visitor_id
    left join attractions a on b.attraction_id = a.attraction_id
    LEFT JOIN {{ ref('stg_amusement_park__membership') }} m
        ON v.membership_id = m.membership_id
    LEFT JOIN {{ ref('stg_amusement_park__addresses') }} ad
        ON v.address_id = ad.address_id
)

select * from enriched
