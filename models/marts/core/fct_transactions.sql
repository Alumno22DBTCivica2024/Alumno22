WITH stg_transactions AS (
    SELECT 
        transaction_id,
        visit_id,
        attraction_id,
        price,
        transaction_time_utc AS transaction_time,
        CAST(transaction_time_utc AS DATE) AS transaction_date,
        EXTRACT(HOUR FROM transaction_time_utc) AS transaction_hour,
        CASE 
            WHEN EXTRACT(HOUR FROM transaction_time_utc) BETWEEN 12 AND 20 THEN TRUE 
            ELSE FALSE 
        END AS is_peak_time,
        TO_CHAR(transaction_time_utc, 'Day') AS day_of_week,
        item_type,
        payment_method,
        _dlt_load_id,
        _dlt_id,
        load_time_utc as load_time
    FROM {{ ref('stg_amusement_park__transactions') }}
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

enhanced_transactions AS (
    SELECT         
        bt.transaction_id,
        bt.visit_id,
        bt.attraction_id,
        a.attraction_name,
        a.attraction_category,
        a.popularity_score,
        a.max_capacity,
        a.location_area,
        m.membership_type AS visitor_type,
        bt.item_type,
        bt.payment_method,
        bt.price,
        m.benefits,
        bt.price - m.benefits AS total_revenue,
        bt.transaction_time,
        bt.transaction_date,
        bt.transaction_hour,
        bt.is_peak_time,
        bt.day_of_week,
        bt._dlt_load_id,
        bt._dlt_id,
        bt.load_time

    FROM stg_transactions bt
    left join attractions a on bt.attraction_id = a.attraction_id
    LEFT JOIN {{ ref('stg_amusement_park__visits') }} v
        ON bt.visit_id = v.visit_id
    LEFT JOIN {{ ref('stg_amusement_park__visitors') }} visitors
        ON v.visitor_id = visitors.visitor_id
    LEFT JOIN {{ ref('stg_amusement_park__membership') }} m
        ON visitors.membership_id = m.membership_id
)

SELECT *
FROM enhanced_transactions

        -- CASE 
        --     WHEN bt.price < 10 THEN 'Low'
        --     WHEN bt.price BETWEEN 10 AND 20 THEN 'Medium'
        --     ELSE 'High'
        -- END AS price_tier,
