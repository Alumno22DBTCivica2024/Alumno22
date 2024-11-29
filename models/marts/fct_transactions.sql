WITH stg_transactions AS (
    SELECT 
        transaction_id,
        visit_id,
        attraction_id,
        price,
        transaction_time_utc AS transaction_time,
        CAST(transaction_time_utc AS DATE) AS transaction_date,
        EXTRACT(HOUR FROM transaction_time_utc AS transaction_hour,
        CASE 
            WHEN EXTRACT(HOUR FROM transaction_time_utc) BETWEEN 12 AND 20 THEN TRUE 
            ELSE FALSE 
        END AS is_peak_time,
        TO_CHAR(transaction_time_utc, 'Day') AS day_of_week,
        item_type,
        payment_method,
        -- CASE 
        --     WHEN payment_method IN ('credit_card', 'online') THEN 'Digital'
        --     ELSE 'Physical'
        -- END AS payment_category,
        _dlt_load_id,
        _dlt_id,
        load_time_utc as load_time
    FROM {{ ref('stg_amusement_park__transactions') }}
),

enhanced_transactions AS (
    SELECT 
        
        bt.transaction_id,
        bt.visit_id,
        bt.attraction_id,
        bt.price,
        m.beneficts
        bt.transaction_time,
        bt.transaction_date,
        bt.transaction_hour,
        bt.is_peak_time,
        bt.day_of_week,
        bt.item_type,
        bt.payment_method,
        -- CASE 
        --     WHEN payment_method IN ('credit_card', 'online') THEN 'Digital'
        --     ELSE 'Physical'
        -- END AS payment_category,
        bt._dlt_load_id,
        bt._dlt_id,
        bt.load_time
        a.category AS attraction_category,
        a.average_duration_minutes AS attraction_duration_minutes,
        -- CASE 
        --     WHEN bt.price < 10 THEN 'Low'
        --     WHEN bt.price BETWEEN 10 AND 20 THEN 'Medium'
        --     ELSE 'High'
        -- END AS price_tier,
        v.membership_type AS visitor_type

    FROM base_transactions bt
    LEFT JOIN {{ ref('stg_amusement_park__attractions') }} a
        ON bt.attraction_id = a.attraction_id
    LEFT JOIN {{ ref('stg_amusement_park__visitors') }} v
        ON bt.visit_id = v.visitor_id
    LEFT JOIN {{ ref('stg_amusement_park__membership') }} m
        ON v.membership_id = m.membership_id
)

SELECT *
FROM enhanced_transactions
